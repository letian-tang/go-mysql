package replication

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"net"
	"strconv"
	"strings"
	"time"
)

type recoveryConnection struct {
	net.Conn
	stop func() bool
}

// RecoveryDialer binds raw transport cancellation to the owner's context.
// It also bounds the handshake; the caller may clear the initial deadline
// after setup if its long-lived stream intentionally has no read timeout.
func RecoveryDialer(ctx context.Context, base client.Dialer) client.Dialer {
	return recoveryDialer(ctx, base)
}

func recoveryDialer(ctx context.Context, base client.Dialer) client.Dialer {
	if base == nil {
		base = (&net.Dialer{}).DialContext
	}
	return func(dialCtx context.Context, network, address string) (net.Conn, error) {
		raw, err := base(dialCtx, network, address)
		if err != nil {
			return nil, err
		}
		conn := &recoveryConnection{Conn: raw}
		_ = raw.SetDeadline(time.Now().Add(10 * time.Second))
		conn.stop = context.AfterFunc(ctx, func() { _ = raw.Close() })
		return conn, nil
	}
}

func (c *recoveryConnection) Close() error {
	if c.stop != nil {
		c.stop()
	}
	return c.Conn.Close()
}

var ErrRecoveryHistoryUnavailable = errors.New("online recovery history unavailable")

type SourceChangedError struct{ Current uint32 }

func (e *SourceChangedError) Error() string {
	return fmt.Sprintf("replication source changed to %d", e.Current)
}

type ReconnectRequiredError struct{ Cause error }

func (e *ReconnectRequiredError) Error() string {
	return fmt.Sprintf("replication reconnect required: %v", e.Cause)
}
func (e *ReconnectRequiredError) Unwrap() error { return e.Cause }

func (b *BinlogSyncer) SourceServerID() uint32 {
	b.m.RLock()
	defer b.m.RUnlock()
	return uint32(b.ServerId)
}

// ConnectForRecovery learns source identity without sending any old coordinate.
func (b *BinlogSyncer) ConnectForRecovery() error {
	b.m.Lock()
	defer b.m.Unlock()
	return b.prepare()
}

// NewFileParser preserves decoding options but shares no remote parser state.
func (b *BinlogSyncer) NewFileParser() *BinlogParser {
	p := b.parser.cloneForPayloadDecode()
	p.SetVerifyChecksum(true)
	return p
}

func recoverDecoderPanic(err *error) {
	if v := recover(); v != nil {
		// Do not include raw binlog data or arbitrary callback panic messages.
		*err = fmt.Errorf("binlog recovery decoder panic (%T)", v)
	}
}

// ParseRecoveryEvent turns malformed decoder panics into instance errors.
func (p *BinlogParser) ParseRecoveryEvent(data []byte) (event *BinlogEvent, err error) {
	defer recoverDecoderPanic(&err)
	return p.Parse(data)
}

// ParseRecoveryFile contains decoder failures in both validation and the
// asynchronous replay producer, instead of allowing a process-wide panic.
func (p *BinlogParser) ParseRecoveryFile(path string, offset int64, onEvent OnEventFunc) (err error) {
	defer recoverDecoderPanic(&err)
	return p.ParseFile(path, offset, onEvent)
}

// FindRecoveryPosition scans transaction boundaries, never file creation time.
// Returning offset 4 intentionally preserves idempotent whole-file replay.
func (b *BinlogSyncer) FindRecoveryPosition(ctx context.Context, timestamp uint32, eventServerID ...uint32) (mysql.Position, error) {
	boundaryMatches := func(e *BinlogEvent) bool {
		return e.Header.Timestamp != 0 && e.Header.Timestamp <= timestamp && (len(eventServerID) == 0 || eventServerID[0] == 0 || e.Header.ServerID == eventServerID[0])
	}
	b.m.RLock()
	cfg := b.cfg
	expected := uint32(b.ServerId)
	b.m.RUnlock()
	p, err := newHistoryProberContext(ctx, cfg)
	if err != nil {
		return mysql.Position{}, err
	}
	defer p.conn.Close()
	identity, err := p.conn.Execute("SHOW VARIABLES LIKE 'server_id'")
	if err != nil {
		return mysql.Position{}, err
	}
	id, err := identity.GetUint(0, 1)
	if err != nil || id == 0 || id > uint64(^uint32(0)) {
		return mysql.Position{}, fmt.Errorf("invalid probe source server_id")
	}
	if expected != 0 && uint32(id) != expected {
		return mysql.Position{}, &SourceChangedError{Current: uint32(id)}
	}
	result, err := p.conn.Execute("SHOW BINARY LOGS")
	if err != nil {
		return mysql.Position{}, err
	}
	var previousPrefix string
	var previousSequence uint64
	for i := 0; i < result.RowNumber(); i++ {
		name, err := result.GetString(i, 0)
		if err != nil {
			return mysql.Position{}, err
		}
		j := strings.LastIndexByte(name, '.')
		if j < 1 {
			return mysql.Position{}, fmt.Errorf("invalid online binlog name")
		}
		seq, err := strconv.ParseUint(name[j+1:], 10, 64)
		if err != nil {
			return mysql.Position{}, fmt.Errorf("invalid online binlog sequence")
		}
		if i > 0 && (name[:j] != previousPrefix || seq != previousSequence+1) {
			return mysql.Position{}, fmt.Errorf("online binlog index has an unverified gap")
		}
		previousPrefix, previousSequence = name[:j], seq
	}
	// Replay the oldest retained file that proves history reaches the rewind
	// boundary. Searching newest-first is unsafe when event timestamps regress.
	for i := 0; i < result.RowNumber(); i++ {
		if err := ctx.Err(); err != nil {
			return mysql.Position{}, err
		}
		name, err := result.GetString(i, 0)
		if err != nil {
			return mysql.Position{}, err
		}
		pos := mysql.Position{Name: name, Pos: 4}
		if err := p.writeDump(pos); err != nil {
			return mysql.Position{}, err
		}
		parser := b.NewFileParser()
		covered := false
		inTransaction := false
		inInitialFile := true
		var inspect func(*BinlogEvent) error
		inspect = func(e *BinlogEvent) error {
			switch v := e.Event.(type) {
			case *TransactionPayloadEvent:
				for _, sub := range v.Events {
					if err := inspect(sub); err != nil {
						return err
					}
				}
			case *GTIDEvent, *MariadbGTIDEvent, *RowsEvent:
				inTransaction = true
			case *XIDEvent:
				inTransaction = false
				covered = covered || (inInitialFile && boundaryMatches(e))
			case *QueryEvent:
				q := strings.TrimSpace(strings.TrimSuffix(strings.ToUpper(strings.TrimSpace(string(v.Query))), ";"))
				if strings.HasPrefix(q, "XA ") {
					return fmt.Errorf("XA transaction recovery is not supported")
				}
				if q == "BEGIN" || q == "START TRANSACTION" {
					inTransaction = true
				} else if !strings.HasPrefix(q, "SAVEPOINT ") && !strings.HasPrefix(q, "RELEASE SAVEPOINT ") && !strings.HasPrefix(q, "ROLLBACK TO ") {
					inTransaction = false
					covered = covered || (inInitialFile && boundaryMatches(e))
				}
			case *RotateEvent:
				if e.Header.Timestamp != 0 {
					if inTransaction {
						return fmt.Errorf("online rotate inside transaction")
					}
					inInitialFile = false
				}
			}
			return nil
		}
		for {
			packet, err := p.conn.ReadPacket()
			if err != nil {
				return mysql.Position{}, err
			}
			if len(packet) == 0 {
				return mysql.Position{}, fmt.Errorf("empty recovery probe packet")
			}
			if packet[0] == mysql.EOF_HEADER {
				break
			}
			if packet[0] == mysql.ERR_HEADER {
				err := p.conn.HandleErrorPacket(packet)
				var sqlErr *mysql.MyError
				if errors.As(err, &sqlErr) && sqlErr.Code == mysql.ER_MASTER_FATAL_ERROR_READING_BINLOG && strings.Contains(sqlErr.Message, "Could not find first log") {
					return mysql.Position{}, fmt.Errorf("%w: online file purged while probing", ErrRecoveryHistoryUnavailable)
				}
				return mysql.Position{}, err
			}
			if packet[0] != mysql.OK_HEADER {
				return mysql.Position{}, fmt.Errorf("unexpected recovery probe packet")
			}
			e, err := parser.ParseRecoveryEvent(packet[1:])
			if err != nil {
				return mysql.Position{}, err
			}
			if err := inspect(e); err != nil {
				return mysql.Position{}, err
			}
			// No need to drain days of subsequent online files once a complete
			// transaction proves the retained history reaches the rewind time.
			if covered && !inTransaction {
				return pos, nil
			}
		}
		if inTransaction && !covered {
			return mysql.Position{}, fmt.Errorf("online file %s ends inside a transaction", name)
		}
		if covered {
			return pos, nil
		}
	}
	return mysql.Position{}, fmt.Errorf("%w: timestamp %d", ErrRecoveryHistoryUnavailable, timestamp)
}
