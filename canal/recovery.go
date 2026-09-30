package canal

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"io"
	"net"
	"strings"
	"time"
)

// RecoveryCheckpoint is a committed consumer boundary. SourceServerID is the
// physical connection identity; ServerID is the original binlog event writer.
type RecoveryCheckpoint struct {
	Position                            mysql.Position
	Timestamp, ServerID, SourceServerID uint32
	ArchiveHostInstanceID               string
}
type RecoveryRequest struct {
	Checkpoint            RecoveryCheckpoint
	CurrentSourceServerID uint32
	// HostInstanceID pins subsequent files to the selected archive chain.
	HostInstanceID         string
	PreviousFile, NextFile string
	// ValidateFile lets the provider validate cache entries and temporary files
	// using the consumer's parser before installing them. Parsing stays in Canal.
	ValidateFile func(string) error
}
type RecoveryFile struct {
	Name, Path, HostInstanceID string
	SourceServerID             uint32
}

// A provider returns io.EOF only after exhausting a verified archive chain.
type RecoveryFileProvider func(context.Context, RecoveryRequest) (*RecoveryFile, error)
type RecoveryBarrier func(context.Context) (RecoveryCheckpoint, error)

// WithRecovery enables coordinated recovery. Register before Run. The legacy
// local downloader remains available to callers that do not opt in.
func (c *Canal) WithRecovery(provider RecoveryFileProvider, barrier RecoveryBarrier) {
	c.recoveryProvider, c.recoveryBarrier = provider, barrier
	c.managedRecovery = true
}

func (c *Canal) RunFromCheckpoint(cp RecoveryCheckpoint) error {
	c.recoveryCheckpoint = cp
	c.expectedSourceServerID = cp.SourceServerID
	c.GetBinlogSyncer().Close()
	if err := c.prepareSyncer(); err != nil {
		return err
	}
	return c.RunFrom(cp.Position)
}

func (c *Canal) recoveryBoundary(ctx context.Context) (RecoveryCheckpoint, error) {
	// Nothing from an incomplete transaction has reached the consumer.
	c.pendingTransaction = nil
	cp := c.recoveryCheckpoint
	if c.recoveryBarrier != nil {
		var err error
		cp, err = c.recoveryBarrier(ctx)
		if err != nil {
			return cp, err
		}
	}
	c.recoveryCheckpoint = cp
	return cp, nil
}

// Buffer rows until their commit. Recovery never exposes half a transaction
// that will be discarded when the source connection is replaced.
func (c *Canal) handleRecoveryEvent(e *replication.BinlogEvent) error {
	if payload, ok := e.Event.(*replication.TransactionPayloadEvent); ok {
		for _, sub := range payload.Events {
			// Compressed subevents have no usable file coordinate. Their commit
			// belongs to the enclosing payload's end, not a zero/internal offset.
			sub.Header.LogPos = e.Header.LogPos
			sub.Header.SourceServerID = e.Header.SourceServerID
			sub.Header.ArchiveHostInstanceID = e.Header.ArchiveHostInstanceID
			if err := c.handleRecoveryEvent(sub); err != nil {
				return err
			}
		}
		return nil
	}
	commit := false
	switch v := e.Event.(type) {
	case *replication.RowsEvent, *replication.GTIDEvent, *replication.MariadbGTIDEvent:
		c.pendingTransaction = append(c.pendingTransaction, e)
		return nil
	case *replication.QueryEvent:
		q := strings.ToUpper(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(string(v.Query)), ";")))
		if strings.HasPrefix(q, "XA ") {
			return fmt.Errorf("XA transaction recovery is not supported")
		}
		if strings.HasPrefix(q, "SAVEPOINT ") || strings.HasPrefix(q, "RELEASE SAVEPOINT ") || strings.HasPrefix(q, "ROLLBACK TO ") {
			return nil
		}
		if q == "BEGIN" || q == "START TRANSACTION" {
			c.pendingTransaction = append(c.pendingTransaction, e)
			return nil
		}
		if q == "ROLLBACK" {
			c.pendingTransaction = nil
		}
		commit = true
	case *replication.XIDEvent:
		commit = true
	case *replication.RotateEvent:
		if len(c.pendingTransaction) != 0 {
			return fmt.Errorf("rotate inside an incomplete transaction")
		}
		if e.Header.Timestamp == 0 {
			if old := c.master.Position().Name; old != "" && old != string(v.NextLogName) {
				return fmt.Errorf("fake rotate changed file without a verified recovery boundary")
			}
			// Fake rotations only identify the live file; they never make a
			// durable transaction boundary or supply historical metadata.
			c.master.Update(mysql.Position{Name: string(v.NextLogName), Pos: uint32(v.Position)})
			return nil
		}
		commit = true
	}
	if !commit {
		return c.handleEvent(e)
	}
	for _, pending := range c.pendingTransaction {
		if err := c.handleEvent(pending); err != nil {
			return err
		}
	}
	c.pendingTransaction = nil
	before := c.master.Position()
	if err := c.handleEvent(e); err != nil {
		return err
	}
	if _, query := e.Event.(*replication.QueryEvent); query && c.master.Position() == before {
		// The existing SQL handler may skip syntax it cannot parse. Do not
		// attach new metadata to the previous event's coordinates.
		return nil
	}
	if e.Header.Timestamp != 0 {
		c.recoveryCheckpoint = RecoveryCheckpoint{Position: c.master.Position(), Timestamp: e.Header.Timestamp, ServerID: e.Header.ServerID, SourceServerID: e.Header.SourceServerID, ArchiveHostInstanceID: e.Header.ArchiveHostInstanceID}
	} else {
		c.recoveryCheckpoint.Position = c.master.Position()
		if e.Header.ArchiveHostInstanceID != "" {
			c.recoveryCheckpoint.SourceServerID = 0
			c.recoveryCheckpoint.ArchiveHostInstanceID = e.Header.ArchiveHostInstanceID
		}
	}
	return nil
}

func isMissingBinlog(err error) bool {
	var e *mysql.MyError
	return errors.As(err, &e) && e.Code == mysql.ER_MASTER_FATAL_ERROR_READING_BINLOG &&
		(strings.Contains(e.Message, "Could not find first log") || strings.Contains(e.Message, "not in the binary log index"))
}

func recoveryTimestamp(timestamp uint32) (uint32, error) {
	if timestamp == 0 {
		return 0, fmt.Errorf("recovery checkpoint has no event timestamp")
	}
	if int64(timestamp) > time.Now().Unix() {
		return 0, fmt.Errorf("recovery checkpoint timestamp is in the future")
	}
	if timestamp <= 300 {
		return 1, nil
	}
	return timestamp - 300, nil
}

func (s *localBinFileAdapterStreamer) managedEvent(ctx context.Context) (*replication.BinlogEvent, error) {
	for {
		if s.BinlogStreamer == nil {
			if err := s.recoverManaged(ctx, s.startErr); err != nil {
				return nil, err
			}
		}
		e, err := s.BinlogStreamer.GetEvent(ctx)
		if err == nil {
			if s.archive != nil {
				if r, ok := e.Event.(*replication.RotateEvent); ok {
					s.archiveNext = string(r.NextLogName)
				}
			}
			return e, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if s.archive != nil {
			if !errors.Is(err, io.EOF) {
				return nil, err
			}
			if len(s.canal.pendingTransaction) != 0 {
				return nil, fmt.Errorf("archived file %s ends inside a transaction", s.archive.Name)
			}
			cp, err := s.canal.recoveryBoundary(ctx)
			if err != nil {
				return nil, err
			}
			req := RecoveryRequest{Checkpoint: cp, CurrentSourceServerID: s.canal.GetBinlogSyncer().SourceServerID(), HostInstanceID: s.archive.HostInstanceID, PreviousFile: s.archive.Name, NextFile: s.archiveNext, ValidateFile: s.validateArchive}
			file, err := s.canal.recoveryProvider(ctx, req)
			if err == nil {
				if file == nil || file.HostInstanceID != s.archive.HostInstanceID {
					return nil, fmt.Errorf("archive host changed without relocating recovery boundary")
				}
				if err := s.installArchive(file); err != nil {
					return nil, err
				}
				continue
			}
			if !errors.Is(err, io.EOF) {
				return nil, err
			}
			// Rejoin by a freshly verified online time boundary, never by guessing
			// the next filename on a different physical source.
			s.archive = nil
			if err := s.startTimeline(ctx, cp, false); err != nil {
				return nil, err
			}
			continue
		}
		var reconnect *replication.ReconnectRequiredError
		var changed *replication.SourceChangedError
		if errors.As(err, &reconnect) && s.canal.cfg.DisableRetrySync {
			return nil, err
		}
		if !errors.As(err, &reconnect) && !errors.As(err, &changed) && !isMissingBinlog(err) {
			return nil, err
		}
		if err := s.recoverManaged(ctx, err); err != nil {
			return nil, err
		}
	}
}

func (s *localBinFileAdapterStreamer) recoverManaged(ctx context.Context, cause error) error {
	cp, err := s.canal.recoveryBoundary(ctx)
	if err != nil {
		return err
	}
	current := s.canal.GetBinlogSyncer().SourceServerID()
	s.canal.GetBinlogSyncer().Close()
	s.canal.master.Update(cp.Position)
	s.canal.expectedSourceServerID = cp.SourceServerID
	if cp.SourceServerID == 0 {
		s.canal.expectedSourceServerID = current
	}
	if err := s.canal.prepareSyncer(); err != nil {
		return err
	}
	if cp.ArchiveHostInstanceID != "" {
		return s.startTimeline(ctx, cp, true)
	}
	if isMissingBinlog(cause) {
		if _, err := recoveryTimestamp(cp.Timestamp); err != nil {
			return err
		}
		if s.canal.recoveryProvider == nil {
			return cause
		}
		file, err := s.canal.recoveryProvider(ctx, RecoveryRequest{Checkpoint: cp, CurrentSourceServerID: current, ValidateFile: s.validateArchive})
		if err != nil {
			return err
		}
		return s.installArchive(file)
	}
	stream, err := s.restartStream(ctx)
	if err == nil {
		s.BinlogStreamer, s.syncMasterStreamer = stream, stream
		s.startErr = nil
		return nil
	}
	var changed *replication.SourceChangedError
	if !errors.As(err, &changed) {
		return err
	}
	return s.startTimeline(ctx, cp, true)
}

// Only transport failures use the existing reconnect policy. Invalid recovery
// metadata, missing history and source changes are decisions for the caller.
func (s *localBinFileAdapterStreamer) restartStream(ctx context.Context) (*replication.BinlogStreamer, error) {
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		stream, err := s.canal.startSyncer()
		if err == nil {
			return stream, nil
		}
		var changed *replication.SourceChangedError
		var sqlErr *mysql.MyError
		var transport net.Error
		transient := errors.As(err, &transport) || errors.Is(err, mysql.ErrBadConn) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
		if !transient || errors.As(err, &changed) || errors.As(err, &sqlErr) || s.canal.cfg.DisableRetrySync || (s.canal.cfg.MaxReconnectAttempts > 0 && attempt+1 >= s.canal.cfg.MaxReconnectAttempts) {
			return nil, err
		}
		s.canal.cfg.Logger.Warn("recovery connection failed; retrying", "attempt", attempt+1)
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
		s.canal.GetBinlogSyncer().Close()
		if err := s.canal.prepareSyncer(); err != nil {
			return nil, err
		}
	}
}

func (s *localBinFileAdapterStreamer) startTimeline(ctx context.Context, cp RecoveryCheckpoint, allowArchive bool) error {
	ts, err := recoveryTimestamp(cp.Timestamp)
	if err != nil {
		return err
	}
	current := s.canal.GetBinlogSyncer().SourceServerID()
	if current == 0 {
		if err := s.canal.GetBinlogSyncer().ConnectForRecovery(); err != nil {
			return err
		}
		current = s.canal.GetBinlogSyncer().SourceServerID()
	}
	pos, err := s.canal.GetBinlogSyncer().FindRecoveryPosition(ctx, ts, cp.ServerID)
	if err != nil {
		if !allowArchive || s.canal.recoveryProvider == nil {
			return err
		}
		if !errors.Is(err, replication.ErrRecoveryHistoryUnavailable) {
			return err
		}
		file, err := s.canal.recoveryProvider(ctx, RecoveryRequest{Checkpoint: cp, CurrentSourceServerID: current, ValidateFile: s.validateArchive})
		if err != nil {
			return err
		}
		return s.installArchive(file)
	}
	s.canal.GetBinlogSyncer().Close()
	s.canal.expectedSourceServerID = current
	s.canal.master.Update(pos)
	if err := s.canal.prepareSyncer(); err != nil {
		return err
	}
	stream, err := s.canal.startSyncer()
	if err != nil {
		return err
	}
	s.BinlogStreamer, s.syncMasterStreamer = stream, stream
	return nil
}

func (s *localBinFileAdapterStreamer) installArchive(file *RecoveryFile) error {
	if file == nil || file.Name == "" || file.Path == "" || file.HostInstanceID == "" {
		return fmt.Errorf("incomplete recovery file")
	}
	if err := s.validateArchive(file.Path); err != nil {
		return err
	}
	s.archive, s.archiveNext = file, ""
	s.canal.cfg.Logger.Info("replaying archived binlog from file head", "file", file.Name, "hostInstanceID", file.HostInstanceID, "sourceServerID", file.SourceServerID)
	s.canal.master.Update(mysql.Position{Name: file.Name, Pos: 4})
	s.BinlogStreamer = s.localStreamer(file.Path, file.SourceServerID, file.HostInstanceID)
	return nil
}

func (s *localBinFileAdapterStreamer) validateArchive(path string) error {
	// Validate the entire file before exposing any of it to the consumer. This
	// also detects a clean event EOF containing an uncommitted transaction.
	open := false
	parser := s.canal.GetBinlogSyncer().NewFileParser()
	var inspect func(*replication.BinlogEvent) error
	inspect = func(e *replication.BinlogEvent) error {
		if err := s.canal.ctx.Err(); err != nil {
			return err
		}
		switch v := e.Event.(type) {
		case *replication.TransactionPayloadEvent:
			for _, sub := range v.Events {
				if err := inspect(sub); err != nil {
					return err
				}
			}
		case *replication.GTIDEvent, *replication.MariadbGTIDEvent, *replication.RowsEvent:
			open = true
		case *replication.XIDEvent:
			open = false
		case *replication.QueryEvent:
			q := strings.ToUpper(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(string(v.Query)), ";")))
			if strings.HasPrefix(q, "XA ") {
				return fmt.Errorf("XA transaction recovery is not supported")
			}
			if q == "BEGIN" || q == "START TRANSACTION" {
				open = true
			} else if !strings.HasPrefix(q, "SAVEPOINT ") && !strings.HasPrefix(q, "RELEASE SAVEPOINT ") && !strings.HasPrefix(q, "ROLLBACK TO ") {
				open = false
			}
		case *replication.RotateEvent:
			if open {
				return fmt.Errorf("archive rotate inside transaction")
			}
		}
		return nil
	}
	if err := parser.ParseRecoveryFile(path, 0, inspect); err != nil {
		return fmt.Errorf("validate archive: %w", err)
	}
	if open {
		return fmt.Errorf("archive ends inside a transaction")
	}
	return nil
}

func (s *localBinFileAdapterStreamer) localStreamer(path string, sourceID uint32, archiveHost ...string) *replication.BinlogStreamer {
	stream := replication.NewBinlogStreamer()
	parser := s.canal.GetBinlogSyncer().NewFileParser()
	go func() {
		err := parser.ParseRecoveryFile(path, 0, func(e *replication.BinlogEvent) error {
			e.Header.SourceServerID = sourceID
			if len(archiveHost) > 0 {
				e.Header.ArchiveHostInstanceID = archiveHost[0]
			}
			return stream.AddEventToStreamerContext(s.canal.ctx, e)
		})
		if err == nil {
			err = io.EOF
		}
		stream.CloseWithError(err)
	}()
	return stream
}
