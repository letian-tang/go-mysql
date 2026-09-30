package canal

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/go-mysql-org/go-mysql/test_util/recoverymock"
	"github.com/stretchr/testify/require"
)

type recoveryHandler struct {
	DummyEventHandler
	rows   map[int32]int32
	cp     RecoveryCheckpoint
	fail   error
	notify chan RecoveryCheckpoint
}

func (h *recoveryHandler) OnRow(e *RowsEvent) error {
	if h.fail != nil {
		return h.fail
	}
	for _, r := range e.Rows {
		h.rows[r[0].(int32)] = r[1].(int32)
	}
	return nil
}
func (h *recoveryHandler) OnPosSynced(head *replication.EventHeader, pos mysql.Position, _ mysql.GTIDSet, _ bool) error {
	if head != nil && head.Timestamp != 0 {
		h.cp = RecoveryCheckpoint{Position: pos, Timestamp: head.Timestamp, ServerID: head.ServerID, SourceServerID: head.SourceServerID, ArchiveHostInstanceID: head.ArchiveHostInstanceID}
	}
	if h.notify != nil {
		h.notify <- h.cp
	}
	return nil
}
func recoveryCanal(t *testing.T) (*Canal, *recoveryHandler) {
	t.Helper()
	src, err := recoverymock.New(700, []string{"bin.000001"}, func(recoverymock.Dump) recoverymock.Reply { return recoverymock.Reply{Wait: true} })
	require.NoError(t, err)
	t.Cleanup(src.Close)
	cfg := NewDefaultConfig()
	cfg.Addr = src.Addr
	cfg.User = "test"
	cfg.Password = ""
	cfg.Dump.ExecutionPath = ""
	c, err := NewCanal(cfg)
	require.NoError(t, err)
	h := &recoveryHandler{rows: map[int32]int32{}, cp: RecoveryCheckpoint{Position: mysql.Position{Name: "bin.000001", Pos: 4}, Timestamp: 1000, ServerID: 11, SourceServerID: 700}}
	c.SetEventHandler(h)
	c.WithRecovery(nil, func(context.Context) (RecoveryCheckpoint, error) { return h.cp, nil })
	c.master.Update(h.cp.Position)
	c.recoveryCheckpoint = h.cp
	table := &schema.Table{Schema: "test", Name: "recovery"}
	table.AddColumn("id", "int", "", "")
	table.AddColumn("value", "int", "", "")
	c.SetTableCache([]byte("test"), []byte("recovery"), table)
	t.Cleanup(c.Close)
	return c, h
}
func parsedEvents(t *testing.T, b *recoverymock.Builder) []*replication.BinlogEvent {
	t.Helper()
	p := replication.NewBinlogParser()
	var events []*replication.BinlogEvent
	for _, raw := range b.Events {
		e, err := p.Parse(raw)
		require.NoError(t, err)
		e.Header.SourceServerID = 700
		events = append(events, e)
	}
	return events
}
func TestRecoveryBuffersCompleteTransactionsAndKeepsOldTimestamps(t *testing.T) {
	c, h := recoveryCanal(t)
	b := recoverymock.NewBuilder(11)
	b.Transaction(90, 1100, 1, 10)
	b.Transaction(90, 1100, 2, 20)
	events := parsedEvents(t, b)
	for _, e := range events[:len(events)-1] {
		require.NoError(t, c.handleRecoveryEvent(e))
	}
	require.Equal(t, map[int32]int32{1: 10}, h.rows)
	require.NoError(t, c.handleRecoveryEvent(events[len(events)-1]))
	require.Equal(t, map[int32]int32{1: 10, 2: 20}, h.rows)
	require.Equal(t, uint32(700), h.cp.SourceServerID)
	require.Equal(t, uint32(11), h.cp.ServerID)
	old := h.cp
	b2 := recoverymock.NewBuilder(11)
	b2.Query(90, "BEGIN")
	b2.Row(90, 999, 999)
	for _, e := range parsedEvents(t, b2) {
		require.NoError(t, c.handleRecoveryEvent(e))
	}
	cp, err := c.recoveryBoundary(context.Background())
	require.NoError(t, err)
	require.Equal(t, old, cp)
	require.NotContains(t, h.rows, int32(999))
	require.Empty(t, c.pendingTransaction)
}
func TestRecoveryHandlerFailureDoesNotCommitBoundary(t *testing.T) {
	c, h := recoveryCanal(t)
	old := h.cp
	h.fail = errors.New("sink unavailable")
	b := recoverymock.NewBuilder(11)
	b.Transaction(900, 1100, 1, 10)
	events := parsedEvents(t, b)
	for _, e := range events[:len(events)-1] {
		require.NoError(t, c.handleRecoveryEvent(e))
	}
	require.Error(t, c.handleRecoveryEvent(events[len(events)-1]))
	require.Equal(t, old, h.cp)
	require.Empty(t, h.rows)
}
func TestRecoveryFakeRotateAndEOFNeverCommit(t *testing.T) {
	c, h := recoveryCanal(t)
	old := h.cp
	require.NoError(t, c.handleRecoveryEvent(&replication.BinlogEvent{Header: &replication.EventHeader{SourceServerID: 900}, Event: &replication.RotateEvent{Position: 4, NextLogName: []byte("bin.000001")}}))
	require.Error(t, c.handleRecoveryEvent(&replication.BinlogEvent{Header: &replication.EventHeader{}, Event: &replication.RotateEvent{Position: 4, NextLogName: []byte("bin.000002")}}))
	require.Equal(t, old, h.cp)
	b := recoverymock.NewBuilder(11)
	b.Transaction(90, 1100, 1, 10)
	b.Add(3, 1100, nil)
	path := filepath.Join(t.TempDir(), "bin.000001")
	require.NoError(t, os.WriteFile(path, b.Bytes(), 0600))
	adapter := &localBinFileAdapterStreamer{canal: c}
	stream := adapter.localStreamer(path, 700)
	count := 0
	for {
		e, err := stream.GetEvent(context.Background())
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		count++
		require.NoError(t, c.handleRecoveryEvent(e))
	}
	require.Equal(t, len(b.Events), count)
	require.Equal(t, uint32(1100), h.cp.Timestamp)
	require.Equal(t, b.Pos-19, h.cp.Position.Pos)
}
func TestRecoveryRejectsMissingAndFutureTime(t *testing.T) {
	_, err := recoveryTimestamp(0)
	require.Error(t, err)
	_, err = recoveryTimestamp(^uint32(0))
	require.Error(t, err)
}

func TestArchiveCheckpointRestartNeverUsesArchiveOffsetOnline(t *testing.T) {
	b := recoverymock.NewBuilder(11)
	b.Transaction(500, 650, 1, 10)
	src, err := recoverymock.New(700, []string{"bin.000003"}, func(d recoverymock.Dump) recoverymock.Reply {
		if d.Position.Name != "bin.000003" || d.Position.Pos != 4 {
			t.Errorf("unverified archive coordinate dumped online: %+v", d)
		}
		return recoverymock.Reply{Events: b.Events, Wait: !d.NonBlock}
	})
	require.NoError(t, err)
	defer src.Close()
	cfg := NewDefaultConfig()
	cfg.Addr = src.Addr
	cfg.User = "test"
	cfg.Password = ""
	cfg.Dump.ExecutionPath = ""
	c, err := NewCanal(cfg)
	require.NoError(t, err)
	h := &recoveryHandler{rows: map[int32]int32{}, notify: make(chan RecoveryCheckpoint, 16)}
	c.SetEventHandler(h)
	c.WithRecovery(nil, nil)
	table := &schema.Table{Schema: "test", Name: "recovery"}
	table.AddColumn("id", "int", "", "")
	table.AddColumn("value", "int", "", "")
	c.SetTableCache([]byte("test"), []byte("recovery"), table)
	done := make(chan error, 1)
	go func() {
		done <- c.RunFromCheckpoint(RecoveryCheckpoint{Position: mysql.Position{Name: "bin.000001", Pos: 123}, Timestamp: 1000, ServerID: 11, ArchiveHostInstanceID: "old-host"})
	}()
	defer func() { c.Close(); require.NoError(t, <-done) }()
	select {
	case got := <-h.notify:
		require.Equal(t, uint32(700), got.SourceServerID)
		require.Empty(t, got.ArchiveHostInstanceID)
	case <-time.After(3 * time.Second):
		t.Fatal("archive restart did not rejoin")
	}
}

func TestArchiveValidationRejectsIncompleteTransactionBeforeRows(t *testing.T) {
	c, h := recoveryCanal(t)
	b := recoverymock.NewBuilder(11)
	b.Query(900, "BEGIN")
	b.Row(900, 999, 999)
	path := filepath.Join(t.TempDir(), "bin.000001")
	require.NoError(t, os.WriteFile(path, b.Bytes(), 0600))
	s := &localBinFileAdapterStreamer{canal: c}
	require.ErrorContains(t, s.installArchive(&RecoveryFile{Name: "bin.000001", Path: path, HostInstanceID: "old-host"}), "transaction")
	require.Empty(t, h.rows)
}

func TestRecoveryCompressedCommitUsesEnclosingFilePosition(t *testing.T) {
	c, h := recoveryCanal(t)
	b := recoverymock.NewBuilder(11)
	b.Transaction(900, 1100, 1, 10)
	events := parsedEvents(t, b)
	// The format descriptor is not inside a transaction payload.
	for _, e := range events[1:] {
		e.Header.LogPos = 0
	}
	outer := &replication.BinlogEvent{
		Header: &replication.EventHeader{LogPos: 1234, SourceServerID: 700},
		Event:  &replication.TransactionPayloadEvent{Events: events[1:]},
	}
	require.NoError(t, c.handleRecoveryEvent(outer))
	require.Equal(t, uint32(1234), h.cp.Position.Pos)
	require.Equal(t, map[int32]int32{1: 10}, h.rows)
}
