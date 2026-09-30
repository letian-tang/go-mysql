package replication_test

import (
	"context"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/test_util/recoverymock"
	"github.com/stretchr/testify/require"
)

func probeSyncer(t *testing.T, src *recoverymock.Source) *replication.BinlogSyncer {
	host, port, err := net.SplitHostPort(src.Addr)
	require.NoError(t, err)
	p, err := strconv.Atoi(port)
	require.NoError(t, err)
	b := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{Host: host, Port: uint16(p), User: "test", ServerID: 99, ManagedRecovery: true, ExpectedSourceServerID: 700})
	t.Cleanup(b.Close)
	return b
}

func TestRecoveryMalformedProbeAndOnlineStreamReturnError(t *testing.T) {
	b := recoverymock.NewBuilder(11)
	b.Add(2, 1000, []byte{0})
	src, err := recoverymock.New(700, []string{"bin.000001"}, func(recoverymock.Dump) recoverymock.Reply { return recoverymock.Reply{Events: b.Events, Wait: true} })
	require.NoError(t, err)
	defer src.Close()
	syncer := probeSyncer(t, src)
	require.NotPanics(t, func() {
		_, err := syncer.FindRecoveryPosition(context.Background(), 1200)
		require.ErrorContains(t, err, "decoder panic")
	})
	stream, err := syncer.StartSync(mysql.Position{Name: "bin.000001", Pos: 4})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for {
		_, err := stream.GetEvent(ctx)
		if err != nil {
			require.ErrorContains(t, err, "decoder panic")
			break
		}
	}
}

type shortenedRecoveryHandshakeDeadline struct{ net.Conn }

func (c shortenedRecoveryHandshakeDeadline) SetDeadline(deadline time.Time) error {
	if !deadline.IsZero() {
		deadline = time.Now().Add(200 * time.Millisecond)
	}
	return c.Conn.SetDeadline(deadline)
}

func TestRecoveryProbeClearsHandshakeDeadlineAfterLongScan(t *testing.T) {
	late := recoverymock.NewBuilder(11)
	late.Transaction(900, 900, 1, 1)
	early := recoverymock.NewBuilder(11)
	early.Transaction(100, 100, 2, 2)
	var dumps atomic.Int32
	src, err := recoverymock.New(700, []string{"bin.000001", "bin.000002"}, func(recoverymock.Dump) recoverymock.Reply {
		if dumps.Add(1) == 1 {
			time.Sleep(400 * time.Millisecond) // Scale the initial 10s deadline only.
			return recoverymock.Reply{Events: late.Events}
		}
		return recoverymock.Reply{Events: early.Events}
	})
	require.NoError(t, err)
	defer src.Close()
	host, port, err := net.SplitHostPort(src.Addr)
	require.NoError(t, err)
	p, err := strconv.Atoi(port)
	require.NoError(t, err)
	dial := func(ctx context.Context, network, address string) (net.Conn, error) {
		raw, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return shortenedRecoveryHandshakeDeadline{raw}, nil
	}
	b := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{Host: host, Port: uint16(p), User: "test", ServerID: 99, ExpectedSourceServerID: 700, ManagedRecovery: true, Dialer: dial})
	defer b.Close()
	pos, err := b.FindRecoveryPosition(context.Background(), 300)
	require.NoError(t, err)
	require.Equal(t, "bin.000002", pos.Name)
}
func TestRecoveryProbeUsesCommitNotFormatDescriptionTime(t *testing.T) {
	b := recoverymock.NewBuilder(11)
	b.Transaction(10, 600, 1, 1)
	src, err := recoverymock.New(700, []string{"bin.000001"}, func(recoverymock.Dump) recoverymock.Reply { return recoverymock.Reply{Events: b.Events} })
	require.NoError(t, err)
	t.Cleanup(src.Close)
	syncer := probeSyncer(t, src)
	_, err = syncer.FindRecoveryPosition(context.Background(), 300)
	require.ErrorIs(t, err, replication.ErrRecoveryHistoryUnavailable)
	pos, err := syncer.FindRecoveryPosition(context.Background(), 600)
	require.NoError(t, err)
	require.Equal(t, mysql.Position{Name: "bin.000001", Pos: 4}, pos)
	src.SetNode(701)
	_, err = syncer.FindRecoveryPosition(context.Background(), 600)
	var changed *replication.SourceChangedError
	require.ErrorAs(t, err, &changed)
}
func TestRecoveryProbeIncompleteTransactionAndCancellation(t *testing.T) {
	b := recoverymock.NewBuilder(11)
	b.Query(10, "BEGIN")
	b.Row(10, 1, 1)
	src, err := recoverymock.New(700, []string{"bin.000001"}, func(recoverymock.Dump) recoverymock.Reply { return recoverymock.Reply{Events: b.Events} })
	require.NoError(t, err)
	t.Cleanup(src.Close)
	_, err = probeSyncer(t, src).FindRecoveryPosition(context.Background(), 600)
	require.ErrorContains(t, err, "transaction")
	hanging, err := recoverymock.New(700, []string{"bin.000001"}, func(recoverymock.Dump) recoverymock.Reply { return recoverymock.Reply{Wait: true} })
	require.NoError(t, err)
	t.Cleanup(hanging.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = probeSyncer(t, hanging).FindRecoveryPosition(ctx, 600)
	require.Error(t, err)
	require.Less(t, time.Since(start), time.Second)
}
func TestManagedStartReportsPhysicalChangeBeforeDump(t *testing.T) {
	src, err := recoverymock.New(701, nil, func(recoverymock.Dump) recoverymock.Reply {
		t.Error("old position must not be dumped on changed source")
		return recoverymock.Reply{Wait: true}
	})
	require.NoError(t, err)
	t.Cleanup(src.Close)
	_, err = probeSyncer(t, src).StartSync(mysql.Position{Name: "bin.000001", Pos: 123})
	var changed *replication.SourceChangedError
	require.ErrorAs(t, err, &changed)
	require.Equal(t, uint32(701), changed.Current)
}
