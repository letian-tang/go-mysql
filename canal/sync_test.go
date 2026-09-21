package canal

import (
	"log/slog"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/require"
)

func TestGetShowBinaryLogQuery(t *testing.T) {
	tests := []struct {
		flavor        string
		serverVersion string
		expected      string
	}{
		{flavor: "mariadb", serverVersion: "10.5.2", expected: "SHOW BINLOG STATUS"},
		{flavor: "mariadb", serverVersion: "10.6.0", expected: "SHOW BINLOG STATUS"},
		{flavor: "mariadb", serverVersion: "10.4.0", expected: "SHOW MASTER STATUS"},
		{flavor: "mysql", serverVersion: "8.4.0", expected: "SHOW BINARY LOG STATUS"},
		{flavor: "mysql", serverVersion: "8.4.1", expected: "SHOW BINARY LOG STATUS"},
		{flavor: "mysql", serverVersion: "8.0.33", expected: "SHOW MASTER STATUS"},
		{flavor: "mysql", serverVersion: "5.7.41", expected: "SHOW MASTER STATUS"},
		{flavor: "other", serverVersion: "1.0.0", expected: "SHOW MASTER STATUS"},
	}

	for _, tt := range tests {
		t.Run(tt.flavor+"_"+tt.serverVersion, func(t *testing.T) {
			got := getShowBinaryLogQuery(tt.flavor, tt.serverVersion)
			require.Equal(t, tt.expected, got)
		})
	}
}

func TestFailoverRecoveryStartsAfterBufferedOldPrimaryEvents(t *testing.T) {
	master := &masterInfo{logger: slog.Default()}
	master.UpdateTimestamp(100)
	syncer := &replication.BinlogSyncer{Failover: true, CurrTimeStamp: 300}
	c := &Canal{master: master, syncer: syncer}

	// This event was received before failover and was already buffered. The
	// boundary has not arrived yet, so it must not be filtered using the
	// network reader's later timestamp.
	require.False(t, c.shouldSkipFailoverRow(90))

	c.beginFailoverRecovery()
	require.Equal(t, uint32(100), c.failoverCutoffTimestamp)
	require.True(t, c.shouldSkipFailoverRow(99))
	require.False(t, c.shouldSkipFailoverRow(100))
	require.Zero(t, c.failoverCutoffTimestamp)
	require.False(t, syncer.Failover)
}
