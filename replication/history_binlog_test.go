package replication

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/test_util"
)

// TestHistoryProberFindBinLog 需要真实 MySQL（binlog 已开启，root 免密），
// 跑法：go test ./replication/ -run TestHistoryProberFindBinLog -host 127.0.0.1 -port 3306 -v
// 验证两件事：
//  1. NON_BLOCK 单连接回拨能跨文件返回正确位置（不挂死）；
//  2. 探测连接以 COM_QUIT 干净退出，服务端 processlist 不残留 Binlog Dump 线程。
func TestHistoryProberFindBinLog(t *testing.T) {
	port, err := strconv.Atoi(*test_util.MysqlPort)
	require.NoError(t, err)

	c, err := client.Connect((*test_util.MysqlHost)+":"+strconv.Itoa(port), "root", "", "")
	if err != nil {
		t.Skipf("no local MySQL, skip: %v", err)
	}
	defer c.Close()

	// 确认 binlog 开启且能拿到位点
	rr, err := c.Execute("SHOW MASTER STATUS")
	require.NoError(t, err)
	if rr.RowNumber() == 0 {
		t.Skip("binlog disabled, skip")
	}
	_, _ = rr.GetString(0, 0)

	// 造一个事件，然后 FLUSH BINARY LOGS 产生新文件，再写一个事件。
	// 之后 masterPos 指向最新文件，其首个事件时间戳 ≈ now，
	// 用 currTimeStamp = now-60s 回拨必须至少退到上一个文件。
	_, err = c.Execute("CREATE TABLE IF NOT EXISTS findbinlog_probe (id INT AUTO_INCREMENT PRIMARY KEY, ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
	require.NoError(t, err)
	_, err = c.Execute("INSERT INTO findbinlog_probe (id) VALUES (1)")
	require.NoError(t, err)
	_, err = c.Execute("FLUSH BINARY LOGS")
	require.NoError(t, err)
	_, err = c.Execute("INSERT INTO findbinlog_probe (id) VALUES (2)")
	require.NoError(t, err)

	rr, err = c.Execute("SHOW MASTER STATUS")
	require.NoError(t, err)
	require.True(t, rr.RowNumber() > 0)
	name, err := rr.GetString(0, 0)
	require.NoError(t, err)
	pos, err := rr.GetUint(0, 1)
	require.NoError(t, err)
	masterPos := mysql.Position{Name: name, Pos: uint32(pos)}

	cfg := BinlogSyncerConfig{
		ServerID: 101,
		Flavor:   mysql.MySQLFlavor,
		Host:     *test_util.MysqlHost,
		Port:     uint16(port),
		User:     "root",
	}

	start := time.Now()
	ret, err := findBinLog(cfg, masterPos, uint32(time.Now().Unix()-60))
	require.NoError(t, err)
	elapsed := time.Since(start)

	t.Logf("masterPos=%v rolled-back pos=%v, cost=%v", masterPos, ret, elapsed)

	// 回拨后的文件必须不晚于 master 文件（字典序即时间序，零填充编号）
	require.True(t, ret.Name <= masterPos.Name,
		"expected rollback to an earlier file, got %v (master %v)", ret.Name, masterPos.Name)
	require.True(t, ret.Pos >= 4)

	// 不应挂死：探测 1-2 个空/小文件应在秒级完成
	require.Less(t, elapsed, 30*time.Second)

	// 探测连接应已干净退出：不存在 server_id∈[1001,2000) 的残留 Binlog Dump 线程
	pl, err := c.Execute("SHOW PROCESSLIST")
	require.NoError(t, err)
	for i := 0; i < pl.RowNumber(); i++ {
		cmd, _ := pl.GetString(i, 4) // Command 列
		require.NotEqual(t, "Binlog Dump", cmd,
			"probe connection leaked a dump thread, server did not process COM_QUIT")
	}
}
