package replication

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/errors"
)

const (
	// eventHeaderLen binlog 事件头长度：
	// timestamp(4) + type(1) + server_id(4) + event_size(4) + log_pos(4) + flags(2)
	eventHeaderLen = 19

	// binlogDumpNonBlock COM_BINLOG_DUMP 的 BINLOG_DUMP_NON_BLOCK 标志（=1）。
	// 探测必须用 NON_BLOCK：dump 线程推流期间不读客户端命令（见 MySQL
	// rpl_binlog_sender.cc，仅有写路径 + thd->killed 检查），NEVER_STOP 模式下
	// 第二个 dump 命令永远躺在 TCP 缓冲里不被处理。NON_BLOCK 下服务器读完
	// 整个文件回 EOF 包、dump 线程退出、连接回到 do_command 命令态，才能在
	// 同一连接上探测下一个文件（mysqlbinlog 多文件回读即此模式，见官方
	// mysqlbinlog.cc: get_dump_flags() { return stop_never ? 0 : BINLOG_DUMP_NON_BLOCK; }）。
	binlogDumpNonBlock = 1

	// probeReadTimeout 单次读取 binlog 事件包的超时，防止探测连接无限阻塞
	probeReadTimeout = 10 * time.Second

	// probeConnectTimeout 建立探测连接的超时
	probeConnectTimeout = 10 * time.Second
)

// historyProber 在单个连接上逐个 binlog 文件回拨，探测文件首个事件的时间戳。
// 旧实现每回拨一个文件就新建一个 BinlogSyncer 并粗暴关闭（不发 COM_QUIT），
// 一次主备切换会在 RDS 上留下 N+1 条 "Aborted connection" 日志。
type historyProber struct {
	conn *client.Conn
	sid  uint32
}

func newHistoryProber(cfg BinlogSyncerConfig) (*historyProber, error) {
	addr := cfg.Host
	if cfg.Port != 0 {
		addr = net.JoinHostPort(cfg.Host, strconv.Itoa(int(cfg.Port)))
	}

	// 沿用旧实现的随机 server_id，避免与正在运行的同步连接冲突
	sid := uint32(rand.New(rand.NewSource(time.Now().Unix())).Intn(1000)) + 1001

	timeoutCtx, cancel := context.WithTimeout(context.Background(), probeConnectTimeout)
	defer cancel()

	c, err := client.ConnectWithDialer(timeoutCtx, "", addr, cfg.User, cfg.Password, "",
		cfg.Dialer, func(c *client.Conn) error {
			c.SetTLSConfig(cfg.TLSConfig)
			c.SetAttributes(map[string]string{"_client_role": "binary_log_listener"})
			c.ReadTimeout = probeReadTimeout
			return nil
		})
	if err != nil {
		return nil, errors.Trace(err)
	}

	// 与官方 BinlogSyncer.registerSlave 一致：声明客户端不理解 checksum
	if _, err := c.Execute(`SET @master_binlog_checksum='NONE', @source_binlog_checksum='NONE'`); err != nil {
		_ = c.Close()
		return nil, errors.Trace(err)
	}

	if cfg.Flavor == mysql.MariaDBFlavor {
		if _, err := c.Execute("SET @mariadb_slave_capability=4"); err != nil {
			_ = c.Close()
			return nil, errors.Errorf("failed to set @mariadb_slave_capability=4: %v", err)
		}
	}

	p := &historyProber{conn: c, sid: sid}
	if err := p.registerSlave(cfg); err != nil {
		_ = c.Close()
		return nil, errors.Trace(err)
	}
	return p, nil
}

// quit 优雅关闭：写 COM_QUIT 后排空残留数据再断开 TCP。
// 前提：firstEventTimestamp 已按 NON_BLOCK 读到 EOF，此时 dump 线程已退出、
// 连接回到命令态，COM_QUIT 会被服务端 do_command 正常读取并干净关闭（FIN），
// 不会像 NEVER_STOP 中途断开那样触发 RST 和 RDS "Aborted connection" 日志。
func (p *historyProber) quit() {
	c := p.conn
	c.ResetSequence()
	data := make([]byte, 5)
	data[4] = mysql.COM_QUIT
	if err := c.WritePacket(data); err != nil {
		_ = c.Close()
		return
	}

	// 服务器处理 QUIT 后会关闭连接，读到 EOF 即排空完毕；2 秒兜底超时
	_ = c.Conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 4096)
	for {
		if _, err := c.Conn.Read(buf); err != nil {
			break
		}
	}
	_ = c.Close()
}

// registerSlave 复刻官方 COM_REGISTER_SLAVE 打包，注册一个临时副本 ID
func (p *historyProber) registerSlave(cfg BinlogSyncerConfig) error {
	p.conn.ResetSequence()

	hostname := cfg.Localhost
	if len(hostname) == 0 {
		hostname, _ = os.Hostname()
	}
	if len(hostname) > 255 {
		hostname = hostname[:255]
	}

	data := make([]byte, 4+1+4+1+len(hostname)+1+len(cfg.User)+1+2+4+4)
	pos := 4
	data[pos] = mysql.COM_REGISTER_SLAVE
	pos++

	binary.LittleEndian.PutUint32(data[pos:], p.sid)
	pos += 4

	data[pos] = uint8(len(hostname))
	pos++
	n := copy(data[pos:], hostname)
	pos += n

	data[pos] = uint8(len(cfg.User))
	pos++
	n = copy(data[pos:], cfg.User)
	pos += n

	data[pos] = uint8(0) // password 为空
	pos++

	binary.LittleEndian.PutUint16(data[pos:], cfg.Port)
	pos += 2

	// replication rank, not used
	binary.LittleEndian.PutUint32(data[pos:], 0)
	pos += 4

	// master ID, 0 is OK
	binary.LittleEndian.PutUint32(data[pos:], 0)

	if err := p.conn.WritePacket(data); err != nil {
		return errors.Trace(err)
	}
	// COM_REGISTER_SLAVE 有 OK 响应包，必须读掉
	if _, err := p.conn.ReadOKPacket(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// writeDump 复刻官方 COM_BINLOG_DUMP（按文件位置）打包
func (p *historyProber) writeDump(pos mysql.Position) error {
	p.conn.ResetSequence()

	data := make([]byte, 4+1+4+2+4+len(pos.Name))
	i := 4
	data[i] = mysql.COM_BINLOG_DUMP
	i++

	binary.LittleEndian.PutUint32(data[i:], pos.Pos)
	i += 4

	binary.LittleEndian.PutUint16(data[i:], binlogDumpNonBlock)
	i += 2

	binary.LittleEndian.PutUint32(data[i:], p.sid)
	i += 4

	copy(data[i:], pos.Name)

	return p.conn.WritePacket(data)
}

// firstEventTimestamp 对指定位置发送 NON_BLOCK dump 命令，返回该文件首个
// 非零时间戳事件（Format_description/Previous_gtids 等）的时间戳。
// binlog 网络流格式：<OK 0x00><event bytes>（见官方 parseEvent 的 Skip OK byte），
// artificial rotate 事件（timestamp=0）会被跳过。
//
// 关键约束：NON_BLOCK 下必须把整个文件读尽（直到服务器回 EOF 包），
// dump 线程才会退出、连接才回到命令态；拿到首个时间戳就返回会留下
// 未消费的在途事件，导致下一次 dump 的响应错位。
func (p *historyProber) firstEventTimestamp(pos mysql.Position) (uint32, error) {
	if err := p.writeDump(pos); err != nil {
		return 0, errors.Trace(err)
	}

	var firstTS uint32
	for {
		data, err := p.conn.ReadPacket()
		if err != nil {
			return 0, errors.Trace(err)
		}
		if len(data) == 0 {
			continue
		}
		switch data[0] {
		case mysql.OK_HEADER:
			// 正常事件包，0x00 后跟事件；记下首个非零时间戳后继续读到 EOF
			if len(data) >= eventHeaderLen+1 {
				ts := binary.LittleEndian.Uint32(data[1:5])
				if ts != 0 && firstTS == 0 {
					firstTS = ts
				}
			}
		case mysql.ERR_HEADER:
			// dump 流中服务器错误以 ERR packet 返回（例如回拨的 binlog 文件已被 purge）
			return 0, p.conn.HandleErrorPacket(data)
		case mysql.EOF_HEADER:
			// NON_BLOCK 下文件读完的正常结束：dump 线程退出，连接回到命令态
			return firstTS, nil
		default:
			return 0, errors.Errorf("unexpected packet header 0x%02x in binlog dump stream", data[0])
		}
	}
}

// findBinLog 查找时间线不早于 currTimeStamp 的 binlog 位置，用于主备切换后回拨。
// 全部探测在单个连接上完成，结束后以 COM_QUIT 优雅关闭，
// 避免在 RDS 上产生大量 "Aborted connection" 日志。
// 出错时返回最近一次确认的位置（至少为入参 pos），由调用方决定是否采用。
func findBinLog(cfg BinlogSyncerConfig, pos mysql.Position, currTimeStamp uint32) (mysql.Position, error) {
	prober, err := newHistoryProber(cfg)
	if err != nil {
		return pos, errors.Trace(err)
	}
	defer prober.quit()

	for {
		ts, err := prober.firstEventTimestamp(pos)
		if err != nil {
			return pos, errors.Trace(err)
		}
		if currTimeStamp >= ts {
			return pos, nil
		}
		name, err := getOtherBinlogName(pos.Name, -1)
		if err != nil {
			return pos, errors.Trace(err)
		}
		pos.Name = name
		pos.Pos = 4
	}
}

func getOtherBinlogName(binlogName string, add int) (string, error) {
	i := strings.LastIndexByte(binlogName, '.')
	if i == -1 {
		return "", errors.New("parse err")
	}
	suffix := binlogName[i+1:]
	numLen := len(suffix)
	seq, _ := strconv.Atoi(suffix)
	seq = seq + add
	return fmt.Sprintf("%s.%0*d", binlogName[:i], numLen, seq), nil
}
