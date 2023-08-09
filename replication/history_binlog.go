package replication

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	. "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/errors"
)

func findBinLog(cfg BinlogSyncerConfig, pos Position, currTimeStamp uint32) *Position {
	// 切换前的最后一条binlog时间，不在Master Status中，报错，说明要回拨了，手工处理
	lineTime, err := findBinlogByTimeLine(BinlogSyncerConfig{
		ServerID:        uint32(rand.New(rand.NewSource(time.Now().Unix())).Intn(1000)) + 1001,
		Flavor:          cfg.Flavor,
		Host:            cfg.Host,
		Port:            cfg.Port,
		User:            cfg.User,
		Password:        cfg.Password,
		RawModeEnabled:  cfg.RawModeEnabled,
		SemiSyncEnabled: cfg.SemiSyncEnabled,
		UseDecimal:      true,
	}, pos)
	if err != nil {
		return nil
	}

	if currTimeStamp >= lineTime {
		// 如果找到的binlog时间不早于当前时间,直接返回
		return &pos
	}
	name, _ := getOtherBinlogName(pos.Name, -1)
	pos.Name = name
	return findBinLog(cfg, pos, currTimeStamp)

}

func findBinlogByTimeLine(cfg BinlogSyncerConfig, position Position) (uint32, error) {
	b := NewBinlogSyncer(cfg)
	return b.FindBinlogByTimeLine(position, 0)
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

func (b *BinlogSyncer) FindBinlogByTimeLine(p Position, timeout time.Duration) (uint32, error) {
	if timeout == 0 {
		timeout = 30 * 3600 * 24 * time.Second
	}
	b.parser.SetRawMode(true)
	s, err := b.StartSync(p)
	if err != nil {
		return 0, errors.Trace(err)
	}
	var headerTimeStamp uint32 = 0
	for {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		e, err := s.GetEvent(ctx)
		cancel()
		if err != nil {
			return 0, err
		}
		if e.Header.Timestamp != 0 {
			headerTimeStamp = e.Header.Timestamp
			break
		}
	}
	b.Close()
	return headerTimeStamp, nil
}
