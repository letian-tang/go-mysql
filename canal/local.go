package canal

import (
	"context"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/errors"
)

// BinlogFileDownloader downloads the binlog file and return the path to it. It's often used to download binlog backup from RDS service.
type BinlogFileDownloader func(mysql.Position) (localBinFilePath string, err error)

// WithLocalBinlogDownloader registers the local bin file downloader,
// that allows download the backup binlog file from RDS service to local
func (c *Canal) WithLocalBinlogDownloader(d BinlogFileDownloader) {
	c.binFileDownloader = d
}

func (c *Canal) adaptLocalBinFileStreamer(remoteBinlogStreamer *replication.BinlogStreamer, err error) (*localBinFileAdapterStreamer, error) {
	return &localBinFileAdapterStreamer{
		BinlogStreamer:     remoteBinlogStreamer,
		syncMasterStreamer: remoteBinlogStreamer,
		canal:              c,
		binFileDownloader:  c.binFileDownloader,
	}, err
}

// localBinFileAdapterStreamer will support to download flushed binlog file for continuous sync in cloud computing platform
type localBinFileAdapterStreamer struct {
	*replication.BinlogStreamer                             // the running streamer, it will be localStreamer or sync master streamer
	syncMasterStreamer          *replication.BinlogStreamer // syncMasterStreamer is the streamer from canal startSyncer
	canal                       *Canal
	binFileDownloader           BinlogFileDownloader
}

// GetEvent will auto switch the local and remote streamer to get binlog event if possible.
func (s *localBinFileAdapterStreamer) GetEvent(ctx context.Context) (*replication.BinlogEvent, error) {
	if s.binFileDownloader == nil { // not support to use local bin file
		return s.BinlogStreamer.GetEvent(ctx)
	}

	ev, err := s.BinlogStreamer.GetEvent(ctx)

	if err == nil {
		switch ev.Event.(type) {
		case *replication.RotateEvent: // RotateEvent means need to change steamer back to sync master to retry sync
			s.BinlogStreamer = s.syncMasterStreamer
		}
		return ev, err
	}

	mysqlErr, ok := err.(*mysql.MyError)
	// only 'Could not find first log' can create local streamer, ignore other errors
	if !ok || mysqlErr.Code != mysql.ER_MASTER_FATAL_ERROR_READING_BINLOG ||
		mysqlErr.Message != "Could not find first log file name in binary log index file" {
		return ev, err
	}

	s.canal.cfg.Logger.Info("Could not find first log, try to download the local binlog for retry")

	// local binlog need next position to find binlog file and begin event
	pos := s.canal.master.Position()
	newStreamer := s.newLocalBinFileStreamer(s.binFileDownloader, pos)
	s.BinlogStreamer = newStreamer

	return newStreamer.GetEvent(ctx)
}

func (s *localBinFileAdapterStreamer) newLocalBinFileStreamer(download BinlogFileDownloader, position mysql.Position) *replication.BinlogStreamer {
	streamer := replication.NewBinlogStreamer()
	binFilePath, err := download(position)
	if err != nil {
		streamer.CloseWithError(errors.New("local binlog file not exist"))
		return streamer
	}
	go func(binFilePath string, streamer *replication.BinlogStreamer) {
		isBegin := false
		err := s.canal.syncer.GetBinlogParser().ParseFile(binFilePath, 0, func(ev *replication.BinlogEvent) error {
			if ev.Header.LogPos == position.Pos {
				isBegin = true
			}
			if isBegin {
				return streamer.AddEventToStreamer(ev)
			}
			return nil
		})
		if err != nil {
			streamer.CloseWithError(err)
		}
	}(binFilePath, streamer)

	return streamer
}
