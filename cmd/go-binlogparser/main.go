package main

import (
	"flag"
	"github.com/go-mysql-org/go-mysql/replication"
)

var name = flag.String("name", "", "binlog file name")
var offset = flag.Int64("offset", 0, "parse start offset")

func main() {
	flag.Parse()

	p := replication.NewBinlogParser()
	strem := replication.NewBinlogStreamer()
	f := func(e *replication.BinlogEvent) error {
		//e.Dump(os.Stdout)
		strem.AddEventToStreamer(e)
		return nil
	}

	err := p.ParseFile(*name, *offset, f)

	if err != nil {
		println(err.Error())
	}
}
