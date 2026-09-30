// Package recoverymock supplies a deterministic, local-only MySQL protocol
// source for recovery tests. It never connects to an external database.
package recoverymock

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"
)

type Dump struct {
	Position       mysql.Position
	NonBlock       bool
	SourceServerID uint32
}
type Reply struct {
	Events     [][]byte
	Err        error
	Wait       bool
	More       <-chan [][]byte
	Disconnect bool
}
type Source struct {
	Addr     string
	Dump     func(Dump) Reply
	mu       sync.Mutex
	node     uint32
	logs     []string
	conns    []net.Conn
	listener net.Listener
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

func New(node uint32, logs []string, dump func(Dump) Reply) (*Source, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Source{Addr: ln.Addr().String(), Dump: dump, node: node, logs: logs, listener: ln, ctx: ctx, cancel: cancel}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			raw, err := ln.Accept()
			if err != nil {
				return
			}
			s.mu.Lock()
			s.conns = append(s.conns, raw)
			node := s.node
			s.mu.Unlock()
			s.wg.Add(1)
			go func() { defer s.wg.Done(); defer raw.Close(); s.serve(raw, node) }()
		}
	}()
	return s, nil
}
func (s *Source) SetNode(node uint32) { s.mu.Lock(); s.node = node; s.mu.Unlock() }
func (s *Source) Close() {
	s.cancel()
	_ = s.listener.Close()
	s.mu.Lock()
	for _, conn := range s.conns {
		_ = conn.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
}
func result(names []string, rows [][]any) *mysql.Result {
	rs, err := mysql.BuildSimpleResultset(names, rows, false)
	if err != nil {
		panic(err)
	}
	return &mysql.Result{Resultset: rs}
}
func (s *Source) serve(raw net.Conn, node uint32) {
	c, err := server.NewConn(raw, "test", "", &server.EmptyHandler{})
	if err != nil {
		return
	}
	for {
		data, err := c.ReadPacket()
		if err != nil || len(data) == 0 {
			return
		}
		switch data[0] {
		case mysql.COM_QUIT:
			return
		case mysql.COM_REGISTER_SLAVE:
			err = c.WriteValue(nil)
		case mysql.COM_QUERY:
			q := strings.ToLower(string(data[1:]))
			var r *mysql.Result
			switch {
			case strings.Contains(q, "server_id"):
				r = result([]string{"Variable_name", "Value"}, [][]any{{"server_id", fmt.Sprint(node)}})
			case strings.Contains(q, "binlog_checksum"):
				r = result([]string{"Variable_name", "Value"}, [][]any{{"BINLOG_CHECKSUM", "NONE"}})
			case strings.Contains(q, "binlog_format"):
				r = result([]string{"Variable_name", "Value"}, [][]any{{"binlog_format", "ROW"}})
			case strings.Contains(q, "binlog_row_image"):
				r = result([]string{"Variable_name", "Value"}, [][]any{{"binlog_row_image", "FULL"}})
			case strings.Contains(q, "version"):
				r = result([]string{"version()"}, [][]any{{"5.7.44"}})
			case strings.Contains(q, "show binary logs"):
				s.mu.Lock()
				rows := make([][]any, len(s.logs))
				for i, n := range s.logs {
					rows[i] = []any{n, 1024}
				}
				s.mu.Unlock()
				r = result([]string{"Log_name", "File_size"}, rows)
			default:
				r = &mysql.Result{}
			}
			err = c.WriteValue(r)
		case mysql.COM_BINLOG_DUMP:
			if len(data) < 11 {
				return
			}
			d := Dump{Position: mysql.Position{Name: string(data[11:]), Pos: binary.LittleEndian.Uint32(data[1:5])}, NonBlock: binary.LittleEndian.Uint16(data[5:7])&1 != 0, SourceServerID: node}
			reply := s.Dump(d)
			if reply.Err != nil {
				err = c.WriteValue(reply.Err)
				break
			}
			write := func(events [][]byte) error {
				for _, e := range events {
					packet := append(make([]byte, 5), e...)
					if err := c.WritePacket(packet); err != nil {
						return err
					}
				}
				return nil
			}
			if err = write(reply.Events); err != nil {
				return
			}
			if reply.Disconnect {
				return
			}
			if reply.More != nil {
				select {
				case more := <-reply.More:
					if err = write(more); err != nil {
						return
					}
				case <-s.ctx.Done():
					return
				}
			}
			if reply.Wait {
				<-s.ctx.Done()
				return
			}
			if err = c.WritePacket([]byte{0, 0, 0, 0, mysql.EOF_HEADER, 0, 0, 0, 0}); err != nil {
				return
			}
		default:
			err = c.WriteValue(nil)
		}
		if err != nil {
			return
		}
		c.ResetSequence()
	}
}

// Builder generates real parseable ROW-format binlog events, including table
// metadata, event offsets and transaction commits (not header-only fixtures).
type Builder struct {
	Events [][]byte
	Pos    uint32
	Writer uint32
}

func NewBuilder(writer uint32) *Builder {
	b := &Builder{Pos: 4, Writer: writer}
	body := make([]byte, 57+40)
	binary.LittleEndian.PutUint16(body, 4)
	copy(body[2:52], "5.5.0")
	body[56] = 19
	body[57+int(2)-1] = 13
	body[57+int(4)-1] = 8
	body[57+int(19)-1] = 8
	body[57+int(23)-1] = 8
	b.Add(15, 1, body)
	return b
}
func (b *Builder) Add(kind byte, ts uint32, body []byte) {
	raw := make([]byte, 19+len(body))
	binary.LittleEndian.PutUint32(raw, ts)
	raw[4] = kind
	binary.LittleEndian.PutUint32(raw[5:9], b.Writer)
	binary.LittleEndian.PutUint32(raw[9:13], uint32(len(raw)))
	b.Pos += uint32(len(raw))
	binary.LittleEndian.PutUint32(raw[13:17], b.Pos)
	copy(raw[19:], body)
	b.Events = append(b.Events, raw)
}
func (b *Builder) Query(ts uint32, q string) {
	body := make([]byte, 13)
	body = append(body, 0)
	body = append(body, q...)
	b.Add(2, ts, body)
}
func (b *Builder) Row(ts uint32, id, value int32) {
	table := []byte{1, 0, 0, 0, 0, 0, 0, 0, 4, 't', 'e', 's', 't', 0, 8, 'r', 'e', 'c', 'o', 'v', 'e', 'r', 'y', 0, 2, 3, 3, 0, 0}
	b.Add(19, ts, table)
	row := []byte{1, 0, 0, 0, 0, 0, 0, 0, 2, 3, 0}
	row = binary.LittleEndian.AppendUint32(row, uint32(id))
	row = binary.LittleEndian.AppendUint32(row, uint32(value))
	b.Add(23, ts, row)
}
func (b *Builder) Commit(ts uint32) { b.Add(16, ts, make([]byte, 8)) }
func (b *Builder) Transaction(rowTS, commitTS uint32, id, value int32) {
	b.Query(rowTS, "BEGIN")
	b.Row(rowTS, id, value)
	b.Commit(commitTS)
}
func (b *Builder) Rotate(ts uint32, name string) {
	body := binary.LittleEndian.AppendUint64(nil, 4)
	body = append(body, name...)
	b.Add(4, ts, body)
}
func (b *Builder) Bytes() []byte {
	data := []byte{0xfe, 'b', 'i', 'n'}
	for _, e := range b.Events {
		data = append(data, e...)
	}
	return data
}
