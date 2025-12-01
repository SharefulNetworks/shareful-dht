package netx

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
	"sync"
)

type MessageHandler func(from string, data []byte)

type TransportProvider interface {
	Listen(addr string, handler MessageHandler) error
	Send(to string, data []byte) error
	Close() error
}

type TCPTransport struct {
	ln net.Listener
	conns sync.Map
	closed chan struct{}
}

func NewTCP() *TCPTransport { return &TCPTransport{closed: make(chan struct{})} }

func (t *TCPTransport) Listen(addr string, handler MessageHandler) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil { return err }
	t.ln = ln
	go func(){
		for {
			c, err := ln.Accept()
			if err != nil { select{ case <-t.closed: return; default: } ; continue }
			go func(conn net.Conn){
				defer conn.Close()
				r := bufio.NewReader(conn)
				for {
					lenb := make([]byte, 4)
					if _, err := io.ReadFull(r, lenb); err != nil { return }
					n := binary.BigEndian.Uint32(lenb)
					buf := make([]byte, n)
					if _, err := io.ReadFull(r, buf); err != nil { return }
					handler(conn.RemoteAddr().String(), buf)
				}
			}(c)
		}
	}()
	return nil
}

func (t *TCPTransport) Send(to string, data []byte) error {
	v, ok := t.conns.Load(to)
	var c net.Conn
	var err error
	if ok { c = v.(net.Conn) } else {
		c, err = net.Dial("tcp", to)
		if err != nil { return err }
		t.conns.Store(to, c)
	}
	w := bufio.NewWriter(c)
	lenb := make([]byte, 4)
	binary.BigEndian.PutUint32(lenb, uint32(len(data)))
	if _, err := w.Write(lenb); err != nil { return err }
	if _, err := w.Write(data); err != nil { return err }
	return w.Flush()
}

func (t *TCPTransport) Close() error {
	close(t.closed)
	if t.ln != nil { _ = t.ln.Close() }
	t.conns.Range(func(_, v any) bool { v.(net.Conn).Close(); return true })
	return nil
}
