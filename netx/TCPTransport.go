package netx

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"net"
	"sync"
)

// TCPTransport - A TCP specific, Transport implementation.
type TCPTransport struct {
	ln       net.Listener
	conns    sync.Map
	closed   chan struct{}
	outQueue chan *Outbound
}

// Outbound - ecapsulates outbound message data.
type Outbound struct {
	to   string
	data []byte
}

func NewTCP() *TCPTransport {

	//instantiate new TCP transport
	newTCPTransport := &TCPTransport{
		closed:   make(chan struct{}),
		outQueue: make(chan *Outbound, 256),
	}

	//start outbound (message) processing
	newTCPTransport.startOutboundProcessing()

	return newTCPTransport
}

func (t *TCPTransport) Listen(addr string, handler MessageHandler) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	t.ln = ln
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				select {
				case <-t.closed:
					return
				default:
				}
				continue
			}
			go func(conn net.Conn) {
				defer conn.Close()
				r := bufio.NewReader(conn)
				for {
					lenb := make([]byte, 4)
					if _, err := io.ReadFull(r, lenb); err != nil {
						return
					}
					n := binary.BigEndian.Uint32(lenb)
					buf := make([]byte, n)
					if _, err := io.ReadFull(r, buf); err != nil {
						return
					}
					handler(conn.RemoteAddr().String(), buf)
				}
			}(c)
		}
	}()
	return nil
}

// Send - Queues the provided (message) data for async dispatch to the provided address and returns immediately.
func (t *TCPTransport) Send(to string, data []byte) error {
	return t.sendAsync(to, data)
}

func (t *TCPTransport) Close() error {
	close(t.closed)
	if t.ln != nil {
		_ = t.ln.Close()
	}
	t.conns.Range(func(_, v any) bool { v.(net.Conn).Close(); return true })
	return nil
}

//private helpers/utility funcions.

// sendSync sends provided data to the specified address, synchronously.
func (t *TCPTransport) sendSync(address string, data []byte) error {
	v, ok := t.conns.Load(address)
	var c net.Conn
	var err error
	if ok {
		c = v.(net.Conn)
	} else {
		c, err = net.Dial("tcp", address)
		if err != nil {
			return err
		}
		t.conns.Store(address, c)
	}
	w := bufio.NewWriter(c)
	lenb := make([]byte, 4)
	binary.BigEndian.PutUint32(lenb, uint32(len(data)))
	if _, err := w.Write(lenb); err != nil {
		return err
	}
	if _, err := w.Write(data); err != nil {
		return err
	}
	return w.Flush()

}

// sendAsync sends provided data to the specified address, asynchronously.
func (t *TCPTransport) sendAsync(to string, data []byte) error {
	select {
	case <-t.closed:
		return errors.New("transport closed")
	case t.outQueue <- &Outbound{to, data}:
		return nil
	default:
		//return fmt.Errorf("send queue full — applying backpressure")
		return nil
	}
}

func (t *TCPTransport) startOutboundProcessing() {
	//start worker goroutines
	for i := 0; i < 4; i++ {
		go t.outQueueDispatcher()
	}
}

func (t *TCPTransport) outQueueDispatcher() {
	for {
		select {
		case <-t.closed:
			return

		case job := <-t.outQueue:
			if err := t.sendSync(job.to, job.data); err != nil {
				log.Printf("Error sending to %s: %v", job.to, err)
			}
		}
	}
}
