package netx

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"

)

type TCPTransport struct {
	ln        net.Listener
	conns     sync.Map // map[string]*pooledConn
	closed    chan struct{}
	closeOnce sync.Once

	outQueue chan *Outbound
	wg       sync.WaitGroup
}

type Outbound struct {
	to   string
	data []byte
}

type pooledConn struct {
	c  net.Conn
	w  *bufio.Writer
	mu sync.Mutex
}

const MaxMsgSize = 1 << 20 // 1MB safety guard; tune if needed

func NewTCP() *TCPTransport {
	t := &TCPTransport{
		closed:   make(chan struct{}),
		outQueue: make(chan *Outbound, 4096), // larger buffer helps tests
	}
	t.startOutboundProcessing()
	return t
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
					if n == 0 || n > MaxMsgSize {
						return
					}

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

func (t *TCPTransport) Send(to string, data []byte) error {
	return t.sendAsync(to, data)
}

func (t *TCPTransport) Close() error {
	t.closeOnce.Do(func() {
		close(t.closed)

		if t.ln != nil {
			_ = t.ln.Close()
		}

		// Close pooled outbound conns
		t.conns.Range(func(_, v any) bool {
			pc := v.(*pooledConn)
			_ = pc.c.Close()
			return true
		})

		// Wait for dispatchers to exit (they exit on <-t.closed)
		t.wg.Wait()
	})
	return nil
}

// CloseConnection - Closes the connection with the specified address where such
//
//	a connection exists. Returns TRUE where the connection exists
//	and was successfully closed or FALSE otherwise.
func (t *TCPTransport) CloseConnection(addr string) error {

	v, ok := t.conns.Load(addr)
	if ok {
		pooledConn := v.(*pooledConn)
		t.conns.Delete(addr)
		return pooledConn.c.Close()
	}
	return fmt.Errorf("No connection found to be associated with address: %s", addr)

}

func (t *TCPTransport) getConn(address string) (*pooledConn, error) {
	v, ok := t.conns.Load(address)
	if ok {
		return v.(*pooledConn), nil
	}

	c, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}

	pc := &pooledConn{
		c: c,
		w: bufio.NewWriter(c),
	}

	actual, loaded := t.conns.LoadOrStore(address, pc)
	if loaded {
		_ = c.Close()
		return actual.(*pooledConn), nil
	}

	return pc, nil
}

func (t *TCPTransport) sendSync(address string, data []byte) error {
	pc, err := t.getConn(address)
	if err != nil {
		return err
	}

	pc.mu.Lock()
	defer pc.mu.Unlock()

	lenb := make([]byte, 4)
	binary.BigEndian.PutUint32(lenb, uint32(len(data)))

	if _, err := pc.w.Write(lenb); err != nil {
		_ = pc.c.Close()
		t.conns.Delete(address)
		return err
	}
	if _, err := pc.w.Write(data); err != nil {
		_ = pc.c.Close()
		t.conns.Delete(address)
		return err
	}
	if err := pc.w.Flush(); err != nil {
		_ = pc.c.Close()
		t.conns.Delete(address)
		return err
	}

	return nil
}

// This version applies backpressure: it blocks until the job is enqueued or transport closes.
// This is the most reliable approach for unit tests (no dropped messages).
func (t *TCPTransport) sendAsync(to string, data []byte) error {
	// Fast check if transport is closed
	select {
	case <-t.closed:
		return errors.New("transport closed")
	default:
	}

	job := &Outbound{to: to, data: data}

	// Attempt to enqueue the job
	select {
	case <-t.closed:
		return errors.New("transport closed")
	case t.outQueue <- job:
		return nil
	default:
		//add log here before returning.
		return errors.New("unable to queue message to be sent, the queue is full; message will be dropped.")
	}
}

func (t *TCPTransport) startOutboundProcessing() {
	for i := 0; i < 4; i++ {
		t.wg.Add(1)
		go func() {
			defer t.wg.Done()
			t.outQueueDispatcher()
		}()
	}
}

func (t *TCPTransport) outQueueDispatcher() {
	for {
		select {
		case <-t.closed:
			return

		case job := <-t.outQueue:
			if job == nil {
				continue
			}
			if err := t.sendSync(job.to, job.data); err != nil {
				log.Printf("Error sending to %s: %v", job.to, err)
			}
		}
	}
}

/*
func  (t *TCPTransport) idleConnChecker() {
	t.wg.Add(1)
	defer t.wg.Done()
	timer := time.NewTicker(config.Config.PooledConnectionIdleTimeout)
	defer timer.Stop()

	for {
		select {
		case <-t.closed:
			return
		case <-timer.C:
			func() {
				defer func() {
					if r := recover(); r != nil {
						fmt.Println("Recovered from panic in refresher tick:", r)
					}
				}()
				n.refresh()
			}()

		}
	}
}
	*/
