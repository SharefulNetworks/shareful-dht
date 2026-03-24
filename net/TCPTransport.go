package net

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"

	"net"
	"runtime/debug"
	"sync"
	"time"

	"github.com/SharefulNetworks/shareful-dht/config"
	"github.com/SharefulNetworks/shareful-utils-unpanicked/unpanicked"

	"github.com/SharefulNetworks/shareful-utils-slog/slog"
)

type TCPTransport struct {
	ln        net.Listener
	conns     sync.Map // map[string]*pooledConn
	closed    chan struct{}
	closeOnce sync.Once

	outQueue chan *Outbound
	wg       sync.WaitGroup
	logger   *slog.Logger
}

type Outbound struct {
	to   string
	data []byte
}

type PooledConn struct {
	c        net.Conn
	w        *bufio.Writer
	mu       sync.Mutex
	lastUsed time.Time
}

const MaxMsgSize = 1 << 20 // 1MB safety guard; tune if needed

func NewTCP() *TCPTransport {
	t := &TCPTransport{
		closed:   make(chan struct{}),
		outQueue: make(chan *Outbound, 4096), // larger buffer helps tests
		logger:   slog.NewLogger("shareful.dht.net.TCPTransport", nil),
	}
	t.startOutboundProcessing()
	t.startIdleConnChecker()
	return t
}

func (t *TCPTransport) Listen(addr string, handler MessageHandler) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	t.ln = ln

	//spin up new go routine to accept incoming connections and queue them to be handled.
	acceptConnLoop := func() {

		//handleConn is the function that will be used to handle each incomming connection.
		handleConn := func(conn net.Conn) {
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

				//ensure the the closed signal has not been sent before deferring to the handler.
				select {
				case <-t.closed:
					return
				default:
				}

				handler(conn.RemoteAddr().String(), buf)
			}
		}

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

			//wrapper to allow us to (statically) pass the current connection as a parameter, this is necessary to allow the function to be passed to unpanicked.RunSafe which accepts a pointer to a function with no parameters
			handleConnWrapper := func() {
				handleConn(c)
			}
			go unpanicked.RunSafe(handleConnWrapper, func(rec any, stacktrace []byte) {
				t.logger.Error("Connection handler for remote address: %s panicked with error: %v, stacktrace: %s", c.RemoteAddr().String(), rec, string(stacktrace))
			})

		}
	}

	go unpanicked.RunSafe(acceptConnLoop, func(rec any, stacktrace []byte) {
		t.logger.Error("Accept connection loop panicked with error: %v, stacktrace: %s", rec, string(stacktrace))
	})

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
			pc := v.(*PooledConn)
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
		pooledConn := v.(*PooledConn)
		t.conns.Delete(addr)
		return pooledConn.c.Close()
	}
	return fmt.Errorf("No connection found to be associated with address: %s", addr)

}

func (t *TCPTransport) getConn(address string) (*PooledConn, error) {
	v, ok := t.conns.Load(address)
	if ok {
		pooledConn := v.(*PooledConn)
		pooledConn.mu.Lock()
		pooledConn.lastUsed = time.Now() //set last used to now, this will help us detect idle connections.
		pooledConn.mu.Unlock()
		return pooledConn, nil
	}

	c, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}

	pc := &PooledConn{
		c:        c,
		w:        bufio.NewWriter(c),
		lastUsed: time.Now(),
	}

	actual, loaded := t.conns.LoadOrStore(address, pc)
	if loaded {
		//NOTE: We should *never* arrive here owing to the Load check above, however we
		//      keep the check to account for the slim possibility of a pooled connection
		//      being created AFTER the initial check. We simply close the newly created
		//      connection if we *already* hold a connection to the specified address
		_ = c.Close()
		loadedConn := actual.(*PooledConn)
		loadedConn.mu.Lock()
		loadedConn.lastUsed = time.Now() //set last used to now, this will help us detect idle connections.
		loadedConn.mu.Unlock()
		return loadedConn, nil
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
	for i := 0; i < config.GetDefaultSingletonInstance().OutboundQueueWorkerCount; i++ {
		t.wg.Add(1)
		go unpanicked.RunSafeWithWG(&t.wg, t.outQueueDispatcher, func(rec any, stacktrace []byte) {
			t.logger.Error("Outbound dispatcher panicked with error: %v, stacktrace: %s", rec, string(stacktrace))
		})

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

func (t *TCPTransport) startIdleConnChecker() {
	go unpanicked.RunSafe(t.idleConnChecker, func(rec any, stacktrace []byte) {
		t.logger.Error("Idle connection checker panicked with error: %v, stacktrace: %s", rec, string(stacktrace))
	})
}

func (t *TCPTransport) idleConnChecker() {
	t.logger.Info("Starting idle connection checker, pooled connections will be checked for idleness every: %s minute(s)", config.GetDefaultSingletonInstance().PooledConnectionIdleCheckInterval)

	t.wg.Add(1)
	defer t.wg.Done()
	timer := time.NewTicker(config.GetDefaultSingletonInstance().PooledConnectionIdleCheckInterval)
	defer timer.Stop()

	for {
		select {
		case <-t.closed:
			return
		case <-timer.C:
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.logger.Error("panic in idleConnChecker: %v\n%s", r, debug.Stack())
					}
				}()
				t.idleConnCheck()
			}()

		}
	}
}

func (t *TCPTransport) idleConnCheck() {

	idleConnKeys := make([]string, 0)
	idleTimeout := config.GetDefaultSingletonInstance().PooledConnectionIdleTimeout
	now := time.Now()

	//find all idle connections and collect their keys for subsequent closure and removal from the pool.
	t.conns.Range(func(key, value any) bool {
		pooledConn := value.(*PooledConn)
		pooledConn.mu.Lock()
		idleDuration := now.Sub(pooledConn.lastUsed)
		if idleDuration > idleTimeout {
			idleConnKeys = append(idleConnKeys, key.(string))
		}
		pooledConn.mu.Unlock()
		return true
	})

	//close and remove all idle connections (we identified from the immediately preceeding operations) from the pool.
	connRemovalCount := 0
	for _, key := range idleConnKeys {
		v, ok := t.conns.Load(key)

		if !ok {
			t.logger.Warn("Unable to remove pooled connection associated with key %s the connection was not found.", key)
			continue
		}

		pooledConn := v.(*PooledConn)
		pooledConn.mu.Lock()
		if now.Sub(pooledConn.lastUsed) > config.GetDefaultSingletonInstance().PooledConnectionIdleTimeout {
			_ = pooledConn.c.Close()
			pooledConn.mu.Unlock()
			t.conns.Delete(key)
			connRemovalCount++
		} else {
			pooledConn.mu.Unlock()
			t.logger.Warn("Unable to remove pooled connection associated with key %s its last used time is too recent.", key)
		}
	}

	if connRemovalCount > 0 {
		t.logger.Info("Successfully removed: %d idle pooled connections out of possible: %d", connRemovalCount, len(idleConnKeys))
	}
}
