package netx

// Transport defines the interface that any transport
// mechanism must implement to be used by the DHT.
type Transport interface {
	Listen(addr string, handler MessageHandler) error
	Send(to string, data []byte) error
	Close() error
	CloseConnection(addr string) error
}

// MessageHandler is the type definition for the callback
// function that is invoked when a message is received
// via a Transport implementation.
type MessageHandler func(from string, data []byte)
