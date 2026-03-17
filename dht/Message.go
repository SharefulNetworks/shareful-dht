package dht

import (
	"github.com/SharefulNetworks/shareful-dht/commons"
	"github.com/SharefulNetworks/shareful-dht/config"
)

// Message models a single DHT message which may be exchanged between peers.
type Message struct {
	headers []MessageHeader
	body    []byte
}

// NewMessage - factory method, creates and returns a pointer to a new Message instance with the provided headers and body.
func NewMessage(headers []MessageHeader, body []byte) *Message {
	return &Message{
		headers: headers,
		body:    body,
	}
}

// NewDefaultMessage - factory method, creates and returns a pointer to a new Message instance
//
//	complete with default headers and the provided body.
func NewDefaultMessage(body []byte) *Message {
	defaultHeaders := generateDefaultHeaders()
	return NewMessage(defaultHeaders, body)
}

// NewDefaultStringMessage - factory method, creates and returns a pointer to a new string-based Message instance
//
//	complete with default headers and the provided body.
func NewDefaultStringMessage(body string) *Message {
	return NewDefaultMessage([]byte(body))
}

// AppendHeader - appends a header, with the provided key and value, to the Message's
// existing headers.
// Note: Whilst NOT mandatory its recommended that external application
//
//	related headers be prefix with "x-" e.g x-myapp-header to prevent pottential
//	conflicts with internal DHT protocol headers.
func (m *Message) AppendHeader(key string, value string) {
	m.headers = append(m.headers, MessageHeader{Key: key, Value: value})
}


func (m *Message) GetHeaders() []commons.MessageHeaderLike {
    out := make([]commons.MessageHeaderLike, len(m.headers))
    for i := range m.headers {
        out[i] = m.headers[i]
    }
    return out
}

func (m *Message) GetBody() []byte {
	return m.body
}

func (m *Message) GetBodyAsString() string {
	return string(m.body)
}

func (m *Message) SetBody(body []byte) {
	m.body = body
}

func (m *Message) SetBodyFromString(body string) {
	m.SetBody([]byte(body))
}

func generateDefaultHeaders() []MessageHeader {
	defaultHeaders := make([]MessageHeader, 0)
	defaultHeaders = append(defaultHeaders, MessageHeader{Key: "version", Value: config.GetDefaultSingletonInstance().MessageVersion})
	defaultHeaders = append(defaultHeaders, MessageHeader{Key: "host", Value: config.GetDefaultSingletonInstance().NodeAddress})
	defaultHeaders = append(defaultHeaders, MessageHeader{Key: "user-agent", Value: "shareful-core-node"}) //eventually this will be set from the config, valid options will initially be: core-node,core-bootstrap-node,community-node.
	return defaultHeaders
}

// MessageHeader models the header of a DHT message, which contains metadata about the message.
type MessageHeader struct {
	Key   string
	Value string
}

func (h MessageHeader) GetKey() string {
	return h.Key
}

func (h MessageHeader) GetValue() string {
	return h.Value
}
