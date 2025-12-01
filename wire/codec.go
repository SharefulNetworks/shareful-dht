package wire

import (
	"encoding/json"
	"fmt"

	"example.com/minidht/proto/dhtpb"
	"google.golang.org/protobuf/proto"
)

// Codec defines the interface for encoding/decoding DHT envelopes.
type Codec interface {
	// Wrap wraps the given payload in a transport envelope.
	// "from" is the sender identity (typically hex-encoded NodeID).
	Wrap(op int, reqID uint64, isResp bool, from string, payload []byte) ([]byte, error)

	// Unwrap decodes a transport envelope into its components.
	// It returns the operation, request id, response flag, sender identity and payload.
	Unwrap(b []byte) (op int, reqID uint64, isResp bool, from string, payload []byte, err error)
}

// ---------------- JSON codec ----------------

type JSONCodec struct{}

type jsonEnvelope struct {
	Op         int             `json:"op"`
	ReqID      uint64          `json:"req_id"`
	IsResponse bool            `json:"is_response"`
	From       string          `json:"from_id"`
	Payload    json.RawMessage `json:"payload"`
}

func (JSONCodec) Wrap(op int, reqID uint64, isResp bool, from string, payload []byte) ([]byte, error) {
	env := jsonEnvelope{
		Op:         op,
		ReqID:      reqID,
		IsResponse: isResp,
		From:       from,
		Payload:    payload,
	}
	return json.Marshal(&env)
}

func (JSONCodec) Unwrap(b []byte) (int, uint64, bool, string, []byte, error) {
	var env jsonEnvelope
	if err := json.Unmarshal(b, &env); err != nil {
		return 0, 0, false, "", nil, err
	}
	return env.Op, env.ReqID, env.IsResponse, env.From, env.Payload, nil
}

// ---------------- Protobuf codec ----------------

type ProtobufCodec struct{}

func (ProtobufCodec) Wrap(op int, reqID uint64, isResp bool, from string, payload []byte) ([]byte, error) {
	env := &dhtpb.Envelope{
		Op:         dhtpb.Op(op),
		ReqId:      reqID,
		IsResponse: isResp,
		Payload:    payload,
		FromId:     []byte(from), // assumes from_id field exists in proto
	}
	return proto.Marshal(env)
}

func (ProtobufCodec) Unwrap(b []byte) (int, uint64, bool, string, []byte, error) {
	var env dhtpb.Envelope
	if err := proto.Unmarshal(b, &env); err != nil {
		return 0, 0, false, "", nil, fmt.Errorf("protobuf unmarshal: %w", err)
	}
	from := string(env.FromId)
	return int(env.Op), env.ReqId, env.IsResponse, from, env.Payload, nil
}
