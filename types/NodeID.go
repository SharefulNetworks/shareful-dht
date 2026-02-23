package types

import (
	"encoding/hex"
	"fmt"
)

const IDBytes = 20
const IDBits = IDBytes * 8

// NodeID - Represents a 160-bit Kademlia Node ID.
type NodeID [IDBytes]byte

func (id NodeID) String() string {
	return hex.EncodeToString(id[:])
}

func NodeIDFromBytes(b []byte) (NodeID, error) {
	if len(b) != IDBytes {
		return NodeID{}, fmt.Errorf("invalid NodeID length: got %d, want %d", len(b), IDBytes)
	}

	var id NodeID
	copy(id[:], b)
	return id, nil
}
