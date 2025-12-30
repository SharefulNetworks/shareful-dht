package types

import "encoding/hex"

const IDBytes = 20
const IDBits = IDBytes * 8

// NodeID - Represents a 160-bit Kademlia Node ID.
type NodeID [IDBytes]byte

func (id NodeID) String() string {
	return hex.EncodeToString(id[:])
}
