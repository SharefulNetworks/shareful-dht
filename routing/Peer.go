package routing

import (
	"time"
	"github.com/SharefulNetworks/shareful-dht/types"
)

// Peer - Represents a known peer in the DHT network.
type Peer struct {
	ID       types.NodeID
	Addr     string
	LastSeen time.Time
}
