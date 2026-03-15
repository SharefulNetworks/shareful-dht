package routing

import (
	"time"

	"github.com/SharefulNetworks/shareful-dht/config"
	"github.com/SharefulNetworks/shareful-dht/types"
)

// Peer - Represents a known peer in the DHT network.
type Peer struct {
	ID                           types.NodeID
	Addr                         string
	LastSeen                     time.Time
	cumulativeConnectionFailures int //the total number of consecutive connection failures to this peer, where this value exceeds the MaxNodeConnectionFailureThreshold defined in the config an event is published to signal the peer's removal from the routing table.
	healthy                      bool
}

func (p *Peer) MarkConnectionFailure() {
	p.cumulativeConnectionFailures++
	if p.healthy && p.cumulativeConnectionFailures >= config.GetDefaultSingletonInstance().MaxNodeConnectionFailureThreshold {
		p.healthy = false
	}
}

func (p *Peer) MarkConnectionSuccess() {
	p.cumulativeConnectionFailures = 0
	if !p.healthy {
		p.healthy = true
	}
}

func (p *Peer) GetCumulativeConnectionFailures() int {
	return p.cumulativeConnectionFailures
}

func (p *Peer) IsHealthy() bool {
	return p.healthy
}
