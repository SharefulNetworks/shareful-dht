package dht

import (
	"github.com/SharefulNetworks/shareful-dht/types"
	"sync"
	"time"
)

type Record struct {
	Key               string
	Value             []byte
	Expiry            time.Time
	IsIndex           bool
	Replicas          []string
	TTL               time.Duration
	Publisher         types.NodeID
	LocalRefreshCount uint64
	mu                sync.RWMutex
}
