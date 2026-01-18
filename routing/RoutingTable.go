package routing

import (
	"math/bits"
	"sort"
	"sync"
	"time"

	"github.com/SharefulNetworks/shareful-dht/types"
)

// RoutingTable - Models a Kademlia compiant routing table.
// It contains multiple K-Buckets, each of which in turn contain multiple peers;
// buckers are arranged according to their respective XOR distance from the local node.
type RoutingTable struct {
	self       types.NodeID
	buckets    []KBucket
	bucketSize int
	mu         sync.RWMutex
}

func NewRoutingTable(self types.NodeID, bucketSize int) *RoutingTable {
	if bucketSize <= 0 {
		bucketSize = 20 // a good default
	}
	rt := &RoutingTable{
		self:       self,
		buckets:    make([]KBucket, types.IDBits), // 160 buckets
		bucketSize: bucketSize,
	}
	return rt
}

// BucketFor returns the bucket index and a pointer to the bucket for a node id.
func (rt *RoutingTable) BucketFor(id types.NodeID) (int, *KBucket) {
	i := rt.bucketIndex(rt.self, id)
	if i < 0 {
		return -1, nil
	}
	return i, &rt.buckets[i]
}

// Update - Upserts (i.e Updates or Inserts) Peer in the correct bucket.
// Eviction strategy here: if bucket full, evict oldest at index 0
// (by shifting all entries to the left by one) then the new value is
// appended to the now vacant last index in the bucket.
func (rt *RoutingTable) Update(id types.NodeID, addr string) {
	i := rt.bucketIndex(rt.self, id)
	if i < 0 {
		return
	}

	rt.mu.Lock()
	defer rt.mu.Unlock()

	b := &rt.buckets[i]
	now := time.Now()

	// already present? refresh + move to end (most recently seen)
	for idx := range b.Peers {
		if b.Peers[idx].ID == id {
			if addr != "" && b.Peers[idx].Addr != addr {
				b.Peers[idx].Addr = addr
			}
			b.Peers[idx].LastSeen = now

			// move-to-end if needed
			if idx != len(b.Peers)-1 {
				c := b.Peers[idx]
				copy(b.Peers[idx:], b.Peers[idx+1:])
				b.Peers[len(b.Peers)-1] = c
			}
			return
		}
	}

	// conversely where the node is not already present, we look to add it providing
	//it has a valid address.
	if addr == "" {
		return
	}

	//where there is adequate capacity in the bucket, append the new peer.
	p := &Peer{ID: id, Addr: addr, LastSeen: now}
	if len(b.Peers) < rt.bucketSize {
		b.Peers = append(b.Peers, p)
		return
	}

	//otherwise where the bucket is full,  evict oldest (LRU) as per Kademlia spec.
	copy(b.Peers[0:], b.Peers[1:])
	b.Peers[len(b.Peers)-1] = p
}

// Remove - Explicitly removes the node with the sepcified id from this routing
//
//	table instance, where it exists. A boolean is then returned to indicate
//	whether or not the target entry was successfully located and expunged;
//	return TRUE where this is the case and FALSE otherwise.
func (rt *RoutingTable) Remove(id types.NodeID) bool {

	var removed bool = false

	i := rt.bucketIndex(rt.self, id)
	if i < 0 {
		return removed
	}

	rt.mu.Lock()
	defer rt.mu.Unlock()
	b := &rt.buckets[i]

	var targetRemovalIndex int = -1
	for idx, curPeer := range b.Peers {
		if curPeer.ID == id {
			targetRemovalIndex = idx
			removed = true
			break
		}
	}

	if targetRemovalIndex >= 0 {
		b.Remove(targetRemovalIndex)
	}

	return removed

}

// GetAddr lets Node.lookupAddrForId stay simple.
func (rt *RoutingTable) GetAddr(id types.NodeID) (string, bool) {
	i := rt.bucketIndex(rt.self, id)
	if i < 0 {
		return "", false
	}

	rt.mu.RLock()
	defer rt.mu.RUnlock()

	b := &rt.buckets[i]
	for _, c := range b.Peers {
		if c.ID == id {
			return c.Addr, true
		}
	}
	return "", false
}

func (rt *RoutingTable) ListKnownPeers() []Peer {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	peers := make([]Peer, 0, 64)
	for i := range rt.buckets {
		for _, p := range rt.buckets[i].Peers {
			peers = append(peers, *p)
		}
	}
	return peers
}

// Closest returns up to count Peers closest to target, across all buckets.
func (rt *RoutingTable) Closest(target types.NodeID, count int) []*Peer {
	rt.mu.RLock()
	all := make([]*Peer, 0, 64)
	for i := range rt.buckets {
		all = append(all, rt.buckets[i].Peers...)
	}
	rt.mu.RUnlock()

	sort.Slice(all, func(i, j int) bool {
		return rt.compareDistance(all[i].ID, all[j].ID, target) < 0
	})

	if count > len(all) {
		count = len(all)
	}
	return all[:count]
}

func (rt *RoutingTable) XORDistanceRank(self, other types.NodeID) int {
	idx := rt.bucketIndex(self, other)
	if idx < 0 { // same node
		return 0
	}
	return idx + 1
}

func (rt *RoutingTable) bucketIndex(self, other types.NodeID) int {
	d := rt.xor(self, other)

	// distance == 0?
	allZero := true
	for i := 0; i < types.IDBytes; i++ {
		if d[i] != 0 {
			allZero = false
			break
		}
	}
	if allZero {
		return -1
	}

	// count leading zeros in big-endian distance
	leading := 0
	for i := 0; i < types.IDBytes; i++ {
		if d[i] == 0 {
			leading += 8
			continue
		}
		leading += bits.LeadingZeros8(d[i])
		break
	}

	// highest set bit position == IDBits-1-leading
	return types.IDBits - 1 - leading
}

func (rt *RoutingTable) xor(a, b types.NodeID) (o types.NodeID) {
	for i := 0; i < types.IDBytes; i++ {
		o[i] = a[i] ^ b[i]
	}
	return
}

func (rt *RoutingTable) compareDistance(a, b, t types.NodeID) int {
	da := rt.xor(a, t)
	db := rt.xor(b, t)
	for i := 0; i < types.IDBytes; i++ {
		if da[i] < db[i] {
			return -1
		}
		if da[i] > db[i] {
			return 1
		}
	}
	return 0
}
