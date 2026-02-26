package routing

import (
	"fmt"
	"math/bits"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/SharefulNetworks/shareful-dht/commons"
	"github.com/SharefulNetworks/shareful-dht/config"
	"github.com/SharefulNetworks/shareful-dht/types"
)

// RoutingTable - Models a Kademlia compiant routing table.
// It contains multiple K-Buckets, each of which in turn contain multiple peers;
// buckers are arranged according to their respective XOR distance from the local node.
type RoutingTable struct {
	self       types.NodeID
	buckets    []KBucket
	bucketSize int
	nodeLike   commons.NodeLike
	mu         sync.RWMutex
	wg         sync.WaitGroup
	stop       chan struct{}
}

func NewRoutingTable(self types.NodeID, bucketSize int, nodeLike commons.NodeLike) *RoutingTable {
	if bucketSize <= 0 {
		bucketSize = 20 // a good default
	}
	rt := &RoutingTable{
		self:       self,
		buckets:    make([]KBucket, types.IDBits), // 160 buckets
		bucketSize: bucketSize,
		stop:       make(chan struct{}),
		nodeLike:   nodeLike,
	}
	rt.startBucketRefresher()
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
	//fmt.Printf("\nNode: %s adding peer with ID: %s and address: %s to routing table. The call graph that lead to this call was: %s \n", rt.nodeLike.GetAddress(), id.String(), addr, debug.Stack())
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
		removed = b.Remove(targetRemovalIndex)
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

// Destroy - Gracefully shuts down the routing table, ensuring that
// any background goroutines are properly terminated and resources are released.
func (rt *RoutingTable) Destroy() {

	//first stop any async tasks from executing
	close(rt.stop)
	rt.wg.Wait()

	//clear all buckets.
	for _, bucket := range rt.buckets {
		bucket.Clear()
	}
	rt.buckets = nil
}

// ListBuckets - Returns a list of the routing table's current buckets.
func (rt *RoutingTable) ListBuckets() []KBucket {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return rt.buckets
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

func (rt *RoutingTable) startBucketRefresher() {
	fmt.Printf("\nINFO: Starting up periodic RoutingTable, bucket refresh process. The process will run every: %f Minutes\n", config.GetDefaultSingletonInstance().BucketRefreshInterval.Minutes())
	go rt.bucketRefresher()
}

// bucketRefresher - Periodically refreshes buckets in the routing table at intervals specified
// by the BucketRefreshInterval configuration parameter. This is useful to ensure that the
// routing table is kept up to date with the current state of the network and to prevent
// stale entries from accumulating in the routing table over time. To facilitate this refresh
// activity a find operation for a randomly generated id that falls within each buckets
// respective key space is undertaken, whilst the requests WILL fail they will force the node
// to discover any new peers that have joined the network, in the process, since the the last time the refresh
// op was ran. The refresh will result in exactly 160 distinct requests (one for each bucket)
// and thus care is taken to ensure that they are staggered, so as to not overwhelm the network
// or indeed the node itself.
func (rt *RoutingTable) bucketRefresher() {
	rt.wg.Add(1)
	defer rt.wg.Done()
	t := time.NewTicker(config.GetDefaultSingletonInstance().BucketRefreshInterval)
	defer t.Stop()

	for {
		select {
		case <-rt.stop:
			return
		case <-t.C:
			func() {
				defer func() {
					if r := recover(); r != nil {
						fmt.Println("Recovered from panic in refresher tick:", r)
					}
				}()
				rt.refreshBuckets()
			}()

		}
	}
}

func (rt *RoutingTable) refreshBuckets() {

	//first find all buckets that have not been updated since the now and
	//the last refresh interval.
	var refreshBatchSize = config.GetDefaultSingletonInstance().BucketRefreshBatchSize

	//local helper function, returns an array of bucket refresh jobsthat corresonds to the buckets
	//that are required to be refreshed.
	computeBucketRefreshJobs := func() []BucketRefreshJob {
		var bucketsToRefresh []BucketRefreshJob
		now := time.Now()
		for i := range rt.buckets {
			bucket := &rt.buckets[i]
			lastRefreshTime := bucket.ComputeLastRefreshTime()
			if now.Sub(lastRefreshTime) >= config.GetDefaultSingletonInstance().BucketRefreshInterval {
				bucketsToRefresh = append(bucketsToRefresh, BucketRefreshJob{
					Bucket:   bucket,
					RandomID: rt.randomIDInBucketRange(rt.self, i),
				})
			}
		}
		return bucketsToRefresh
	}

	//then for each bucket that is due for a refresh, we generate a random id that falls within the respective
	//key space of the bucket and then we undertake a find operation for that id. This will result in the node
	//discovering any new peers that have joined the network since the last refresh operation was ran.
	bucketRefreshJobs := computeBucketRefreshJobs()

	for len(bucketRefreshJobs) > 0 {
		fmt.Printf("\nProcessing: %d bucket refresh jobs\n", len(bucketRefreshJobs))
		// Process refresh jobs in batches
		batchSize := refreshBatchSize
		if len(bucketRefreshJobs) < batchSize {
			batchSize = len(bucketRefreshJobs)
		}

		for i := 0; i < batchSize; i++ {
			job := bucketRefreshJobs[i]
			jobBucketLastRefreshBeforeFind := job.Bucket.GetLastRefreshTime()
			rt.nodeLike.FindRaw(job.RandomID)   //will be a random id within the keyspace range of the bucket.
			job.Bucket.ComputeLastRefreshTime() //recompute the buckets last refresh post the find operation.
			jobBucketLastRefreshAfterFind := job.Bucket.GetLastRefreshTime()
			//if not new nodes were added as a result of the find operation, we still update
			//the buckets last refresh time to now.
			if jobBucketLastRefreshBeforeFind.Equal(jobBucketLastRefreshAfterFind) {
				job.Bucket.UpdateLastRefreshTime()
			}

		}

		// Remove processed jobs from the list
		//bucketRefreshJobs = bucketRefreshJobs[batchSize:]

		//recompute the bucket refresh jobs as some pending buckets may have been updated
		//by this interations find operations.
		bucketRefreshJobs = computeBucketRefreshJobs()

		// Sleep briefly between batches to avoid overwhelming the network or the node itself.
		if len(bucketRefreshJobs) > 0 {
			time.Sleep(config.GetDefaultSingletonInstance().BucketRefreshBatchDelayInterval)
		}
	}

}

func (rt *RoutingTable) randomIDInBucketRange(self types.NodeID, bucketIndex int) types.NodeID {
	var id types.NodeID
	copy(id[:], self[:])

	byteIndex := bucketIndex / 8
	bitIndex := bucketIndex % 8
	mask := byte(1 << (7 - bitIndex))

	// 1. Flip the bucket bit (this guarantees XOR distance ∈ [2^i, 2^(i+1)))
	id[byteIndex] ^= mask

	// 2. Randomise bits *after* the bucket bit in the same byte
	id[byteIndex] &= ^(mask - 1)
	id[byteIndex] |= byte(rand.Intn(1 << (7 - bitIndex)))

	// 3. Randomise all following bytes
	for i := byteIndex + 1; i < types.IDBytes; i++ {
		id[i] = byte(rand.Intn(256))
	}

	return id
}

// BucketRefreshJob - Models a single bucket refresh job.
type BucketRefreshJob struct {
	Bucket   *KBucket
	RandomID types.NodeID
}
