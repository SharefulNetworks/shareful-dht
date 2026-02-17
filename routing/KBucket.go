package routing

import (
	"slices"
	"time"
)

// KBucket - Models a single Kademlia K-Bucket.
type KBucket struct {
	Peers []*Peer
	lastRefresh time.Time
}

//Size - Returns the number of peers in this bucket.
func (kb *KBucket) Size() int {
	return len(kb.Peers)
}

// Remove - Removes peer from this bucket at the specified index.
func (kb *KBucket) Remove(index int) bool {

	if index < 0 || index > len(kb.Peers) || len(kb.Peers) < 1 {
		return false
	}

	kb.Peers = slices.Delete(kb.Peers, index, index+1)
	return true
}

//Clear - Clears all peers from this bucket.
func (kb *KBucket) Clear() {
	kb.Peers = nil
}

// We compute the nodes last refresh time as the time of the most
// recent peer addition or update to this bucket, as this is the
// point at which we can be sure that the bucket was last active
// and thus up to date with the current state of the network.
// This is useful to determine when a bucket is due for a refresh,
// as per Kademlia spec.
func (kb *KBucket) ComputeLastRefreshTime() time.Time {
	if len(kb.Peers) < 1{
		return kb.lastRefresh
	}

	var lastRefreshTime time.Time
	for _, peer := range kb.Peers {
		if peer.LastSeen.After(lastRefreshTime) {
			lastRefreshTime = peer.LastSeen
		}
	}
	if lastRefreshTime.After(kb.lastRefresh){
         kb.lastRefresh = lastRefreshTime
	}
	return  kb.lastRefresh
}


//UpdateLastRefreshTime - Updates the buckets last refresh time to the current time.
func (kb *KBucket) UpdateLastRefreshTime(){
	kb.lastRefresh = time.Now()
}

func (kb *KBucket) GetLastRefreshTime()time.Time{
	return kb.lastRefresh;
}