package routing

import "slices"

// KBucket - Models a single Kademlia K-Bucket.
type KBucket struct {
	Peers []*Peer
}

func (kb *KBucket) size() int {
	return len(kb.Peers)
}

//Remove - Removes peer from this bucket at the specified index.
func (kb *KBucket) Remove(index int) bool {

	if index < 0 || index > len(kb.Peers) || len(kb.Peers) < 1 {
		return false
	}

	kb.Peers = slices.Delete(kb.Peers, index, index+1)
	return true
}


