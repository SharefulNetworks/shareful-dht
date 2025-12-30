package routing

// KBucket - Models a single Kademlia K-Bucket.
type KBucket struct {
	Peers []*Peer
}

func (kb *KBucket) size() int {
	return len(kb.Peers)
}
