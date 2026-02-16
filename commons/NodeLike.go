package commons

import "github.com/SharefulNetworks/shareful-dht/types"

// NodeLike - Provides some subset of the public interface methods exported by
// the dht.Node struct. its principally used to allow subsystems to call Node methods
// without having to import it's parent dht package, which may well result in
// circular dependency issues, on account of the top-level Node struct's import of
// the majority of subsystem packages.
type NodeLike interface {

	//Find - Attempts to lookup value with the provided key in the DHT.
	Find(key string) ([]byte, bool)

	//FindRaw - Attempts to look value associated with the provided raw NodeID Key in the DHT
	FindRaw(key types.NodeID) ([]byte, bool)

}
