package routing

import "github.com/SharefulNetworks/shareful-dht/types"

type RoutingTableListener interface {

	//OnPeerUnhealthyStateChange is called when a peer is deemed unhealthy, where the peer's
	//cumulative connection failures exceeds the MaxNodeConnectionFailureThreshold
	OnPeerUnhealthyStateChange(peerId types.NodeID, peerAddr string)
	
}
