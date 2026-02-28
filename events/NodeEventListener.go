package events

import "github.com/SharefulNetworks/shareful-dht/commons"

type NodeEventListener[T commons.RecordIndexEntryLike] interface {

	//OnIndexUpdated is called when the node receives an index update event, which occurs when a peer in the network updates an index entry that is relevant to the node. The event contains information about the updated index entries, the publisher of the update, and the publisher's address. This allows the node to react to changes in the network and to keep its local state up to date with the latest information about index entries and their associated peers.
	OnIndexUpdated(event IndexUpdateEvent[T])

	//OnMessageReceived(message dht.Message, senderId types.NodeID, senderAddress string)

}
