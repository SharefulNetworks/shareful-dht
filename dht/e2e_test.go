package dht

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/SharefulNetworks/shareful-dht/config"
	"github.com/SharefulNetworks/shareful-dht/netx"
	"github.com/SharefulNetworks/shareful-dht/types"
)

/*****************************************************************************************************************
 *                                             CORE E2E TESTS
 *
 * THE BELOW TESTS ARE INTENDED TO VALIDATE THE CORE FUNCTIONALITY OF THE DHT IN A
 * SIMPLIFIED, REDUCED TEST NETWORK ENVIRONMENT.
 ******************************************************************************************************************/

func Test_Create_And_Find_Standard_Entry_Value(t *testing.T) {

	//create new default test context
	ctx := NewDefaultTestContext(t)

	//obtain nodes from the test context
	n1 := ctx.Nodes[0]
	n2 := ctx.Nodes[1]

	//store an STANDARD entry to the DHT via node 1.
	peer1StoreErr := n1.Store("alpha", []byte("v"))
	if peer1StoreErr != nil {
		t.Fatal("Error occurred whilst Peer Node 1 was trying to store entry:", peer1StoreErr)
	}

	//after a short delay attempt to retrieve the data FROM the DHT, via node 2.
	time.Sleep(1000 * time.Millisecond)
	if v, ok := n2.Find("alpha"); !ok || string(v) != "v" {
		t.Fatalf("FindRemote failed %q", string(v))
	}
}

func Test_Create_And_Find_Index_Entry_Value(t *testing.T) {

	//create new default test context
	ctx := NewDefaultTestContext(t)

	//obtain nodes from the test context
	n1 := ctx.Nodes[0]
	n2 := ctx.Nodes[1]

	//store entries from both nodes under the same key
	key := "leaf/x"
	peer1StoreIndexErr := n1.StoreIndex(key, IndexEntry{Source: key, Target: "super/" + n1.ID.String(), UpdatedUnix: time.Now().UnixNano()})
	if peer1StoreIndexErr != nil {
		t.Fatalf("Error occurred whilst Peer Node 1 was trying to store index entry: %v", peer1StoreIndexErr)
	}

	peer2IndexIndexStoreErr := n2.StoreIndex(key, IndexEntry{Source: key, Target: "super/" + n2.ID.String(), UpdatedUnix: time.Now().UnixNano()})
	if peer2IndexIndexStoreErr != nil {
		t.Fatalf("Error occurred whilst Peer Node 2 was trying to store index entry: %v", peer2IndexIndexStoreErr)
	}

	//after a short delay attempt to retrieve the merged index FROM the DHT, via node 1 and node 2
	time.Sleep(3000 * time.Millisecond)
	if ents, ok := n1.FindIndex(key); !ok || len(ents) < 2 {
		t.Fatalf("expected merged index >=2")
	}
	time.Sleep(4000 * time.Millisecond)
	if ents, ok := n2.FindIndex(key); !ok || len(ents) < 2 {
		t.Fatalf("expected merged index >=2")

	}
}

func Test_Create_And_Delete_Standard_Entry_Value(t *testing.T) {

	//create new default test context
	ctx := NewDefaultTestContext(t)

	//obtain nodes from the test context
	n1 := ctx.Nodes[0]
	n2 := ctx.Nodes[1]

	//store an STANDARD entry to the DHT via node 1.
	peer1StoreErr := n1.Store("alpha", []byte("v"))
	if peer1StoreErr != nil {
		t.Fatal("Error occurred whilst Peer Node 1 was trying to store entry:", peer1StoreErr)
	}

	//after a short delay attempt to retrieve the data FROM the DHT, via node 2.
	time.Sleep(1000 * time.Millisecond)
	if v, ok := n2.Find("alpha"); !ok || string(v) != "v" {
		t.Fatalf("FindRemote failed %q", string(v))
	}

	//next explicitly delete the entry via node 1
	if err := n1.Delete("alpha"); err != nil {
		t.Fatal("Error occurred whilst deleting entry 'alpha':", err)
	}

	//after a short delay attempt to retrieve the data FROM the DHT, via node 2.
	//where the delete has been properly propogated the find operation should now fail.
	//NB: We must allow a delay greater than the Janitor interval to ensure the deletion is actioned.
	time.Sleep(11000 * time.Millisecond)
	if v, ok := n2.Find("alpha"); ok {
		t.Fatalf("FindRemote should have failed but returned %q", string(v))
	}

}

func Test_Create_And_Delete_Standard_Entry_Value_With_Non_Existent_Key(t *testing.T) {

	//create new default test context
	ctx := NewDefaultTestContext(t)

	//obtain nodes from the test context
	n1 := ctx.Nodes[0]
	n2 := ctx.Nodes[1]

	//store an STANDARD entry to the DHT via node 1.
	peer1StoreErr := n1.Store("alpha", []byte("v"))
	if peer1StoreErr != nil {
		t.Fatal("Error occurred whilst Peer Node 1 was trying to store entry:", peer1StoreErr)
	}

	//after a short delay attempt to retrieve the data FROM the DHT, via node 2.
	time.Sleep(1000 * time.Millisecond)
	if v, ok := n2.Find("alpha"); !ok || string(v) != "v" {
		t.Fatalf("FindRemote failed %q", string(v))
	}

	//next explicitly attempt to delete the entry with a non existing key, via node 1
	//this should cause a deletion faliure error.
	if err := n1.Delete("wrong key"); err == nil {
		t.Fatalf("expected error to occur whilst atempting to delete non-existing entry 'alpha2'")
	}

	//then to be absolutely sure the deletion was not actioned nor propergated we attempt to
	//find the entry again via node 2.
	time.Sleep(3000 * time.Millisecond)
	if v, ok := n2.Find("alpha"); !ok || string(v) != "v" {
		t.Fatalf("FindRemote after non-actioned delete failed %q", string(v))
	}

}

func Test_Create_And_Delete_Standard_Entry_Value_With_PublisherId_Mismatch(t *testing.T) {

	//create new default test context
	ctx := NewDefaultTestContext(t)

	//obtain nodes from the test context
	n1 := ctx.Nodes[0]
	n2 := ctx.Nodes[1]

	//store an STANDARD entry to the DHT via node 1.
	peer1StoreErr := n1.Store("alpha", []byte("v"))
	if peer1StoreErr != nil {
		t.Fatal("Error occurred whilst Peer Node 1 was trying to store entry:", peer1StoreErr)
	}

	//after a short delay attempt to retrieve the data FROM the DHT, via node 2.
	time.Sleep(1000 * time.Millisecond)
	if v, ok := n2.Find("alpha"); !ok || string(v) != "v" {
		t.Fatalf("FindRemote failed %q", string(v))
	}

	//next explicitly attempt to delete the entry via NODE 2
	//note: node 2 did not create the entry and this its publisher id will not mach the
	//publisher id stored associated with the entry and thus the deletion should fail
	if err := n2.Delete("alpha"); err == nil {
		t.Fatalf("expected error to occur whilst atempting to delete entry via a node that did not create it.")
	} else {
		t.Log("Error occurred as expected:", err)
	}

	//in order to verify that the node was not deleted,after a short delay attempt to retreive the entry
	//locally, via the node that created it, node 1 in this case.
	time.Sleep(3000 * time.Millisecond)
	if v, ok := n1.Find("alpha"); !ok || string(v) != "v" {
		t.Fatalf("FindRemote after non-actioned delete failed %q", string(v))
	}

	//then to be absolutely sure the deletion was not actioned nor propergated we attempt to
	//find the entry again via node 2.
	time.Sleep(3000 * time.Millisecond)
	if v, ok := n2.Find("alpha"); !ok || string(v) != "v" {
		t.Fatalf("FindRemote after non-actioned delete failed %q", string(v))
	}

}

func Test_Create_And_Delete_Index_Entry_Value(t *testing.T) {

	//create new default test context
	ctx := NewDefaultTestContext(t)

	//obtain nodes from the test context
	n1 := ctx.Nodes[0]
	n2 := ctx.Nodes[1]

	//store entries from both nodes under the same key
	key := "leaf/x"
	peer1StoreIndexErr := n1.StoreIndex(key, IndexEntry{Source: key, Target: "super/" + n1.ID.String(), UpdatedUnix: time.Now().UnixNano()})
	if peer1StoreIndexErr != nil {
		t.Fatalf("Error occurred whilst Peer Node 1 was trying to store index entry: %v", peer1StoreIndexErr)
	}

	peer2IndexIndexStoreErr := n2.StoreIndex(key, IndexEntry{Source: key, Target: "super/" + n2.ID.String(), UpdatedUnix: time.Now().UnixNano()})
	if peer2IndexIndexStoreErr != nil {
		t.Fatalf("Error occurred whilst Peer Node 2 was trying to store index entry: %v", peer2IndexIndexStoreErr)
	}

	//after a short delay attempt to retrieve the merged index FROM the DHT, via node 1 and node 2
	time.Sleep(3000 * time.Millisecond)
	if ents, ok := n1.FindIndex(key); !ok || len(ents) < 2 {
		t.Fatalf("expected merged index >=2")
	}
	time.Sleep(4000 * time.Millisecond)
	if ents, ok := n2.FindIndex(key); !ok || len(ents) < 2 {
		t.Fatalf("expected merged index >=2")

	}

	//next explicitly delete one of the index entries via node 1 (here the index source(i.e key) and top-level key happen to be the same.
	if err := n1.DeleteIndex(key, key); err != nil {
		t.Fatalf("Error occurred whilst deleting index entry '%s': %v", key, err)
	}

	//after a short delay to allow the deletion to propergate attempt to retreive the index from the DHT via node 2.
	//which should now only contain a single entry
	time.Sleep(6000 * time.Millisecond)
	if ents, ok := n2.FindIndex(key); !ok || len(ents) != 1 {
		t.Fatalf("expected merged index = 1")
	}

}

func Test_Create_And_Delete_Index_Entry_Value_With_Non_Existent_Key(t *testing.T) {

	//create new default test context
	ctx := NewDefaultTestContext(t)

	//obtain nodes from the test context
	n1 := ctx.Nodes[0]
	n2 := ctx.Nodes[1]

	//store entries from both nodes under the same key
	key := "leaf/x"
	peer1StoreIndexErr := n1.StoreIndex(key, IndexEntry{Source: key, Target: "super/" + n1.ID.String(), UpdatedUnix: time.Now().UnixNano()})
	if peer1StoreIndexErr != nil {
		t.Fatalf("Error occurred whilst Peer Node 1 was trying to store index entry: %v", peer1StoreIndexErr)
	}

	peer2IndexIndexStoreErr := n2.StoreIndex(key, IndexEntry{Source: key, Target: "super/" + n2.ID.String(), UpdatedUnix: time.Now().UnixNano()})
	if peer2IndexIndexStoreErr != nil {
		t.Fatalf("Error occurred whilst Peer Node 2 was trying to store index entry: %v", peer2IndexIndexStoreErr)
	}

	//after a short delay attempt to retrieve the merged index FROM the DHT, via node 1 and node 2
	time.Sleep(3000 * time.Millisecond)
	if ents, ok := n1.FindIndex(key); !ok || len(ents) < 2 {
		t.Fatalf("expected merged index >=2")
	}
	time.Sleep(4000 * time.Millisecond)
	if ents, ok := n2.FindIndex(key); !ok || len(ents) < 2 {
		t.Fatalf("expected merged index >=2")

	}

	//next explicitly delete one of the index entries via node 1
	//NB: We ensure we pass in an erroneous key
	if err := n1.DeleteIndex("wrong key", key); err == nil {
		t.Fatalf("expected error to occur whilst deleting non existence index entry: %s", key)
	}

	//after a short delay verify that the deltion operation was aborted
	//by checking the length of the index it should still be equal to two.
	time.Sleep(6000 * time.Millisecond)
	if ents, ok := n1.FindIndex(key); !ok || len(ents) != 2 {
		t.Fatalf("expected merged index = 2")
	}

	time.Sleep(7000 * time.Millisecond)
	if ents, ok := n2.FindIndex(key); !ok || len(ents) != 2 {
		t.Fatalf("expected merged index = 2")
	}

}

func Test_Create_And_Delete_Index_Entry_Value_With_PublisherId_Mismatch(t *testing.T) {

	//create new default test context
	ctx := NewConfigurableTestContext(t, 3, nil, true)

	//obtain nodes from the test context
	n1 := ctx.Nodes[0]
	n2 := ctx.Nodes[1]
	n3 := ctx.Nodes[2]

	//store index entries from the first two nodes under the same key
	key := "leaf/x"
	peer1StoreIndexErr := n1.StoreIndex(key, IndexEntry{Source: key, Target: "super/" + n1.ID.String(), UpdatedUnix: time.Now().UnixNano()})
	if peer1StoreIndexErr != nil {
		t.Fatalf("Error occurred whilst Peer Node 1 was trying to store index entry: %v", peer1StoreIndexErr)
	}

	peer2IndexIndexStoreErr := n2.StoreIndex(key, IndexEntry{Source: key, Target: "super/" + n2.ID.String(), UpdatedUnix: time.Now().UnixNano()})
	if peer2IndexIndexStoreErr != nil {
		t.Fatalf("Error occurred whilst Peer Node 2 was trying to store index entry: %v", peer2IndexIndexStoreErr)
	}

	//next fetch entries from node 3 to ensure the storage operation was successsfully propogated
	time.Sleep(3000 * time.Millisecond)
	if ents, ok := n3.FindIndex(key); !ok || len(ents) != 2 {
		t.Fatalf("expected merged index = 2")
	}

	//next attempt to explicitly delete either of the index entries by node 3, this should fail
	//as node 3 is not the original publisher of either entry and thus its publisher id will not match
	//either of the stored entries.
	if err := n3.DeleteIndex(key, key); err == nil {
		t.Fatalf("expected error to occur whilst deleting index entry with missmatching publisher id: %s", key)
	} else {
		t.Log("Error occurred as expected", err)
	}

	//after a short delay to allow any propergated deletion to take effect, attempt to retreive the index from
	// the DHT, which should still contain both entries
	time.Sleep(6000 * time.Millisecond)
	if ents, ok := n3.FindIndex(key); !ok || len(ents) != 2 {
		t.Fatalf("expected merged index = 2")
	}

}

func Test_Standard_Entry_Auto_Expiration(t *testing.T) {

	//create new configurable test context, so we can create three nodes.
	ctx := NewConfigurableTestContext(t, 3, nil, false)

	//obtain nodes from the test context
	n1 := ctx.Nodes[0]
	n2 := ctx.Nodes[1]
	n3 := ctx.Nodes[2]

	//store an STANDARD entry to the DHT via node 1, we directly call the *WithTTL variant to set a short ttl
	peer1StoreErr := n1.StoreWithTTL("alpha", []byte("v"), 10*time.Second, n1.ID, true)
	if peer1StoreErr != nil {
		t.Fatal("Error occurred whilst Peer Node 1 was trying to store entry:", peer1StoreErr)
	}

	//store a standard entry to the DHT via node 2,we directly call the *WithTTL variant to set a short ttl
	peer2StoreErr := n2.StoreWithTTL("beta", []byte("w"), 10*time.Second, n2.ID, true)
	if peer2StoreErr != nil {
		t.Fatal("Error occurred whilst Peer Node 2 was trying to store entry:", peer2StoreErr)
	}

	//after a short delay attempt to retrieve the data FROM the DHT, via an alternate node
	//to the node that created it, ensure the store operation was propagated.
	time.Sleep(1000 * time.Millisecond)
	if v, ok := n2.Find("alpha"); !ok || string(v) != "v" {
		t.Fatalf("FindRemote failed %q", string(v))
	}

	time.Sleep(2000 * time.Millisecond)
	if w, ok := n1.Find("beta"); !ok || string(w) != "w" {
		t.Fatalf("FindRemote failed %q", string(w))
	}

	time.Sleep(2500 * time.Millisecond)
	if w, ok := n3.Find("beta"); !ok || string(w) != "w" {
		t.Fatalf("FindRemote failed %q", string(w))
	}

	time.Sleep(3000 * time.Millisecond)
	if v, ok := n3.Find("alpha"); !ok || string(v) != "v" {
		t.Fatalf("FindRemote failed %q", string(v))
	}

	//next we crucially close node 1 and thereby prevent it from refeshing its entries.
	n1.Close()

	//next we wait for a period longer that the ttl (10 seconds) to allow time for the closed nodes
	//entries to expire.
	time.Sleep(12 * time.Second)

	//finally attempt to retreive node 1's entry via node 2 and 3 which should fail, in both cases.
	time.Sleep(3000 * time.Millisecond)
	_, node3FundOK := n3.Find("alpha")
	if node3FundOK {
		t.Fatal("Expected lookup for entry to fail on account of it having been expired.")
	} else {
		t.Log("Expired entry was not found as expected.")
	}

	time.Sleep(3500 * time.Millisecond)
	_, node2FundOK := n2.Find("alpha")
	if node2FundOK {
		t.Fatal("Expected lookup for entry to fail on account of it having been expired.")
	} else {
		t.Log("Expired entry was not found as expected.")
	}

}

func Test_Index_Entry_Auto_Expiration(t *testing.T) {

	//explicitly set a short TTL duration to allow us to validate the auto expiration of index entries.
	cfg := config.GetDefaultSingletonInstance()
	cfg.UseProtobuf = true
	cfg.RequestTimeout = 2000 * time.Millisecond
	cfg.DefaultEntryTTL = 30 * time.Second
	cfg.DefaultIndexEntryTTL = 15 * time.Second
	cfg.RefreshInterval = 5 * time.Second
	cfg.JanitorInterval = 10 * time.Second

	//create new default test context
	ctx := NewConfigurableTestContext(t, 3, cfg, false)

	//obtain nodes from the test context
	n1 := ctx.Nodes[0]
	n2 := ctx.Nodes[1]
	n3 := ctx.Nodes[2]

	t.Logf("STORAGE TIME: %s", time.Now().String())

	//store index entries from the first two nodes under the same key
	key := "leaf/x"
	peer1StoreIndexErr := n1.StoreIndex(key, IndexEntry{Source: key, Target: "super/" + n1.ID.String(), UpdatedUnix: time.Now().UnixNano()})
	if peer1StoreIndexErr != nil {
		t.Fatalf("Error occurred whilst Peer Node 1 was trying to store index entry: %v", peer1StoreIndexErr)
	}

	peer2IndexIndexStoreErr := n2.StoreIndex(key, IndexEntry{Source: key, Target: "super/" + n2.ID.String(), UpdatedUnix: time.Now().UnixNano()})
	if peer2IndexIndexStoreErr != nil {
		t.Fatalf("Error occurred whilst Peer Node 2 was trying to store index entry: %v", peer2IndexIndexStoreErr)
	}

	//next fetch entries from node 3 to ensure the storage operation was successsfully propogated
	time.Sleep(3000 * time.Millisecond)
	if ents, ok := n3.FindIndex(key); !ok || len(ents) != 2 {
		t.Fatalf("expected merged index = 2")
	} else {

		t.Log("The found entries were:")
		t.Log(ents)
	}

	//next close down node 2 and thereby prevent it from refreshing its index keys, which should ultimately cause
	//them to expire.
	time.Sleep(3000 * time.Millisecond)
	n2.Close()

	//next allow sufficient time for the TTL duration (15 seconds in this case) to elapse
	time.Sleep(18 * time.Second)

	//finally attempt to retrieve the collection of entries associated with the key which
	//should now be of length 1 as node 2's entry should have automatically expired.
	if ents, ok := n3.FindIndex(key); !ok || len(ents) != 1 {
		t.Fatalf("expected merged index count to now equal: 1, it actually was equal to: " + strconv.Itoa(len(ents)))
	} else {
		t.Log("The only remaining entry was:")
		t.Log(ents[0])
	}
}

/*****************************************************************************************************************
 *                                        FULL NETWORK E2E TESTS
 *
 * THE BELOW TESTS ARE INTENDED TO SIMULATE HOW THE DHT WILL FUNCTION IN A
 * LARGER NETWORK ENVIORNMENT, COMPRISED OF A MULTITUDE OF CORE BOOTSTRAP AND/OR
 * STANDARD NODES.
 ******************************************************************************************************************/

func Test_Full_Network_Complete_Mesh_Interconnectivity(t *testing.T) {

	desiredNodeCount := 42

	//create new configurable test context
	ctx := NewConfigurableTestContext(t, desiredNodeCount, nil, false)

	//pause for a short time to allow the context to create the fully connected mesh network
	time.Sleep(12000 * time.Millisecond)

	//next pick a node at random and validate that it indeed has a view of all nodes in the network
	//which will be the desiredNodeCount minus 1 to account for the fact that the node
	//will obviously  not be included in its own peer list.
	node := ctx.Nodes[15]
	if len(node.ListPeersAsString()) == (desiredNodeCount - 1) {
		t.Log()
		t.Logf("Node: %d has address: %s and has a peer list length of: %d", 15, node.Addr, len(node.ListPeersAsString()))
		t.Log()
	} else {
		t.Fatalf("Expected node to have a peer list length of: %d but actually had a length of: %d", desiredNodeCount-1, len(node.ListPeersAsString()))
	}

}

func Test_Full_Network_Core_Bootstrap_Nodes_Interconnectivity(t *testing.T) {

	//prepare our core network, bootstrap node addresses.
	coreNetworkBootstrapNodeAddrs := []string{":7401", ":7402", ":7403", ":7404", ":7405"}

	//next call into our helper function to create a new configurable test context complete
	//with core bootstrap nodes. For the purposes of this test we DO NOT specify any standard nodes
	//we are ONLY concerend with testing the interconnectivity of the core bootstrap nodes.
	//NB: We set the connect delay to -1 to prompt the node to use the default connection delay as
	//    specified in the preveiling configuration.
	ctx := NewConfigurableTestContextWithBootstrapAddresses(t, 0, nil, coreNetworkBootstrapNodeAddrs, -1, 0)

	//next we wait some time for the bootstrap process to complete on each node, by default
	//each node will wait 20 seconds before attempting to actually connect to the provided
	//bootstrap addresses
	time.Sleep(30000 * time.Millisecond)

	//grab reference to our (now hopefully bootstrapped nodes)
	n1 := ctx.BootstrapNodes[0]
	n2 := ctx.BootstrapNodes[1]
	n3 := ctx.BootstrapNodes[2]
	n4 := ctx.BootstrapNodes[3]
	n5 := ctx.BootstrapNodes[4]

	//next we validate that each node has a full view of the core network by checking
	//that each node has a peer list length equal to the total number of core nodes minus itself.
	expectedPeerListLength := len(coreNetworkBootstrapNodeAddrs) - 1

	if len(n1.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 1 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n1.ListPeersAsString()))
	}

	if len(n2.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 2 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n2.ListPeersAsString()))
	}

	if len(n3.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 3 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n3.ListPeersAsString()))
	}

	if len(n4.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 4 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n4.ListPeersAsString()))
	}

	if len(n5.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 5 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n5.ListPeersAsString()))
	}

}

func Test_Full_Network_Core_Bootstrap_Nodes_Interconnectivity_And_Standard_Nodes_Connectivity(t *testing.T) {

	//prepare our core network, bootstrap node addresses.
	coreNetworkBootstrapNodeAddrs := []string{":7401", ":7402", ":7403", ":7404", ":7405"}

	//desired standard node count (we pick a number that is evenly divisiable by the number of core nodes
	// to simply the connection distibutation validation logic) we pick 20 here (4 standard nodes per core node)
	standardNodeMultiplier := 4
	standardNodeCount := standardNodeMultiplier * len(coreNetworkBootstrapNodeAddrs)

	//next call into our helper function to create a new configurable test context complete
	//with core bootstrap nodes AND 20 standard nodes. The function will attempt to evenly
	//distribute connections to the core nodes from these standard nodes.
	ctx := NewConfigurableTestContextWithBootstrapAddresses(t, standardNodeCount, nil, coreNetworkBootstrapNodeAddrs, 0, 0)

	//next we wait some time for the bootstrap process to complete on each node, by default
	//each node will wait 20 seconds before attempting to actually connect to the provided
	//bootstrap addresses
	time.Sleep(40000 * time.Millisecond)

	//grab reference to our (now hopefully bootstrapped nodes)
	n1 := ctx.BootstrapNodes[0]
	n2 := ctx.BootstrapNodes[1]
	n3 := ctx.BootstrapNodes[2]
	n4 := ctx.BootstrapNodes[3]
	n5 := ctx.BootstrapNodes[4]

	//next we validate that each node has a full view of the core network and their respective
	//directly connected standard nodes by checking that each node has a peer list length
	//equal the the number of core nodes minus 1 (itself) pluss the number of standard nodes
	//connected to it (which should be equal to the standardNodeMultiplier)
	expectedPeerListLength := (len(coreNetworkBootstrapNodeAddrs) - 1) + standardNodeMultiplier

	if len(n1.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 1 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n1.ListPeersAsString()))
	}

	if len(n2.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 2 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n2.ListPeersAsString()))
	}

	if len(n3.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 3 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n3.ListPeersAsString()))
	}

	if len(n4.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 4 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n4.ListPeersAsString()))
	}

	if len(n5.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 5 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n5.ListPeersAsString()))
	}
}

func Test_Full_Network_Bootstrap_Node_To_Standard_Node_Find_Standard_Entry(t *testing.T) {

	//prepare our core network, bootstrap node addresses.
	coreNetworkBootstrapNodeAddrs := []string{":7401", ":7402", ":7403", ":7404", ":7405"}

	//desired standard node count (we pick a number that is evenly divisiable by the number of core nodes
	// to simply the connection distibutation validation logic) we pick 20 here (4 standard nodes per core node)
	standardNodeMultiplier := 4
	standardNodeCount := standardNodeMultiplier * len(coreNetworkBootstrapNodeAddrs)

	//next call into our helper function to create a new configurable test context complete
	//with core bootstrap nodes AND 20 standard nodes. The function will attempt to evenly
	//distribute connections to the core nodes from these standard nodes.
	ctx := NewConfigurableTestContextWithBootstrapAddresses(t, standardNodeCount, nil, coreNetworkBootstrapNodeAddrs, -1, 300)

	//next we wait some time for the bootstrap process to complete on each node, by default
	//each node will wait 20 seconds before attempting to actually connect to the provided
	//bootstrap addresses
	time.Sleep(40000 * time.Millisecond)

	//grab reference to our (now hopefully bootstrapped nodes)
	n1 := ctx.BootstrapNodes[0]
	n2 := ctx.BootstrapNodes[1]
	n3 := ctx.BootstrapNodes[2]
	n4 := ctx.BootstrapNodes[3]
	n5 := ctx.BootstrapNodes[4]

	//next we validate that each bootstrap node has a full view of the core network and their respective
	//directly connected standard nodes by checking that each node has a peer list length
	//equal the the number of core nodes minus 1 (itself) plus the number of standard nodes
	//connected to it (which should be equal to the standardNodeMultiplier)
	expectedPeerListLength := (len(coreNetworkBootstrapNodeAddrs) - 1) + standardNodeMultiplier

	if len(n1.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 1 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n1.ListPeersAsString()))
	}

	if len(n2.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 2 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n2.ListPeersAsString()))
	}

	if len(n3.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 3 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n3.ListPeersAsString()))
	}

	if len(n4.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 4 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n4.ListPeersAsString()))
	}

	if len(n5.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 5 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n5.ListPeersAsString()))
	}

	//NEXT WE DEAL WITH STORING ENTRIES TO SOME RANDOM SUBSET OF THE STANDARD NODES IN THE NETWORK...

	//Firstly, here we define a helper function to pick random indexes according to the specified count.
	var selectRandomNodeIndexes func([]int, int, int) []int
	selectRandomNodeIndexes = func(picked []int, totalCount int, desiredCount int) []int {
		if desiredCount > totalCount {
			t.Fatal("Count exceeds available entries.")
		}
		if len(picked) == desiredCount {
			return picked
		}

		//select a random index between 0 and createdStandardNodeCount -1
		//and append it to our index
		randIdx := rand.Intn(totalCount - 1)
		if !slices.Contains(picked, randIdx) {
			picked = append(picked, randIdx)
		}
		return selectRandomNodeIndexes(picked, totalCount, desiredCount)
	}

	//next obtain references to the complete list of standard nodes from the test context.
	allStandardNodes := ctx.Nodes

	//var to hold the number of nodes we should randomly select
	randomNodeSelectionCount := 5

	//pick. small subset of stabdard nodes, at random, to store entries to.
	randomlySelectedIndexes := selectRandomNodeIndexes(make([]int, 0), len(allStandardNodes), randomNodeSelectionCount)
	fmt.Println()
	fmt.Printf("Selected random indexes were: %v", randomlySelectedIndexes)

	//select nodes at the random indexes
	var randomlySelectedStandardNodes []*Node
	for _, currentSelIdx := range randomlySelectedIndexes {
		randomlySelectedStandardNodes = append(randomlySelectedStandardNodes, allStandardNodes[currentSelIdx])
	}

	//call into our helper function to pepare some sample data for us to store, we set the
	//sample entry count equal to the number of standard nodes we randomly selected for the
	//purposes of this test.
	sampleData := prepSampleEntryData(t, randomNodeSelectionCount)

	//iterate over the sample data, storing each entry to the corresponding
	//randomly selected node as the current index.
	curIdx := 0
	for k, v := range *sampleData {
		randomlySelectedStandardNodes[curIdx].Store(k, v)
		curIdx++
	}

	//allow some time for the storage operation to complete and be propagated across the network
	t.Log("Allowing time for storage of entries to random standard nodes to propergate...")
	time.Sleep(20000 * time.Millisecond)

	//next vaerify that ALL entries can be found by all bootstrap nodes. To do this we iterate over the sample
	//key-value pair entry data, and attempt to find each entry on each bootstrap node. We also validate the
	//the stored data, which will always be set equal to the key in the sample sata.
	allBootstrapNodes := ctx.BootstrapNodes
	for k := range *sampleData {

		//iterate over the bootstrap nodes a
		for _, curBootstrapNode := range allBootstrapNodes {

			//if we cannot find an entry for the current key via this node OR if the entry doesn't have the
			//expected value, we fail the test.
			if val, ok := curBootstrapNode.Find(k); !ok && string(val) != k {
				t.Fatalf("FindRemote failed on bootstrap node: %s for resource with key: %s", curBootstrapNode.ID, k)
			} else {
				t.Log()
				t.Logf("Find operations for key %s succeded on bootstrap node: %s the corresponding value was: %s", k, curBootstrapNode.Addr, string(val))
			}
		}
	}

}

func Test_Full_Network_Bootstrap_Node_To_Standard_Node_Find_Index_Entry(t *testing.T) {

	//prepare our core network, bootstrap node addresses.
	coreNetworkBootstrapNodeAddrs := []string{":7401", ":7402", ":7403", ":7404", ":7405"}

	//desired standard node count (we pick a number that is evenly divisiable by the number of core nodes
	// to simply the connection distibutation validation logic) we pick 20 here (4 standard nodes per core node)
	standardNodeMultiplier := 4
	standardNodeCount := standardNodeMultiplier * len(coreNetworkBootstrapNodeAddrs)

	//next call into our helper function to create a new configurable test context complete
	//with core bootstrap nodes AND 20 standard nodes. The function will attempt to evenly
	//distribute connections to the core nodes from these standard nodes.
	ctx := NewConfigurableTestContextWithBootstrapAddresses(t, standardNodeCount, nil, coreNetworkBootstrapNodeAddrs, -1, 300)

	//next we wait some time for the bootstrap process to complete on each node, by default
	//each node will wait 20 seconds before attempting to actually connect to the provided
	//bootstrap addresses
	time.Sleep(40000 * time.Millisecond)

	//grab reference to our (now hopefully bootstrapped nodes)
	n1 := ctx.BootstrapNodes[0]
	n2 := ctx.BootstrapNodes[1]
	n3 := ctx.BootstrapNodes[2]
	n4 := ctx.BootstrapNodes[3]
	n5 := ctx.BootstrapNodes[4]

	//next we validate that each bootstrap node has a full view of the core network and their respective
	//directly connected standard nodes by checking that each node has a peer list length
	//equal the the number of core nodes minus 1 (itself) plus the number of standard nodes
	//connected to it (which should be equal to the standardNodeMultiplier)
	expectedPeerListLength := (len(coreNetworkBootstrapNodeAddrs) - 1) + standardNodeMultiplier

	if len(n1.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 1 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n1.ListPeersAsString()))
	}

	if len(n2.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 2 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n2.ListPeersAsString()))
	}

	if len(n3.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 3 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n3.ListPeersAsString()))
	}

	if len(n4.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 4 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n4.ListPeersAsString()))
	}

	if len(n5.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 5 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n5.ListPeersAsString()))
	}

	//NEXT WE DEAL WITH STORING ENTRIES TO SOME RANDOM SUBSET OF THE STANDARD NODES IN THE NETWORK...

	//Firstly, here we define a helper function to pick random indexes according to the specified count.
	var selectRandomNodeIndexes func([]int, int, int) []int
	selectRandomNodeIndexes = func(picked []int, totalCount int, desiredCount int) []int {
		if desiredCount > totalCount {
			t.Fatal("Count exceeds available entries.")
		}
		if len(picked) == desiredCount {
			return picked
		}

		//select a random index between 0 and createdStandardNodeCount -1
		//and append it to our index
		randIdx := rand.Intn(totalCount - 1)
		if !slices.Contains(picked, randIdx) {
			picked = append(picked, randIdx)
		}
		return selectRandomNodeIndexes(picked, totalCount, desiredCount)
	}

	//next obtain references to the complete list of standard nodes from the test context.
	allStandardNodes := ctx.Nodes

	//var to hold the number of nodes we should randomly select
	randomNodeSelectionCount := 5

	//pick. small subset of stabdard nodes, at random, to store entries to.
	randomlySelectedIndexes := selectRandomNodeIndexes(make([]int, 0), len(allStandardNodes), randomNodeSelectionCount)
	fmt.Println()
	fmt.Printf("Selected random indexes were: %v", randomlySelectedIndexes)

	//select nodes at the random indexes
	var randomlySelectedStandardNodes []*Node
	for _, currentSelIdx := range randomlySelectedIndexes {
		randomlySelectedStandardNodes = append(randomlySelectedStandardNodes, allStandardNodes[currentSelIdx])
	}

	//call into our helper function to pepare some sample data for us to store, we set the
	//sample entry count equal to the number of standard nodes we randomly selected for the
	//purposes of this test.
	sampleData := prepSampleEntryData(t, randomNodeSelectionCount)

	//iterate over the sample data, storing each entry to the corresponding
	//randomly selected node as the current index.
	curIdx := 0
	for k, v := range *sampleData {
		randomlySelectedStandardNodes[curIdx].StoreIndex(k, IndexEntry{Source: k, Target: string(v), UpdatedUnix: time.Now().UnixNano()})
		curIdx++
	}

	//allow some time for the storage operation to complete and be propagated across the network
	t.Log("Allowing time for storage of entries to random standard nodes to propergate...")
	time.Sleep(20000 * time.Millisecond)

	//next vaerify that ALL entries can be found by all bootstrap nodes. To do this we iterate over the sample
	//key-value pair entry data, and attempt to find each entry on each bootstrap node. We also validate the
	//the stored data, which will always be set equal to the key in the sample sata.
	allBootstrapNodes := ctx.BootstrapNodes
	for k, v := range *sampleData {

		//iterate over the bootstrap nodes a
		for _, curBootstrapNode := range allBootstrapNodes {

			//if we cannot find an entry for the current key via this node OR if the entry doesn't have the
			//expected value, we fail the test.
			if indexEntries, ok := curBootstrapNode.FindIndex(k); !ok {
				t.Fatalf("FindIndex failed on bootstrap node: %s for resource with key: %s", curBootstrapNode.ID, k)
			} else {
				if len(indexEntries) == 0 {
					t.Fatalf("FindIndex failed on bootstrap node: %s for resource with key: %s, no entries were found", curBootstrapNode.ID, k)
				}
				if string(indexEntries[0].Target) != string(v) {
					t.Fatalf("FindIndex failed on bootstrap node: %s for resource with key: %s, expected value: %s but got value: %s", curBootstrapNode.ID, k, string(v), string(indexEntries[0].Target))
				}
				t.Log()
				t.Logf("FindIndex operations for key %s succeded on bootstrap node: %s the corresponding value was: %s", k, curBootstrapNode.Addr, string(indexEntries[0].Target))
			}
		}
	}

}

func Test_Full_Network_Bootstrap_Node_To_Standard_Node_Find_Standard_Entry_With_Disjoint_Peer_List_Pairings(t *testing.T) {

	//prepare our core network, bootstrap node addresses.
	coreNetworkBootstrapNodeAddrs := []string{":7401", ":7402", ":7403", ":7404", ":7405"}

	//desired standard node count (we pick a number that is evenly divisiable by the number of core nodes
	// to simply the connection distibutation validation logic) we pick 20 here (4 standard nodes per core node)
	standardNodeMultiplier := 4
	standardNodeCount := standardNodeMultiplier * len(coreNetworkBootstrapNodeAddrs)

	//next call into our helper function to create a new configurable test context complete
	//with core bootstrap nodes AND 20 standard nodes. The function will attempt to evenly
	//distribute connections to the core nodes from these standard nodes.
	ctx := NewConfigurableTestContextWithBootstrapAddresses(t, standardNodeCount, nil, coreNetworkBootstrapNodeAddrs, -1, 300)

	//next we wait some time for the bootstrap process to complete on each node, by default
	//each node will wait 20 seconds before attempting to actually connect to the provided
	//bootstrap addresses
	time.Sleep(40000 * time.Millisecond)

	//grab reference to our (now hopefully bootstrapped nodes)
	n1 := ctx.BootstrapNodes[0]
	n2 := ctx.BootstrapNodes[1]
	n3 := ctx.BootstrapNodes[2]
	n4 := ctx.BootstrapNodes[3]
	n5 := ctx.BootstrapNodes[4]

	//next we validate that each bootstrap node has a full view of the core network and their respective
	//directly connected standard nodes by checking that each node has a peer list length
	//equal the the number of core nodes minus 1 (itself) plus the number of standard nodes
	//connected to it (which should be equal to the standardNodeMultiplier)
	expectedPeerListLength := (len(coreNetworkBootstrapNodeAddrs) - 1) + standardNodeMultiplier

	if len(n1.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 1 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n1.ListPeersAsString()))
	}

	if len(n2.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 2 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n2.ListPeersAsString()))
	}

	if len(n3.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 3 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n3.ListPeersAsString()))
	}

	if len(n4.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 4 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n4.ListPeersAsString()))
	}

	if len(n5.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 5 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n5.ListPeersAsString()))
	}

	//CRUCIALLY, UNLIKE THE IMMEDIATELY PRECEEDING TEST WE PICK A **DISJOINT SET** OF STANDARD/BOOTSTRAP NODES TO
	//STORE THE ENTRIES TO. THAT IS: A BOOTSTRAP NODE SHOULD **NEVER** EXECUTE A FIND OPERATION ON A NODE ITS
	//ALREADY CONNECTED TO. THIS IS THE FIRST STEP IN ENSURING THAT FIND REQUESTS ACTUALLY MAKE IT TO THE WIDER NETWORK.

	//our helper function, that will produce a disjoint set pairing of
	//the provided bootstrap nodes and the specified array of all standard nodes
	//such that no selected standard node will have a direct link to the super node
	//its been paired with. To do this we carefully define the "Compare" and "KeySelector" functions
	//in the options object.
	disjointSetOpts := DisjointSetOpts[*Node, string]{
		Compare: func(a, b *Node) bool { return slices.Contains(b.ListPeerIdsAsStrings(), a.ID.String()) },
		//KeySelector: func(item *Node) string { return item.ID.String() },
	}

	nodePairingCount := len(ctx.BootstrapNodes)
	var disjointNodePairings []Pairing[*Node]
	var createPairingErr error
	disjointNodePairings, createPairingErr = CreateDisjointPairings(
		ctx.BootstrapNodes,
		ctx.Nodes,
		disjointSetOpts,
		nodePairingCount,
	)

	if createPairingErr != nil {
		t.Errorf("An error occurred whilst attempting to create disjoint set of node pairings: %o", createPairingErr)
	}

	//we may validate that we DO indeed have a disjoint set paring of nodes BEFORE undertaking
	//any storage operations by using our helper function
	isDisjointNodePairings, _ := IsDisjointPairing(
		disjointNodePairings,
		disjointSetOpts,
	)

	if !isDisjointNodePairings {
		t.Errorf("The specified set pairings were NOT disjoint, BEFORE storage operations were attempted.")
	}

	//call into our helper function to pepare some sample data for us to store, we set the
	//sample entry count equal to the number of bootstrap/standard node pairings we created.
	//NB: The sample data will be ultimately decomposed into separate key/value sets in order
	//    to allow for iteration over the entries in a deterministic fashion; iterating over
	//    the map, directly, is not guarenteed to return values in a consistent order.
	sampleData := prepSampleEntryData(t, nodePairingCount)

	//produce a keyset so we are able to deterministicly pick the same indexes
	//in sequence, from our sample data, for the duration/entire bounds of this function.
	var sampleDataKeySet []string
	for k := range *sampleData {
		sampleDataKeySet = append(sampleDataKeySet, k)
	}

	//next iterate over sample data array and store each entry to the
	//corresponding STANDARD node in each pairing.
	//NOTE: Order is important here, you'll note that in the above call to CreateDisjointPairings
	//      we specify the Bootstrap nodes as SET A, thus in the resulting node pairing data
	//      the Bootstrap node will be NODE 1 and the Standard node will be set as NODE 2
	storeToStandardNodesInPairings := func() {

		var storeErr error = nil
		for i, curSampleDataKey := range sampleDataKeySet {
			curSampleDataValue := (*sampleData)[curSampleDataKey]
			curPairing := disjointNodePairings[i] //.Store(k, v)
			curPairingStandardNode := curPairing.Node2
			storeErr = curPairingStandardNode.Store(curSampleDataKey, curSampleDataValue)

			if storeErr != nil {
				t.Fatalf("store failed (i=%d, key=%q, node=%s): %v",
					i, curSampleDataKey, curPairingStandardNode.Addr, storeErr)
			}
		}

	}

	storeToStandardNodesInPairings()

	//allow some time for the storage operation to complete and be propagated across the network
	t.Log("Allowing time for storage of entries to random standard nodes to propergate...")
	time.Sleep(28000 * time.Millisecond)

	//test if the set pairings are still disjoint, they likely WILL NOT be now
	//as the storage operation implicitly requires a node to undertake some degree
	//of network discovery in order to resolve the closest peers (from data key) to store the data to.
	isDisjointNodePairingsAfter, _ := IsDisjointPairing(
		disjointNodePairings,
		disjointSetOpts,
	)

	if !isDisjointNodePairingsAfter {
		t.Log("****** The specified set pairings were NOT disjoint, AFTER storage operations were attempted. Attempting to drop UNIONS to return pairings to a disjoint state")

		//declare our resolver function which will take a union pairing and make it disjoint
		//this implementation  will ensure that a standard node does not
		//contain a bootstrap node in its peer list and vice-versa
		disjointSetOpts2 := DisjointSetOpts[*Node, string]{

			Compare: func(a, b *Node) bool { return slices.Contains(b.ListPeerIdsAsStrings(), a.ID.String()) },
			Resolver: func(unionPairing Pairing[*Node]) Pairing[*Node] {

				//remove reference to bootstrap node in standard node peer list and vice-versa
				unionPairing.Node2.DropPeer(unionPairing.Node1.ID)
				unionPairing.Node1.DropPeer(unionPairing.Node2.ID)
				return unionPairing
			},
		}

		//call our ToDisjoint function to revert the pairing to a disjointed state
		disjointNodePairings, _ = ToDisjoint(disjointNodePairings, disjointSetOpts2)

	}

	//next recheck if the pairings are disjoint, they now should be
	areNowDisjoint, _ := IsDisjointPairing(
		disjointNodePairings,
		disjointSetOpts)

	//if they are still not disjoint fail the test.
	if !areNowDisjoint {
		t.Error("Node pairings are still not Disjoint after the call to: ToDisjoint()")
	}

	fmt.Println(len(disjointNodePairings[0].Node2.ListPeerIdsAsStrings()))

	//next attempt to look up value stored to each selected standard node via it's
	//associated bootstrap pairing. The pairing ensures that no pre-existing link
	//between the given bootstrap node and the standard node.
	for i, pairing := range disjointNodePairings {
		dataKey := sampleDataKeySet[i]
		dataValue := (*sampleData)[dataKey]
		pairingBootstrapNode := pairing.Node1

		queryStart := time.Now()
		if v, ok := pairingBootstrapNode.Find(dataKey); !ok || string(v) != string(dataValue) {
			fmt.Println("Failed Peer List Count: " + strconv.Itoa(len(pairingBootstrapNode.ListPeerIdsAsStrings())))
			fmt.Println("Failed Query Duration: ")
			queryDuration := time.Since(queryStart)
			fmt.Println(queryDuration)
			t.Fatalf("Find failed on node pairing %d", i)
		}
		queryDuration := time.Since(queryStart)
		fmt.Println("Query Duration: ")
		fmt.Println(queryDuration)

		fmt.Println("Peer List Count" + strconv.Itoa(len(pairingBootstrapNode.ListPeerIdsAsStrings())))

	}
}

func Test_Full_Network_Bootstrap_Node_To_Standard_Node_Find_Index_Entry_With_Disjoint_Peer_List_Pairings(t *testing.T) {

	//prepare our core network, bootstrap node addresses.
	coreNetworkBootstrapNodeAddrs := []string{":7401", ":7402", ":7403", ":7404", ":7405"}

	//desired standard node count (we pick a number that is evenly divisiable by the number of core nodes
	// to simply the connection distibutation validation logic) we pick 20 here (4 standard nodes per core node)
	standardNodeMultiplier := 4
	standardNodeCount := standardNodeMultiplier * len(coreNetworkBootstrapNodeAddrs)

	//next call into our helper function to create a new configurable test context complete
	//with core bootstrap nodes AND 20 standard nodes. The function will attempt to evenly
	//distribute connections to the core nodes from these standard nodes.
	ctx := NewConfigurableTestContextWithBootstrapAddresses(t, standardNodeCount, nil, coreNetworkBootstrapNodeAddrs, -1, 300)

	//next we wait some time for the bootstrap process to complete on each node, by default
	//each node will wait 20 seconds before attempting to actually connect to the provided
	//bootstrap addresses
	time.Sleep(40000 * time.Millisecond)

	//grab reference to our (now hopefully bootstrapped nodes)
	n1 := ctx.BootstrapNodes[0]
	n2 := ctx.BootstrapNodes[1]
	n3 := ctx.BootstrapNodes[2]
	n4 := ctx.BootstrapNodes[3]
	n5 := ctx.BootstrapNodes[4]

	//next we validate that each bootstrap node has a full view of the core network and their respective
	//directly connected standard nodes by checking that each node has a peer list length
	//equal the the number of core nodes minus 1 (itself) plus the number of standard nodes
	//connected to it (which should be equal to the standardNodeMultiplier)
	expectedPeerListLength := (len(coreNetworkBootstrapNodeAddrs) - 1) + standardNodeMultiplier

	if len(n1.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 1 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n1.ListPeersAsString()))
	}

	if len(n2.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 2 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n2.ListPeersAsString()))
	}

	if len(n3.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 3 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n3.ListPeersAsString()))
	}

	if len(n4.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 4 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n4.ListPeersAsString()))
	}

	if len(n5.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 5 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n5.ListPeersAsString()))
	}

	//CRUCIALLY, UNLIKE THE IMMEDIATELY PRECEEDING TEST WE PICK A **DISJOINT SET** OF STANDARD/BOOTSTRAP NODES TO
	//STORE THE ENTRIES TO. THAT IS: A BOOTSTRAP NODE SHOULD **NEVER** EXECUTE A FIND OPERATION ON A NODE ITS
	//ALREADY CONNECTED TO. THIS IS THE FIRST STEP IN ENSURING THAT FIND REQUESTS ACTUALLY MAKE IT TO THE WIDER NETWORK.

	//our helper function, that will produce a disjoint set pairing of
	//the provided bootstrap nodes and the specified array of all standard nodes
	//such that no selected standard node will have a direct link to the super node
	//its been paired with. To do this we carefully define the "Compare" and "KeySelector" functions
	//in the options object.
	disjointSetOpts := DisjointSetOpts[*Node, string]{
		Compare: func(a, b *Node) bool { return slices.Contains(b.ListPeerIdsAsStrings(), a.ID.String()) },
		//KeySelector: func(item *Node) string { return item.ID.String() },
	}

	nodePairingCount := len(ctx.BootstrapNodes)
	var disjointNodePairings []Pairing[*Node]
	var createPairingErr error
	disjointNodePairings, createPairingErr = CreateDisjointPairings(
		ctx.BootstrapNodes,
		ctx.Nodes,
		disjointSetOpts,
		nodePairingCount,
	)

	if createPairingErr != nil {
		t.Errorf("An error occurred whilst attempting to create disjoint set of node pairings: %o", createPairingErr)
	}

	//we may validate that we DO indeed have a disjoint set paring of nodes BEFORE undertaking
	//any storage operations by using our helper function
	isDisjointNodePairings, _ := IsDisjointPairing(
		disjointNodePairings,
		disjointSetOpts,
	)

	if !isDisjointNodePairings {
		t.Errorf("The specified set pairings were NOT disjoint, BEFORE storage operations were attempted.")
	}

	//call into our helper function to pepare some sample data for us to store, we set the
	//sample entry count equal to the number of bootstrap/standard node pairings we created.
	//NB: The sample data will be ultimately decomposed into separate key/value sets in order
	//    to allow for iteration over the entries in a deterministic fashion; iterating over
	//    the map, directly, is not guarenteed to return values in a consistent order.
	sampleData := prepSampleEntryData(t, nodePairingCount)

	//produce a keyset so we are able to deterministicly pick the same indexes
	//in sequence, from our sample data, for the duration/entire bounds of this function.
	var sampleDataKeySet []string
	for k := range *sampleData {
		sampleDataKeySet = append(sampleDataKeySet, k)
	}

	//next iterate over sample data array and store each entry to the
	//corresponding STANDARD node in each pairing.
	//NOTE: Order is important here, you'll note that in the above call to CreateDisjointPairings
	//      we specify the Bootstrap nodes as SET A, thus in the resulting node pairing data
	//      the Bootstrap node will be NODE 1 and the Standard node will be set as NODE 2
	storeToStandardNodesInPairings := func() {

		var storeErr error = nil
		for i, curSampleDataKey := range sampleDataKeySet {
			curSampleDataValue := (*sampleData)[curSampleDataKey]
			curPairing := disjointNodePairings[i] //.Store(k, v)
			curPairingStandardNode := curPairing.Node2
			storeErr = curPairingStandardNode.StoreIndex(curSampleDataKey, IndexEntry{Source: curSampleDataKey, Target: string(curSampleDataValue), UpdatedUnix: time.Now().UnixNano()})

			if storeErr != nil {
				t.Fatalf("store failed (i=%d, key=%q, node=%s): %v",
					i, curSampleDataKey, curPairingStandardNode.Addr, storeErr)
			}
		}

	}

	storeToStandardNodesInPairings()

	//allow some time for the storage operation to complete and be propagated across the network
	t.Log("Allowing time for storage of entries to random standard nodes to propergate...")
	time.Sleep(28000 * time.Millisecond)

	//test if the set pairings are still disjoint, they likely WILL NOT be now
	//as the storage operation implicitly requires a node to undertake some degree
	//of network discovery in order to resolve the closest peers (from data key) to store the data to.
	isDisjointNodePairingsAfter, _ := IsDisjointPairing(
		disjointNodePairings,
		disjointSetOpts,
	)

	if !isDisjointNodePairingsAfter {
		t.Log("****** The specified set pairings were NOT disjoint, AFTER storage operations were attempted. Attempting to drop UNIONS to return pairings to a disjoint state")

		//declare our resolver function which will take a union pairing and make it disjoint
		//this implementation  will ensure that a standard node does not
		//contain a bootstrap node in its peer list and vice-versa
		disjointSetOpts2 := DisjointSetOpts[*Node, string]{

			Compare: func(a, b *Node) bool { return slices.Contains(b.ListPeerIdsAsStrings(), a.ID.String()) },
			Resolver: func(unionPairing Pairing[*Node]) Pairing[*Node] {

				//remove reference to bootstrap node in standard node peer list and vice-versa
				unionPairing.Node2.DropPeer(unionPairing.Node1.ID)
				unionPairing.Node1.DropPeer(unionPairing.Node2.ID)
				return unionPairing
			},
		}

		//call our ToDisjoint function to revert the pairing to a disjointed state
		disjointNodePairings, _ = ToDisjoint(disjointNodePairings, disjointSetOpts2)

	}

	//next recheck if the pairings are disjoint, they now should be
	areNowDisjoint, _ := IsDisjointPairing(
		disjointNodePairings,
		disjointSetOpts)

	//if they are still not disjoint fail the test.
	if !areNowDisjoint {
		t.Error("Node pairings are still not Disjoint after the call to: ToDisjoint()")
	}

	fmt.Println(len(disjointNodePairings[0].Node2.ListPeerIdsAsStrings()))

	//next attempt to look up value stored to each selected standard node via it's
	//associated bootstrap pairing. The pairing ensures that no pre-existing link
	//between the given bootstrap node and the standard node.
	for i, pairing := range disjointNodePairings {
		dataKey := sampleDataKeySet[i]
		dataValue := (*sampleData)[dataKey]
		pairingBootstrapNode := pairing.Node1
		var indexEntries []IndexEntry
		var ok bool

		queryStart := time.Now()
		//if v, ok := pairingBootstrapNode.Find(dataKey); !ok || string(v) != string(dataValue) {
		if indexEntries, ok = pairingBootstrapNode.FindIndex(dataKey); !ok {
			fmt.Println("Failed Peer List Count: " + strconv.Itoa(len(pairingBootstrapNode.ListPeerIdsAsStrings())))
			fmt.Println("Failed Query Duration: ")
			queryDuration := time.Since(queryStart)
			fmt.Println(queryDuration)
			fmt.Printf("Index entries are: %v", indexEntries)
			t.Fatalf("Find failed on node pairing %d", i)
		}

		if len(indexEntries) < 1 {
			t.Fatalf("Failed. The record was found but no index entries were returned on node pairing %d", i)
		}

		if string(indexEntries[0].Target) != string(dataValue) {
			t.Fatalf("Find failed on node pairing %d, expected value: %s but got value: %s", i, string(dataValue), string(indexEntries[0].Target))
		}

		queryDuration := time.Since(queryStart)
		fmt.Println("Query Duration: ")
		fmt.Println(queryDuration)

		fmt.Println("Peer List Count" + strconv.Itoa(len(pairingBootstrapNode.ListPeerIdsAsStrings())))

	}
}

func Test_Full_Network_Bootstrap_Node_To_Standard_Node_Find_Standard_Entry_With_One_Level_Of_Indirection_And_Sparse_Peer_List(t *testing.T) {

	//prepare our core network, bootstrap node addresses.
	coreNetworkBootstrapNodeAddrs := []string{":7401", ":7402", ":7403", ":7404", ":7405"}

	//desired standard node count (we pick a number that is evenly divisiable by the number of core nodes
	// to simply the connection distibutation validation logic) we pick 20 here (4 standard nodes per core node)
	standardNodeMultiplier := 4
	standardNodeCount := standardNodeMultiplier * len(coreNetworkBootstrapNodeAddrs)

	//next call into our helper function to create a new configurable test context complete
	//with core bootstrap nodes AND 20 standard nodes. The function will attempt to evenly
	//distribute connections to the core nodes from these standard nodes.
	ctx := NewConfigurableTestContextWithBootstrapAddresses(t, standardNodeCount, nil, coreNetworkBootstrapNodeAddrs, -1, 300)

	//fmt.Printf("Replication factor is: %d", ctx.Config.ReplicationFactor)
	//next we wait some time for the bootstrap process to complete on each node, by default
	//each node will wait 20 seconds before attempting to actually connect to the provided
	//bootstrap addresses
	time.Sleep(40000 * time.Millisecond)

	//grab reference to our (now hopefully bootstrapped nodes)
	n1 := ctx.BootstrapNodes[0]
	n2 := ctx.BootstrapNodes[1]
	n3 := ctx.BootstrapNodes[2]
	n4 := ctx.BootstrapNodes[3]
	n5 := ctx.BootstrapNodes[4]

	//next we validate that each bootstrap node has a full view of the core network and their respective
	//directly connected standard nodes by checking that each node has a peer list length
	//equal the the number of core nodes minus 1 (itself) plus the number of standard nodes
	//connected to it (which should be equal to the standardNodeMultiplier)
	expectedPeerListLength := (len(coreNetworkBootstrapNodeAddrs) - 1) + standardNodeMultiplier

	if len(n1.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 1 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n1.ListPeersAsString()))
	}

	if len(n2.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 2 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n2.ListPeersAsString()))
	}

	if len(n3.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 3 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n3.ListPeersAsString()))
	}

	if len(n4.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 4 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n4.ListPeersAsString()))
	}

	if len(n5.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 5 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n5.ListPeersAsString()))
	}

	//OK next we add a brand new node to the network, however we only connect it to a SINGLE
	//other bootstrap node, which will ultimately result in it having a sparse peer list.
	//we then later take care to select a entirely DIFFERENT bootstrap node (from the one
	//the new node connected to) to undertake the lookup operation.
	entryNode, _ := NewNode("entryNode", ":1981", netx.NewTCP(), ctx.Config, NT_ENTRY)
	footHoldBootstrapNode := ctx.BootstrapNodes[rand.Intn(len(ctx.BootstrapNodes)-1)] //select another bootstrap node,at random, that this edge bootsrap node can use to get a foothold on the network
	entryNode.Bootstrap([]string{footHoldBootstrapNode.Addr}, 7000)                   //after some nominal time has elapsed, attempt to bootstrap the edge node

	//wait for the bootstrap of our edge node to the network foothold node to complete,
	//we wait double the connect delay time.
	time.Sleep(14000 * time.Millisecond)

	//Next we pick a small subset of STANDARD nodes at random, to store the data to.
	targetStorageNodeCount := 3

	//stores our chosen indexes
	randomlySelectedNodeIndexes := make([]int, 0)

	//local function to choose some unqiue STANDARD nodes to store data to.
	//forward-leke declaration to allow the func to be called recursively.
	var uniqueRandomSelectionFunc func(int)
	uniqueRandomSelectionFunc = func(count int) {

		if len(randomlySelectedNodeIndexes) == count {
			return
		}

		randIdxVal := rand.Intn(len(ctx.Nodes) - 1)
		if !slices.Contains(randomlySelectedNodeIndexes, randIdxVal) {
			randomlySelectedNodeIndexes = append(randomlySelectedNodeIndexes, randIdxVal)
		}

		uniqueRandomSelectionFunc(count)

	}

	//call our local random node selection which will popuilate the above array of indexes.
	uniqueRandomSelectionFunc(targetStorageNodeCount)

	//before going any further check that our requested amount of standard node indexes
	//have been randomly selected
	if len(randomlySelectedNodeIndexes) != targetStorageNodeCount {
		t.Errorf("Incorrect number of storage node selected, expected: %d and actually got: %d", targetStorageNodeCount, len(randomlySelectedNodeIndexes))

	}

	//obtain reference to STANDARD nodes at our randomly selected indexes.
	randomlySelectedNodes := make([]*Node, 0)
	for _, currentIdx := range randomlySelectedNodeIndexes {
		randomlySelectedNodes = append(randomlySelectedNodes, ctx.Nodes[currentIdx])
	}

	//generate sample data that will be later stored to a small subset of STANDARD nodes.
	sampleData := prepSampleEntryData(t, targetStorageNodeCount)

	//since have our sample data count and seleted nodes are equal we may loop
	//over any of the two collection and store data to the corresponding indexes.
	currentCount := 0
	for k, v := range *sampleData {
		curNode := randomlySelectedNodes[currentCount]
		storeErr := curNode.Store(k, v)
		if storeErr != nil {
			t.Fatalf("An error occurred whilst attempting to store entry to node at index: %d", currentCount)
		}
		currentCount++
	}

	t.Log()
	t.Logf("@@@@FootholdBootstrapNode data store length pre store operation is: %d", footHoldBootstrapNode.DataStoreLength())

	//allow some time for the storage operation to complete and be propagated across the network
	t.Log("Allowing time for storage of entries to random standard nodes to propergate...")
	time.Sleep(15000 * time.Millisecond)

	t.Log()
	t.Logf("@@@@FootholdBootstrapNode data store length post store operation is: %d", footHoldBootstrapNode.DataStoreLength())

	//log the edge nodes peer list count BEFORE the find operation (hint: should be equal to 1)
	t.Logf("Edge bootstrap node peer list count BEFORE find operation: %d", entryNode.PeerCount())

	//attempt to find each sample data entry via our edge bootstrap node which is one level
	//of indirection removed from the core bootstrap nodes.
	findExecCount := 1
	for k, expectedVal := range *sampleData {

		//ensure that an entry is retreived for each provided key and further that it has the expected value.
		v, ok := entryNode.Find(k)
		if !ok {
			t.Errorf("Failed to find entry key=%s at execution index: %d", k, findExecCount)
		}
		if string(v) != string(expectedVal) {
			t.Errorf("Wrong value for key=%s got=%q want=%q", k, v, expectedVal)
		}

		findExecCount++
	}

	//log the edge nodes peer list count AFTER the find operation, this will indicate
	//how much of the network the node has been able to automatically discover via the
	//DHT's internal, recursive lookup process.
	time.Sleep(5000 * time.Millisecond)
	t.Logf("Edge bootstrap node peer list count AFTER find operation: %d", entryNode.PeerCount())

	//clean up
	t.Cleanup(func() {
		entryNode.Close()
	})

}

func Test_Full_Network_Bootstrap_Node_To_Standard_Node_Find_Index_Entry_With_One_Level_Of_Indirection_And_Sparse_Peer_List(t *testing.T) {

	//prepare our core network, bootstrap node addresses.
	coreNetworkBootstrapNodeAddrs := []string{":7401", ":7402", ":7403", ":7404", ":7405"}

	//desired standard node count (we pick a number that is evenly divisiable by the number of core nodes
	// to simply the connection distibutation validation logic) we pick 20 here (4 standard nodes per core node)
	standardNodeMultiplier := 4
	standardNodeCount := standardNodeMultiplier * len(coreNetworkBootstrapNodeAddrs)

	//next call into our helper function to create a new configurable test context complete
	//with core bootstrap nodes AND 20 standard nodes. The function will attempt to evenly
	//distribute connections to the core nodes from these standard nodes.
	ctx := NewConfigurableTestContextWithBootstrapAddresses(t, standardNodeCount, nil, coreNetworkBootstrapNodeAddrs, -1, 300)

	//fmt.Printf("Replication factor is: %d", ctx.Config.ReplicationFactor)
	//next we wait some time for the bootstrap process to complete on each node, by default
	//each node will wait 20 seconds before attempting to actually connect to the provided
	//bootstrap addresses
	time.Sleep(40000 * time.Millisecond)

	//grab reference to our (now hopefully bootstrapped nodes)
	n1 := ctx.BootstrapNodes[0]
	n2 := ctx.BootstrapNodes[1]
	n3 := ctx.BootstrapNodes[2]
	n4 := ctx.BootstrapNodes[3]
	n5 := ctx.BootstrapNodes[4]

	//next we validate that each bootstrap node has a full view of the core network and their respective
	//directly connected standard nodes by checking that each node has a peer list length
	//equal the the number of core nodes minus 1 (itself) plus the number of standard nodes
	//connected to it (which should be equal to the standardNodeMultiplier)
	expectedPeerListLength := (len(coreNetworkBootstrapNodeAddrs) - 1) + standardNodeMultiplier

	if len(n1.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 1 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n1.ListPeersAsString()))
	}

	if len(n2.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 2 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n2.ListPeersAsString()))
	}

	if len(n3.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 3 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n3.ListPeersAsString()))
	}

	if len(n4.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 4 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n4.ListPeersAsString()))
	}

	if len(n5.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 5 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n5.ListPeersAsString()))
	}

	//OK next we add a brand new node to the network, however we only connect it to a SINGLE
	//other bootstrap node, which will ultimately result in it having a sparse peer list.
	//we then later take care to select a entirely DIFFERENT bootstrap node (from the one
	//the new node connected to) to undertake the lookup operation.
	entryNode, _ := NewNode("entryNode", ":1981", netx.NewTCP(), ctx.Config, NT_ENTRY)
	footHoldBootstrapNode := ctx.BootstrapNodes[rand.Intn(len(ctx.BootstrapNodes)-1)] //select another bootstrap node,at random, that this edge bootsrap node can use to get a foothold on the network
	entryNode.Bootstrap([]string{footHoldBootstrapNode.Addr}, 7000)                   //after some nominal time has elapsed, attempt to bootstrap the edge node

	//wait for the bootstrap of our edge node to the network foothold node to complete,
	//we wait double the connect delay time.
	time.Sleep(14000 * time.Millisecond)

	//Next we pick a small subset of STANDARD nodes at random, to store the data to.
	targetStorageNodeCount := 3

	//stores our chosen indexes
	randomlySelectedNodeIndexes := make([]int, 0)

	//local function to choose some unqiue STANDARD nodes to store data to.
	//forward-leke declaration to allow the func to be called recursively.
	var uniqueRandomSelectionFunc func(int)
	uniqueRandomSelectionFunc = func(count int) {

		if len(randomlySelectedNodeIndexes) == count {
			return
		}

		randIdxVal := rand.Intn(len(ctx.Nodes) - 1)
		if !slices.Contains(randomlySelectedNodeIndexes, randIdxVal) {
			randomlySelectedNodeIndexes = append(randomlySelectedNodeIndexes, randIdxVal)
		}

		uniqueRandomSelectionFunc(count)

	}

	//call our local random node selection which will popuilate the above array of indexes.
	uniqueRandomSelectionFunc(targetStorageNodeCount)

	//before going any further check that our requested amount of standard node indexes
	//have been randomly selected
	if len(randomlySelectedNodeIndexes) != targetStorageNodeCount {
		t.Errorf("Incorrect number of storage node selected, expected: %d and actually got: %d", targetStorageNodeCount, len(randomlySelectedNodeIndexes))

	}

	//obtain reference to STANDARD nodes at our randomly selected indexes.
	randomlySelectedNodes := make([]*Node, 0)
	for _, currentIdx := range randomlySelectedNodeIndexes {
		randomlySelectedNodes = append(randomlySelectedNodes, ctx.Nodes[currentIdx])
	}

	//generate sample data that will be later stored to a small subset of STANDARD nodes.
	sampleData := prepSampleEntryData(t, targetStorageNodeCount)

	//since have our sample data count and seleted nodes are equal we may loop
	//over any of the two collection and store data to the corresponding indexes.
	currentCount := 0
	for k, v := range *sampleData {
		curNode := randomlySelectedNodes[currentCount]
		storeErr := curNode.StoreIndex(k, IndexEntry{Source: k, Target: string(v), UpdatedUnix: time.Now().UnixNano()})
		if storeErr != nil {
			t.Fatalf("An error occurred whilst attempting to store entry to node at index: %d", currentCount)
		}
		currentCount++
	}

	t.Log()
	t.Logf("@@@@FootholdBootstrapNode data store length pre store operation is: %d", footHoldBootstrapNode.DataStoreLength())

	//allow some time for the storage operation to complete and be propagated across the network
	t.Log("Allowing time for storage of entries to random standard nodes to propergate...")
	time.Sleep(15000 * time.Millisecond)

	t.Log()
	t.Logf("@@@@FootholdBootstrapNode data store length post store operation is: %d", footHoldBootstrapNode.DataStoreLength())

	//log the edge nodes peer list count BEFORE the find operation (hint: should be equal to 1)
	t.Logf("Edge bootstrap node peer list count BEFORE find operation: %d", entryNode.PeerCount())

	//attempt to find each sample data entry via our edge bootstrap node which is one level
	//of indirection removed from the core bootstrap nodes.
	findExecCount := 1
	for k, expectedVal := range *sampleData {

		//ensure that an entry is retreived for each provided key and further that it has the expected value.
		indexEntries, ok := entryNode.FindIndex(k)
		if !ok {
			t.Errorf("Failed to find entry key=%s at execution index: %d", k, findExecCount)
		}

		if len(indexEntries) < 1 {
			t.Errorf("Failed. The record was found but no index entries were returned for key=%s at execution index: %d", k, findExecCount)
		}
		if string(indexEntries[0].Target) != string(expectedVal) {
			t.Errorf("Wrong value for key=%s got=%q want=%q at execution index: %d", k, indexEntries[0].Target, expectedVal, findExecCount)
		}

		findExecCount++
	}

	//log the edge nodes peer list count AFTER the find operation, this will indicate
	//how much of the network the node has been able to automatically discover via the
	//DHT's internal, recursive lookup process.
	time.Sleep(5000 * time.Millisecond)
	t.Logf("Edge bootstrap node peer list count AFTER find operation: %d", entryNode.PeerCount())

	//clean up
	t.Cleanup(func() {
		entryNode.Close()
	})

}

func Test_Full_Network_Bootstrap_Node_To_Standard_Node_Find_Standard_Entry_With_Two_Levels_Of_Indirection_And_Sparse_Peer_List(t *testing.T) {

	//prepare our core network, bootstrap node addresses.
	coreNetworkBootstrapNodeAddrs := []string{":7401", ":7402", ":7403", ":7404", ":7405"}

	//desired standard node count (we pick a number that is evenly divisiable by the number of core nodes
	// to simply the connection distibutation validation logic) we pick 20 here (4 standard nodes per core node)
	standardNodeMultiplier := 4
	standardNodeCount := standardNodeMultiplier * len(coreNetworkBootstrapNodeAddrs)

	//next call into our helper function to create a new configurable test context complete
	//with core bootstrap nodes AND 20 standard nodes. The function will attempt to evenly
	//distribute connections to the core nodes from these standard nodes.
	ctx := NewConfigurableTestContextWithBootstrapAddresses(t, standardNodeCount, nil, coreNetworkBootstrapNodeAddrs, -1, 300)

	//fmt.Printf("Replication factor is: %d", ctx.Config.ReplicationFactor)
	//next we wait some time for the bootstrap process to complete on each node, by default
	//each node will wait 20 seconds before attempting to actually connect to the provided
	//bootstrap addresses
	time.Sleep(40000 * time.Millisecond)

	//grab reference to our (now hopefully bootstrapped nodes)
	n1 := ctx.BootstrapNodes[0]
	n2 := ctx.BootstrapNodes[1]
	n3 := ctx.BootstrapNodes[2]
	n4 := ctx.BootstrapNodes[3]
	n5 := ctx.BootstrapNodes[4]

	//next we validate that each bootstrap node has a full view of the core network and their respective
	//directly connected standard nodes by checking that each node has a peer list length
	//equal the the number of core nodes minus 1 (itself) plus the number of standard nodes
	//connected to it (which should be equal to the standardNodeMultiplier)
	expectedPeerListLength := (len(coreNetworkBootstrapNodeAddrs) - 1) + standardNodeMultiplier

	if len(n1.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 1 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n1.ListPeersAsString()))
	}

	if len(n2.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 2 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n2.ListPeersAsString()))
	}

	if len(n3.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 3 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n3.ListPeersAsString()))
	}

	if len(n4.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 4 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n4.ListPeersAsString()))
	}

	if len(n5.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 5 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n5.ListPeersAsString()))
	}

	//OK next we add a brand new node to the network, however we only connect it to a SINGLE
	//other bootstrap node, which will ultimately result in it having a sparse peer list.
	//we then later take care to select a entirely DIFFERENT bootstrap node (from the one
	//the new node connected to) to undertake the lookup operation.
	entryNode, _ := NewNode("entryNode", ":1981", netx.NewTCP(), ctx.Config, NT_ENTRY)
	footHoldBootstrapNode := ctx.BootstrapNodes[rand.Intn(len(ctx.BootstrapNodes)-1)] //select another bootstrap node,at random, that this edge bootsrap node can use to get a foothold on the network
	entryNode.Bootstrap([]string{footHoldBootstrapNode.Addr}, 7000)                   //after some nominal time has elapsed, attempt to bootstrap the edge node

	//next create another brand new node (of type EXTERNAL this time) and provide it bootstrap method with the address of the
	//ENTRY node created in the immediately preceeding instructions, thereby setting up a single thread of
	//of interconnectivity from the External node to the Entry node and finally to  the foothold bootstrap node
	//which should in turn provide the External node with full network reachability.
	externalNode, _ := NewNode("externalNode", ":1982", netx.NewTCP(), ctx.Config, NT_EXTERNAL)
	externalNode.Bootstrap([]string{entryNode.Addr}, 7000) //after some nominal time has elapsed, attempt to bootstrap the edge node

	//wait for the bootstrap of our edge node to the network foothold node to complete,
	//we wait double the connect delay time.
	time.Sleep(14000 * time.Millisecond)

	//Next we pick a small subset of STANDARD nodes at random, to store the data to.
	targetStorageNodeCount := 3

	//stores our chosen indexes
	randomlySelectedNodeIndexes := make([]int, 0)

	//local function to choose some unqiue STANDARD nodes to store data to.
	//forward-leke declaration to allow the func to be called recursively.
	var uniqueRandomSelectionFunc func(int)
	uniqueRandomSelectionFunc = func(count int) {

		if len(randomlySelectedNodeIndexes) == count {
			return
		}

		randIdxVal := rand.Intn(len(ctx.Nodes) - 1)
		if !slices.Contains(randomlySelectedNodeIndexes, randIdxVal) {
			randomlySelectedNodeIndexes = append(randomlySelectedNodeIndexes, randIdxVal)
		}

		uniqueRandomSelectionFunc(count)

	}

	//call our local random node selection which will popuilate the above array of indexes.
	uniqueRandomSelectionFunc(targetStorageNodeCount)

	//before going any further check that our requested amount of standard node indexes
	//have been randomly selected
	if len(randomlySelectedNodeIndexes) != targetStorageNodeCount {
		t.Errorf("Incorrect number of storage node selected, expected: %d and actually got: %d", targetStorageNodeCount, len(randomlySelectedNodeIndexes))

	}

	//obtain reference to STANDARD nodes at our randomly selected indexes.
	randomlySelectedNodes := make([]*Node, 0)
	for _, currentIdx := range randomlySelectedNodeIndexes {
		randomlySelectedNodes = append(randomlySelectedNodes, ctx.Nodes[currentIdx])
	}

	//generate sample data that will be later stored to a small subset of STANDARD nodes.
	sampleData := prepSampleEntryData(t, targetStorageNodeCount)

	//since have our sample data count and seleted nodes are equal we may loop
	//over any of the two collection and store data to the corresponding indexes.
	currentCount := 0
	for k, v := range *sampleData {
		curNode := randomlySelectedNodes[currentCount]
		storeErr := curNode.Store(k, v)
		if storeErr != nil {
			t.Fatalf("An error occurred whilst attempting to store entry to node at index: %d", currentCount)
		}
		currentCount++
	}

	t.Log()
	t.Logf("@@@@FootholdBootstrapNode data store length pre store operation is: %d", footHoldBootstrapNode.DataStoreLength())

	//allow some time for the storage operation to complete and be propagated across the network
	t.Log("Allowing time for storage of entries to random standard nodes to propergate...")
	time.Sleep(15000 * time.Millisecond)

	t.Log()
	t.Logf("@@@@FootholdBootstrapNode data store length post store operation is: %d", footHoldBootstrapNode.DataStoreLength())

	//log the edge nodes peer list count BEFORE the find operation (hint: should be equal to 1)
	t.Logf("Edge bootstrap node peer list count BEFORE find operation: %d", entryNode.PeerCount())

	//attempt to find each sample data entry via our edge bootstrap node which is one level
	//of indirection removed from the core bootstrap nodes.
	findExecCount := 1
	for k, expectedVal := range *sampleData {

		//ensure that an entry is retreived for each provided key and further that it has the expected value.
		v, ok := externalNode.Find(k)
		if !ok {
			t.Errorf("Failed to find entry key=%s at execution index: %d", k, findExecCount)
		}
		if string(v) != string(expectedVal) {
			t.Errorf("Wrong value for key=%s got=%q want=%q", k, v, expectedVal)
		}

		findExecCount++
	}

	//log the edge nodes peer list count AFTER the find operation, this will indicate
	//how much of the network the node has been able to automatically discover via the
	//DHT's internal, recursive lookup process.
	time.Sleep(5000 * time.Millisecond)
	t.Logf("Edge bootstrap node peer list count AFTER find operation: %d", entryNode.PeerCount())

	//clean up
	t.Cleanup(func() {
		entryNode.Close()
		externalNode.Close()
	})
}

func Test_Full_Network_Bootstrap_Node_To_Standard_Node_Find_Index_Entry_With_Two_Levels_Of_Indirection_And_Sparse_Peer_List(t *testing.T) {

	//prepare our core network, bootstrap node addresses.
	coreNetworkBootstrapNodeAddrs := []string{":7401", ":7402", ":7403", ":7404", ":7405"}

	//desired standard node count (we pick a number that is evenly divisiable by the number of core nodes
	// to simply the connection distibutation validation logic) we pick 20 here (4 standard nodes per core node)
	standardNodeMultiplier := 4
	standardNodeCount := standardNodeMultiplier * len(coreNetworkBootstrapNodeAddrs)

	//next call into our helper function to create a new configurable test context complete
	//with core bootstrap nodes AND 20 standard nodes. The function will attempt to evenly
	//distribute connections to the core nodes from these standard nodes.
	ctx := NewConfigurableTestContextWithBootstrapAddresses(t, standardNodeCount, nil, coreNetworkBootstrapNodeAddrs, -1, 300)

	//fmt.Printf("Replication factor is: %d", ctx.Config.ReplicationFactor)
	//next we wait some time for the bootstrap process to complete on each node, by default
	//each node will wait 20 seconds before attempting to actually connect to the provided
	//bootstrap addresses
	time.Sleep(40000 * time.Millisecond)

	//grab reference to our (now hopefully bootstrapped nodes)
	n1 := ctx.BootstrapNodes[0]
	n2 := ctx.BootstrapNodes[1]
	n3 := ctx.BootstrapNodes[2]
	n4 := ctx.BootstrapNodes[3]
	n5 := ctx.BootstrapNodes[4]

	//next we validate that each bootstrap node has a full view of the core network and their respective
	//directly connected standard nodes by checking that each node has a peer list length
	//equal the the number of core nodes minus 1 (itself) plus the number of standard nodes
	//connected to it (which should be equal to the standardNodeMultiplier)
	expectedPeerListLength := (len(coreNetworkBootstrapNodeAddrs) - 1) + standardNodeMultiplier

	if len(n1.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 1 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n1.ListPeersAsString()))
	}

	if len(n2.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 2 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n2.ListPeersAsString()))
	}

	if len(n3.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 3 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n3.ListPeersAsString()))
	}

	if len(n4.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 4 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n4.ListPeersAsString()))
	}

	if len(n5.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 5 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n5.ListPeersAsString()))
	}

	//OK next we add a brand new node to the network, however we only connect it to a SINGLE
	//other bootstrap node, which will ultimately result in it having a sparse peer list.
	//we then later take care to select a entirely DIFFERENT bootstrap node (from the one
	//the new node connected to) to undertake the lookup operation.
	entryNode, _ := NewNode("entryNode", ":1981", netx.NewTCP(), ctx.Config, NT_ENTRY)
	footHoldBootstrapNode := ctx.BootstrapNodes[rand.Intn(len(ctx.BootstrapNodes)-1)] //select another bootstrap node,at random, that this edge bootsrap node can use to get a foothold on the network
	entryNode.Bootstrap([]string{footHoldBootstrapNode.Addr}, 7000)                   //after some nominal time has elapsed, attempt to bootstrap the edge node

	//next create another brand new node (of type EXTERNAL this time) and provide it bootstrap method with the address of the
	//ENTRY node created in the immediately preceeding instructions, thereby setting up a single thread of
	//of interconnectivity from the External node to the Entry node and finally to  the foothold bootstrap node
	//which should in turn provide the External node with full network reachability.
	externalNode, _ := NewNode("externalNode", ":1982", netx.NewTCP(), ctx.Config, NT_EXTERNAL)
	externalNode.Bootstrap([]string{entryNode.Addr}, 7000) //after some nominal time has elapsed, attempt to bootstrap the edge node

	//wait for the bootstrap of our edge node to the network foothold node to complete,
	//we wait double the connect delay time.
	time.Sleep(14000 * time.Millisecond)

	//Next we pick a small subset of STANDARD nodes at random, to store the data to.
	targetStorageNodeCount := 3

	//stores our chosen indexes
	randomlySelectedNodeIndexes := make([]int, 0)

	//local function to choose some unqiue STANDARD nodes to store data to.
	//forward-leke declaration to allow the func to be called recursively.
	var uniqueRandomSelectionFunc func(int)
	uniqueRandomSelectionFunc = func(count int) {

		if len(randomlySelectedNodeIndexes) == count {
			return
		}

		randIdxVal := rand.Intn(len(ctx.Nodes) - 1)
		if !slices.Contains(randomlySelectedNodeIndexes, randIdxVal) {
			randomlySelectedNodeIndexes = append(randomlySelectedNodeIndexes, randIdxVal)
		}

		uniqueRandomSelectionFunc(count)

	}

	//call our local random node selection which will popuilate the above array of indexes.
	uniqueRandomSelectionFunc(targetStorageNodeCount)

	//before going any further check that our requested amount of standard node indexes
	//have been randomly selected
	if len(randomlySelectedNodeIndexes) != targetStorageNodeCount {
		t.Errorf("Incorrect number of storage node selected, expected: %d and actually got: %d", targetStorageNodeCount, len(randomlySelectedNodeIndexes))

	}

	//obtain reference to STANDARD nodes at our randomly selected indexes.
	randomlySelectedNodes := make([]*Node, 0)
	for _, currentIdx := range randomlySelectedNodeIndexes {
		randomlySelectedNodes = append(randomlySelectedNodes, ctx.Nodes[currentIdx])
	}

	//generate sample data that will be later stored to a small subset of STANDARD nodes.
	sampleData := prepSampleEntryData(t, targetStorageNodeCount)

	//since have our sample data count and seleted nodes are equal we may loop
	//over any of the two collection and store data to the corresponding indexes.
	currentCount := 0
	for k, v := range *sampleData {
		curNode := randomlySelectedNodes[currentCount]
		storeErr := curNode.StoreIndex(k, IndexEntry{Source: k, Target: string(v), UpdatedUnix: time.Now().UnixNano()})
		if storeErr != nil {
			t.Fatalf("An error occurred whilst attempting to store entry to node at index: %d", currentCount)
		}
		currentCount++
	}

	t.Log()
	t.Logf("@@@@FootholdBootstrapNode data store length pre store operation is: %d", footHoldBootstrapNode.DataStoreLength())

	//allow some time for the storage operation to complete and be propagated across the network
	t.Log("Allowing time for storage of entries to random standard nodes to propergate...")
	time.Sleep(15000 * time.Millisecond)

	t.Log()
	t.Logf("@@@@FootholdBootstrapNode data store length post store operation is: %d", footHoldBootstrapNode.DataStoreLength())

	//log the edge nodes peer list count BEFORE the find operation (hint: should be equal to 1)
	t.Logf("Edge bootstrap node peer list count BEFORE find operation: %d", entryNode.PeerCount())

	//attempt to find each sample data entry via our edge bootstrap node which is one level
	//of indirection removed from the core bootstrap nodes.
	findExecCount := 1
	for k, expectedVal := range *sampleData {

		//ensure that an entry is retreived for each provided key and further that it has the expected value.
		indexEntries, ok := externalNode.FindIndex(k)
		if !ok {
			t.Errorf("Failed to find entry key=%s at execution index: %d", k, findExecCount)
		}
		if len(indexEntries) < 1 {
			t.Errorf("Failed. The record was found but no index entries were returned for key=%s at execution index: %d", k, findExecCount)
		}
		if string(indexEntries[0].Target) != string(expectedVal) {
			t.Errorf("Wrong value for key=%s got=%q want=%q at execution index: %d", k, indexEntries[0].Target, expectedVal, findExecCount)
		}

		findExecCount++
	}

	//log the edge nodes peer list count AFTER the find operation, this will indicate
	//how much of the network the node has been able to automatically discover via the
	//DHT's internal, recursive lookup process.
	time.Sleep(5000 * time.Millisecond)
	t.Logf("Edge bootstrap node peer list count AFTER find operation: %d", entryNode.PeerCount())

	//clean up
	t.Cleanup(func() {
		entryNode.Close()
		externalNode.Close()
	})
}

func Test_Full_Network_Standard_Node_To_Standard_Node_Find_Standard_Entry(t *testing.T) {

	/**
	    The first of a series of tests that aims to test the end-to-end retreival
	    of data from one standard node from another. Of course each of the standard
		nodes will have a relatively narrow view of the network (a single direct connection
		to a core bootstrap node) Later tests will more closely model the production
		environment, where each standard node is indirectly connected to the core network
		via a chain of one or more ENTRY nodes.
	*/

	//prepare our core network, bootstrap node addresses.
	coreNetworkBootstrapNodeAddrs := []string{":7401", ":7402", ":7403", ":7404", ":7405"}

	//desired standard node count (we pick a number that is evenly divisiable by the number of core nodes
	// to simply the connection distibutation validation logic) we pick 20 here (4 standard nodes per core node)
	standardNodeMultiplier := 4
	standardNodeCount := standardNodeMultiplier * len(coreNetworkBootstrapNodeAddrs)

	//next call into our helper function to create a new configurable test context complete
	//with core bootstrap nodes AND 20 standard nodes. The function will attempt to evenly
	//distribute connections to the core nodes from these standard nodes.
	ctx := NewConfigurableTestContextWithBootstrapAddresses(t, standardNodeCount, nil, coreNetworkBootstrapNodeAddrs, -1, 300)

	//next we wait some time for the bootstrap process to complete on each node, by default
	//each node will wait 20 seconds before attempting to actually connect to the provided
	//bootstrap addresses
	time.Sleep(40000 * time.Millisecond)

	//grab reference to our (now hopefully bootstrapped nodes)
	n1 := ctx.BootstrapNodes[0]
	n2 := ctx.BootstrapNodes[1]
	n3 := ctx.BootstrapNodes[2]
	n4 := ctx.BootstrapNodes[3]
	n5 := ctx.BootstrapNodes[4]

	//next we validate that each bootstrap node has a full view of the core network and their respective
	//directly connected standard nodes by checking that each node has a peer list length
	//equal the the number of core nodes minus 1 (itself) plus the number of standard nodes
	//connected to it (which should be equal to the standardNodeMultiplier)
	expectedPeerListLength := (len(coreNetworkBootstrapNodeAddrs) - 1) + standardNodeMultiplier

	if len(n1.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 1 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n1.ListPeersAsString()))
	}

	if len(n2.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 2 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n2.ListPeersAsString()))
	}

	if len(n3.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 3 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n3.ListPeersAsString()))
	}

	if len(n4.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 4 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n4.ListPeersAsString()))
	}

	if len(n5.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 5 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n5.ListPeersAsString()))
	}

	//NEXT WE DEAL WITH STORING ENTRIES TO SOME RANDOM SUBSET OF THE STANDARD NODES IN THE NETWORK...

	//Firstly, here we define a helper function to pick random indexes according to the specified count.
	var selectRandomNodeIndexes func([]int, int, int) []int
	selectRandomNodeIndexes = func(picked []int, totalCount int, desiredCount int) []int {
		if desiredCount > totalCount {
			t.Fatal("Count exceeds available entries.")
		}
		if len(picked) == desiredCount {
			return picked
		}

		//select a random index between 0 and createdStandardNodeCount -1
		//and append it to our index
		randIdx := rand.Intn(totalCount - 1)
		if !slices.Contains(picked, randIdx) {
			picked = append(picked, randIdx)
		}
		return selectRandomNodeIndexes(picked, totalCount, desiredCount)
	}

	//next obtain references to the complete list of standard nodes from the test context.
	allStandardNodes := ctx.Nodes

	//var to hold the number of nodes we should randomly select
	randomNodeSelectionCount := 5

	//pick. small subset of stabdard nodes, at random, to store entries to.
	randomlySelectedIndexes := selectRandomNodeIndexes(make([]int, 0), len(allStandardNodes), randomNodeSelectionCount)
	fmt.Println()
	fmt.Printf("Selected random indexes for storage were: %v", randomlySelectedIndexes)

	//select nodes at the random indexes
	var randomlySelectedStandardNodes []*Node
	for _, currentSelIdx := range randomlySelectedIndexes {
		randomlySelectedStandardNodes = append(randomlySelectedStandardNodes, allStandardNodes[currentSelIdx])
	}

	//call into our helper function to pepare some sample data for us to store, we set the
	//sample entry count equal to the number of standard nodes we randomly selected for the
	//purposes of this test.
	sampleData := prepSampleEntryData(t, randomNodeSelectionCount)

	//iterate over the sample data, storing each entry to the corresponding
	//randomly selected node as the current index.
	curIdx := 0
	for k, v := range *sampleData {
		storageErr := randomlySelectedStandardNodes[curIdx].Store(k, v)
		if storageErr != nil {
			t.Fatalf("Failed to store entry with key: %s and value: %s on standard node: %s", k, v, randomlySelectedStandardNodes[curIdx].ID)
		}
		curIdx++
	}

	//allow some time for the storage operation to complete and be propagated across the network
	t.Log("Allowing time for storage of entries to random standard nodes to propergate...")
	time.Sleep(20000 * time.Millisecond)

	//next select a disjoint set of standard nodes to undertake the find operation, that is:
	//none of the nodes selected should have been used to store entries earlier as this would
	//obviously result in a short circuit of the lookup process; a node will always be
	//able to find an entry directly stored to itself..

	//to ensure we don't pick the same nodes we pass in the same list of randomly selected
	//standard nodes used for storage earlier as the exclusion list and then double the
	//desired node count.
	randomlySelectedIndexesForFind := selectRandomNodeIndexes(randomlySelectedIndexes, len(allStandardNodes), randomNodeSelectionCount*2)

	//next shift off the first randomNodeSelectionCount indexes from the list of randomly selected indexes for find.
	//to ensure we only have indexes that were NOT used for storage earlier.
	randomlySelectedIndexesForFind = randomlySelectedIndexesForFind[randomNodeSelectionCount:]
	fmt.Printf("Selected random indexes for find were: %v", randomlySelectedIndexesForFind)

	//next obtain references to the standard nodes at the selected indexes.
	var randomlySelectedStandardNodesForFind []*Node
	for _, currentSelIdx := range randomlySelectedIndexesForFind {
		randomlySelectedStandardNodesForFind = append(randomlySelectedStandardNodesForFind, allStandardNodes[currentSelIdx])
	}

	//now we have our disjoint set of standard nodes to undertake the find operation on each node.
	for _, curStandardNodeForFind := range randomlySelectedStandardNodesForFind {
		for k := range *sampleData {
			if val, ok := curStandardNodeForFind.Find(k); !ok && string(val) != k {
				t.Fatalf("FindRemote failed on standard node: %s for resource with key: %s", curStandardNodeForFind.ID, k)
			}
		}
	}

}

func Test_Full_Network_Standard_Node_To_Standard_Node_Find_Index_Entry(t *testing.T) {

	/**
	    The first of a series of tests that aims to test the end-to-end retreival
	    of data from one standard node from another. Of course each of the standard
		nodes will have a relatively narrow view of the network (a single direct connection
		to a core bootstrap node) Later tests will more closely model the production
		environment, where each standard node is indirectly connected to the core network
		via a chain of one or more ENTRY nodes.
	*/

	//prepare our core network, bootstrap node addresses.
	coreNetworkBootstrapNodeAddrs := []string{":7401", ":7402", ":7403", ":7404", ":7405"}

	//desired standard node count (we pick a number that is evenly divisiable by the number of core nodes
	// to simply the connection distibutation validation logic) we pick 20 here (4 standard nodes per core node)
	standardNodeMultiplier := 4
	standardNodeCount := standardNodeMultiplier * len(coreNetworkBootstrapNodeAddrs)

	//next call into our helper function to create a new configurable test context complete
	//with core bootstrap nodes AND 20 standard nodes. The function will attempt to evenly
	//distribute connections to the core nodes from these standard nodes.
	ctx := NewConfigurableTestContextWithBootstrapAddresses(t, standardNodeCount, nil, coreNetworkBootstrapNodeAddrs, -1, 300)

	//next we wait some time for the bootstrap process to complete on each node, by default
	//each node will wait 20 seconds before attempting to actually connect to the provided
	//bootstrap addresses
	time.Sleep(40000 * time.Millisecond)

	//grab reference to our (now hopefully bootstrapped nodes)
	n1 := ctx.BootstrapNodes[0]
	n2 := ctx.BootstrapNodes[1]
	n3 := ctx.BootstrapNodes[2]
	n4 := ctx.BootstrapNodes[3]
	n5 := ctx.BootstrapNodes[4]

	//next we validate that each bootstrap node has a full view of the core network and their respective
	//directly connected standard nodes by checking that each node has a peer list length
	//equal the the number of core nodes minus 1 (itself) plus the number of standard nodes
	//connected to it (which should be equal to the standardNodeMultiplier)
	expectedPeerListLength := (len(coreNetworkBootstrapNodeAddrs) - 1) + standardNodeMultiplier

	if len(n1.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 1 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n1.ListPeersAsString()))
	}

	if len(n2.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 2 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n2.ListPeersAsString()))
	}

	if len(n3.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 3 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n3.ListPeersAsString()))
	}

	if len(n4.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 4 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n4.ListPeersAsString()))
	}

	if len(n5.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 5 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n5.ListPeersAsString()))
	}

	//NEXT WE DEAL WITH STORING ENTRIES TO SOME RANDOM SUBSET OF THE STANDARD NODES IN THE NETWORK...

	//Firstly, here we define a helper function to pick random indexes according to the specified count.
	var selectRandomNodeIndexes func([]int, int, int) []int
	selectRandomNodeIndexes = func(picked []int, totalCount int, desiredCount int) []int {
		if desiredCount > totalCount {
			t.Fatal("Count exceeds available entries.")
		}
		if len(picked) == desiredCount {
			return picked
		}

		//select a random index between 0 and createdStandardNodeCount -1
		//and append it to our index
		randIdx := rand.Intn(totalCount - 1)
		if !slices.Contains(picked, randIdx) {
			picked = append(picked, randIdx)
		}
		return selectRandomNodeIndexes(picked, totalCount, desiredCount)
	}

	//next obtain references to the complete list of standard nodes from the test context.
	allStandardNodes := ctx.Nodes

	//var to hold the number of nodes we should randomly select
	randomNodeSelectionCount := 5

	//pick. small subset of stabdard nodes, at random, to store entries to.
	randomlySelectedIndexes := selectRandomNodeIndexes(make([]int, 0), len(allStandardNodes), randomNodeSelectionCount)
	fmt.Println()
	fmt.Printf("Selected random indexes for storage were: %v", randomlySelectedIndexes)

	//select nodes at the random indexes
	var randomlySelectedStandardNodes []*Node
	for _, currentSelIdx := range randomlySelectedIndexes {
		randomlySelectedStandardNodes = append(randomlySelectedStandardNodes, allStandardNodes[currentSelIdx])
	}

	//call into our helper function to pepare some sample data for us to store, we set the
	//sample entry count equal to the number of standard nodes we randomly selected for the
	//purposes of this test.
	sampleData := prepSampleEntryData(t, randomNodeSelectionCount)

	//iterate over the sample data, storing each entry to the corresponding
	//randomly selected node as the current index.
	curIdx := 0
	for k, v := range *sampleData {
		storageErr := randomlySelectedStandardNodes[curIdx].StoreIndex(k, IndexEntry{Source: k, Target: string(v), UpdatedUnix: time.Now().UnixNano()})
		if storageErr != nil {
			t.Fatalf("Failed to store entry with key: %s and value: %s on standard node: %s", k, v, randomlySelectedStandardNodes[curIdx].ID)
		}
		curIdx++
	}

	//allow some time for the storage operation to complete and be propagated across the network
	t.Log("Allowing time for storage of entries to random standard nodes to propergate...")
	time.Sleep(20000 * time.Millisecond)

	//next select a disjoint set of standard nodes to undertake the find operation, that is:
	//none of the nodes selected should have been used to store entries earlier as this would
	//obviously result in a short circuit of the lookup process; a node will always be
	//able to find an entry directly stored to itself..

	//to ensure we don't pick the same nodes we pass in the same list of randomly selected
	//standard nodes used for storage earlier as the exclusion list and then double the
	//desired node count.
	randomlySelectedIndexesForFind := selectRandomNodeIndexes(randomlySelectedIndexes, len(allStandardNodes), randomNodeSelectionCount*2)

	//next shift off the first randomNodeSelectionCount indexes from the list of randomly selected indexes for find.
	//to ensure we only have indexes that were NOT used for storage earlier.
	randomlySelectedIndexesForFind = randomlySelectedIndexesForFind[randomNodeSelectionCount:]
	fmt.Printf("Selected random indexes for find were: %v", randomlySelectedIndexesForFind)

	//next obtain references to the standard nodes at the selected indexes.
	var randomlySelectedStandardNodesForFind []*Node
	for _, currentSelIdx := range randomlySelectedIndexesForFind {
		randomlySelectedStandardNodesForFind = append(randomlySelectedStandardNodesForFind, allStandardNodes[currentSelIdx])
	}

	//now we have our disjoint set of standard nodes to undertake the find operation on each node.
	for _, curStandardNodeForFind := range randomlySelectedStandardNodesForFind {
		for k, v := range *sampleData {
			if indexEntries, ok := curStandardNodeForFind.FindIndex(k); !ok {
				t.Fatalf("FindIndex failed on standard node: %s for resource with key: %s", curStandardNodeForFind.ID, k)
			} else {
				//ensure that the returned index entries are not empty
				if len(indexEntries) == 0 {
					t.Fatalf("FindIndex returned empty index entries on standard node: %s for resource with key: %s", curStandardNodeForFind.ID, k)
				}
				//ensure that the returned index entry has the expected target value
				if string(indexEntries[0].Target) != string(v) {
					t.Fatalf("FindIndex returned wrong value on standard node: %s for resource with key: %s got: %s want: %s", curStandardNodeForFind.ID, k, indexEntries[0].Target, v)
				}
			}
		}
	}

}

func Test_Full_Network_Standard_Node_To_Standard_Node_Find_Standard_Entry_With_One_Level_Of_Indirection_And_Sparse_Peer_List(t *testing.T) {

	//prepare our core network, bootstrap node addresses.
	coreNetworkBootstrapNodeAddrs := []string{":7401", ":7402", ":7403", ":7404", ":7405"}

	//desired standard node count (we pick a number that is evenly divisiable by the number of core nodes
	// to simply the connection distibutation validation logic) we pick 20 here (4 standard nodes per core node)
	standardNodeMultiplier := 4
	standardNodeCount := standardNodeMultiplier * len(coreNetworkBootstrapNodeAddrs)

	//next call into our helper function to create a new configurable test context complete
	//with core bootstrap nodes AND 20 standard nodes. The function will attempt to evenly
	//distribute connections to the core nodes from these standard nodes.
	ctx := NewConfigurableTestContextWithBootstrapAddresses(t, standardNodeCount, nil, coreNetworkBootstrapNodeAddrs, -1, 300)

	//fmt.Printf("Replication factor is: %d", ctx.Config.ReplicationFactor)
	//next we wait some time for the bootstrap process to complete on each node, by default
	//each node will wait 20 seconds before attempting to actually connect to the provided
	//bootstrap addresses
	time.Sleep(40000 * time.Millisecond)

	//grab reference to our (now hopefully bootstrapped nodes)
	n1 := ctx.BootstrapNodes[0]
	n2 := ctx.BootstrapNodes[1]
	n3 := ctx.BootstrapNodes[2]
	n4 := ctx.BootstrapNodes[3]
	n5 := ctx.BootstrapNodes[4]

	//next we validate that each bootstrap node has a full view of the core network and their respective
	//directly connected standard nodes by checking that each node has a peer list length
	//equal the the number of core nodes minus 1 (itself) plus the number of standard nodes
	//connected to it (which should be equal to the standardNodeMultiplier)
	expectedPeerListLength := (len(coreNetworkBootstrapNodeAddrs) - 1) + standardNodeMultiplier

	if len(n1.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 1 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n1.ListPeersAsString()))
	}

	if len(n2.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 2 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n2.ListPeersAsString()))
	}

	if len(n3.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 3 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n3.ListPeersAsString()))
	}

	if len(n4.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 4 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n4.ListPeersAsString()))
	}

	if len(n5.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 5 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n5.ListPeersAsString()))
	}

	//OK next we add two brand new (ENTRY) nodes to the network, however we only connect them to a SINGLE
	//other bootstrap node, which will ultimately result in them both having a sparse peer list.
	entryNode1, _ := NewNode("entryNode1", ":1981", netx.NewTCP(), ctx.Config, NT_ENTRY)
	footHoldBootstrapNode := ctx.BootstrapNodes[rand.Intn(len(ctx.BootstrapNodes)-1)] //select another bootstrap node,at random, that this edge bootsrap node can use to get a foothold on the network
	entryNode1.Bootstrap([]string{footHoldBootstrapNode.Addr}, 7000)                  //after some nominal time has elapsed, attempt to bootstrap the edge node
	entryNode2, _ := NewNode("entryNode2", ":1982", netx.NewTCP(), ctx.Config, NT_ENTRY)
	footHoldBootstrapNode2 := ctx.BootstrapNodes[rand.Intn(len(ctx.BootstrapNodes)-1)] //select another bootstrap node,at random, that this edge bootsrap node can use to get a foothold on the network
	entryNode2.Bootstrap([]string{footHoldBootstrapNode2.Addr}, 7000)                  //after some nominal time has elapsed, attempt to bootstrap the edge node

	//wait for the bootstrap of our edge node to the network foothold node to complete,
	//we wait double the connect delay time.
	time.Sleep(14000 * time.Millisecond)

	//Next we pick a small subset of STANDARD nodes at random, to store the data to.
	targetStorageNodeCount := 3

	//stores our chosen indexes
	randomlySelectedNodeIndexes := make([]int, 0)

	//local function to choose some unqiue STANDARD nodes to store data to.
	//forward-leke declaration to allow the func to be called recursively.
	var uniqueRandomSelectionFunc func(int)
	uniqueRandomSelectionFunc = func(count int) {

		if len(randomlySelectedNodeIndexes) == count {
			return
		}

		randIdxVal := rand.Intn(len(ctx.Nodes) - 1)
		if !slices.Contains(randomlySelectedNodeIndexes, randIdxVal) {
			randomlySelectedNodeIndexes = append(randomlySelectedNodeIndexes, randIdxVal)
		}

		uniqueRandomSelectionFunc(count)

	}

	//call our local random node selection which will popuilate the above array of indexes.
	uniqueRandomSelectionFunc(targetStorageNodeCount)

	//before going any further check that our requested amount of standard node indexes
	//have been randomly selected
	if len(randomlySelectedNodeIndexes) != targetStorageNodeCount {
		t.Errorf("Incorrect number of storage node selected, expected: %d and actually got: %d", targetStorageNodeCount, len(randomlySelectedNodeIndexes))

	}

	//obtain reference to STANDARD nodes at our randomly selected indexes.
	randomlySelectedNodes := make([]*Node, 0)
	for _, currentIdx := range randomlySelectedNodeIndexes {
		randomlySelectedNodes = append(randomlySelectedNodes, ctx.Nodes[currentIdx])
	}

	//generate sample data that will be later stored to a small subset of STANDARD nodes.
	sampleData := prepSampleEntryData(t, targetStorageNodeCount)

	//since have our sample data count and seleted nodes are equal we may loop
	//over any of the two collection and store data to the corresponding indexes.
	currentCount := 0
	for k, v := range *sampleData {
		curNode := randomlySelectedNodes[currentCount]
		storeErr := curNode.Store(k, v)
		if storeErr != nil {
			t.Fatalf("An error occurred whilst attempting to store entry to node at index: %d the error was: %v", currentCount, storeErr)
		}
		currentCount++
	}

	t.Log()
	t.Logf("@@@@FootholdBootstrapNode data store length pre store operation is: %d", footHoldBootstrapNode.DataStoreLength())

	//allow some time for the storage operation to complete and be propagated across the network
	t.Log("Allowing time for storage of entries to random standard nodes to propergate...")
	time.Sleep(15000 * time.Millisecond)

	t.Log()
	t.Logf("@@@@FootholdBootstrapNode data store length post store operation is: %d", footHoldBootstrapNode.DataStoreLength())

	//log the edge nodes peer list count BEFORE the find operation (hint: should be equal to 1)
	entryNode1RemovalIdLog := make([]types.NodeID, 0)
	if entryNode1.PeerCount() > 1 {
		for _, peerId := range entryNode1.ListPeerIds() {
			//if the current id is NOT equal to nodes assigned foothold
			//node queue it for deletion.
			if peerId.String() != footHoldBootstrapNode.ID.String() {
				entryNode1RemovalIdLog = append(entryNode1RemovalIdLog, peerId)
			}
		}
	}

	entryNode2RemovalIdLog := make([]types.NodeID, 0)
	if entryNode2.PeerCount() > 1 {
		for _, peerId := range entryNode2.ListPeerIds() {
			//if the current id is NOT equal to nodes assigned foothold
			//node queue it for deletion.
			if peerId.String() != footHoldBootstrapNode2.ID.String() {
				entryNode2RemovalIdLog = append(entryNode2RemovalIdLog, peerId)
			}
		}
	}

	t.Log()
	t.Logf("Entry node 1 peer removal id log count: %d", len(entryNode1RemovalIdLog))
	t.Logf("Entry node 2 peer removal id log count: %d", len(entryNode2RemovalIdLog))
	for _, removeId := range entryNode1RemovalIdLog {
		entryNode1.DropPeer(removeId)
	}

	for _, removeId := range entryNode2RemovalIdLog {
		entryNode2.DropPeer(removeId)
	}

	t.Logf("Entry node 1 peer list count BEFORE find operation: %d", entryNode1.PeerCount())
	t.Logf("Entry node 2 peer list count BEFORE find operation: %d", entryNode2.PeerCount())
	//attempt to find each sample data entry via our edge, entry node which are one level
	//of indirection removed from the core bootstrap nodes.
	findExecCount := 1
	for k, expectedVal := range *sampleData {

		//ensure that an entry is retreived for each provided key
		//and further that it has the expected value.
		v, ok := entryNode1.Find(k)
		if !ok {
			t.Errorf("Failed to find entry key=%s at execution index: %d on edge node: %s", k, findExecCount, entryNode1.ID.String())
		}
		if string(v) != string(expectedVal) {
			t.Errorf("Wrong value for key=%s got=%q want=%q", k, v, expectedVal)
		}

		v2, ok2 := entryNode2.Find(k)
		if !ok2 {
			t.Errorf("Failed to find entry key=%s at execution index: %d on edge node: %s", k, findExecCount, entryNode2.ID.String())
		}
		if string(v2) != string(expectedVal) {
			t.Errorf("Wrong value for key=%s got=%q want=%q", k, v2, expectedVal)
		}

		findExecCount++
	}

	//log the edge nodes peer list count AFTER the find operation, this will indicate
	//how much of the network the node has been able to automatically discover via the
	//DHT's internal, recursive lookup process.
	time.Sleep(5000 * time.Millisecond)
	t.Logf("Entry node: %s peer list count AFTER find operation: %d", entryNode1.ID.String(), entryNode1.PeerCount())
	t.Logf("Entry node: %s peer list count AFTER find operation: %d", entryNode2.ID.String(), entryNode2.PeerCount())

	//clean up
	t.Cleanup(func() {
		entryNode1.Close()
		entryNode2.Close()
	})
}

func Test_Full_Network_Standard_Node_To_Standard_Node_Find_Index_Entry_With_One_Level_Of_Indirection_And_Sparse_Peer_List(t *testing.T) {

	//prepare our core network, bootstrap node addresses.
	coreNetworkBootstrapNodeAddrs := []string{":7401", ":7402", ":7403", ":7404", ":7405"}

	//desired standard node count (we pick a number that is evenly divisiable by the number of core nodes
	// to simply the connection distibutation validation logic) we pick 20 here (4 standard nodes per core node)
	standardNodeMultiplier := 4
	standardNodeCount := standardNodeMultiplier * len(coreNetworkBootstrapNodeAddrs)

	//next call into our helper function to create a new configurable test context complete
	//with core bootstrap nodes AND 20 standard nodes. The function will attempt to evenly
	//distribute connections to the core nodes from these standard nodes.
	ctx := NewConfigurableTestContextWithBootstrapAddresses(t, standardNodeCount, nil, coreNetworkBootstrapNodeAddrs, -1, 300)

	//fmt.Printf("Replication factor is: %d", ctx.Config.ReplicationFactor)
	//next we wait some time for the bootstrap process to complete on each node, by default
	//each node will wait 20 seconds before attempting to actually connect to the provided
	//bootstrap addresses
	time.Sleep(40000 * time.Millisecond)

	//grab reference to our (now hopefully bootstrapped nodes)
	n1 := ctx.BootstrapNodes[0]
	n2 := ctx.BootstrapNodes[1]
	n3 := ctx.BootstrapNodes[2]
	n4 := ctx.BootstrapNodes[3]
	n5 := ctx.BootstrapNodes[4]

	//next we validate that each bootstrap node has a full view of the core network and their respective
	//directly connected standard nodes by checking that each node has a peer list length
	//equal the the number of core nodes minus 1 (itself) plus the number of standard nodes
	//connected to it (which should be equal to the standardNodeMultiplier)
	expectedPeerListLength := (len(coreNetworkBootstrapNodeAddrs) - 1) + standardNodeMultiplier

	if len(n1.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 1 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n1.ListPeersAsString()))
	}

	if len(n2.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 2 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n2.ListPeersAsString()))
	}

	if len(n3.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 3 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n3.ListPeersAsString()))
	}

	if len(n4.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 4 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n4.ListPeersAsString()))
	}

	if len(n5.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 5 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n5.ListPeersAsString()))
	}

	//OK next we add two brand new (ENTRY) nodes to the network, however we only connect them to a SINGLE
	//other bootstrap node, which will ultimately result in them both having a sparse peer list.
	entryNode1, _ := NewNode("entryNode1", ":1981", netx.NewTCP(), ctx.Config, NT_ENTRY)
	footHoldBootstrapNode := ctx.BootstrapNodes[rand.Intn(len(ctx.BootstrapNodes)-1)] //select another bootstrap node,at random, that this edge bootsrap node can use to get a foothold on the network
	entryNode1.Bootstrap([]string{footHoldBootstrapNode.Addr}, 7000)                  //after some nominal time has elapsed, attempt to bootstrap the edge node
	entryNode2, _ := NewNode("entryNode2", ":1982", netx.NewTCP(), ctx.Config, NT_ENTRY)
	footHoldBootstrapNode2 := ctx.BootstrapNodes[rand.Intn(len(ctx.BootstrapNodes)-1)] //select another bootstrap node,at random, that this edge bootsrap node can use to get a foothold on the network
	entryNode2.Bootstrap([]string{footHoldBootstrapNode2.Addr}, 7000)                  //after some nominal time has elapsed, attempt to bootstrap the edge node

	//wait for the bootstrap of our edge node to the network foothold node to complete,
	//we wait double the connect delay time.
	time.Sleep(14000 * time.Millisecond)

	//Next we pick a small subset of STANDARD nodes at random, to store the data to.
	targetStorageNodeCount := 3

	//stores our chosen indexes
	randomlySelectedNodeIndexes := make([]int, 0)

	//local function to choose some unqiue STANDARD nodes to store data to.
	//forward-leke declaration to allow the func to be called recursively.
	var uniqueRandomSelectionFunc func(int)
	uniqueRandomSelectionFunc = func(count int) {

		if len(randomlySelectedNodeIndexes) == count {
			return
		}

		randIdxVal := rand.Intn(len(ctx.Nodes) - 1)
		if !slices.Contains(randomlySelectedNodeIndexes, randIdxVal) {
			randomlySelectedNodeIndexes = append(randomlySelectedNodeIndexes, randIdxVal)
		}

		uniqueRandomSelectionFunc(count)

	}

	//call our local random node selection which will popuilate the above array of indexes.
	uniqueRandomSelectionFunc(targetStorageNodeCount)

	//before going any further check that our requested amount of standard node indexes
	//have been randomly selected
	if len(randomlySelectedNodeIndexes) != targetStorageNodeCount {
		t.Errorf("Incorrect number of storage node selected, expected: %d and actually got: %d", targetStorageNodeCount, len(randomlySelectedNodeIndexes))

	}

	//obtain reference to STANDARD nodes at our randomly selected indexes.
	randomlySelectedNodes := make([]*Node, 0)
	for _, currentIdx := range randomlySelectedNodeIndexes {
		randomlySelectedNodes = append(randomlySelectedNodes, ctx.Nodes[currentIdx])
	}

	//generate sample data that will be later stored to a small subset of STANDARD nodes.
	sampleData := prepSampleEntryData(t, targetStorageNodeCount)

	//since have our sample data count and seleted nodes are equal we may loop
	//over any of the two collection and store data to the corresponding indexes.
	currentCount := 0
	for k, v := range *sampleData {
		curNode := randomlySelectedNodes[currentCount]
		storeErr := curNode.StoreIndex(k, IndexEntry{Source: k, Target: string(v), UpdatedUnix: time.Now().UnixNano()})
		if storeErr != nil {
			t.Fatalf("An error occurred whilst attempting to store entry to node at index: %d the error was: %v", currentCount, storeErr)
		}
		currentCount++
	}

	t.Log()
	t.Logf("@@@@FootholdBootstrapNode data store length pre store operation is: %d", footHoldBootstrapNode.DataStoreLength())

	//allow some time for the storage operation to complete and be propagated across the network
	t.Log("Allowing time for storage of entries to random standard nodes to propergate...")
	time.Sleep(15000 * time.Millisecond)

	t.Log()
	t.Logf("@@@@FootholdBootstrapNode data store length post store operation is: %d", footHoldBootstrapNode.DataStoreLength())

	//log the edge nodes peer list count BEFORE the find operation (hint: should be equal to 1)
	entryNode1RemovalIdLog := make([]types.NodeID, 0)
	if entryNode1.PeerCount() > 1 {
		for _, peerId := range entryNode1.ListPeerIds() {
			//if the current id is NOT equal to nodes assigned foothold
			//node queue it for deletion.
			if peerId.String() != footHoldBootstrapNode.ID.String() {
				entryNode1RemovalIdLog = append(entryNode1RemovalIdLog, peerId)
			}
		}
	}

	entryNode2RemovalIdLog := make([]types.NodeID, 0)
	if entryNode2.PeerCount() > 1 {
		for _, peerId := range entryNode2.ListPeerIds() {
			//if the current id is NOT equal to nodes assigned foothold
			//node queue it for deletion.
			if peerId.String() != footHoldBootstrapNode2.ID.String() {
				entryNode2RemovalIdLog = append(entryNode2RemovalIdLog, peerId)
			}
		}
	}

	t.Log()
	t.Logf("Entry node 1 peer removal id log count: %d", len(entryNode1RemovalIdLog))
	t.Logf("Entry node 2 peer removal id log count: %d", len(entryNode2RemovalIdLog))
	for _, removeId := range entryNode1RemovalIdLog {
		entryNode1.DropPeer(removeId)
	}

	for _, removeId := range entryNode2RemovalIdLog {
		entryNode2.DropPeer(removeId)
	}

	t.Logf("Entry node 1 peer list count BEFORE find operation: %d", entryNode1.PeerCount())
	t.Logf("Entry node 2 peer list count BEFORE find operation: %d", entryNode2.PeerCount())
	//attempt to find each sample data entry via our edge, entry node which are one level
	//of indirection removed from the core bootstrap nodes.
	findExecCount := 1
	for k, expectedVal := range *sampleData {

		//ensure that an entry is retreived for each provided key
		//and further that it has the expected value.
		indexEntries, ok := entryNode1.FindIndex(k)
		if !ok {
			t.Errorf("Failed to find entry key=%s at execution index: %d on edge node: %s", k, findExecCount, entryNode1.ID.String())
		}
		if len(indexEntries) == 0 {
			t.Errorf("Found no index entries for key=%s at execution index: %d on edge node: %s", k, findExecCount, entryNode1.ID.String())
		}
		if string(indexEntries[0].Target) != string(expectedVal) {
			t.Errorf("Wrong value for key=%s got=%q want=%q", k, indexEntries[0].Target, expectedVal)
		}

		indexEntries2, ok2 := entryNode2.FindIndex(k)
		if !ok2 {
			t.Errorf("Failed to find entry key=%s at execution index: %d on edge node: %s", k, findExecCount, entryNode2.ID.String())
		}
		if len(indexEntries2) == 0 {
			t.Errorf("Found no index entries for key=%s at execution index: %d on edge node: %s", k, findExecCount, entryNode2.ID.String())
		}
		if string(indexEntries2[0].Target) != string(expectedVal) {
			t.Errorf("Wrong value for key=%s got=%q want=%q", k, indexEntries2[0].Target, expectedVal)
		}

		findExecCount++
	}

	//log the edge nodes peer list count AFTER the find operation, this will indicate
	//how much of the network the node has been able to automatically discover via the
	//DHT's internal, recursive lookup process.
	time.Sleep(5000 * time.Millisecond)
	t.Logf("Entry node: %s peer list count AFTER find operation: %d", entryNode1.ID.String(), entryNode1.PeerCount())
	t.Logf("Entry node: %s peer list count AFTER find operation: %d", entryNode2.ID.String(), entryNode2.PeerCount())

	//clean up
	t.Cleanup(func() {
		entryNode1.Close()
		entryNode2.Close()
	})
}

func Test_Full_Network_Standard_Node_To_Standard_Node_Find_Standard_Entry_With_Two_Level_Of_Indirection_And_Sparse_Peer_List(t *testing.T) {

	//prepare our core network, bootstrap node addresses.
	coreNetworkBootstrapNodeAddrs := []string{":7401", ":7402", ":7403", ":7404", ":7405"}

	//desired standard node count (we pick a number that is evenly divisiable by the number of core nodes
	// to simply the connection distibutation validation logic) we pick 20 here (4 standard nodes per core node)
	standardNodeMultiplier := 4
	standardNodeCount := standardNodeMultiplier * len(coreNetworkBootstrapNodeAddrs)

	//next call into our helper function to create a new configurable test context complete
	//with core bootstrap nodes AND 20 standard nodes. The function will attempt to evenly
	//distribute connections to the core nodes from these standard nodes.
	ctx := NewConfigurableTestContextWithBootstrapAddresses(t, standardNodeCount, nil, coreNetworkBootstrapNodeAddrs, -1, 300)

	//fmt.Printf("Replication factor is: %d", ctx.Config.ReplicationFactor)
	//next we wait some time for the bootstrap process to complete on each node, by default
	//each node will wait 20 seconds before attempting to actually connect to the provided
	//bootstrap addresses
	time.Sleep(40000 * time.Millisecond)

	//grab reference to our (now hopefully bootstrapped nodes)
	n1 := ctx.BootstrapNodes[0]
	n2 := ctx.BootstrapNodes[1]
	n3 := ctx.BootstrapNodes[2]
	n4 := ctx.BootstrapNodes[3]
	n5 := ctx.BootstrapNodes[4]

	//next we validate that each bootstrap node has a full view of the core network and their respective
	//directly connected standard nodes by checking that each node has a peer list length
	//equal the the number of core nodes minus 1 (itself) plus the number of standard nodes
	//connected to it (which should be equal to the standardNodeMultiplier)
	expectedPeerListLength := (len(coreNetworkBootstrapNodeAddrs) - 1) + standardNodeMultiplier

	if len(n1.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 1 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n1.ListPeersAsString()))
	}

	if len(n2.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 2 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n2.ListPeersAsString()))
	}

	if len(n3.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 3 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n3.ListPeersAsString()))
	}

	if len(n4.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 4 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n4.ListPeersAsString()))
	}

	if len(n5.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 5 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n5.ListPeersAsString()))
	}

	//OK next we add two brand new (ENTRY) nodes to the network, however we only connect them to a SINGLE
	//other bootstrap node, which will ultimately result in them both having a sparse peer list.
	entryNode1, _ := NewNode("entryNode1", ":1981", netx.NewTCP(), ctx.Config, NT_ENTRY)
	footHoldBootstrapNode := ctx.BootstrapNodes[rand.Intn(len(ctx.BootstrapNodes)-1)] //select another bootstrap node,at random, that this edge bootsrap node can use to get a foothold on the network
	entryNode1.Bootstrap([]string{footHoldBootstrapNode.Addr}, 7000)                  //after some nominal time has elapsed, attempt to bootstrap the edge node
	entryNode2, _ := NewNode("entryNode2", ":1982", netx.NewTCP(), ctx.Config, NT_ENTRY)
	footHoldBootstrapNode2 := ctx.BootstrapNodes[rand.Intn(len(ctx.BootstrapNodes)-1)] //select another bootstrap node,at random, that this edge bootsrap node can use to get a foothold on the network
	entryNode2.Bootstrap([]string{footHoldBootstrapNode2.Addr}, 7000)                  //after some nominal time has elapsed, attempt to bootstrap the edge node

	//finally add our EXTERNAL nodes which will bootstrap via the ENTRY nodes created above.
	externalNode1, _ := NewNode("externalNode1", ":1983", netx.NewTCP(), ctx.Config, NT_EXTERNAL)
	externalNode1.Bootstrap([]string{entryNode1.Addr}, 7000)
	externalNode2, _ := NewNode("externalNode2", ":1984", netx.NewTCP(), ctx.Config, NT_EXTERNAL)
	externalNode2.Bootstrap([]string{entryNode2.Addr}, 7000)

	//wait for the bootstrap of our edge node to the network foothold node to complete,
	//we wait double the connect delay time.
	time.Sleep(14000 * time.Millisecond)

	//Next we pick a small subset of STANDARD nodes at random, to store the data to.
	targetStorageNodeCount := 3

	//stores our chosen indexes
	randomlySelectedNodeIndexes := make([]int, 0)

	//local function to choose some unqiue STANDARD nodes to store data to.
	//forward-leke declaration to allow the func to be called recursively.
	var uniqueRandomSelectionFunc func(int)
	uniqueRandomSelectionFunc = func(count int) {

		if len(randomlySelectedNodeIndexes) == count {
			return
		}

		randIdxVal := rand.Intn(len(ctx.Nodes) - 1)
		if !slices.Contains(randomlySelectedNodeIndexes, randIdxVal) {
			randomlySelectedNodeIndexes = append(randomlySelectedNodeIndexes, randIdxVal)
		}

		uniqueRandomSelectionFunc(count)

	}

	//call our local random node selection which will popuilate the above array of indexes.
	uniqueRandomSelectionFunc(targetStorageNodeCount)

	//before going any further check that our requested amount of standard node indexes
	//have been randomly selected
	if len(randomlySelectedNodeIndexes) != targetStorageNodeCount {
		t.Errorf("Incorrect number of storage node selected, expected: %d and actually got: %d", targetStorageNodeCount, len(randomlySelectedNodeIndexes))

	}

	//obtain reference to STANDARD nodes at our randomly selected indexes.
	randomlySelectedNodes := make([]*Node, 0)
	for _, currentIdx := range randomlySelectedNodeIndexes {
		randomlySelectedNodes = append(randomlySelectedNodes, ctx.Nodes[currentIdx])
	}

	//generate sample data that will be later stored to a small subset of STANDARD nodes.
	sampleData := prepSampleEntryData(t, targetStorageNodeCount)

	//since have our sample data count and seleted nodes are equal we may loop
	//over any of the two collection and store data to the corresponding indexes.
	currentCount := 0
	for k, v := range *sampleData {
		curNode := randomlySelectedNodes[currentCount]
		storeErr := curNode.Store(k, v)
		if storeErr != nil {
			t.Fatalf("An error occurred whilst attempting to store entry to node at index: %d", currentCount)
		}
		currentCount++
	}

	t.Log()
	t.Logf("@@@@FootholdBootstrapNode data store length pre store operation is: %d", footHoldBootstrapNode.DataStoreLength())

	//allow some time for the storage operation to complete and be propagated across the network
	t.Log("Allowing time for storage of entries to random standard nodes to propergate...")
	time.Sleep(15000 * time.Millisecond)

	t.Log()
	t.Logf("@@@@FootholdBootstrapNode data store length post store operation is: %d", footHoldBootstrapNode.DataStoreLength())

	//just incase the store operation resulted in propegation of peer info to our entry nodes
	//we now forcibly prune their peer lists back to just the foothold bootstrap node.
	entryNode1RemovalIdLog := make([]types.NodeID, 0)
	if entryNode1.PeerCount() > 1 {
		for _, peerId := range entryNode1.ListPeerIds() {
			//if the current id is NOT equal to nodes assigned foothold
			//node queue it for deletion.
			if peerId.String() != footHoldBootstrapNode.ID.String() {
				entryNode1RemovalIdLog = append(entryNode1RemovalIdLog, peerId)
			}
		}
	}

	entryNode2RemovalIdLog := make([]types.NodeID, 0)
	if entryNode2.PeerCount() > 1 {
		for _, peerId := range entryNode2.ListPeerIds() {
			//if the current id is NOT equal to nodes assigned foothold
			//node queue it for deletion.
			if peerId.String() != footHoldBootstrapNode2.ID.String() {
				entryNode2RemovalIdLog = append(entryNode2RemovalIdLog, peerId)
			}
		}
	}

	t.Log()
	t.Logf("Entry node 1 peer removal id log count: %d", len(entryNode1RemovalIdLog))
	t.Logf("Entry node 2 peer removal id log count: %d", len(entryNode2RemovalIdLog))
	for _, removeId := range entryNode1RemovalIdLog {
		entryNode1.DropPeer(removeId)
	}

	for _, removeId := range entryNode2RemovalIdLog {
		entryNode2.DropPeer(removeId)
	}

	//log the edge nodes peer list count BEFORE the find operation (hint: should be equal to 1)
	t.Logf("Entry node 1 peer list count BEFORE find operation: %d", entryNode1.PeerCount())
	t.Logf("Entry node 2 peer list count BEFORE find operation: %d", entryNode2.PeerCount())

	//next do the same for the external nodes
	externalNode1RemovalIdLog := make([]types.NodeID, 0)
	if externalNode1.PeerCount() > 1 {
		for _, peerId := range externalNode1.ListPeerIds() {
			//if the current id is NOT equal to nodes assigned foothold
			//node queue it for deletion.
			if peerId.String() != entryNode1.ID.String() {
				externalNode1RemovalIdLog = append(externalNode1RemovalIdLog, peerId)
			}
		}
	}

	externalNode2RemovalIdLog := make([]types.NodeID, 0)
	if externalNode2.PeerCount() > 1 {
		for _, peerId := range externalNode2.ListPeerIds() {
			//if the current id is NOT equal to nodes assigned foothold
			//node queue it for deletion.
			if peerId.String() != entryNode2.ID.String() {
				externalNode2RemovalIdLog = append(externalNode2RemovalIdLog, peerId)
			}
		}
	}

	t.Log()
	t.Logf("External node 1 peer removal id log count: %d", len(externalNode1RemovalIdLog))
	t.Logf("External node 2 peer removal id log count: %d", len(externalNode2RemovalIdLog))
	for _, removeId := range externalNode1RemovalIdLog {
		externalNode1.DropPeer(removeId)
	}

	for _, removeId := range externalNode2RemovalIdLog {
		externalNode2.DropPeer(removeId)
	}

	t.Logf("External node 1 peer list count BEFORE find operation: %d", externalNode1.PeerCount())
	t.Logf("External node 2 peer list count BEFORE find operation: %d", externalNode2.PeerCount())

	//attempt to find each sample data entry via our edge, entry node which are one level
	//of indirection removed from the core bootstrap nodes.
	findExecCount := 1
	for k, expectedVal := range *sampleData {

		//ensure that an entry is retreived for each provided key
		//and further that it has the expected value.
		v, ok := externalNode1.Find(k)
		if !ok {
			t.Errorf("Failed to find entry key=%s at execution index: %d on edge node: %s", k, findExecCount, externalNode1.ID.String())
		}
		if string(v) != string(expectedVal) {
			t.Errorf("Wrong value for key=%s got=%q want=%q", k, v, expectedVal)
		}

		v2, ok2 := externalNode2.Find(k)
		if !ok2 {
			t.Errorf("Failed to find entry key=%s at execution index: %d on edge node: %s", k, findExecCount, externalNode2.ID.String())
		}
		if string(v2) != string(expectedVal) {
			t.Errorf("Wrong value for key=%s got=%q want=%q", k, v2, expectedVal)
		}

		findExecCount++
	}

	//log the edge nodes peer list count AFTER the find operation, this will indicate
	//how much of the network the node has been able to automatically discover via the
	//DHT's internal, recursive lookup process.
	time.Sleep(5000 * time.Millisecond)
	t.Logf("External node: %s peer list count AFTER find operation: %d", externalNode1.ID.String(), externalNode1.PeerCount())
	t.Logf("External node: %s peer list count AFTER find operation: %d", externalNode2.ID.String(), externalNode2.PeerCount())

	//clean up
	t.Cleanup(func() {
		entryNode1.Close()
		entryNode2.Close()
		externalNode1.Close()
		externalNode2.Close()
	})
}

func Test_Full_Network_Standard_Node_To_Standard_Node_Find_Index_Entry_With_Two_Level_Of_Indirection_And_Sparse_Peer_List(t *testing.T) {

	//prepare our core network, bootstrap node addresses.
	coreNetworkBootstrapNodeAddrs := []string{":7401", ":7402", ":7403", ":7404", ":7405"}

	//desired standard node count (we pick a number that is evenly divisiable by the number of core nodes
	// to simply the connection distibutation validation logic) we pick 20 here (4 standard nodes per core node)
	standardNodeMultiplier := 4
	standardNodeCount := standardNodeMultiplier * len(coreNetworkBootstrapNodeAddrs)

	//next call into our helper function to create a new configurable test context complete
	//with core bootstrap nodes AND 20 standard nodes. The function will attempt to evenly
	//distribute connections to the core nodes from these standard nodes.
	ctx := NewConfigurableTestContextWithBootstrapAddresses(t, standardNodeCount, nil, coreNetworkBootstrapNodeAddrs, -1, 300)

	//fmt.Printf("Replication factor is: %d", ctx.Config.ReplicationFactor)
	//next we wait some time for the bootstrap process to complete on each node, by default
	//each node will wait 20 seconds before attempting to actually connect to the provided
	//bootstrap addresses
	time.Sleep(40000 * time.Millisecond)

	//grab reference to our (now hopefully bootstrapped nodes)
	n1 := ctx.BootstrapNodes[0]
	n2 := ctx.BootstrapNodes[1]
	n3 := ctx.BootstrapNodes[2]
	n4 := ctx.BootstrapNodes[3]
	n5 := ctx.BootstrapNodes[4]

	//next we validate that each bootstrap node has a full view of the core network and their respective
	//directly connected standard nodes by checking that each node has a peer list length
	//equal the the number of core nodes minus 1 (itself) plus the number of standard nodes
	//connected to it (which should be equal to the standardNodeMultiplier)
	expectedPeerListLength := (len(coreNetworkBootstrapNodeAddrs) - 1) + standardNodeMultiplier

	if len(n1.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 1 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n1.ListPeersAsString()))
	}

	if len(n2.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 2 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n2.ListPeersAsString()))
	}

	if len(n3.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 3 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n3.ListPeersAsString()))
	}

	if len(n4.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 4 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n4.ListPeersAsString()))
	}

	if len(n5.ListPeersAsString()) != expectedPeerListLength {
		t.Fatalf("Expected Node 5 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n5.ListPeersAsString()))
	}

	//OK next we add two brand new (ENTRY) nodes to the network, however we only connect them to a SINGLE
	//other bootstrap node, which will ultimately result in them both having a sparse peer list.
	entryNode1, _ := NewNode("entryNode1", ":1981", netx.NewTCP(), ctx.Config, NT_ENTRY)
	footHoldBootstrapNode := ctx.BootstrapNodes[rand.Intn(len(ctx.BootstrapNodes)-1)] //select another bootstrap node,at random, that this edge bootsrap node can use to get a foothold on the network
	entryNode1.Bootstrap([]string{footHoldBootstrapNode.Addr}, 7000)                  //after some nominal time has elapsed, attempt to bootstrap the edge node
	entryNode2, _ := NewNode("entryNode2", ":1982", netx.NewTCP(), ctx.Config, NT_ENTRY)
	footHoldBootstrapNode2 := ctx.BootstrapNodes[rand.Intn(len(ctx.BootstrapNodes)-1)] //select another bootstrap node,at random, that this edge bootsrap node can use to get a foothold on the network
	entryNode2.Bootstrap([]string{footHoldBootstrapNode2.Addr}, 7000)                  //after some nominal time has elapsed, attempt to bootstrap the edge node

	//finally add our EXTERNAL nodes which will bootstrap via the ENTRY nodes created above.
	externalNode1, _ := NewNode("externalNode1", ":1983", netx.NewTCP(), ctx.Config, NT_EXTERNAL)
	externalNode1.Bootstrap([]string{entryNode1.Addr}, 7000)
	externalNode2, _ := NewNode("externalNode2", ":1984", netx.NewTCP(), ctx.Config, NT_EXTERNAL)
	externalNode2.Bootstrap([]string{entryNode2.Addr}, 7000)

	//wait for the bootstrap of our edge node to the network foothold node to complete,
	//we wait double the connect delay time.
	time.Sleep(14000 * time.Millisecond)

	//Next we pick a small subset of STANDARD nodes at random, to store the data to.
	targetStorageNodeCount := 3

	//stores our chosen indexes
	randomlySelectedNodeIndexes := make([]int, 0)

	//local function to choose some unqiue STANDARD nodes to store data to.
	//forward-leke declaration to allow the func to be called recursively.
	var uniqueRandomSelectionFunc func(int)
	uniqueRandomSelectionFunc = func(count int) {

		if len(randomlySelectedNodeIndexes) == count {
			return
		}

		randIdxVal := rand.Intn(len(ctx.Nodes) - 1)
		if !slices.Contains(randomlySelectedNodeIndexes, randIdxVal) {
			randomlySelectedNodeIndexes = append(randomlySelectedNodeIndexes, randIdxVal)
		}

		uniqueRandomSelectionFunc(count)

	}

	//call our local random node selection which will popuilate the above array of indexes.
	uniqueRandomSelectionFunc(targetStorageNodeCount)

	//before going any further check that our requested amount of standard node indexes
	//have been randomly selected
	if len(randomlySelectedNodeIndexes) != targetStorageNodeCount {
		t.Errorf("Incorrect number of storage node selected, expected: %d and actually got: %d", targetStorageNodeCount, len(randomlySelectedNodeIndexes))

	}

	//obtain reference to STANDARD nodes at our randomly selected indexes.
	randomlySelectedNodes := make([]*Node, 0)
	for _, currentIdx := range randomlySelectedNodeIndexes {
		randomlySelectedNodes = append(randomlySelectedNodes, ctx.Nodes[currentIdx])
	}

	//generate sample data that will be later stored to a small subset of STANDARD nodes.
	sampleData := prepSampleEntryData(t, targetStorageNodeCount)

	//since have our sample data count and seleted nodes are equal we may loop
	//over any of the two collection and store data to the corresponding indexes.
	currentCount := 0
	for k, v := range *sampleData {
		curNode := randomlySelectedNodes[currentCount]
		storeErr := curNode.StoreIndex(k, IndexEntry{Source: k, Target: string(v), UpdatedUnix: time.Now().UnixNano()})
		if storeErr != nil {
			t.Fatalf("An error occurred whilst attempting to store entry to node at index: %d", currentCount)
		}
		currentCount++
	}

	t.Log()
	t.Logf("@@@@FootholdBootstrapNode data store length pre store operation is: %d", footHoldBootstrapNode.DataStoreLength())

	//allow some time for the storage operation to complete and be propagated across the network
	t.Log("Allowing time for storage of entries to random standard nodes to propergate...")
	time.Sleep(15000 * time.Millisecond)

	t.Log()
	t.Logf("@@@@FootholdBootstrapNode data store length post store operation is: %d", footHoldBootstrapNode.DataStoreLength())

	//just incase the store operation resulted in propegation of peer info to our entry nodes
	//we now forcibly prune their peer lists back to just the foothold bootstrap node.
	entryNode1RemovalIdLog := make([]types.NodeID, 0)
	if entryNode1.PeerCount() > 1 {
		for _, peerId := range entryNode1.ListPeerIds() {
			//if the current id is NOT equal to nodes assigned foothold
			//node queue it for deletion.
			if peerId.String() != footHoldBootstrapNode.ID.String() {
				entryNode1RemovalIdLog = append(entryNode1RemovalIdLog, peerId)
			}
		}
	}

	entryNode2RemovalIdLog := make([]types.NodeID, 0)
	if entryNode2.PeerCount() > 1 {
		for _, peerId := range entryNode2.ListPeerIds() {
			//if the current id is NOT equal to nodes assigned foothold
			//node queue it for deletion.
			if peerId.String() != footHoldBootstrapNode2.ID.String() {
				entryNode2RemovalIdLog = append(entryNode2RemovalIdLog, peerId)
			}
		}
	}

	t.Log()
	t.Logf("Entry node 1 peer removal id log count: %d", len(entryNode1RemovalIdLog))
	t.Logf("Entry node 2 peer removal id log count: %d", len(entryNode2RemovalIdLog))
	for _, removeId := range entryNode1RemovalIdLog {
		entryNode1.DropPeer(removeId)
	}

	for _, removeId := range entryNode2RemovalIdLog {
		entryNode2.DropPeer(removeId)
	}

	//log the edge nodes peer list count BEFORE the find operation (hint: should be equal to 1)
	t.Logf("Entry node 1 peer list count BEFORE find operation: %d", entryNode1.PeerCount())
	t.Logf("Entry node 2 peer list count BEFORE find operation: %d", entryNode2.PeerCount())

	//next do the same for the external nodes
	externalNode1RemovalIdLog := make([]types.NodeID, 0)
	if externalNode1.PeerCount() > 1 {
		for _, peerId := range externalNode1.ListPeerIds() {
			//if the current id is NOT equal to nodes assigned foothold
			//node queue it for deletion.
			if peerId.String() != entryNode1.ID.String() {
				externalNode1RemovalIdLog = append(externalNode1RemovalIdLog, peerId)
			}
		}
	}

	externalNode2RemovalIdLog := make([]types.NodeID, 0)
	if externalNode2.PeerCount() > 1 {
		for _, peerId := range externalNode2.ListPeerIds() {
			//if the current id is NOT equal to nodes assigned foothold
			//node queue it for deletion.
			if peerId.String() != entryNode2.ID.String() {
				externalNode2RemovalIdLog = append(externalNode2RemovalIdLog, peerId)
			}
		}
	}

	t.Log()
	t.Logf("External node 1 peer removal id log count: %d", len(externalNode1RemovalIdLog))
	t.Logf("External node 2 peer removal id log count: %d", len(externalNode2RemovalIdLog))
	for _, removeId := range externalNode1RemovalIdLog {
		externalNode1.DropPeer(removeId)
	}

	for _, removeId := range externalNode2RemovalIdLog {
		externalNode2.DropPeer(removeId)
	}

	t.Logf("External node 1 peer list count BEFORE find operation: %d", externalNode1.PeerCount())
	t.Logf("External node 2 peer list count BEFORE find operation: %d", externalNode2.PeerCount())

	//attempt to find each sample data entry via our edge, entry node which are one level
	//of indirection removed from the core bootstrap nodes.
	findExecCount := 1
	for k, expectedVal := range *sampleData {

		//ensure that an entry is retreived for each provided key
		//and further that it has the expected value.
		indexEntries, ok := externalNode1.FindIndex(k)
		if !ok {
			t.Errorf("Failed to find entry key=%s at execution index: %d on edge node: %s", k, findExecCount, externalNode1.ID.String())
		}
		if len(indexEntries) == 0 {
			t.Errorf("Found no index entries for key=%s at execution index: %d on edge node: %s", k, findExecCount, externalNode1.ID.String())
		}
		if string(indexEntries[0].Target) != string(expectedVal) {
			t.Errorf("Wrong value for key=%s got=%q want=%q", k, indexEntries[0].Target, expectedVal)
		}

		indexEntries2, ok2 := externalNode2.FindIndex(k)
		if !ok2 {
			t.Errorf("Failed to find entry key=%s at execution index: %d on edge node: %s", k, findExecCount, externalNode2.ID.String())
		}
		if len(indexEntries2) == 0 {
			t.Errorf("Found no index entries for key=%s at execution index: %d on edge node: %s", k, findExecCount, externalNode2.ID.String())
		}
		if string(indexEntries2[0].Target) != string(expectedVal) {
			t.Errorf("Wrong value for key=%s got=%q want=%q", k, indexEntries2[0].Target, expectedVal)
		}

		findExecCount++
	}

	//log the edge nodes peer list count AFTER the find operation, this will indicate
	//how much of the network the node has been able to automatically discover via the
	//DHT's internal, recursive lookup process.
	time.Sleep(5000 * time.Millisecond)
	t.Logf("External node: %s peer list count AFTER find operation: %d", externalNode1.ID.String(), externalNode1.PeerCount())
	t.Logf("External node: %s peer list count AFTER find operation: %d", externalNode2.ID.String(), externalNode2.PeerCount())

	//clean up
	t.Cleanup(func() {
		entryNode1.Close()
		entryNode2.Close()
		externalNode1.Close()
		externalNode2.Close()
	})
}

/*****************************************************************************************************************
 *                                     HELPER/UTILITY TYPES AND FUNCTIONS FOR E2E TESTS
 ******************************************************************************************************************/

// TestContext is used to hold context info for e2e tests
type TestContext struct {
	Nodes          []*Node
	Config         *config.Config
	BootstrapNodes []*Node
}

// NewDefaultTestContext creates a new default test context with two connected nodes
func NewDefaultTestContext(t *testing.T) *TestContext {
	t.Helper()

	//prepare config.
	cfg := config.GetDefaultSingletonInstance()
	cfg.UseProtobuf = true // set true after generating pb
	cfg.RequestTimeout = 2000 * time.Millisecond
	cfg.DefaultEntryTTL = 30 * time.Second
	cfg.RefreshInterval = 5 * time.Second
	cfg.JanitorInterval = 10 * time.Second

	//create nodes
	n1, _ := NewNode("node1", ":9321", netx.NewTCP(), cfg, NT_CORE)
	n2, _ := NewNode("node2", ":9322", netx.NewTCP(), cfg, NT_CORE)
	Nodes := []*Node{n1, n2}

	//we now bootstrap via the connect public interface method.
	if err := n1.Connect(n2.Addr); err != nil {
		t.Fatal("Error occurred whilst Peer Node 1 was trying to connect to Peer Node 2:", err)
	}

	//finally defer context cleanup
	t.Cleanup(func() {
		for _, n := range Nodes {
			n.Close()
		}

	})

	//return the context.
	return &TestContext{
		Config: cfg,
		Nodes:  Nodes,
	}
}

func NewConfigurableTestContext(t *testing.T, nodeCount int, conf *config.Config, printPeerMap bool) *TestContext {
	t.Helper()

	var cfg *config.Config
	if conf == nil {
		//prepare default config if a config has not been provided
		cfg = config.GetDefaultSingletonInstance()
		cfg.UseProtobuf = true
		cfg.RequestTimeout = 2000 * time.Millisecond
		cfg.DefaultEntryTTL = 30 * time.Second
		cfg.RefreshInterval = 5 * time.Second
		cfg.JanitorInterval = 10 * time.Second

	} else {
		cfg = conf
	}

	//attempt to create the requested number of nodes specified via the node count.
	Nodes := make([]*Node, 0)
	startingIP := 8999
	for i := 0; i < nodeCount; i++ {
		startingIP++
		nodeIP := startingIP + 1
		nodeNameStr := "node" + strconv.Itoa(i+1)
		nodeIpStr := strconv.Itoa(nodeIP)
		node, _ := NewNode(nodeNameStr, ":"+nodeIpStr, netx.NewTCP(), cfg, NT_CORE)
		Nodes = append(Nodes, node)
	}

	//connect each node to every other node to ensure full mesh connectivity, this only needs to be done from one side
	//as the Connect method will handle the bi-directional peer addition.
	for i := 0; i < nodeCount; i++ {
		for j := i + 1; j < nodeCount; j++ {
			if err := Nodes[i].Connect(Nodes[j].Addr); err != nil {
				t.Fatalf("Error occurred whilst Peer Node %d was trying to connect to Peer Node %d: %v", i+1, j+1, err)
			}
		}
	}

	//if the user has requested a peer mapping, output it to the console.
	if printPeerMap {
		for nodeIdx, node := range Nodes {
			t.Logf("Node: %d has address: %s and the following peers: %v", nodeIdx, node.Addr, node.ListPeersAsString())
			t.Log()
		}
	}

	//finally defer context cleanup
	t.Cleanup(func() {
		for _, n := range Nodes {
			n.Close()
		}

	})

	return &TestContext{
		Config: cfg,
		Nodes:  Nodes,
	}

}

func NewConfigurableTestContextWithBootstrapAddresses(t *testing.T, standardNodeCount int, conf *config.Config, bootstrapAddresses []string, connectDelayMillis int, refreshTime int) *TestContext {
	t.Helper()

	if len(bootstrapAddresses) <= 0 {
		t.Fatalf("Failed to create configurable test context: Bootstrap Nodes address count must be greater than zero.")
	}

	if standardNodeCount <= 0 {
		t.Log("WARNING: The requested standard node count was less than 1 thus the configurable context is solely comprised of core, bootstrap nodes.")
	}

	var cfg *config.Config
	if conf == nil {
		//prepare default config if a config has not been provided
		cfg = config.GetDefaultSingletonInstance()
		cfg.UseProtobuf = true
		cfg.RequestTimeout = 2000 * time.Millisecond
		cfg.DefaultEntryTTL = 30 * time.Second
		cfg.RefreshInterval = 5 * time.Second
		cfg.JanitorInterval = 10 * time.Second
		cfg.ReplicationFactor = 1
		if refreshTime > 0 {
			cfg.RefreshInterval = time.Duration(refreshTime) * time.Second
			cfg.JanitorInterval = time.Duration(refreshTime*2) * time.Second
		}

	} else {
		cfg = conf
	}

	//first create and bootstrap the core network (bootstrap) nodes.
	bootstrapNodes := make([]*Node, 0)
	for i, addr := range bootstrapAddresses {
		bootstrapNode, instantiationErr := NewNode("bootstrapNode"+strconv.Itoa(i), addr, netx.NewTCP(), cfg, NT_CORE)
		if instantiationErr != nil {
			t.Fatalf("Failed to create bootstrap node %d at address %s: %v", i+1, addr, instantiationErr)
		}
		bootstrapNode.Bootstrap(bootstrapAddresses, connectDelayMillis) //crucially bootstrap the node, witch the provided addresses. The node will ignore its own address.
		bootstrapNodes = append(bootstrapNodes, bootstrapNode)

	}

	//Next create some STANDARD test nodes.
	standardNodes := make([]*Node, 0)
	startingIP := 8999
	for i := 0; i < standardNodeCount; i++ {
		startingIP++
		nodeIP := startingIP + 1
		nodeNameStr := "node" + strconv.Itoa(i+1)
		nodeIpStr := strconv.Itoa(nodeIP)
		node, instantiationErr := NewNode(nodeNameStr, ":"+nodeIpStr, netx.NewTCP(), cfg, NT_EXTERNAL)
		if instantiationErr != nil {
			t.Fatalf("Failed to create standard node %d at address %s: %v", i+1, ":"+nodeIpStr, instantiationErr)
		}
		standardNodes = append(standardNodes, node)
	}

	//evenly distribute connection of standard nodes across core bootstrap nodes
	for i, node := range standardNodes {
		bootstrapNode := bootstrapNodes[i%len(bootstrapNodes)]
		if err := node.Connect(bootstrapNode.Addr); err != nil {
			t.Fatalf("Error occurred whilst Peer Node %d was trying to connect to Bootstrap Node %s: %v", i+1, bootstrapNode.Addr, err)
		}
	}

	//finally defer context cleanup
	t.Cleanup(func() {

		//next clean up bootstrap nodes
		for _, n := range bootstrapNodes {
			n.Close()
		}

		//then clean up standard nodes
		for _, n := range standardNodes {
			n.Close()
		}

	})

	return &TestContext{
		Config:         cfg,
		Nodes:          standardNodes,
		BootstrapNodes: bootstrapNodes,
	}

}

func prepSampleEntryData(t *testing.T, standardEntryCount int) *map[string][]byte {
	t.Helper()
	sampleEntries := make(map[string][]byte)
	for i := 0; i < standardEntryCount; i++ {
		curEntryKey := generateRandomStringKey()
		sampleEntries[curEntryKey] = []byte(curEntryKey)
	}

	//cleanup entries once test concludes.
	t.Cleanup(func() {
		clear(sampleEntries)
	})

	return &sampleEntries
}

func generateRandomStringKey() string {
	randNum := rand.Intn(math.MaxInt)
	randNumStr := strconv.Itoa(randNum)
	return randNumStr
}

/******************************************************************************************************************
 *           SET THEORY HELPER FUNCTIONS (THESE COULD LATER BE ABSTRACTED OUT INTO A SEPARATE OSS LIB)
 ******************************************************************************************************************/

// CreateDisjointPairings - creates an array of disjoint pairings from the two provided sets: setA and setB.
func CreateDisjointPairings[T any, K comparable](setA, setB []T, opts DisjointSetOpts[T, K], targetPairingCount int) ([]Pairing[T], error) {

	//A valid (custom) comparator function must be provided
	if opts.Compare == nil {
		return nil, errors.New("A valid comparator function must be provided.")
	}

	//check both sets are of adequate length.
	if len(setA) < targetPairingCount {
		return nil, errors.New("The provided SET A contained less elements than the target pairing count.")
	}

	if len(setB) < targetPairingCount {
		return nil, errors.New("The provided SET B contained less elements than the target pairing count.")
	}

	//begin disjoint pairing operation
	var pairings []Pairing[T]
	for _, setaItem := range setA {
		for _, setbItem := range setB {
			if !opts.Compare(setaItem, setbItem) {
				newPairing := Pairing[T]{Node1: setaItem, Node2: setbItem}
				pairings = append(pairings, newPairing)
				break
			}
		}
		//exit early where we have reached the desired number of pairings, irrespective
		//of the length of seta
		if len(pairings) == targetPairingCount {
			break
		}
	}

	if len(pairings) == targetPairingCount {
		return pairings, nil
	} else {
		return pairings, fmt.Errorf("Was unable to create the specified number of pairings, only %d of %d pairs was created,", len(pairings), targetPairingCount)
	}
}

// IsDisjointPairing - test if each of the provided pairings are disjoint according to the provided
//
//	comparator function.
func IsDisjointPairing[T any, K comparable](pairings []Pairing[T], opts DisjointSetOpts[T, K]) (bool, error) {

	//A valid (custom) comparator function must be provided
	if opts.Compare == nil {
		return false, errors.New("A valid comparator function must be provided.")
	}

	//begin isDisjointPairing ops
	for _, currentPairing := range pairings {

		if opts.Compare(currentPairing.Node1, currentPairing.Node2) {
			return false, nil
		}
	}

	return true, nil

}

func ToDisjoint[T any, K comparable](pairings []Pairing[T], opts DisjointSetOpts[T, K]) ([]Pairing[T], error) {

	var disjointPairings []Pairing[T]

	//A valid (custom) comparator function must be provided
	if opts.Compare == nil {
		return nil, errors.New("A valid comparator function must be provided.")
	}

	if opts.Resolver == nil {
		return nil, errors.New("A valid (disjoint) resolver function must be provided.")
	}

	for _, currentPairing := range pairings {

		if opts.Compare(currentPairing.Node1, currentPairing.Node2) {

			//if we have arrived here there is some UNION between the
			//current pairing we thus call our resolver function to
			//revert the pairing to a disjoint state and append the pairing
			//to our list
			disjointedPairing := opts.Resolver(currentPairing)
			disjointPairings = append(disjointPairings, disjointedPairing)
		} else {
			disjointPairings = append(disjointPairings, currentPairing)
		}
	}

	return disjointPairings, nil
}

func ToSparsePairings(pairings []Pairing[*Node], bootstrapNodes []*Node) []Pairing[*Node] {

	//forward-like, function declaration to allow us to call the fuction recursively.
	var selectRandomNode func([]*Node, *Node, int, int) (*Node, error)
	selectRandomNode = func(allNodes []*Node, callerNode *Node, retryCount int, maxRetries int) (*Node, error) {

		selected := rand.Intn(len(bootstrapNodes) - 1)

		if bootstrapNodes[selected].ID.String() != callerNode.ID.String() {
			return bootstrapNodes[selected], nil
		} else if retryCount < maxRetries {
			return selectRandomNode(allNodes, callerNode, (retryCount - 1), maxRetries)
		} else {
			return nil, errors.New("Unable to select random node from provide set of nodes")
		}
	}

	for _, currentPairing := range pairings {

		//remove reference to all but 1 bootstrap node peer, selected at random
		//from the current bootstrap node peer list.
		curBN := currentPairing.Node1
		randRetainedBN, randomSelectErr := selectRandomNode(bootstrapNodes, curBN, 0, len(bootstrapNodes)*2)
		if randomSelectErr != nil {
			panic("Unable to select random node for sparse pairing")
		}
		for _, curBootstrapNode := range bootstrapNodes {

			if curBootstrapNode.ID.String() == curBN.ID.String() || curBootstrapNode.ID.String() == randRetainedBN.ID.String() {
				continue
			}

			curBN.DropPeer(curBootstrapNode.ID)
		}

		//at this point the bootstrap node in the current pairing will only
		//have access to the RETAINED bootstrap node above and any standard
		//nodes its been paried with. we now remove all standard nodes
		//i.e. nodes with ids NOT equal to the retained node above.
		bootstrapNodePeerIds := curBN.ListPeerIds()
		for _, curPeerId := range bootstrapNodePeerIds {
			if curPeerId.String() != randRetainedBN.ID.String() {
				curBN.DropPeer(curPeerId)
			}
		}

		//where the STANDARD node is concerned, where it has a peer count
		//greater than 1, we remove all nodes in its peer list apart from
		//a single super node.

	}

	return pairings
}

func IsSparsePairing(pairings []Pairing[*Node]) bool {

	for _, pairing := range pairings {

		pairingBootstrapNode := pairing.Node1
		pairingStandardNode := pairing.Node2

		if pairingBootstrapNode.PeerCount() > 1 || pairingStandardNode.PeerCount() > 1 {
			fmt.Println("!!!!NOT SPARSE!!!!!")
			return false
		}
	}

	return true
}

// FilterDisjointOpts - A struct encapsulating all supported options that may be passed to the IsDisjoint function.
//
//	T is the element type, K is the key type used for hashing (must be comparable)
type DisjointSetOpts[T any, K comparable] struct {

	//Compare - Determines if the provided sets a and b are equal
	Compare func(a, b T) bool

	//KeySelector - Derives a key from the privided item which may be the identity of the item or the value of one of its properties.
	KeySelector func(item T) K

	//Resolver - A resolver function which will take the provided union set pairing and make it disjoint.
	Resolver func(Pairing[T]) Pairing[T]
}

// Represents a pairing between two nodes.
type Pairing[T any] struct {
	Node1   T
	Node2   T
	DataKey string
}
