package dht

import (
	"fmt"
	"math"
	"math/rand"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/SharefulNetworks/shareful-dht/netx"
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
	if v, ok := n2.FindRemote("alpha"); !ok || string(v) != "v" {
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
	peer1StoreIndexErr := n1.StoreIndexValue(key, IndexEntry{Source: key, Target: "super/" + n1.ID.String(), UpdatedUnix: time.Now().UnixNano()}, 15*time.Second)
	if peer1StoreIndexErr != nil {
		t.Fatalf("Error occurred whilst Peer Node 1 was trying to store index entry: %v", peer1StoreIndexErr)
	}

	peer2IndexIndexStoreErr := n2.StoreIndexValue(key, IndexEntry{Source: key, Target: "super/" + n2.ID.String(), UpdatedUnix: time.Now().UnixNano()}, 15*time.Second)
	if peer2IndexIndexStoreErr != nil {
		t.Fatalf("Error occurred whilst Peer Node 2 was trying to store index entry: %v", peer2IndexIndexStoreErr)
	}

	//after a short delay attempt to retrieve the merged index FROM the DHT, via node 1 and node 2
	time.Sleep(3000 * time.Millisecond)
	if ents, ok := n1.FindIndexRemote(key); !ok || len(ents) < 2 {
		t.Fatalf("expected merged index >=2")
	}
	time.Sleep(4000 * time.Millisecond)
	if ents, ok := n2.FindIndexRemote(key); !ok || len(ents) < 2 {
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
	if v, ok := n2.FindRemote("alpha"); !ok || string(v) != "v" {
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
	if v, ok := n2.FindRemote("alpha"); ok {
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
	if v, ok := n2.FindRemote("alpha"); !ok || string(v) != "v" {
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
	if v, ok := n2.FindRemote("alpha"); !ok || string(v) != "v" {
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
	if v, ok := n2.FindRemote("alpha"); !ok || string(v) != "v" {
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
	if v, ok := n1.FindRemote("alpha"); !ok || string(v) != "v" {
		t.Fatalf("FindRemote after non-actioned delete failed %q", string(v))
	}

	//then to be absolutely sure the deletion was not actioned nor propergated we attempt to
	//find the entry again via node 2.
	time.Sleep(3000 * time.Millisecond)
	if v, ok := n2.FindRemote("alpha"); !ok || string(v) != "v" {
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
	peer1StoreIndexErr := n1.StoreIndexValue(key, IndexEntry{Source: key, Target: "super/" + n1.ID.String(), UpdatedUnix: time.Now().UnixNano()}, 15*time.Second)
	if peer1StoreIndexErr != nil {
		t.Fatalf("Error occurred whilst Peer Node 1 was trying to store index entry: %v", peer1StoreIndexErr)
	}

	peer2IndexIndexStoreErr := n2.StoreIndexValue(key, IndexEntry{Source: key, Target: "super/" + n2.ID.String(), UpdatedUnix: time.Now().UnixNano()}, 15*time.Second)
	if peer2IndexIndexStoreErr != nil {
		t.Fatalf("Error occurred whilst Peer Node 2 was trying to store index entry: %v", peer2IndexIndexStoreErr)
	}

	//after a short delay attempt to retrieve the merged index FROM the DHT, via node 1 and node 2
	time.Sleep(3000 * time.Millisecond)
	if ents, ok := n1.FindIndexRemote(key); !ok || len(ents) < 2 {
		t.Fatalf("expected merged index >=2")
	}
	time.Sleep(4000 * time.Millisecond)
	if ents, ok := n2.FindIndexRemote(key); !ok || len(ents) < 2 {
		t.Fatalf("expected merged index >=2")

	}

	//next explicitly delete one of the index entries via node 1 (here the index source(i.e key) and top-level key happen to be the same.
	if err := n1.DeleteIndex(key, key); err != nil {
		t.Fatalf("Error occurred whilst deleting index entry '%s': %v", key, err)
	}

	//after a short delay to allow the deletion to propergate attempt to retreive the index from the DHT via node 2.
	//which should now only contain a single entry
	time.Sleep(6000 * time.Millisecond)
	if ents, ok := n2.FindIndexRemote(key); !ok || len(ents) != 1 {
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
	peer1StoreIndexErr := n1.StoreIndexValue(key, IndexEntry{Source: key, Target: "super/" + n1.ID.String(), UpdatedUnix: time.Now().UnixNano()}, 15*time.Second)
	if peer1StoreIndexErr != nil {
		t.Fatalf("Error occurred whilst Peer Node 1 was trying to store index entry: %v", peer1StoreIndexErr)
	}

	peer2IndexIndexStoreErr := n2.StoreIndexValue(key, IndexEntry{Source: key, Target: "super/" + n2.ID.String(), UpdatedUnix: time.Now().UnixNano()}, 15*time.Second)
	if peer2IndexIndexStoreErr != nil {
		t.Fatalf("Error occurred whilst Peer Node 2 was trying to store index entry: %v", peer2IndexIndexStoreErr)
	}

	//after a short delay attempt to retrieve the merged index FROM the DHT, via node 1 and node 2
	time.Sleep(3000 * time.Millisecond)
	if ents, ok := n1.FindIndexRemote(key); !ok || len(ents) < 2 {
		t.Fatalf("expected merged index >=2")
	}
	time.Sleep(4000 * time.Millisecond)
	if ents, ok := n2.FindIndexRemote(key); !ok || len(ents) < 2 {
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
	if ents, ok := n1.FindIndexRemote(key); !ok || len(ents) != 2 {
		t.Fatalf("expected merged index = 2")
	}

	time.Sleep(7000 * time.Millisecond)
	if ents, ok := n2.FindIndexRemote(key); !ok || len(ents) != 2 {
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
	peer1StoreIndexErr := n1.StoreIndexValue(key, IndexEntry{Source: key, Target: "super/" + n1.ID.String(), UpdatedUnix: time.Now().UnixNano()}, 15*time.Second)
	if peer1StoreIndexErr != nil {
		t.Fatalf("Error occurred whilst Peer Node 1 was trying to store index entry: %v", peer1StoreIndexErr)
	}

	peer2IndexIndexStoreErr := n2.StoreIndexValue(key, IndexEntry{Source: key, Target: "super/" + n2.ID.String(), UpdatedUnix: time.Now().UnixNano()}, 15*time.Second)
	if peer2IndexIndexStoreErr != nil {
		t.Fatalf("Error occurred whilst Peer Node 2 was trying to store index entry: %v", peer2IndexIndexStoreErr)
	}

	//next fetch entries from node 3 to ensure the storage operation was successsfully propogated
	time.Sleep(3000 * time.Millisecond)
	if ents, ok := n3.FindIndexRemote(key); !ok || len(ents) != 2 {
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
	if ents, ok := n3.FindIndexRemote(key); !ok || len(ents) != 2 {
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
	if v, ok := n2.FindRemote("alpha"); !ok || string(v) != "v" {
		t.Fatalf("FindRemote failed %q", string(v))
	}

	time.Sleep(2000 * time.Millisecond)
	if w, ok := n1.FindRemote("beta"); !ok || string(w) != "w" {
		t.Fatalf("FindRemote failed %q", string(w))
	}

	time.Sleep(2500 * time.Millisecond)
	if w, ok := n3.FindRemote("beta"); !ok || string(w) != "w" {
		t.Fatalf("FindRemote failed %q", string(w))
	}

	time.Sleep(3000 * time.Millisecond)
	if v, ok := n3.FindRemote("alpha"); !ok || string(v) != "v" {
		t.Fatalf("FindRemote failed %q", string(v))
	}

	//next we crucially close node 1 and thereby prevent it from refeshing its entries.
	n1.Close()

	//next we wait for a period longer that the ttl (10 seconds) to allow time for the closed nodes
	//entries to expire.
	time.Sleep(12 * time.Second)

	//finally attempt to retreive node 1's entry via node 2 and 3 which should fail, in both cases.
	time.Sleep(3000 * time.Millisecond)
	_, node3FundOK := n3.FindRemote("alpha")
	if node3FundOK {
		t.Fatal("Expected lookup for entry to fail on account of it having been expired.")
	} else {
		t.Log("Expired entry was not found as expected.")
	}

	time.Sleep(3500 * time.Millisecond)
	_, node2FundOK := n2.FindRemote("alpha")
	if node2FundOK {
		t.Fatal("Expected lookup for entry to fail on account of it having been expired.")
	} else {
		t.Log("Expired entry was not found as expected.")
	}

}

func Test_Index_Entry_Auto_Expiration(t *testing.T) {

	//create new default test context
	ctx := NewConfigurableTestContext(t, 3, nil, false)

	//obtain nodes from the test context
	n1 := ctx.Nodes[0]
	n2 := ctx.Nodes[1]
	n3 := ctx.Nodes[2]

	t.Logf("STORAGE TIME: %s", time.Now().String())

	//store index entries from the first two nodes under the same key
	key := "leaf/x"
	peer1StoreIndexErr := n1.StoreIndexValue(key, IndexEntry{Source: key, Target: "super/" + n1.ID.String(), UpdatedUnix: time.Now().UnixNano()}, 12*time.Second)
	if peer1StoreIndexErr != nil {
		t.Fatalf("Error occurred whilst Peer Node 1 was trying to store index entry: %v", peer1StoreIndexErr)
	}

	peer2IndexIndexStoreErr := n2.StoreIndexValue(key, IndexEntry{Source: key, Target: "super/" + n2.ID.String(), UpdatedUnix: time.Now().UnixNano()}, 12*time.Second)
	if peer2IndexIndexStoreErr != nil {
		t.Fatalf("Error occurred whilst Peer Node 2 was trying to store index entry: %v", peer2IndexIndexStoreErr)
	}

	//next fetch entries from node 3 to ensure the storage operation was successsfully propogated
	time.Sleep(3000 * time.Millisecond)
	if ents, ok := n3.FindIndexRemote(key); !ok || len(ents) != 2 {
		t.Fatalf("expected merged index = 2")
	} else {

		t.Log("The found entries were:")
		t.Log(ents)
	}

	//next close down node 2 and thereby prevent it from refreshing its index keys, which should ultimately cause
	//them to expire.
	time.Sleep(3000 * time.Millisecond)
	n2.Close()

	//next allow sufficient time for the TTL duration (12 seconds in this case) to elapse
	time.Sleep(15 * time.Second)

	//finally attempt to retrieve the collection of entries associated with the key which
	//should now be of length 1 as node 2's entry should have automatically expired.
	if ents, ok := n3.FindIndexRemote(key); !ok || len(ents) != 1 {
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
	if len(node.ListPeers()) == (desiredNodeCount - 1) {
		t.Log()
		t.Logf("Node: %d has address: %s and has a peer list length of: %d", 15, node.Addr, len(node.ListPeers()))
		t.Log()
	} else {
		t.Fatalf("Expected node to have a peer list length of: %d but actually had a length of: %d", desiredNodeCount-1, len(node.ListPeers()))
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
	ctx := NewConfigurableTestContextWithBootstrapAddresses(t, 0, nil, coreNetworkBootstrapNodeAddrs, -1)

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

	if len(n1.ListPeers()) != expectedPeerListLength {
		t.Fatalf("Expected Node 1 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n1.ListPeers()))
	}

	if len(n2.ListPeers()) != expectedPeerListLength {
		t.Fatalf("Expected Node 2 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n2.ListPeers()))
	}

	if len(n3.ListPeers()) != expectedPeerListLength {
		t.Fatalf("Expected Node 3 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n3.ListPeers()))
	}

	if len(n4.ListPeers()) != expectedPeerListLength {
		t.Fatalf("Expected Node 4 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n4.ListPeers()))
	}

	if len(n5.ListPeers()) != expectedPeerListLength {
		t.Fatalf("Expected Node 5 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n5.ListPeers()))
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
	ctx := NewConfigurableTestContextWithBootstrapAddresses(t, standardNodeCount, nil, coreNetworkBootstrapNodeAddrs, 0)

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

	if len(n1.ListPeers()) != expectedPeerListLength {
		t.Fatalf("Expected Node 1 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n1.ListPeers()))
	}

	if len(n2.ListPeers()) != expectedPeerListLength {
		t.Fatalf("Expected Node 2 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n2.ListPeers()))
	}

	if len(n3.ListPeers()) != expectedPeerListLength {
		t.Fatalf("Expected Node 3 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n3.ListPeers()))
	}

	if len(n4.ListPeers()) != expectedPeerListLength {
		t.Fatalf("Expected Node 4 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n4.ListPeers()))
	}

	if len(n5.ListPeers()) != expectedPeerListLength {
		t.Fatalf("Expected Node 5 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n5.ListPeers()))
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
	ctx := NewConfigurableTestContextWithBootstrapAddresses(t, standardNodeCount, nil, coreNetworkBootstrapNodeAddrs, -1)

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

	if len(n1.ListPeers()) != expectedPeerListLength {
		t.Fatalf("Expected Node 1 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n1.ListPeers()))
	}

	if len(n2.ListPeers()) != expectedPeerListLength {
		t.Fatalf("Expected Node 2 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n2.ListPeers()))
	}

	if len(n3.ListPeers()) != expectedPeerListLength {
		t.Fatalf("Expected Node 3 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n3.ListPeers()))
	}

	if len(n4.ListPeers()) != expectedPeerListLength {
		t.Fatalf("Expected Node 4 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n4.ListPeers()))
	}

	if len(n5.ListPeers()) != expectedPeerListLength {
		t.Fatalf("Expected Node 5 to have peer list length of: %d but actually had length of: %d", expectedPeerListLength, len(n5.ListPeers()))
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
	//samplke entry count equal to the number of standard nodes we randomly selected for the
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
			if val, ok := curBootstrapNode.FindRemote(k); !ok && string(val) != k {
				t.Fatalf("FindRemote failed on bootstrap node: %s for resource with key: %s", curBootstrapNode.ID, k)
			} else {
				t.Log()
				t.Logf("Find operations for key %s succeded on bootstrap node: %s the corresponding value was: %s", k, curBootstrapNode.Addr, string(val))
			}
		}
	}

}


func Test_Full_Network_Bootstrap_Node_To_Standard_Node_Find_Standard_Entry_With_Disjoint_Replica_Set(t *testing.T) {

}

/*****************************************************************************************************************
 *                                     HELPER/UTILITY TYPES AND FUNCTIONS FOR E2E TESTS
 ******************************************************************************************************************/

// TestContext is used to hold context info for e2e tests
type TestContext struct {
	Nodes          []*Node
	Config         *Config
	BootstrapNodes []*Node
}

// NewDefaultTestContext creates a new default test context with two connected nodes
func NewDefaultTestContext(t *testing.T) *TestContext {
	t.Helper()

	//prepare config.
	cfg := DefaultConfig()
	cfg.UseProtobuf = true // set true after generating pb
	cfg.RequestTimeout = 2000 * time.Millisecond
	cfg.DefaultTTL = 30 * time.Second
	cfg.RefreshInterval = 5 * time.Second
	cfg.JanitorInterval = 10 * time.Second

	//create nodes
	n1 := NewNode("node1", ":9321", netx.NewTCP(), cfg)
	n2 := NewNode("node2", ":9322", netx.NewTCP(), cfg)
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
		Config: &cfg,
		Nodes:  Nodes,
	}
}

func NewConfigurableTestContext(t *testing.T, nodeCount int, config *Config, printPeerMap bool) *TestContext {
	t.Helper()

	var cfg *Config
	if config == nil {
		//prepare default config if a config has not been provided
		cf := DefaultConfig()
		cfg = &cf
		cfg.UseProtobuf = true
		cfg.RequestTimeout = 2000 * time.Millisecond
		cfg.DefaultTTL = 30 * time.Second
		cfg.RefreshInterval = 5 * time.Second
		cfg.JanitorInterval = 10 * time.Second

	} else {
		cfg = config
	}

	//attempt to create the requested number of nodes specified via the node count.
	Nodes := make([]*Node, 0)
	startingIP := 8999
	for i := 0; i < nodeCount; i++ {
		startingIP++
		nodeIP := startingIP + 1
		nodeNameStr := "node" + strconv.Itoa(i+1)
		nodeIpStr := strconv.Itoa(nodeIP)
		node := NewNode(nodeNameStr, ":"+nodeIpStr, netx.NewTCP(), *cfg)
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
			t.Logf("Node: %d has address: %s and the following peers: %v", nodeIdx, node.Addr, node.ListPeers())
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

func NewConfigurableTestContextWithBootstrapAddresses(t *testing.T, standardNodeCount int, config *Config, bootstrapAddresses []string, connectDelayMillis int) *TestContext {
	t.Helper()

	if len(bootstrapAddresses) <= 0 {
		t.Fatalf("Failed to create configurable test context: Bootstrap Nodes address count must be greater than zero.")
	}

	if standardNodeCount <= 0 {
		t.Log("WARNING: The requested standard node count was less than 1 thus the configurable context is solely comprised of core, bootstrap nodes.")
	}

	var cfg *Config
	if config == nil {
		//prepare default config if a config has not been provided
		cf := DefaultConfig()
		cfg = &cf
		cfg.UseProtobuf = true
		cfg.RequestTimeout = 2000 * time.Millisecond
		cfg.DefaultTTL = 30 * time.Second
		cfg.RefreshInterval = 5 * time.Second
		cfg.JanitorInterval = 10 * time.Second

	} else {
		cfg = config
	}

	//first create and bootstrap the core network (bootstrap) nodes.
	bootstrapNodes := make([]*Node, 0)
	for i, addr := range bootstrapAddresses {
		bootstrapNode := NewNode("bootstrapNode"+strconv.Itoa(i), addr, netx.NewTCP(), *cfg)
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
		node := NewNode(nodeNameStr, ":"+nodeIpStr, netx.NewTCP(), *cfg)
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
