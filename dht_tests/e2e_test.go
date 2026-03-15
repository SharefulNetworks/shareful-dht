package dhttests

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/SharefulNetworks/shareful-dht/config"
	"github.com/SharefulNetworks/shareful-dht/dht"
	"github.com/SharefulNetworks/shareful-dht/events"
	"github.com/SharefulNetworks/shareful-dht/netx"
	"github.com/SharefulNetworks/shareful-dht/routing"
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
	peer1StoreIndexErr := n1.StoreIndex(key, dht.RecordIndexEntry{Source: key, Target: "super/" + n1.ID.String(), UpdatedUnix: time.Now().UnixNano()})
	if peer1StoreIndexErr != nil {
		t.Fatalf("Error occurred whilst Peer Node 1 was trying to store index entry: %v", peer1StoreIndexErr)
	}

	peer2IndexIndexStoreErr := n2.StoreIndex(key, dht.RecordIndexEntry{Source: key, Target: "super/" + n2.ID.String(), UpdatedUnix: time.Now().UnixNano()})
	if peer2IndexIndexStoreErr != nil {
		t.Fatalf("Error occurred whilst Peer Node 2 was trying to store index entry: %v", peer2IndexIndexStoreErr)
	}

	//after a short delay attempt to retrieve the merged index FROM the DHT, via node 1 and node 2
	time.Sleep(3000 * time.Millisecond)
	if ents, ok := n1.FindIndex(key); !ok || len(ents) < 2 {
		t.Fatalf("expected merged index >=2, length was: %d", len(ents))
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
	peer1StoreIndexErr := n1.StoreIndex(key, dht.RecordIndexEntry{Source: key, Target: "super/" + n1.ID.String(), UpdatedUnix: time.Now().UnixNano()})
	if peer1StoreIndexErr != nil {
		t.Fatalf("Error occurred whilst Peer Node 1 was trying to store index entry: %v", peer1StoreIndexErr)
	}

	peer2IndexIndexStoreErr := n2.StoreIndex(key, dht.RecordIndexEntry{Source: key, Target: "super/" + n2.ID.String(), UpdatedUnix: time.Now().UnixNano()})
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
	fmt.Printf("Calling delete on node: %x", n1.ID)
	if err := n1.DeleteIndex(key, key, false); err != nil {
		t.Fatalf("Error occurred whilst deleting index entry '%s': %v", key, err)
	}

	//after a short delay to allow the deletion to propergate attempt to retreive the index from the DHT via node 2.
	//which should now only contain a single entry
	time.Sleep(6000 * time.Millisecond)
	var ok bool
	var entsPostDelete []dht.RecordIndexEntry
	entsPostDelete, ok = n2.FindIndex(key)

	fmt.Println("#####Found entries AFTER deletion are: ")
	for _, curE := range entsPostDelete {
		fmt.Printf("\n Publisher: %s Key: %s Value: %s \n", curE.Publisher.String(), curE.Source, curE.Target)
	}

	if !ok {
		t.Fatalf("Was unable to find specified IndexEntry, post deletion.")
	}

	if len(entsPostDelete) != 1 {
		t.Fatalf("Expected remaining entry count to be 1, the actual entry count was: %d", len(entsPostDelete))
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
	peer1StoreIndexErr := n1.StoreIndex(key, dht.RecordIndexEntry{Source: key, Target: "super/" + n1.ID.String(), UpdatedUnix: time.Now().UnixNano()})
	if peer1StoreIndexErr != nil {
		t.Fatalf("Error occurred whilst Peer Node 1 was trying to store index entry: %v", peer1StoreIndexErr)
	}

	peer2IndexIndexStoreErr := n2.StoreIndex(key, dht.RecordIndexEntry{Source: key, Target: "super/" + n2.ID.String(), UpdatedUnix: time.Now().UnixNano()})
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
	if err := n1.DeleteIndex("wrong key", key, false); err == nil {
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
	peer1StoreIndexErr := n1.StoreIndex(key, dht.RecordIndexEntry{Source: key, Target: "super/" + n1.ID.String(), UpdatedUnix: time.Now().UnixNano()})
	if peer1StoreIndexErr != nil {
		t.Fatalf("Error occurred whilst Peer Node 1 was trying to store index entry: %v", peer1StoreIndexErr)
	}

	peer2IndexIndexStoreErr := n2.StoreIndex(key, dht.RecordIndexEntry{Source: key, Target: "super/" + n2.ID.String(), UpdatedUnix: time.Now().UnixNano()})
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
	if err := n3.DeleteIndex(key, key, false); err == nil {
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

	//explicitly set a short TTL duration to allow us to validate the auto expiration of index entries.
	cfg := config.GetDefaultSingletonInstance()
	cfg.UseProtobuf = true
	cfg.RequestTimeout = 2000 * time.Millisecond
	cfg.DefaultEntryTTL = 15 * time.Second
	cfg.DefaultIndexEntryTTL = 15 * time.Second
	cfg.RefreshInterval = 5 * time.Second
	cfg.JanitorInterval = 10 * time.Second

	//create new configurable test context, so we can create three nodes.
	ctx := NewConfigurableTestContext(t, 3, cfg, false)

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
	n1.Shutdown()

	//next we wait for a period longer that the ttl (10 seconds) to allow time for the closed nodes
	//entries to expire.
	time.Sleep(18 * time.Second)

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

	//finally reset config to prevent our custom settings from leaking into subsequent tests
	config.Reset()
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
	peer1StoreIndexErr := n1.StoreIndex(key, dht.RecordIndexEntry{Source: key, Target: "super/" + n1.ID.String(), UpdatedUnix: time.Now().UnixNano()})
	if peer1StoreIndexErr != nil {
		t.Fatalf("Error occurred whilst Peer Node 1 was trying to store index entry: %v", peer1StoreIndexErr)
	}

	peer2IndexIndexStoreErr := n2.StoreIndex(key, dht.RecordIndexEntry{Source: key, Target: "super/" + n2.ID.String(), UpdatedUnix: time.Now().UnixNano()})
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
	n2.Shutdown()

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

	//finally reset config to prevent our custom settings from leaking into subsequent tests
	config.Reset()
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
	var randomlySelectedStandardNodes []*dht.Node
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
	var randomlySelectedStandardNodes []*dht.Node
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
		randomlySelectedStandardNodes[curIdx].StoreIndex(k, dht.RecordIndexEntry{Source: k, Target: string(v), UpdatedUnix: time.Now().UnixNano()})
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
	disjointSetOpts := DisjointSetOpts[*dht.Node, string]{
		Compare: func(a, b *dht.Node) bool { return slices.Contains(b.ListPeerIdsAsStrings(), a.ID.String()) },
		//KeySelector: func(item *dht.Node) string { return item.ID.String() },
	}

	nodePairingCount := len(ctx.BootstrapNodes)
	var disjointNodePairings []Pairing[*dht.Node]
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
		disjointSetOpts2 := DisjointSetOpts[*dht.Node, string]{

			Compare: func(a, b *dht.Node) bool { return slices.Contains(b.ListPeerIdsAsStrings(), a.ID.String()) },
			Resolver: func(unionPairing Pairing[*dht.Node]) Pairing[*dht.Node] {

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
	disjointSetOpts := DisjointSetOpts[*dht.Node, string]{
		Compare: func(a, b *dht.Node) bool { return slices.Contains(b.ListPeerIdsAsStrings(), a.ID.String()) },
		//KeySelector: func(item *dht.Node) string { return item.ID.String() },
	}

	nodePairingCount := len(ctx.BootstrapNodes)
	var disjointNodePairings []Pairing[*dht.Node]
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
			storeErr = curPairingStandardNode.StoreIndex(curSampleDataKey, dht.RecordIndexEntry{Source: curSampleDataKey, Target: string(curSampleDataValue), UpdatedUnix: time.Now().UnixNano()})

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
		disjointSetOpts2 := DisjointSetOpts[*dht.Node, string]{

			Compare: func(a, b *dht.Node) bool { return slices.Contains(b.ListPeerIdsAsStrings(), a.ID.String()) },
			Resolver: func(unionPairing Pairing[*dht.Node]) Pairing[*dht.Node] {

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
		var indexEntries []dht.RecordIndexEntry
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
	entryNode, _ := dht.NewNode("entryNode", ":1981", netx.NewTCP(), ctx.Config, dht.NT_ENTRY)
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
	randomlySelectedNodes := make([]*dht.Node, 0)
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
		entryNode.Shutdown()
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
	entryNode, _ := dht.NewNode("entryNode", ":1981", netx.NewTCP(), ctx.Config, dht.NT_ENTRY)
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
	randomlySelectedNodes := make([]*dht.Node, 0)
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
		storeErr := curNode.StoreIndex(k, dht.RecordIndexEntry{Source: k, Target: string(v), UpdatedUnix: time.Now().UnixNano()})
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
		entryNode.Shutdown()
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
	entryNode, _ := dht.NewNode("entryNode", ":1981", netx.NewTCP(), ctx.Config, dht.NT_ENTRY)
	footHoldBootstrapNode := ctx.BootstrapNodes[rand.Intn(len(ctx.BootstrapNodes)-1)] //select another bootstrap node,at random, that this edge bootsrap node can use to get a foothold on the network
	entryNode.Bootstrap([]string{footHoldBootstrapNode.Addr}, 7000)                   //after some nominal time has elapsed, attempt to bootstrap the edge node

	//next create another brand new node (of type EXTERNAL this time) and provide it bootstrap method with the address of the
	//ENTRY node created in the immediately preceeding instructions, thereby setting up a single thread of
	//of interconnectivity from the External node to the Entry node and finally to  the foothold bootstrap node
	//which should in turn provide the External node with full network reachability.
	externalNode, _ := dht.NewNode("externalNode", ":1982", netx.NewTCP(), ctx.Config, dht.NT_EXTERNAL)
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
	randomlySelectedNodes := make([]*dht.Node, 0)
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
		entryNode.Shutdown()
		externalNode.Shutdown()
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
	entryNode, _ := dht.NewNode("entryNode", ":1981", netx.NewTCP(), ctx.Config, dht.NT_ENTRY)
	footHoldBootstrapNode := ctx.BootstrapNodes[rand.Intn(len(ctx.BootstrapNodes)-1)] //select another bootstrap node,at random, that this edge bootsrap node can use to get a foothold on the network
	entryNode.Bootstrap([]string{footHoldBootstrapNode.Addr}, 7000)                   //after some nominal time has elapsed, attempt to bootstrap the edge node

	//next create another brand new node (of type EXTERNAL this time) and provide it bootstrap method with the address of the
	//ENTRY node created in the immediately preceeding instructions, thereby setting up a single thread of
	//of interconnectivity from the External node to the Entry node and finally to  the foothold bootstrap node
	//which should in turn provide the External node with full network reachability.
	externalNode, _ := dht.NewNode("externalNode", ":1982", netx.NewTCP(), ctx.Config, dht.NT_EXTERNAL)
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
	randomlySelectedNodes := make([]*dht.Node, 0)
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
		storeErr := curNode.StoreIndex(k, dht.RecordIndexEntry{Source: k, Target: string(v), UpdatedUnix: time.Now().UnixNano()})
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
		entryNode.Shutdown()
		externalNode.Shutdown()
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
	var randomlySelectedStandardNodes []*dht.Node
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
	var randomlySelectedStandardNodesForFind []*dht.Node
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
	var randomlySelectedStandardNodes []*dht.Node
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
		storageErr := randomlySelectedStandardNodes[curIdx].StoreIndex(k, dht.RecordIndexEntry{Source: k, Target: string(v), UpdatedUnix: time.Now().UnixNano()})
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
	var randomlySelectedStandardNodesForFind []*dht.Node
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
	entryNode1, _ := dht.NewNode("entryNode1", ":1981", netx.NewTCP(), ctx.Config, dht.NT_ENTRY)
	footHoldBootstrapNode := ctx.BootstrapNodes[rand.Intn(len(ctx.BootstrapNodes)-1)] //select another bootstrap node,at random, that this edge bootsrap node can use to get a foothold on the network
	entryNode1.Bootstrap([]string{footHoldBootstrapNode.Addr}, 7000)                  //after some nominal time has elapsed, attempt to bootstrap the edge node
	entryNode2, _ := dht.NewNode("entryNode2", ":1982", netx.NewTCP(), ctx.Config, dht.NT_ENTRY)
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
	randomlySelectedNodes := make([]*dht.Node, 0)
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
		entryNode1.Shutdown()
		entryNode2.Shutdown()
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
	entryNode1, _ := dht.NewNode("entryNode1", ":1981", netx.NewTCP(), ctx.Config, dht.NT_ENTRY)
	footHoldBootstrapNode := ctx.BootstrapNodes[rand.Intn(len(ctx.BootstrapNodes)-1)] //select another bootstrap node,at random, that this edge bootsrap node can use to get a foothold on the network
	entryNode1.Bootstrap([]string{footHoldBootstrapNode.Addr}, 7000)                  //after some nominal time has elapsed, attempt to bootstrap the edge node
	entryNode2, _ := dht.NewNode("entryNode2", ":1982", netx.NewTCP(), ctx.Config, dht.NT_ENTRY)
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
	randomlySelectedNodes := make([]*dht.Node, 0)
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
		storeErr := curNode.StoreIndex(k, dht.RecordIndexEntry{Source: k, Target: string(v), UpdatedUnix: time.Now().UnixNano()})
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
		entryNode1.Shutdown()
		entryNode2.Shutdown()
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
	entryNode1, _ := dht.NewNode("entryNode1", ":1981", netx.NewTCP(), ctx.Config, dht.NT_ENTRY)
	footHoldBootstrapNode := ctx.BootstrapNodes[rand.Intn(len(ctx.BootstrapNodes)-1)] //select another bootstrap node,at random, that this edge bootsrap node can use to get a foothold on the network
	entryNode1.Bootstrap([]string{footHoldBootstrapNode.Addr}, 7000)                  //after some nominal time has elapsed, attempt to bootstrap the edge node
	entryNode2, _ := dht.NewNode("entryNode2", ":1982", netx.NewTCP(), ctx.Config, dht.NT_ENTRY)
	footHoldBootstrapNode2 := ctx.BootstrapNodes[rand.Intn(len(ctx.BootstrapNodes)-1)] //select another bootstrap node,at random, that this edge bootsrap node can use to get a foothold on the network
	entryNode2.Bootstrap([]string{footHoldBootstrapNode2.Addr}, 7000)                  //after some nominal time has elapsed, attempt to bootstrap the edge node

	//finally add our EXTERNAL nodes which will bootstrap via the ENTRY nodes created above.
	externalNode1, _ := dht.NewNode("externalNode1", ":1983", netx.NewTCP(), ctx.Config, dht.NT_EXTERNAL)
	externalNode1.Bootstrap([]string{entryNode1.Addr}, 7000)
	externalNode2, _ := dht.NewNode("externalNode2", ":1984", netx.NewTCP(), ctx.Config, dht.NT_EXTERNAL)
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
	randomlySelectedNodes := make([]*dht.Node, 0)
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
		entryNode1.Shutdown()
		entryNode2.Shutdown()
		externalNode1.Shutdown()
		externalNode2.Shutdown()
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
	entryNode1, _ := dht.NewNode("entryNode1", ":1981", netx.NewTCP(), ctx.Config, dht.NT_ENTRY)
	footHoldBootstrapNode := ctx.BootstrapNodes[rand.Intn(len(ctx.BootstrapNodes)-1)] //select another bootstrap node,at random, that this edge bootsrap node can use to get a foothold on the network
	entryNode1.Bootstrap([]string{footHoldBootstrapNode.Addr}, 7000)                  //after some nominal time has elapsed, attempt to bootstrap the edge node
	entryNode2, _ := dht.NewNode("entryNode2", ":1982", netx.NewTCP(), ctx.Config, dht.NT_ENTRY)
	footHoldBootstrapNode2 := ctx.BootstrapNodes[rand.Intn(len(ctx.BootstrapNodes)-1)] //select another bootstrap node,at random, that this edge bootsrap node can use to get a foothold on the network
	entryNode2.Bootstrap([]string{footHoldBootstrapNode2.Addr}, 7000)                  //after some nominal time has elapsed, attempt to bootstrap the edge node

	//finally add our EXTERNAL nodes which will bootstrap via the ENTRY nodes created above.
	externalNode1, _ := dht.NewNode("externalNode1", ":1983", netx.NewTCP(), ctx.Config, dht.NT_EXTERNAL)
	externalNode1.Bootstrap([]string{entryNode1.Addr}, 7000)
	externalNode2, _ := dht.NewNode("externalNode2", ":1984", netx.NewTCP(), ctx.Config, dht.NT_EXTERNAL)
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
	randomlySelectedNodes := make([]*dht.Node, 0)
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
		storeErr := curNode.StoreIndex(k, dht.RecordIndexEntry{Source: k, Target: string(v), UpdatedUnix: time.Now().UnixNano()})
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
		entryNode1.Shutdown()
		entryNode2.Shutdown()
		externalNode1.Shutdown()
		externalNode2.Shutdown()
	})
}

func Test_Full_Network_Core_Bootstrap_Nodes_Interconnectivity_And_Standard_Nodes_Connectivity_And_Neighboorhood_Discovery(t *testing.T) {

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
	time.Sleep(20000 * time.Millisecond)

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

	//wait a period of time that exceeds the PostBootstrapNeighboorhoodResolutionDelay duration in the
	//config then validate that any one of the nodes now has an expanded view of the network as indicated
	//by it having a peer list length that exceeds the expected peer list length calculated above.
	fmt.Printf("\nNode peer list length prior to neighbourhood discovery: %d\n", len(n1.ListPeersAsString()))
	time.Sleep(ctx.Config.PostBootstrapNeighboorhoodResolutionDelay + (5 * time.Second))
	fmt.Printf("\nNode peer list length after neighbourhood discovery: %d\n", len(n1.ListPeersAsString()))
	if len(n1.ListPeersAsString()) <= expectedPeerListLength {
		t.Fatalf("Expected Node 1 to have peer list length greater than: %d after neighbourhood discovery but actually had length of: %d", expectedPeerListLength, len(n1.ListPeersAsString()))
	}
}

func Test_Full_Network_Core_Bootstrap_Nodes_Interconnectivity_And_Standard_Nodes_Connectivity_And_Automatic_RoutingTable_Bucket_Refresh(t *testing.T) {

	//define custom config with a short routing table bucket refresh interval other than
	//this much of the default config values will remain unchanged.
	bucketRefreshInterval := 240 * time.Second
	cfg := config.GetDefaultSingletonInstance()
	cfg.UseProtobuf = true
	cfg.BucketRefreshInterval = bucketRefreshInterval
	cfg.BucketRefreshBatchDelayInterval = 5 * time.Second //wait 5 seconds between batches
	cfg.BucketRefreshBatchSize = 10

	//prepare our core network, bootstrap node addresses.
	coreNetworkBootstrapNodeAddrs := []string{":7401", ":7402", ":7403", ":7404", ":7405"}

	//next call into our helper function to create a new configurable test context complete
	//with core bootstrap nodes AND 20 standard nodes. The function will attempt to evenly
	//distribute connections to the core nodes from these standard nodes.
	ctx := NewConfigurableTestContextWithBootstrapAddresses(t, 0, nil, coreNetworkBootstrapNodeAddrs, 0, 0)

	//next we wait some time for the bootstrap process to complete on each node, by default
	//each node will wait 20 seconds before attempting to actually connect to the provided
	//bootstrap addresses
	time.Sleep(20000 * time.Millisecond)

	//grab reference to our (now hopefully bootstrapped nodes)
	n1 := ctx.BootstrapNodes[0]

	//wait sufficient time for at least one bucket refresh cycle to complete:
	//1)initial delay is: 30 seconds,
	//2)Delay between batches is 5 Seconds and there a 10 buckets in each batch.
	//3)160 buckets at 10 buckets per batch equates to 16 batches.
	//4)16 batches at 5 seconds per batch equates to 80 seconds, plus the initial 240 second delay.
	//5) we wait a little longer than this to be sure that the refresh has fully completed.
	time.Sleep(130*time.Second + bucketRefreshInterval)

	//after refresh is completed print the size of each bucket
	for i, currentBucket := range n1.GetRoutingTable().ListBuckets() {
		fmt.Printf("\nBucket %d size after refresh: %d\n", i, currentBucket.Size())
	}

}

func Test_Full_Network_Create_Index_Entry_And_Validate_Sync(t *testing.T) {

	/*
		For the purposes we will create a set of nodes and select
		two nodes which will not be permitted to reference one another
		as part of their replica set when storing entries. To do this
		we blacklist each of the selected nodes from one another. We then
		store a new Index Record to each node, with the SAME key.

		We then  validate synchronization by confirming that each node
		has a copy of the others IndexEntry, in the Record associated
		with the key, in their respective local data store. The fact that
		the two nodes cannot be in eaches respective replica set
		ensures that they could have only have learnt of each others
		IndexEntrys via the Index Synchronization mechanism.
	*/

	//define node addresses for each set
	bootstrapNodes := []string{":7401", ":7402", ":7403", ":7404", ":7405"}

	//init nodes
	ctx := NewConfigurableTestContextWithBootstrapAddresses(t, 0, nil, bootstrapNodes, 0, 0)

	//slightly alter the default config to extend the sync delay, this will allow us
	//to verify that the pair of nodes that we select are NOT in each others replica set
	//prior to the synchronization process.
	ctx.Config.IndexSyncDelay = 40 * time.Second

	//allow sufficient time for the bootstrap process to complete.
	time.Sleep(30000 * time.Millisecond)

	//chose any two nodes, we blacklist them from one another to prevent them
	//choosing each other as replica set peers. We want to ensure that the nodes
	//ONLY discover each others entries via the IndexSync mechanism.
	node1 := ctx.BootstrapNodes[0]
	node2 := ctx.BootstrapNodes[3]
	node1.AddToBlacklist(node2.Addr)
	node2.AddToBlacklist(node1.Addr)

	//define shared key
	key := "leaf/x"

	node1StoreErr := node1.StoreIndex(key, dht.RecordIndexEntry{Source: key, Target: "super/" + node1.ID.String()})
	if node1StoreErr != nil {
		t.Fatalf("Error occurred whilst Peer Node 1 was trying to store index entry: %v", node1StoreErr)
	}

	node2StoreErr := node2.StoreIndex(key, dht.RecordIndexEntry{Source: key, Target: "super/" + node2.ID.String()})
	if node2StoreErr != nil {

		t.Fatalf("Error occurred whilst Peer Node 2 was trying to store index entry: %v", node2StoreErr)
	}

	//wait for approx half of the sync delay before validating that the nodes are not in
	//each other replica set, post the storage operation.
	if slices.Contains(node1.ListPeerAddresses(), node2.Addr) {
		fmt.Printf("\nNode 1 peer list INSIDE: %v\n", node1.ListPeerAddresses())
		t.Fatalf("Node 1 peer list should NOT contain Node 2 but it does, peer list: %v", node1.ListPeerAddresses())
	}

	if slices.Contains(node2.ListPeerAddresses(), node1.Addr) {
		fmt.Printf("\nNode 2 peer list INSIDE: %v\n", node2.ListPeerAddresses())
		t.Fatalf("Node 2 peer list should NOT contain Node 1 but it does, peer list: %v", node2.ListPeerAddresses())
	}

	//now we have confirmed that the nodes ARE NOT in each others replica set (as a result of the blacklist)
	//we wait half the sync delay time and unblacklist the nodes so they are able to exchange
	//SYNC INDEX messages.
	time.Sleep(ctx.Config.IndexSyncDelay / 2)
	node1.RemoveFromBlacklist(node2.Addr)
	node2.RemoveFromBlacklist(node1.Addr)

	//pause to allow some time for storage opp 2 to propergate.
	time.Sleep(20000*time.Millisecond + ctx.Config.IndexSyncDelay)

	indexFromNode1, found := node1.FindIndexLocal(key)
	if !found || len(indexFromNode1) != 2 {
		t.Fatalf("Expected node1 data store to equal 2 actual length was: %d", len(indexFromNode1))
	}

	//look uop entries in local store
	indexFromNode2, found := node2.FindIndexLocal(key)
	if !found || len(indexFromNode2) != 2 {
		t.Fatalf("Expected node2 data store to equal 2 actual length was: %d", len(indexFromNode2))
	}

	t.Logf("\n Node 1 data store entries are: \n %v", indexFromNode1)
	t.Logf("\n Node 2 data store entries are: \n %v", indexFromNode2)

}

func Test_Full_Network_Create_Index_Entry_And_Validate_Sync_And_Index_Update_Event_Publication(t *testing.T) {

	/*
		For the purposes we will create a set of nodes and select
		two nodes which will not be permitted to reference one another
		as part of their replica set when storing entries. To do this
		we blacklist each of the selected nodes from one another. We then
		store a new Index Record to each node, with the SAME key.

		We then  validate synchronization by confirming that each node
		has a copy of the others IndexEntry, in the Record associated
		with the key, in their respective local data store. The fact that
		the two nodes cannot be in eaches respective replica set
		ensures that they could have only have learnt of each others
		IndexEntrys via the Index Synchronization mechanism.
	*/

	//define node addresses for each set
	bootstrapNodes := []string{":7401", ":7402", ":7403", ":7404", ":7405"}

	//init nodes
	ctx := NewConfigurableTestContextWithBootstrapAddresses(t, 0, nil, bootstrapNodes, 0, 0)

	//slightly alter the default config to extend the sync delay, this will allow us
	//to verify that the pair of nodes that we select are NOT in each others replica set
	//prior to the synchronization process.
	ctx.Config.IndexSyncDelay = 40 * time.Second

	//allow sufficient time for the bootstrap process to complete.
	time.Sleep(30000 * time.Millisecond)

	//chose any two nodes, we blacklist them from one another to prevent them
	//choosing each other as replica set peers. We want to ensure that the nodes
	//ONLY discover each others entries via the IndexSync mechanism.
	node1 := ctx.BootstrapNodes[0]
	node2 := ctx.BootstrapNodes[3]
	node1.AddToBlacklist(node2.Addr)
	node2.AddToBlacklist(node1.Addr)

	//register to receive events from both nodes, via our test node event listener which will simply
	//add any received events to an internal queue that we can later query to validate that
	//events were received to our nodes as expected.
	node1EventListener := NewTestNodeEventListener()
	node2EventListener := NewTestNodeEventListener()
	node1.AppendNodeEventListener("node1", node1EventListener)
	node2.AppendNodeEventListener("node2", node2EventListener)

	//define shared key
	key := "leaf/x"

	//store our test index record 1 taking care to set EnableIndexUpdateEvents to true, this will ensure events are published when this index is updated.
	node1StoreErr := node1.StoreIndex(key, dht.RecordIndexEntry{Source: key, Target: "super/" + node1.ID.String(), EnableIndexUpdateEvents: true})
	if node1StoreErr != nil {
		t.Fatalf("Error occurred whilst Peer Node 1 was trying to store index entry: %v", node1StoreErr)
	}

	//store our test index record 2 taking care to set EnableIndexUpdateEvents to true, this will ensure events are published when this index is updated.
	node2StoreErr := node2.StoreIndex(key, dht.RecordIndexEntry{Source: key, Target: "super/" + node2.ID.String(), EnableIndexUpdateEvents: true})
	if node2StoreErr != nil {

		t.Fatalf("Error occurred whilst Peer Node 2 was trying to store index entry: %v", node2StoreErr)
	}

	//wait for approx half of the sync delay before validating that the nodes are not in
	//each other replica set, post the storage operation.
	if slices.Contains(node1.ListPeerAddresses(), node2.Addr) {
		fmt.Printf("\nNode 1 peer list INSIDE: %v\n", node1.ListPeerAddresses())
		t.Fatalf("Node 1 peer list should NOT contain Node 2 but it does, peer list: %v", node1.ListPeerAddresses())
	}

	if slices.Contains(node2.ListPeerAddresses(), node1.Addr) {
		fmt.Printf("\nNode 2 peer list INSIDE: %v\n", node2.ListPeerAddresses())
		t.Fatalf("Node 2 peer list should NOT contain Node 1 but it does, peer list: %v", node2.ListPeerAddresses())
	}

	//now we have confirmed that the nodes ARE NOT in each others replica set (as a result of the blacklist)
	//we wait half the sync delay time and unblacklist the nodes so they are able to exchange
	//SYNC INDEX messages.
	time.Sleep(ctx.Config.IndexSyncDelay / 2)
	node1.RemoveFromBlacklist(node2.Addr)
	node2.RemoveFromBlacklist(node1.Addr)

	//pause to allow some time for storage opp 2 to propergate.
	time.Sleep(10000*time.Millisecond + ctx.Config.IndexSyncDelay)

	indexFromNode1, found := node1.FindIndexLocal(key)
	if !found || len(indexFromNode1) != 2 {
		t.Fatalf("Expected node1 data store to equal 2 actual length was: %d", len(indexFromNode1))
	}

	//look uop entries in local store
	indexFromNode2, found := node2.FindIndexLocal(key)
	if !found || len(indexFromNode2) != 2 {
		t.Fatalf("Expected node2 data store to equal 2 actual length was: %d", len(indexFromNode2))
	}

	time.Sleep(5000 * time.Millisecond)
	//Next to validate receipt of the events on both nodes we pull the event store
	//from their respectie listeners. We expect Node1 to have received an event from Node2
	//and vice versa, indicating that each node received an index update event as a result of
	//the other nodes store operation and subsequent synchronization.
	node1Events := node1EventListener.GetReceivedIndexUpdateEvents()
	node2Events := node2EventListener.GetReceivedIndexUpdateEvents()

	//verify node 1 received and Index Update Event from Node 2
	if len(node1Events) == 0 {
		t.Fatal("Node 1 did not receive any index update events but was expected to receive at least one.")
	} else {
		t.Logf("\nNode 1 received %d index update events\n", len(node1Events))
	}

	foundEventFromNode2 := false
	for _, curNode1Event := range node1Events {
		fmt.Printf("\n Node: %s Received event from node @: %s\n", node1.Addr, curNode1Event.GetPublisherAddress())
		if curNode1Event.GetPublisherAddress() == node2.Addr {
			t.Logf("Node 1 received expected index update event: %v", curNode1Event)
			foundEventFromNode2 = true
			break
		}
	}
	if !foundEventFromNode2 {
		t.Fatal("Node 1 did not receive expected index update event from Node 2")
	}

	//verify node 2 received and Index Update Event from Node 1
	if len(node2Events) == 0 {
		t.Fatal("Node 2 did not receive any index update events but was expected to receive at least one.")
	}

	foundEventFromNode1 := false
	for _, curNode2Event := range node2Events {
		fmt.Printf("\n Node: %s Received event from node @: %s\n", node2.Addr, curNode2Event.GetPublisherAddress())
		if curNode2Event.GetPublisherAddress() == node1.Addr {
			t.Logf("Node 2 received expected index update event: %v", curNode2Event)
			foundEventFromNode1 = true
			break
		}
	}
	if !foundEventFromNode1 {
		t.Fatal("Node 2 did not receive expected index update event from Node 1")
	}

	t.Logf("\n Node 1 data store entries are: \n %v", indexFromNode1)
	t.Logf("\n Node 2 data store entries are: \n %v", indexFromNode2)

}

func Test_Full_Network_Create_Index_Entry_And_Validate_Sync_And_Index_Update_Event_Publication_When_Index_Update_Events_Globally_Disabled(t *testing.T) {

	/*
		Here we DISABLE Index update eventsglobally via the config, this should result
		in no events being dispatched irrespective of whether EnableIndexUpdateEvents is
		set to true at the record level.

	*/

	//define node addresses for each set
	bootstrapNodes := []string{":7401", ":7402", ":7403", ":7404", ":7405"}

	//init nodes
	ctx := NewConfigurableTestContextWithBootstrapAddresses(t, 0, nil, bootstrapNodes, 0, 0)

	//critically we DISABLE index update events globally via the config.
	ctx.Config.IndexUpdateEventsEnabled = false

	//slightly alter the default config to extend the sync delay, this will allow us
	//to verify that the pair of nodes that we select are NOT in each others replica set
	//prior to the synchronization process.
	ctx.Config.IndexSyncDelay = 40 * time.Second

	//allow sufficient time for the bootstrap process to complete.
	time.Sleep(30000 * time.Millisecond)

	//chose any two nodes, we blacklist them from one another to prevent them
	//choosing each other as replica set peers. We want to ensure that the nodes
	//ONLY discover each others entries via the IndexSync mechanism.
	node1 := ctx.BootstrapNodes[0]
	node2 := ctx.BootstrapNodes[3]
	node1.AddToBlacklist(node2.Addr)
	node2.AddToBlacklist(node1.Addr)

	//register to receive events from both nodes, via our test node event listener which will simply
	//add any received events to an internal queue that we can later query to validate that
	//events were received to our nodes as expected.
	node1EventListener := NewTestNodeEventListener()
	node2EventListener := NewTestNodeEventListener()
	node1.AppendNodeEventListener("node1", node1EventListener)
	node2.AppendNodeEventListener("node2", node2EventListener)

	//define shared key
	key := "leaf/x"

	//store our test index record 1 taking care to set EnableIndexUpdateEvents to true, as IndexUpdateEvents are globally disabled this should take no effect; events should still not be published.
	node1StoreErr := node1.StoreIndex(key, dht.RecordIndexEntry{Source: key, Target: "super/" + node1.ID.String(), EnableIndexUpdateEvents: true})
	if node1StoreErr != nil {
		t.Fatalf("Error occurred whilst Peer Node 1 was trying to store index entry: %v", node1StoreErr)
	}

	//store our test index record 2 taking care to set EnableIndexUpdateEvents to true,  as IndexUpdateEvents are globally disabled this should take no effect; events should still not be published.
	node2StoreErr := node2.StoreIndex(key, dht.RecordIndexEntry{Source: key, Target: "super/" + node2.ID.String(), EnableIndexUpdateEvents: true})
	if node2StoreErr != nil {

		t.Fatalf("Error occurred whilst Peer Node 2 was trying to store index entry: %v", node2StoreErr)
	}

	//wait for approx half of the sync delay before validating that the nodes are not in
	//each other replica set, post the storage operation.
	if slices.Contains(node1.ListPeerAddresses(), node2.Addr) {
		fmt.Printf("\nNode 1 peer list INSIDE: %v\n", node1.ListPeerAddresses())
		t.Fatalf("Node 1 peer list should NOT contain Node 2 but it does, peer list: %v", node1.ListPeerAddresses())
	}

	if slices.Contains(node2.ListPeerAddresses(), node1.Addr) {
		fmt.Printf("\nNode 2 peer list INSIDE: %v\n", node2.ListPeerAddresses())
		t.Fatalf("Node 2 peer list should NOT contain Node 1 but it does, peer list: %v", node2.ListPeerAddresses())
	}

	//now we have confirmed that the nodes ARE NOT in each others replica set (as a result of the blacklist)
	//we wait half the sync delay time and unblacklist the nodes so they are able to exchange
	//SYNC INDEX messages.
	time.Sleep(ctx.Config.IndexSyncDelay / 2)
	node1.RemoveFromBlacklist(node2.Addr)
	node2.RemoveFromBlacklist(node1.Addr)

	//pause to allow some time for storage opp 2 to propergate.
	time.Sleep(10000*time.Millisecond + ctx.Config.IndexSyncDelay)

	indexFromNode1, found := node1.FindIndexLocal(key)
	if !found || len(indexFromNode1) != 2 {
		t.Fatalf("Expected node1 data store to equal 2 actual length was: %d", len(indexFromNode1))
	}

	//look uop entries in local store
	indexFromNode2, found := node2.FindIndexLocal(key)
	if !found || len(indexFromNode2) != 2 {
		t.Fatalf("Expected node2 data store to equal 2 actual length was: %d", len(indexFromNode2))
	}

	time.Sleep(5000 * time.Millisecond)
	//Next to validate receipt of the events on both nodes we pull the event store
	//from their respectie listeners. We expect Node1 to have received an event from Node2
	//and vice versa, indicating that each node received an index update event as a result of
	//the other nodes store operation and subsequent synchronization.
	node1Events := node1EventListener.GetReceivedIndexUpdateEvents()
	node2Events := node2EventListener.GetReceivedIndexUpdateEvents()

	//verify node 1 received and Index Update Event from Node 2
	if len(node1Events) > 0 {
		t.Fatalf("Node 1 received %d index update events but was expected to receive 0 since IndexUpdateEvents are globally disabled", len(node1Events))
	}

	//verify node 2 received and Index Update Event from Node 1
	if len(node2Events) > 0 {
		t.Fatalf("Node 2 received %d index update events but was expected to receive 0 since IndexUpdateEvents are globally disabled", len(node2Events))
	}

	t.Logf("\n Node 1 data store entries are: \n %v", indexFromNode1)
	t.Logf("\n Node 2 data store entries are: \n %v", indexFromNode2)

	//critically we RE-ENABLE index update events globally so as to not leak this config change into other tests.
	ctx.Config.IndexUpdateEventsEnabled = true

}

func Test_Full_Network_Create_Index_Entry_And_Validate_Sync_And_Index_Update_Event_Publication_When_Index_Update_Events_Individually_Disabled(t *testing.T) {

	/*
		Here we DISABLE Index update events individually at the IndexRecord level, this should result
		in no events being dispatched.

	*/

	//define node addresses for each set
	bootstrapNodes := []string{":7401", ":7402", ":7403", ":7404", ":7405"}

	//init nodes
	ctx := NewConfigurableTestContextWithBootstrapAddresses(t, 0, nil, bootstrapNodes, 0, 0)

	//slightly alter the default config to extend the sync delay, this will allow us
	//to verify that the pair of nodes that we select are NOT in each others replica set
	//prior to the synchronization process.
	ctx.Config.IndexSyncDelay = 40 * time.Second

	//allow sufficient time for the bootstrap process to complete.
	time.Sleep(30000 * time.Millisecond)

	//chose any two nodes, we blacklist them from one another to prevent them
	//choosing each other as replica set peers. We want to ensure that the nodes
	//ONLY discover each others entries via the IndexSync mechanism.
	node1 := ctx.BootstrapNodes[0]
	node2 := ctx.BootstrapNodes[3]
	node1.AddToBlacklist(node2.Addr)
	node2.AddToBlacklist(node1.Addr)

	//register to receive events from both nodes, via our test node event listener which will simply
	//add any received events to an internal queue that we can later query to validate that
	//events were received to our nodes as expected.
	node1EventListener := NewTestNodeEventListener()
	node2EventListener := NewTestNodeEventListener()
	node1.AppendNodeEventListener("node1", node1EventListener)
	node2.AppendNodeEventListener("node2", node2EventListener)

	//define shared key
	key := "leaf/x"

	//store our test index record 1, note EnableIndexUpdateEvents is not set and will default to false, even though IndexUpdateEvents ARE globally enabled.
	node1StoreErr := node1.StoreIndex(key, dht.RecordIndexEntry{Source: key, Target: "super/" + node1.ID.String()})
	if node1StoreErr != nil {
		t.Fatalf("Error occurred whilst Peer Node 1 was trying to store index entry: %v", node1StoreErr)
	}

	//store our test index record 2, note EnableIndexUpdateEvents is not set and will default to false, even though IndexUpdateEvents ARE globally enabled.
	node2StoreErr := node2.StoreIndex(key, dht.RecordIndexEntry{Source: key, Target: "super/" + node2.ID.String()})
	if node2StoreErr != nil {

		t.Fatalf("Error occurred whilst Peer Node 2 was trying to store index entry: %v", node2StoreErr)
	}

	//wait for approx half of the sync delay before validating that the nodes are not in
	//each other replica set, post the storage operation.
	if slices.Contains(node1.ListPeerAddresses(), node2.Addr) {
		fmt.Printf("\nNode 1 peer list INSIDE: %v\n", node1.ListPeerAddresses())
		t.Fatalf("Node 1 peer list should NOT contain Node 2 but it does, peer list: %v", node1.ListPeerAddresses())
	}

	if slices.Contains(node2.ListPeerAddresses(), node1.Addr) {
		fmt.Printf("\nNode 2 peer list INSIDE: %v\n", node2.ListPeerAddresses())
		t.Fatalf("Node 2 peer list should NOT contain Node 1 but it does, peer list: %v", node2.ListPeerAddresses())
	}

	//now we have confirmed that the nodes ARE NOT in each others replica set (as a result of the blacklist)
	//we wait half the sync delay time and unblacklist the nodes so they are able to exchange
	//SYNC INDEX messages.
	time.Sleep(ctx.Config.IndexSyncDelay / 2)
	node1.RemoveFromBlacklist(node2.Addr)
	node2.RemoveFromBlacklist(node1.Addr)

	//pause to allow some time for storage opp 2 to propergate.
	time.Sleep(10000*time.Millisecond + ctx.Config.IndexSyncDelay)

	indexFromNode1, found := node1.FindIndexLocal(key)
	if !found || len(indexFromNode1) != 2 {
		t.Fatalf("Expected node1 data store to equal 2 actual length was: %d", len(indexFromNode1))
	}

	//look uop entries in local store
	indexFromNode2, found := node2.FindIndexLocal(key)
	if !found || len(indexFromNode2) != 2 {
		t.Fatalf("Expected node2 data store to equal 2 actual length was: %d", len(indexFromNode2))
	}

	time.Sleep(5000 * time.Millisecond)
	//Next to validate receipt of the events on both nodes we pull the event store
	//from their respectie listeners. We expect Node1 to have received an event from Node2
	//and vice versa, indicating that each node received an index update event as a result of
	//the other nodes store operation and subsequent synchronization.
	node1Events := node1EventListener.GetReceivedIndexUpdateEvents()
	node2Events := node2EventListener.GetReceivedIndexUpdateEvents()

	//verify node 1 received and Index Update Event from Node 2
	if len(node1Events) > 0 {
		t.Fatalf("Node 1 received %d index update events but was expected to receive 0 since IndexUpdateEvents are globally disabled", len(node1Events))
	}

	//verify node 2 received and Index Update Event from Node 1
	if len(node2Events) > 0 {
		t.Fatalf("Node 2 received %d index update events but was expected to receive 0 since IndexUpdateEvents are globally disabled", len(node2Events))
	}

	t.Logf("\n Node 1 data store entries are: \n %v", indexFromNode1)
	t.Logf("\n Node 2 data store entries are: \n %v", indexFromNode2)

}

func Test_Full_Network_Create_Index_Entry_And_Validate_Sync_On_Deletion(t *testing.T) {

	/*
		We create two new index entries (one per node) under a shared key, pause for
		a breif moment to allow the creation to propergate before deleting one of the entries
		associated with one of the nodes. We then pause again to allow the deletion operation
		to also propergate before validating that the entry in question has been
		removed from BOTH nodes.

	*/

	//define node addresses for each set
	bootstrapNodes := []string{":7401", ":7402", ":7403", ":7404", ":7405"}

	//init nodes
	ctx := NewConfigurableTestContextWithBootstrapAddresses(t, 0, nil, bootstrapNodes, 0, 0)

	//slightly alter the default config to extend the sync delay, this will allow us
	//to verify that the pair of nodes that we select are NOT in each others replica set
	//prior to the synchronization process.
	ctx.Config.IndexSyncDelay = 40 * time.Second

	//allow sufficient time for the bootstrap process to complete.
	time.Sleep(30000 * time.Millisecond)

	//chose any two nodes, we blacklist them from one another to prevent them
	//choosing each other as replica set peers. We want to ensure that the nodes
	//ONLY discover each others entries via the IndexSync mechanism.
	node1 := ctx.BootstrapNodes[0]
	node2 := ctx.BootstrapNodes[3]
	node1.AddToBlacklist(node2.Addr)
	node2.AddToBlacklist(node1.Addr)

	//print node ids and addresses to the console for debugging purposes
	fmt.Printf("\nNode 1 ID: %s Address: %s\n", node1.ID.String(), node1.Addr)
	fmt.Printf("\nNode 2 ID: %s Address: %s\n", node2.ID.String(), node2.Addr)

	//define shared key
	key := "leaf/x"

	node1StoreErr := node1.StoreIndex(key, dht.RecordIndexEntry{Source: key, Target: "super/" + node1.ID.String()})
	if node1StoreErr != nil {
		t.Fatalf("Error occurred whilst Peer Node 1 was trying to store index entry: %v", node1StoreErr)
	}

	node2StoreErr := node2.StoreIndex(key, dht.RecordIndexEntry{Source: key, Target: "super/" + node2.ID.String()})
	if node2StoreErr != nil {

		t.Fatalf("Error occurred whilst Peer Node 2 was trying to store index entry: %v", node2StoreErr)
	}

	//wait for approx half of the sync delay before validating that the nodes are not in
	//each other replica set, post the storage operation.
	if slices.Contains(node1.ListPeerAddresses(), node2.Addr) {
		fmt.Printf("\nNode 1 peer list INSIDE: %v\n", node1.ListPeerAddresses())
		t.Fatalf("Node 1 peer list should NOT contain Node 2 but it does, peer list: %v", node1.ListPeerAddresses())
	}

	if slices.Contains(node2.ListPeerAddresses(), node1.Addr) {
		fmt.Printf("\nNode 2 peer list INSIDE: %v\n", node2.ListPeerAddresses())
		t.Fatalf("Node 2 peer list should NOT contain Node 1 but it does, peer list: %v", node2.ListPeerAddresses())
	}

	//now we have confirmed that the nodes ARE NOT in each others replica set (as a result of the blacklist)
	//we wait half the sync delay time and unblacklist the nodes so they are able to exchange
	//SYNC INDEX messages.
	time.Sleep(ctx.Config.IndexSyncDelay / 2)
	node1.RemoveFromBlacklist(node2.Addr)
	node2.RemoveFromBlacklist(node1.Addr)

	//pause to allow some time for storage opp 2 to propergate.
	time.Sleep(20000*time.Millisecond + ctx.Config.IndexSyncDelay)

	indexEntrieFromNode1PreDeletion, found := node1.FindIndexLocal(key)
	if !found || len(indexEntrieFromNode1PreDeletion) != 2 {
		t.Fatalf("Expected node1 data store to equal 2 actual length was: %d", len(indexEntrieFromNode1PreDeletion))
	}

	//look uop entries in local store
	indexEntriesFromNode2PreDeletion, found := node2.FindIndexLocal(key)
	if !found || len(indexEntriesFromNode2PreDeletion) != 2 {
		t.Fatalf("Expected node2 data store to equal 2 actual length was: %d", len(indexEntriesFromNode2PreDeletion))
	}

	//add nodes back to blacklist to prevernt the delete operation from including them in the replica set
	//of nodes the delete operation will be propogated to.
	node1.AddToBlacklist(node2.Addr)
	node2.AddToBlacklist(node1.Addr)

	//next we delete the entry from one of the nodes, which should, in turn, trigger the
	//sync process and notify the other node that the entry has been removed.
	//NOTE: For the purposes of the test key and indexSource are set to equal values.
	node1.DeleteIndex(key, key, false)

	//after a short pause remove nodes from the black list to allow
	//sync index messages to be exchanged between the nodes.
	time.Sleep(ctx.Config.IndexSyncDelay / 2)
	node1.RemoveFromBlacklist(node2.Addr)
	node2.RemoveFromBlacklist(node1.Addr)

	//pause to allow the deletion to propergate and trigger the synchronization process.
	time.Sleep(20000*time.Millisecond + ctx.Config.IndexSyncDelay)

	indexEntriesFromNode1PostDeletion, found := node1.FindIndexLocal(key)
	if len(indexEntriesFromNode1PostDeletion) != 1 {
		t.Fatalf("Expected node1 data store to equal 1 actual length was: %d", len(indexEntriesFromNode1PostDeletion))
	}

	indexEntriesFromNode2PostDeletion, found := node2.FindIndexLocal(key)
	if len(indexEntriesFromNode2PostDeletion) != 1 {
		t.Fatalf("Expected node2 data store to equal 1 actual length was: %d", len(indexEntriesFromNode2PostDeletion))
	}

	//since we deleted index from NODE 1, we further validate that the only remaining
	//entry on BOTH nodes is that belonging to Node 2.
	if indexEntriesFromNode1PostDeletion[0].GetPublisherAddr() != node2.Addr {
		t.Fatalf("Expected remaining index entry in node 1 data store to be that of Node 2 but it was: %v", indexEntriesFromNode1PostDeletion[0])
	}

	if indexEntriesFromNode2PostDeletion[0].GetPublisherAddr() != node2.Addr {
		t.Fatalf("Expected remaining index entry in node 2 data store to be that of Node 2 but it was: %v", indexEntriesFromNode2PostDeletion[0])
	}

	t.Logf("\n Node 1 data store entries are: \n %v", indexEntriesFromNode1PostDeletion)
	t.Logf("\n Node 2 data store entries are: \n %v", indexEntriesFromNode2PostDeletion)

}

func Test_Full_Network_Create_Index_Entry_And_Validate_Sync_And_Index_Update_Deletion_Event_Publication(t *testing.T) {

	/*
		We create two new index entries (one per node) under a shared key, pause for
		a breif moment to allow the creation to propergate before deleting one of the entries
		associated with one of the nodes. We then pause again to allow the deletion operation
		to also propergate before validating that the entry in question has been
		removed from BOTH nodes and requisite events were published.

	*/

	//define node addresses for each set
	bootstrapNodes := []string{":7401", ":7402", ":7403", ":7404", ":7405"}

	//init nodes
	ctx := NewConfigurableTestContextWithBootstrapAddresses(t, 0, nil, bootstrapNodes, 0, 0)

	//slightly alter the default config to extend the sync delay, this will allow us
	//to verify that the pair of nodes that we select are NOT in each others replica set
	//prior to the synchronization process.
	ctx.Config.IndexSyncDelay = 40 * time.Second

	//allow sufficient time for the bootstrap process to complete.
	time.Sleep(30000 * time.Millisecond)

	//chose any two nodes, we blacklist them from one another to prevent them
	//choosing each other as replica set peers. We want to ensure that the nodes
	//ONLY discover each others entries via the IndexSync mechanism.
	node1 := ctx.BootstrapNodes[0]
	node2 := ctx.BootstrapNodes[3]
	node1.AddToBlacklist(node2.Addr)
	node2.AddToBlacklist(node1.Addr)

	//before storing the entry we attach listeners to both nodes in order to be notified
	//of the deletion, out TestListener will merely append all events it receives to
	//an internal collection which may then query later for validaton purposes.
	node1Listener := NewTestNodeEventListener()
	node2Listener := NewTestNodeEventListener()
	node1.AppendNodeEventListener("Node1Lsnr", node1Listener)
	node2.AppendNodeEventListener("Node2Lsnr", node2Listener)

	//define shared key
	key := "leaf/x"

	node1StoreErr := node1.StoreIndex(key, dht.RecordIndexEntry{Source: key, Target: "super/" + node1.ID.String(), EnableIndexUpdateEvents: true})
	if node1StoreErr != nil {
		t.Fatalf("Error occurred whilst Peer Node 1 was trying to store index entry: %v", node1StoreErr)
	}

	node2StoreErr := node2.StoreIndex(key, dht.RecordIndexEntry{Source: key, Target: "super/" + node2.ID.String(), EnableIndexUpdateEvents: true})
	if node2StoreErr != nil {

		t.Fatalf("Error occurred whilst Peer Node 2 was trying to store index entry: %v", node2StoreErr)
	}

	//wait for approx half of the sync delay before validating that the nodes are not in
	//each other replica set, post the storage operation.
	if slices.Contains(node1.ListPeerAddresses(), node2.Addr) {
		fmt.Printf("\nNode 1 peer list INSIDE: %v\n", node1.ListPeerAddresses())
		t.Fatalf("Node 1 peer list should NOT contain Node 2 but it does, peer list: %v", node1.ListPeerAddresses())
	}

	if slices.Contains(node2.ListPeerAddresses(), node1.Addr) {
		fmt.Printf("\nNode 2 peer list INSIDE: %v\n", node2.ListPeerAddresses())
		t.Fatalf("Node 2 peer list should NOT contain Node 1 but it does, peer list: %v", node2.ListPeerAddresses())
	}

	//now we have confirmed that the nodes ARE NOT in each others replica set (as a result of the blacklist)
	//we wait half the sync delay time and unblacklist the nodes so they are able to exchange
	//SYNC INDEX messages.
	time.Sleep(ctx.Config.IndexSyncDelay / 2)
	node1.RemoveFromBlacklist(node2.Addr)
	node2.RemoveFromBlacklist(node1.Addr)

	//pause to allow some time for storage opp 2 to propergate.
	time.Sleep(20000*time.Millisecond + ctx.Config.IndexSyncDelay)

	indexEntrieFromNode1PreDeletion, found := node1.FindIndexLocal(key)
	if !found || len(indexEntrieFromNode1PreDeletion) != 2 {
		t.Fatalf("Expected node1 data store to equal 2 actual length was: %d", len(indexEntrieFromNode1PreDeletion))
	}

	//look uop entries in local store
	indexEntriesFromNode2PreDeletion, found := node2.FindIndexLocal(key)
	if !found || len(indexEntriesFromNode2PreDeletion) != 2 {
		t.Fatalf("Expected node2 data store to equal 2 actual length was: %d", len(indexEntriesFromNode2PreDeletion))
	}

	//before deletion we will also need to black list the nodes from one another
	//again as delete, like store, will internally attempt to build up a replica
	//set to propergate the deletion to, for sync events to work the nodes in question
	//MUST NOT be present in each others replica set.
	/*
		node1.AddToBlacklist(node2.Addr)
		node2.AddToBlacklist(node1.Addr)
	*/

	//next we delete the entry from one of the nodes, which should, in turn, trigger the
	//sync process and notify the other node that the entry has been removed.
	//NOTE: For the purposes of the test key and indexSource are set to equal values.
	deletionErr := node1.DeleteIndex(key, key, false)
	if deletionErr != nil {
		t.Fatalf("LOCAL DELETION FAILED: An error occurred whilst deleting index entry from Node 1: %v", deletionErr)
	} else {
		t.Logf("LOCAL DELETION SUCCEEDED: %s", key)
	}

	//propergation will happen after 40 seconds, as per the delay we set above thus
	//once half this time has elapsed we unblacklist the nodes from one another so
	//they are able to exchange the necessary SYNC messages.
	/*
		time.Sleep(ctx.Config.IndexSyncDelay / 2)
		node1.RemoveFromBlacklist(node2.Addr)
		node2.RemoveFromBlacklist(node1.Addr)
	*/

	//next we wait the remaining delay time plus a little extra to allow for the deletion
	//to fully propergate and be processed by both nodes, which should result in the
	//deletion of the relevant index entry from both nodes.
	time.Sleep(ctx.Config.IndexSyncDelay + (7000 * time.Millisecond))

	indexEntriesFromNode1PostDeletion, found := node1.FindIndexLocal(key)
	if len(indexEntriesFromNode1PostDeletion) != 1 {
		t.Fatalf("Expected node1 data store to equal 1 actual length was: %d", len(indexEntriesFromNode1PostDeletion))
	}

	indexEntriesFromNode2PostDeletion, found := node2.FindIndexLocal(key)
	if len(indexEntriesFromNode2PostDeletion) != 1 {
		t.Logf("\n Node 2 Data store entries are as follows: %v\n", indexEntriesFromNode2PostDeletion)
		t.Fatalf("Expected node2 data store to equal 1 actual length was: %d", len(indexEntriesFromNode2PostDeletion))

	}

	//since we deleted index from NODE 1, we further validate that the only remaining
	//entry on BOTH nodes is that belonging to Node 2.
	if indexEntriesFromNode1PostDeletion[0].GetPublisherAddr() != node2.Addr {
		t.Fatalf("Expected remaining index entry in node 1 data store to be that of Node 2 but it was: %v", indexEntriesFromNode1PostDeletion[0])
	}

	if indexEntriesFromNode2PostDeletion[0].GetPublisherAddr() != node2.Addr {
		t.Fatalf("Expected remaining index entry in node 2 data store to be that of Node 2 but it was: %v", indexEntriesFromNode2PostDeletion[0])
	}

	//node 1 should have received exactly ONE event, related to the initial storage operation
	//that preceeded the deletion. As the deletion was executed on this node IT SHOULD not
	//hhave received a second deletion event related to itself
	if len(node1Listener.GetReceivedIndexUpdateEvents()) != 1 {
		t.Fatalf("Expected Node 1 to have received 1 index update event but it received: %d", len(node1Listener.GetReceivedIndexUpdateEvents()))
	}

	//node 2 should have received exactly TWO events, one for the initial store operation and one for the subsequent deletion,
	// we validate that this is the case here by pulling the received events from our test listener
	// and checking the length of the resulting collection.
	if len(node2Listener.GetReceivedIndexUpdateEvents()) != 2 {
		t.Fatalf("Expected Node 2 to have received 2 index update events but it received: %d", len(node2Listener.GetReceivedIndexUpdateEvents()))
	}

	t.Logf("\n Node 1 data store entries are: \n %v", indexEntriesFromNode1PostDeletion)
	t.Logf("\n Node 2 data store entries are: \n %v", indexEntriesFromNode2PostDeletion)

}

func Test_Full_Network_Create_Index_Entry_And_Validate_Sync_And_Index_Update_Deletion_Event_Publication_When_Index_Update_Events_Globally_Disabled(t *testing.T) {

	/*
		We create two new index entries (one per node) under a shared key, pause for
		a breif moment to allow the creation to propergate before deleting one of the entries
		associated with one of the nodes. We then pause again to allow the deletion operation
		to also propergate before validating that the entry in question has been
		removed from BOTH nodes. However here we DO NOT expect any events were published
		as we explicitly disabled all Index Update events globally via the config.

	*/

	//define node addresses for each set
	bootstrapNodes := []string{":7401", ":7402", ":7403", ":7404", ":7405"}

	//init nodes
	ctx := NewConfigurableTestContextWithBootstrapAddresses(t, 0, nil, bootstrapNodes, 0, 0)

	//critically we DISABLE index update events globally via the config.
	ctx.Config.IndexUpdateEventsEnabled = false

	//slightly alter the default config to extend the sync delay, this will allow us
	//to verify that the pair of nodes that we select are NOT in each others replica set
	//prior to the synchronization process.
	ctx.Config.IndexSyncDelay = 40 * time.Second

	//allow sufficient time for the bootstrap process to complete.
	time.Sleep(30000 * time.Millisecond)

	//chose any two nodes, we blacklist them from one another to prevent them
	//choosing each other as replica set peers. We want to ensure that the nodes
	//ONLY discover each others entries via the IndexSync mechanism.
	node1 := ctx.BootstrapNodes[0]
	node2 := ctx.BootstrapNodes[3]
	node1.AddToBlacklist(node2.Addr)
	node2.AddToBlacklist(node1.Addr)

	//before storing the entry we attach listeners to both nodes in order to be notified
	//of the deletion, out TestListener will merely append all events it receives to
	//an internal collection which may then query later for validaton purposes.
	node1Listener := NewTestNodeEventListener()
	node2Listener := NewTestNodeEventListener()
	node1.AppendNodeEventListener("Node1Lsnr", node1Listener)
	node2.AppendNodeEventListener("Node2Lsnr", node2Listener)

	//define shared key
	key := "leaf/x"

	node1StoreErr := node1.StoreIndex(key, dht.RecordIndexEntry{Source: key, Target: "super/" + node1.ID.String(), EnableIndexUpdateEvents: true})
	if node1StoreErr != nil {
		t.Fatalf("Error occurred whilst Peer Node 1 was trying to store index entry: %v", node1StoreErr)
	}

	node2StoreErr := node2.StoreIndex(key, dht.RecordIndexEntry{Source: key, Target: "super/" + node2.ID.String(), EnableIndexUpdateEvents: true})
	if node2StoreErr != nil {

		t.Fatalf("Error occurred whilst Peer Node 2 was trying to store index entry: %v", node2StoreErr)
	}

	//wait for approx half of the sync delay before validating that the nodes are not in
	//each other replica set, post the storage operation.
	if slices.Contains(node1.ListPeerAddresses(), node2.Addr) {
		fmt.Printf("\nNode 1 peer list INSIDE: %v\n", node1.ListPeerAddresses())
		t.Fatalf("Node 1 peer list should NOT contain Node 2 but it does, peer list: %v", node1.ListPeerAddresses())
	}

	if slices.Contains(node2.ListPeerAddresses(), node1.Addr) {
		fmt.Printf("\nNode 2 peer list INSIDE: %v\n", node2.ListPeerAddresses())
		t.Fatalf("Node 2 peer list should NOT contain Node 1 but it does, peer list: %v", node2.ListPeerAddresses())
	}

	//now we have confirmed that the nodes ARE NOT in each others replica set (as a result of the blacklist)
	//we wait half the sync delay time and unblacklist the nodes so they are able to exchange
	//SYNC INDEX messages.
	time.Sleep(ctx.Config.IndexSyncDelay / 2)
	node1.RemoveFromBlacklist(node2.Addr)
	node2.RemoveFromBlacklist(node1.Addr)

	//pause to allow some time for storage opp 2 to propergate.
	time.Sleep(20000*time.Millisecond + ctx.Config.IndexSyncDelay)

	indexEntrieFromNode1PreDeletion, found := node1.FindIndexLocal(key)
	if !found || len(indexEntrieFromNode1PreDeletion) != 2 {
		t.Fatalf("Expected node1 data store to equal 2 actual length was: %d", len(indexEntrieFromNode1PreDeletion))
	}

	//look uop entries in local store
	indexEntriesFromNode2PreDeletion, found := node2.FindIndexLocal(key)
	if !found || len(indexEntriesFromNode2PreDeletion) != 2 {
		t.Fatalf("Expected node2 data store to equal 2 actual length was: %d", len(indexEntriesFromNode2PreDeletion))
	}

	//before deletion we will also need to black list the nodes from one another
	//again as delete, like store, will internally attempt to build up a replica
	//set to propergate the deletion to, for sync events to work the nodes in question
	//MUST NOT be present in each others replica set.
	/*
		node1.AddToBlacklist(node2.Addr)
		node2.AddToBlacklist(node1.Addr)
	*/

	//next we delete the entry from one of the nodes, which should, in turn, trigger the
	//sync process and notify the other node that the entry has been removed.
	//NOTE: For the purposes of the test key and indexSource are set to equal values.
	deletionErr := node1.DeleteIndex(key, key, false)
	if deletionErr != nil {
		t.Fatalf("LOCAL DELETION FAILED: An error occurred whilst deleting index entry from Node 1: %v", deletionErr)
	} else {
		t.Logf("LOCAL DELETION SUCCEEDED: %s", key)
	}

	//propergation will happen after 40 seconds, as per the delay we set above thus
	//once half this time has elapsed we unblacklist the nodes from one another so
	//they are able to exchange the necessary SYNC messages.
	/*
		time.Sleep(ctx.Config.IndexSyncDelay / 2)
		node1.RemoveFromBlacklist(node2.Addr)
		node2.RemoveFromBlacklist(node1.Addr)
	*/

	//next we wait the remaining delay time plus a little extra to allow for the deletion
	//to fully propergate and be processed by both nodes, which should result in the
	//deletion of the relevant index entry from both nodes.
	time.Sleep(ctx.Config.IndexSyncDelay + (7000 * time.Millisecond))

	indexEntriesFromNode1PostDeletion, found := node1.FindIndexLocal(key)
	if len(indexEntriesFromNode1PostDeletion) != 1 {
		t.Fatalf("Expected node1 data store to equal 1 actual length was: %d", len(indexEntriesFromNode1PostDeletion))
	}

	indexEntriesFromNode2PostDeletion, found := node2.FindIndexLocal(key)
	if len(indexEntriesFromNode2PostDeletion) != 1 {
		t.Logf("\n Node 2 Data store entries are as follows: %v\n", indexEntriesFromNode2PostDeletion)
		t.Fatalf("Expected node2 data store to equal 1 actual length was: %d", len(indexEntriesFromNode2PostDeletion))

	}

	//since we deleted index from NODE 1, we further validate that the only remaining
	//entry on BOTH nodes is that belonging to Node 2.
	if indexEntriesFromNode1PostDeletion[0].GetPublisherAddr() != node2.Addr {
		t.Fatalf("Expected remaining index entry in node 1 data store to be that of Node 2 but it was: %v", indexEntriesFromNode1PostDeletion[0])
	}

	if indexEntriesFromNode2PostDeletion[0].GetPublisherAddr() != node2.Addr {
		t.Fatalf("Expected remaining index entry in node 2 data store to be that of Node 2 but it was: %v", indexEntriesFromNode2PostDeletion[0])
	}

	//node 1 should have received exactly ONE event, related to the initial storage operation
	//that preceeded the deletion. As the deletion was executed on this node IT SHOULD not
	//hhave received a second deletion event related to itself
	if len(node1Listener.GetReceivedIndexUpdateEvents()) > 0 {
		t.Fatalf("Expected Node 1 to have received 0 index update events but it received: %d", len(node1Listener.GetReceivedIndexUpdateEvents()))
	}

	//node 2 should have received exactly TWO events, one for the initial store operation and one for the subsequent deletion,
	// we validate that this is the case here by pulling the received events from our test listener
	// and checking the length of the resulting collection.
	if len(node2Listener.GetReceivedIndexUpdateEvents()) > 0 {
		t.Fatalf("Expected Node 2 to have received 0 index update events but it received: %d", len(node2Listener.GetReceivedIndexUpdateEvents()))
	}

	t.Logf("\n Node 1 data store entries are: \n %v", indexEntriesFromNode1PostDeletion)
	t.Logf("\n Node 2 data store entries are: \n %v", indexEntriesFromNode2PostDeletion)

	//critically we RE-ENABLE index update events globally via the config to ensure that we do not impact other tests.
	ctx.Config.IndexUpdateEventsEnabled = true
}

func Test_Full_Network_Create_Index_Entry_And_Validate_Sync_And_Index_Update_Deletion_Event_Publication_When_Index_Update_Events_Individually_Disabled(t *testing.T) {

	/*
		We create two new index entries (one per node) under a shared key, pause for
		a breif moment to allow the creation to propergate before deleting one of the entries
		associated with one of the nodes. We then pause again to allow the deletion operation
		to also propergate before validating that the entry in question has been
		removed from BOTH nodes. However here we DO NOT expect any events were published
		as we explicitly disabled all Index Update events globally via the config.

	*/

	//define node addresses for each set
	bootstrapNodes := []string{":7401", ":7402", ":7403", ":7404", ":7405"}

	//init nodes
	ctx := NewConfigurableTestContextWithBootstrapAddresses(t, 0, nil, bootstrapNodes, 0, 0)

	//slightly alter the default config to extend the sync delay, this will allow us
	//to verify that the pair of nodes that we select are NOT in each others replica set
	//prior to the synchronization process.
	ctx.Config.IndexSyncDelay = 40 * time.Second

	//allow sufficient time for the bootstrap process to complete.
	time.Sleep(30000 * time.Millisecond)

	//chose any two nodes, we blacklist them from one another to prevent them
	//choosing each other as replica set peers. We want to ensure that the nodes
	//ONLY discover each others entries via the IndexSync mechanism.
	node1 := ctx.BootstrapNodes[0]
	node2 := ctx.BootstrapNodes[3]
	node1.AddToBlacklist(node2.Addr)
	node2.AddToBlacklist(node1.Addr)

	//before storing the entry we attach listeners to both nodes in order to be notified
	//of the deletion, out TestListener will merely append all events it receives to
	//an internal collection which may then query later for validaton purposes.
	node1Listener := NewTestNodeEventListener()
	node2Listener := NewTestNodeEventListener()
	node1.AppendNodeEventListener("Node1Lsnr", node1Listener)
	node2.AppendNodeEventListener("Node2Lsnr", node2Listener)

	//define shared key
	key := "leaf/x"

	// critically we DISABLE index update events on the individual store operations themselves via the RecordIndexEntry parameter (or more specifically the omission of it
	// as the parameter defaults to false), this will allow us to validate that even if index update
	// events are enabled globally, via the config, if they are disabled at the individual operation level
	// then no events will be received.
	node1StoreErr := node1.StoreIndex(key, dht.RecordIndexEntry{Source: key, Target: "super/" + node1.ID.String()})
	if node1StoreErr != nil {
		t.Fatalf("Error occurred whilst Peer Node 1 was trying to store index entry: %v", node1StoreErr)
	}

	node2StoreErr := node2.StoreIndex(key, dht.RecordIndexEntry{Source: key, Target: "super/" + node2.ID.String()})
	if node2StoreErr != nil {

		t.Fatalf("Error occurred whilst Peer Node 2 was trying to store index entry: %v", node2StoreErr)
	}

	//wait for approx half of the sync delay before validating that the nodes are not in
	//each other replica set, post the storage operation.
	if slices.Contains(node1.ListPeerAddresses(), node2.Addr) {
		fmt.Printf("\nNode 1 peer list INSIDE: %v\n", node1.ListPeerAddresses())
		t.Fatalf("Node 1 peer list should NOT contain Node 2 but it does, peer list: %v", node1.ListPeerAddresses())
	}

	if slices.Contains(node2.ListPeerAddresses(), node1.Addr) {
		fmt.Printf("\nNode 2 peer list INSIDE: %v\n", node2.ListPeerAddresses())
		t.Fatalf("Node 2 peer list should NOT contain Node 1 but it does, peer list: %v", node2.ListPeerAddresses())
	}

	//now we have confirmed that the nodes ARE NOT in each others replica set (as a result of the blacklist)
	//we wait half the sync delay time and unblacklist the nodes so they are able to exchange
	//SYNC INDEX messages.
	time.Sleep(ctx.Config.IndexSyncDelay / 2)
	node1.RemoveFromBlacklist(node2.Addr)
	node2.RemoveFromBlacklist(node1.Addr)

	//pause to allow some time for storage opp 2 to propergate.
	time.Sleep(20000*time.Millisecond + ctx.Config.IndexSyncDelay)

	indexEntrieFromNode1PreDeletion, found := node1.FindIndexLocal(key)
	if !found || len(indexEntrieFromNode1PreDeletion) != 2 {
		t.Fatalf("Expected node1 data store to equal 2 actual length was: %d", len(indexEntrieFromNode1PreDeletion))
	}

	//look uop entries in local store
	indexEntriesFromNode2PreDeletion, found := node2.FindIndexLocal(key)
	if !found || len(indexEntriesFromNode2PreDeletion) != 2 {
		t.Fatalf("Expected node2 data store to equal 2 actual length was: %d", len(indexEntriesFromNode2PreDeletion))
	}

	//before deletion we will also need to black list the nodes from one another
	//again as delete, like store, will internally attempt to build up a replica
	//set to propergate the deletion to, for sync events to work the nodes in question
	//MUST NOT be present in each others replica set.
	/*
		node1.AddToBlacklist(node2.Addr)
		node2.AddToBlacklist(node1.Addr)
	*/

	//next we delete the entry from one of the nodes, which should, in turn, trigger the
	//sync process and notify the other node that the entry has been removed.
	//NOTE: For the purposes of the test key and indexSource are set to equal values.
	deletionErr := node1.DeleteIndex(key, key, false)
	if deletionErr != nil {
		t.Fatalf("LOCAL DELETION FAILED: An error occurred whilst deleting index entry from Node 1: %v", deletionErr)
	} else {
		t.Logf("LOCAL DELETION SUCCEEDED: %s", key)
	}

	//propergation will happen after 40 seconds, as per the delay we set above thus
	//once half this time has elapsed we unblacklist the nodes from one another so
	//they are able to exchange the necessary SYNC messages.
	/*
		time.Sleep(ctx.Config.IndexSyncDelay / 2)
		node1.RemoveFromBlacklist(node2.Addr)
		node2.RemoveFromBlacklist(node1.Addr)
	*/

	//next we wait the remaining delay time plus a little extra to allow for the deletion
	//to fully propergate and be processed by both nodes, which should result in the
	//deletion of the relevant index entry from both nodes.
	time.Sleep(ctx.Config.IndexSyncDelay + (7000 * time.Millisecond))

	indexEntriesFromNode1PostDeletion, found := node1.FindIndexLocal(key)
	if len(indexEntriesFromNode1PostDeletion) != 1 {
		t.Fatalf("Expected node1 data store to equal 1 actual length was: %d", len(indexEntriesFromNode1PostDeletion))
	}

	indexEntriesFromNode2PostDeletion, found := node2.FindIndexLocal(key)
	if len(indexEntriesFromNode2PostDeletion) != 1 {
		t.Logf("\n Node 2 Data store entries are as follows: %v\n", indexEntriesFromNode2PostDeletion)
		t.Fatalf("Expected node2 data store to equal 1 actual length was: %d", len(indexEntriesFromNode2PostDeletion))

	}

	//since we deleted index from NODE 1, we further validate that the only remaining
	//entry on BOTH nodes is that belonging to Node 2.
	if indexEntriesFromNode1PostDeletion[0].GetPublisherAddr() != node2.Addr {
		t.Fatalf("Expected remaining index entry in node 1 data store to be that of Node 2 but it was: %v", indexEntriesFromNode1PostDeletion[0])
	}

	if indexEntriesFromNode2PostDeletion[0].GetPublisherAddr() != node2.Addr {
		t.Fatalf("Expected remaining index entry in node 2 data store to be that of Node 2 but it was: %v", indexEntriesFromNode2PostDeletion[0])
	}

	//node 1 should have received exactly ONE event, related to the initial storage operation
	//that preceeded the deletion. As the deletion was executed on this node IT SHOULD not
	//hhave received a second deletion event related to itself
	if len(node1Listener.GetReceivedIndexUpdateEvents()) > 0 {
		t.Fatalf("Expected Node 1 to have received 0 index update events but it received: %d", len(node1Listener.GetReceivedIndexUpdateEvents()))
	}

	//node 2 should have received exactly TWO events, one for the initial store operation and one for the subsequent deletion,
	// we validate that this is the case here by pulling the received events from our test listener
	// and checking the length of the resulting collection.
	if len(node2Listener.GetReceivedIndexUpdateEvents()) > 0 {
		t.Fatalf("Expected Node 2 to have received 0 index update events but it received: %d", len(node2Listener.GetReceivedIndexUpdateEvents()))
	}

	t.Logf("\n Node 1 data store entries are: \n %v", indexEntriesFromNode1PostDeletion)
	t.Logf("\n Node 2 data store entries are: \n %v", indexEntriesFromNode2PostDeletion)

}

func Test_Full_Network_Create_Index_Entry_And_Validate_Sync_With_Auto_Sync_Replica_Conflict_Resolution(t *testing.T) {

	/*
		Here we extend the Test_Full_Network_Create_Index_Entry_And_Validate_Sync test
		a little further by verifying that nodes now auto resolve the replica set/sync
		conflict that may arrise in smaller network when a node pick another as a replica
		that is also a first party publisher of the same index being stored. This should
		now happen WITHOUT calls to blacklist.

	*/

	//define node addresses for each set, we choose 3 to ensure they are each in each others replica set to begin with; the default replica set size is 3.
	bootstrapNodes := []string{":7401", ":7402", ":7403"}

	//init nodes
	ctx := NewConfigurableTestContextWithBootstrapAddresses(t, 0, nil, bootstrapNodes, 0, 0)

	//slightly alter the default config to extend the sync delay, this will allow us
	//to verify that the pair of nodes that we select are NOT in each others replica set
	//prior to the synchronization process.
	ctx.Config.IndexSyncDelay = 40 * time.Second

	//allow sufficient time for the bootstrap process to complete.
	time.Sleep(30000 * time.Millisecond)

	//chose any two nodes..
	node1 := ctx.BootstrapNodes[0]
	node2 := ctx.BootstrapNodes[2]

	//define shared key
	key := "leaf/x"

	node1StoreErr := node1.StoreIndex(key, dht.RecordIndexEntry{Source: key, Target: "super/" + node1.ID.String()})
	if node1StoreErr != nil {
		t.Fatalf("Error occurred whilst Peer Node 1 was trying to store index entry: %v", node1StoreErr)
	}

	//pause to allow some time for the first storage operation to propergate, this is vital to this
	//specific test as the entry must be present on node 2 for the new conflict resolution logic
	//in the store method to lookup the entry locally and ensure its not included in the replica set.
	time.Sleep(5000 * time.Millisecond)

	node2StoreErr := node2.StoreIndex(key, dht.RecordIndexEntry{Source: key, Target: "super/" + node2.ID.String()})
	if node2StoreErr != nil {

		t.Fatalf("Error occurred whilst Peer Node 2 was trying to store index entry: %v", node2StoreErr)
	}

	//pause for a breif time to allow the second storage operation to propergate.
	time.Sleep(5000 * time.Millisecond)

	//after a short time we validate that node 1 still only contains a SINGLE entry: ITSELF; node 2
	//entry should not have propergated to node 1 (via replica set) as a result of the conflict resolution logic
	// in the store method which should prevent node 1 from include 2 in it's replica set as they
	//are both first party publishers of the same index entry.
	indexFromNode1BeforeSync, found := node1.FindIndexLocal(key)
	if len(indexFromNode1BeforeSync) != 1 {
		t.Fatalf("Expected node1 data store BEFORE SYNC to equal 1 actual length was: %d", len(indexFromNode1BeforeSync))
	} else {
		t.Logf("\n Node 1 data store entries BEFORE SYNC are: \n %v", indexFromNode1BeforeSync)
	}

	//pause to allow some time for storage opp 2 to propergate, which SHOULD undertake the
	//Sync Index process once the ctx.Config.IndexSyncDelay time has elapsed. THIS will
	//propegate the storage to node 1 rather than via replica-set propergation, which would
	//have been detected and blocked by the conflict resolution logic in the store method.
	time.Sleep(20000*time.Millisecond + ctx.Config.IndexSyncDelay)

	indexFromNode1, found := node1.FindIndexLocal(key)
	if !found || len(indexFromNode1) != 2 {
		t.Fatalf("Expected node1 data store to equal 2 actual length was: %d", len(indexFromNode1))
	}

	//look uop entries in local store
	indexFromNode2, found := node2.FindIndexLocal(key)
	if !found || len(indexFromNode2) != 2 {
		t.Fatalf("Expected node2 data store to equal 2 actual length was: %d", len(indexFromNode2))
	}

	t.Logf("\n Node 1 data store entries are: \n %v", indexFromNode1)
	t.Logf("\n Node 2 data store entries are: \n %v", indexFromNode2)

}

func Test_Full_Network_Create_Index_Entry_And_Validate_Sync_With_Auto_Sync_Replica_Conflict_Resolution_with_Event_Publication_Enabled(t *testing.T) {

	/*
		Here we extend the
		Test_Full_Network_Create_Index_Entry_And_Validate_Sync_With_Auto_Sync_Replica_Conflict_Resolution
		test to ensure requisite event it also published.

	*/

	//define node addresses for each set, we choose 3 to ensure they are each in each others replica set to begin with; the default replica set size is 3.
	bootstrapNodes := []string{":7401", ":7402", ":7403"}

	//init nodes
	ctx := NewConfigurableTestContextWithBootstrapAddresses(t, 0, nil, bootstrapNodes, 0, 0)

	//slightly alter the default config to extend the sync delay, this will allow us
	//to verify that the pair of nodes that we select are NOT in each others replica set
	//prior to the synchronization process.
	ctx.Config.IndexSyncDelay = 40 * time.Second

	//allow sufficient time for the bootstrap process to complete.
	time.Sleep(30000 * time.Millisecond)

	//chose any two nodes..
	node1 := ctx.BootstrapNodes[0]
	node2 := ctx.BootstrapNodes[2]

	//before storing the entry we attach listeners to both nodes in order to be notified
	//of the deletion, out TestListener will merely append all events it receives to
	//an internal collection which may then query later for validaton purposes.
	node1Listener := NewTestNodeEventListener()
	node2Listener := NewTestNodeEventListener()
	node1.AppendNodeEventListener("Node1Lsnr", node1Listener)
	node2.AppendNodeEventListener("Node2Lsnr", node2Listener)

	//define shared key
	key := "leaf/x"

	//store data via node 1 critically, for the purposes of this test, we ensure Update events are enabled.
	node1StoreErr := node1.StoreIndex(key, dht.RecordIndexEntry{Source: key, Target: "super/" + node1.ID.String(), EnableIndexUpdateEvents: true})
	if node1StoreErr != nil {
		t.Fatalf("Error occurred whilst Peer Node 1 was trying to store index entry: %v", node1StoreErr)
	}

	//pause to allow some time for the first storage operation to propergate, this is vital to this
	//specific test as the entry must be present on node 2 for the new conflict resolution logic
	//in the store method to lookup the entry locally and ensure its not included in the replica set.
	time.Sleep(5000 * time.Millisecond)

	//store data via node 2 critically, for the purposes of this test, we ensure Update events are enabled.
	node2StoreErr := node2.StoreIndex(key, dht.RecordIndexEntry{Source: key, Target: "super/" + node2.ID.String(), EnableIndexUpdateEvents: true})
	if node2StoreErr != nil {

		t.Fatalf("Error occurred whilst Peer Node 2 was trying to store index entry: %v", node2StoreErr)
	}

	//pause for a breif time to allow the second storage operation to propergate.
	time.Sleep(5000 * time.Millisecond)

	//after a short time we validate that node 1 still only contains a SINGLE entry: ITSELF; node 2
	//entry should not have propergated to node 1 (via replica set) as a result of the conflict resolution logic
	// in the store method which should prevent node 1 from include 2 in it's replica set as they
	//are both first party publishers of the same index entry.
	indexFromNode1BeforeSync, found := node1.FindIndexLocal(key)
	if len(indexFromNode1BeforeSync) != 1 {
		t.Fatalf("Expected node1 data store BEFORE SYNC to equal 1 actual length was: %d", len(indexFromNode1BeforeSync))
	} else {
		t.Logf("\n Node 1 data store entries BEFORE SYNC are: \n %v", indexFromNode1BeforeSync)
	}

	//pause to allow some time for storage opp 2 to propergate, which SHOULD undertake the
	//Sync Index process once the ctx.Config.IndexSyncDelay time has elapsed. THIS will
	//propegate the storage to node 1 rather than via replica-set propergation, which would
	//have been detected and blocked by the conflict resolution logic in the store method.
	time.Sleep(20000*time.Millisecond + ctx.Config.IndexSyncDelay)

	indexFromNode1, found := node1.FindIndexLocal(key)
	if !found || len(indexFromNode1) != 2 {
		t.Fatalf("Expected node1 data store to equal 2 actual length was: %d", len(indexFromNode1))
	}

	//look uop entries in local store
	indexFromNode2, found := node2.FindIndexLocal(key)
	if !found || len(indexFromNode2) != 2 {
		t.Fatalf("Expected node2 data store to equal 2 actual length was: %d", len(indexFromNode2))
	}

	//we expect node 1 to have received a single event from node 2
	if len(node1Listener.GetReceivedIndexUpdateEvents()) != 1 {
		t.Fatalf("Expected Node 1 to have received 1 index update event but it received: %d", len(node1Listener.GetReceivedIndexUpdateEvents()))

	} else {
		t.Logf("\n Node 1 received the following index update event: \n %v", node1Listener.GetReceivedIndexUpdateEvents()[0])
	}

	t.Logf("\n Node 1 data store entries are: \n %v", indexFromNode1)
	t.Logf("\n Node 2 data store entries are: \n %v", indexFromNode2)

}

func Test_Full_Network_Create_Index_Entry_And_Validate_Sync_With_Auto_Sync_Replica_Conflict_Resolution_with_Event_Publication_Enabled_Store_Twice(t *testing.T) {

	/*
		Here we extend the
		Test_Full_Network_Create_Index_Entry_And_Validate_Sync_With_Auto_Sync_Replica_Conflict_Resolution_with_Event_Publication_Enabled_Store_Twice
		test to ensure than existing index:
		1)The first party publihers are still mutually excluded from each others replica set.
		2)The SYNC INDEX process results in the republishing and receipt of events on both nodes.

	*/

	//define node addresses for each set, we choose 3 to ensure they are each in each others replica set to begin with; the default replica set size is 3.
	bootstrapNodes := []string{":7401", ":7402", ":7403"}

	//init nodes
	ctx := NewConfigurableTestContextWithBootstrapAddresses(t, 0, nil, bootstrapNodes, 0, 0)

	//slightly alter the default config to extend the sync delay, this will allow us
	//to verify that the pair of nodes that we select are NOT in each others replica set
	//prior to the synchronization process.
	ctx.Config.IndexSyncDelay = 40 * time.Second

	//allow sufficient time for the bootstrap process to complete.
	time.Sleep(30000 * time.Millisecond)

	//chose any two nodes..
	node1 := ctx.BootstrapNodes[0]
	node2 := ctx.BootstrapNodes[2]

	//before storing the entry we attach listeners to both nodes in order to be notified
	//of the deletion, out TestListener will merely append all events it receives to
	//an internal collection which may then query later for validaton purposes.
	node1Listener := NewTestNodeEventListener()
	node2Listener := NewTestNodeEventListener()
	node1.AppendNodeEventListener("Node1Lsnr", node1Listener)
	node2.AppendNodeEventListener("Node2Lsnr", node2Listener)

	//define shared key
	key := "leaf/x"

	//store data via node 1 critically, for the purposes of this test, we ensure Update events are enabled.
	node1StoreErr := node1.StoreIndex(key, dht.RecordIndexEntry{Source: key, Target: "super/" + node1.ID.String(), EnableIndexUpdateEvents: true})
	if node1StoreErr != nil {
		t.Fatalf("Error occurred whilst Peer Node 1 was trying to store index entry: %v", node1StoreErr)
	}

	//pause to allow some time for the first storage operation to propergate, this is vital to this
	//specific test as the entry must be present on node 2 for the new conflict resolution logic
	//in the store method to lookup the entry locally and ensure its not included in the replica set.
	time.Sleep(5000 * time.Millisecond)

	//store data via node 2 critically, for the purposes of this test, we ensure Update events are enabled.
	node2StoreErr := node2.StoreIndex(key, dht.RecordIndexEntry{Source: key, Target: "super/" + node2.ID.String(), EnableIndexUpdateEvents: true})
	if node2StoreErr != nil {

		t.Fatalf("Error occurred whilst Peer Node 2 was trying to store index entry: %v", node2StoreErr)
	}

	//pause for a breif time to allow the second storage operation to propergate.
	time.Sleep(5000 * time.Millisecond)

	//after a short time we validate that node 1 still only contains a SINGLE entry: ITSELF; node 2
	//entry should not have propergated to node 1 (via replica set) as a result of the conflict resolution logic
	// in the store method which should prevent node 1 from include 2 in it's replica set as they
	//are both first party publishers of the same index entry.
	indexFromNode1BeforeSync, found := node1.FindIndexLocal(key)
	if len(indexFromNode1BeforeSync) != 1 {
		t.Fatalf("Expected node1 data store BEFORE SYNC to equal 1 actual length was: %d", len(indexFromNode1BeforeSync))
	} else {
		t.Logf("\n Node 1 data store entries BEFORE SYNC are: \n %v", indexFromNode1BeforeSync)
	}

	//pause to allow some time for storage opp 2 to propergate, which SHOULD undertake the
	//Sync Index process once the ctx.Config.IndexSyncDelay time has elapsed. THIS will
	//propegate the storage to node 1 rather than via replica-set propergation, which would
	//have been detected and blocked by the conflict resolution logic in the store method.
	time.Sleep(20000*time.Millisecond + ctx.Config.IndexSyncDelay)

	indexFromNode1, found := node1.FindIndexLocal(key)
	if !found || len(indexFromNode1) != 2 {
		t.Fatalf("Expected node1 data store to equal 2 actual length was: %d", len(indexFromNode1))
	}

	//look uop entries in local store
	indexFromNode2, found := node2.FindIndexLocal(key)
	if !found || len(indexFromNode2) != 2 {
		t.Fatalf("Expected node2 data store to equal 2 actual length was: %d", len(indexFromNode2))
	}

	//we expect node 1 to have received a single event from node 2
	if len(node1Listener.GetReceivedIndexUpdateEvents()) != 1 {
		t.Fatalf("Expected Node 1 to have received 1 index update event but it received: %d", len(node1Listener.GetReceivedIndexUpdateEvents()))
	} else {
		t.Logf("\n Node 1 received the following index update event: \n %v", node1Listener.GetReceivedIndexUpdateEvents()[0])
	}

	//we expect node 2 to have received no events.
	if len(node2Listener.GetReceivedIndexUpdateEvents()) != 1 {
		t.Fatalf("Expected Node 2 to have received 0 index update events but it received: %d", len(node2Listener.GetReceivedIndexUpdateEvents()))
	} else {
		t.Logf("\n Node 2 received %d events", len(node2Listener.GetReceivedIndexUpdateEvents()))
	}

	t.Logf("\n Node 1 data store entries are: \n %v", indexFromNode1)
	t.Logf("\n Node 2 data store entries are: \n %v", indexFromNode2)

	fmt.Println("PAUSING BREIFLY BEFORE NEXT ROUND OF STORAGE REQUESTS;")
	fmt.Println()
	time.Sleep(10000 * time.Millisecond)

	//store data via node 1 critically, for the purposes of this test, we ensure Update events are enabled.
	node1Store2Err := node1.StoreIndex(key, dht.RecordIndexEntry{Source: key, Target: "super/" + node1.ID.String() + "-two", EnableIndexUpdateEvents: true})
	if node1Store2Err != nil {
		t.Fatalf("Error occurred whilst Peer Node 1 was trying to store index entry: %v", node1Store2Err)
	}

	//pause to allow some time for the first storage operation to propergate, this is vital to this
	//specific test as the entry must be present on node 2 for the new conflict resolution logic
	//in the store method to lookup the entry locally and ensure its not included in the replica set.
	time.Sleep(5000 * time.Millisecond)

	//store data via node 2 critically, for the purposes of this test, we ensure Update events are enabled.
	node2Store2Err := node2.StoreIndex(key, dht.RecordIndexEntry{Source: key, Target: "super/" + node2.ID.String() + "-two", EnableIndexUpdateEvents: true})
	if node2Store2Err != nil {

		t.Fatalf("Error occurred whilst Peer Node 2 was trying to store index entry: %v", node2Store2Err)
	}

	//pause for a breif time to allow the second storage operation to propergate.
	//NOTE: We MUST wait at least the ctx.Config.IndexSyncDelay duration to ensure
	//      the SYNC process has been undertaken.
	time.Sleep(20000*time.Millisecond + ctx.Config.IndexSyncDelay)

	indexFromNode1AfterSecondStore, found := node1.FindIndexLocal(key)
	if !found || len(indexFromNode1AfterSecondStore) != 2 {
		t.Fatalf("Expected node1 data store to equal 2 actual length was: %d", len(indexFromNode1AfterSecondStore))
	}

	//look uop entries in local store
	indexFromNode2AfterSecondStore, found := node2.FindIndexLocal(key)
	if !found || len(indexFromNode2AfterSecondStore) != 2 {
		t.Fatalf("Expected node2 data store to equal 2 actual length was: %d", len(indexFromNode2AfterSecondStore))
	}

	//to validate that the second storage completed successfully we validate that sn additional event
	//has been received to both of our listeners...

	// node 1 should have received TWO events, one for the initial store operation and one for the subsequent store operation,
	// we validate that this is the case here by pulling the received events from our test listener
	// and checking the length of the resulting collection.
	if len(node1Listener.GetReceivedIndexUpdateEvents()) != 2 {
		t.Fatalf("Expected Node 1 to have received 2 index update events but it received: %d", len(node1Listener.GetReceivedIndexUpdateEvents()))
	} else {
		t.Logf("\n Node 1 received the following index update events: \n %v", node1Listener.GetReceivedIndexUpdateEvents())
	}

	//node 2 should have received one event from the second storage operation, as it was not a co publisher
	//at the time the first store operation was executed by node 1 and was instead iinitially included in node 1's replica set.
	if len(node2Listener.GetReceivedIndexUpdateEvents()) != 2 {
		t.Logf("\n IN ERROR: Node 2 received the following index update events: \n")
		for i, event := range node2Listener.GetReceivedIndexUpdateEvents() {
			t.Logf("\n Event %d: %v\n", i, event)
		}

		t.Fatalf("Expected Node 2 to have received 2 index update events but it received: %d", len(node2Listener.GetReceivedIndexUpdateEvents()))
	} else {
		t.Logf("\n Node 2 received the following index update event: \n %v", node2Listener.GetReceivedIndexUpdateEvents())
	}

}

func Test_Full_Network_Create_Index_Entry_And_Validate_Sync_With_Auto_Sync_Replica_Conflict_Resolution_With_Three_Keys_And_Multi_Store_Rounds(t *testing.T) {

	//define node addresses for each set, we choose 5 nodes and select 3 of them as first-party publishers.
	bootstrapNodes := []string{":7401", ":7402", ":7403", ":7404", ":7405"}

	//init nodes
	ctx := NewConfigurableTestContextWithBootstrapAddresses(t, 0, nil, bootstrapNodes, 0, 0)

	//slightly alter the default config to extend the sync delay, this mirrors timing patterns from related tests.
	ctx.Config.IndexSyncDelay = 40 * time.Second

	//allow sufficient time for the bootstrap process to complete.
	time.Sleep(30000 * time.Millisecond)

	//select 3 nodes from the 5-node network for repeated index storage.
	node1 := ctx.BootstrapNodes[0]
	node2 := ctx.BootstrapNodes[2]
	node3 := ctx.BootstrapNodes[4]

	//define 3 shared keys for all selected publishers.
	keys := []string{"leaf/x", "leaf/y", "leaf/z"}

	//local helper to store all keys for a node with a specific target suffix.
	storeAllKeys := func(node *dht.Node, suffix string) {
		for _, key := range keys {
			err := node.StoreIndex(key, dht.RecordIndexEntry{Source: key, Target: "super/" + node.ID.String() + suffix, UpdatedUnix: time.Now().UnixNano()})
			if err != nil {
				t.Fatalf("Error occurred whilst %s was trying to store index entry for key %s: %v", node.Addr, key, err)
			}
		}
	}

	//round 1
	storeAllKeys(node1, "")
	time.Sleep(5000 * time.Millisecond)
	storeAllKeys(node2, "")
	time.Sleep(5000 * time.Millisecond)
	storeAllKeys(node3, "")

	//round 2
	time.Sleep(5000 * time.Millisecond)
	storeAllKeys(node1, "-two")
	time.Sleep(5000 * time.Millisecond)
	storeAllKeys(node2, "-two")
	time.Sleep(5000 * time.Millisecond)
	storeAllKeys(node3, "-two")

	//round 3
	time.Sleep(5000 * time.Millisecond)
	storeAllKeys(node1, "-three")
	time.Sleep(5000 * time.Millisecond)
	storeAllKeys(node2, "-three")
	time.Sleep(5000 * time.Millisecond)
	storeAllKeys(node3, "-three")

	//round 4: node1 and node2 only.
	time.Sleep(5000 * time.Millisecond)
	storeAllKeys(node1, "-four")
	time.Sleep(5000 * time.Millisecond)
	storeAllKeys(node2, "-four")

	//wait sufficient time for sync/propagation across the full network.
	time.Sleep(20000*time.Millisecond + ctx.Config.IndexSyncDelay)

	//expected final targets: node1 and node2 at version four, node3 at version three.
	expectedTargets := map[string]bool{
		"super/" + node1.ID.String() + "-four":  true,
		"super/" + node2.ID.String() + "-four":  true,
		"super/" + node3.ID.String() + "-three": true,
	}

	//validate each node has exactly 3 entries per key and all are the latest versions.
	for _, currentNode := range ctx.BootstrapNodes {
		for _, key := range keys {
			entries, found := currentNode.FindIndexLocal(key)
			if !found {
				t.Fatalf("Expected node %s to contain local index entries for key %s but found none", currentNode.Addr, key)
			}

			if len(entries) != 3 {
				t.Fatalf("Expected node %s to have exactly 3 entries for key %s but got: %d", currentNode.Addr, key, len(entries))
			}

			actualTargets := make(map[string]bool)
			for _, entry := range entries {
				actualTargets[entry.Target] = true
			}

			for expectedTarget := range expectedTargets {
				if !actualTargets[expectedTarget] {
					t.Fatalf("Expected node %s key %s to contain latest target %s but it was missing; actual targets: %v", currentNode.Addr, key, expectedTarget, actualTargets)
				}
			}
		}
	}

}

func Test_Full_Network_Create_Index_Entry_And_Validate_Sync_With_Auto_Sync_Replica_Conflict_Resolution_With_Three_Keys_And_Multi_Store_Rounds_And_Event_Validation(t *testing.T) {

	/*
		Extends Test_Full_Network_Create_Index_Entry_And_Validate_Sync_With_Auto_Sync_Replica_Conflict_Resolution_With_Three_Keys_And_Multi_Store_Rounds
		by additionally registering a TestNodeEventListener on each of the 3 co-publisher nodes and verifying:

		1) Each co-publisher receives at least the minimum expected number of IndexUpdateEvents across all keys.
		   Co-publishers exchange updates exclusively via the SYNC INDEX mechanism (they are mutually excluded
		   from each other's replica sets), so event delivery is asynchronous and the count is bounded below.

		2) The last IndexUpdateEvent received per key on every co-publisher carries exactly 3 entries (one per
		   publisher) and each entry target reflects the latest version stored:
		      - node1 and node2 both reached round four  ("-four" suffix)
		      - node3 reached round three ("-three" suffix)
	*/

	//define node addresses for each set, we choose 5 nodes and select 3 of them as first-party publishers.
	bootstrapNodes := []string{":7401", ":7402", ":7403", ":7404", ":7405"}

	//init nodes
	ctx := NewConfigurableTestContextWithBootstrapAddresses(t, 0, nil, bootstrapNodes, 0, 0)

	//slightly alter the default config to extend the sync delay, this mirrors timing patterns from related tests.
	ctx.Config.IndexSyncDelay = 40 * time.Second

	//allow sufficient time for the bootstrap process to complete.
	time.Sleep(30000 * time.Millisecond)

	//select 3 nodes from the 5-node network for repeated index storage.
	node1 := ctx.BootstrapNodes[0]
	node2 := ctx.BootstrapNodes[2]
	node3 := ctx.BootstrapNodes[4]

	//register event listeners on the 3 co-publisher nodes only; they are the only nodes
	//that exchange updates via the SYNC INDEX mechanism and should therefore receive events.
	node1Listener := NewTestNodeEventListener()
	node2Listener := NewTestNodeEventListener()
	node3Listener := NewTestNodeEventListener()

	node1.AppendNodeEventListener("Node1Lsnr", node1Listener)
	node2.AppendNodeEventListener("Node2Lsnr", node2Listener)
	node3.AppendNodeEventListener("Node3Lsnr", node3Listener)

	//define 3 shared keys for all selected publishers.
	keys := []string{"leaf/x", "leaf/y", "leaf/z"}

	//local helper to store all keys for a node with a specific target suffix.
	storeAllKeys := func(node *dht.Node, suffix string) {
		for _, key := range keys {
			err := node.StoreIndex(key, dht.RecordIndexEntry{Source: key, Target: "super/" + node.ID.String() + suffix, UpdatedUnix: time.Now().UnixNano(), EnableIndexUpdateEvents: true})
			if err != nil {
				t.Fatalf("Error occurred whilst %s was trying to store index entry for key %s: %v", node.Addr, key, err)
			}
		}
	}

	//round 1
	storeAllKeys(node1, "")
	time.Sleep(5000 * time.Millisecond)
	storeAllKeys(node2, "")
	time.Sleep(5000 * time.Millisecond)
	storeAllKeys(node3, "")

	//round 2
	time.Sleep(5000 * time.Millisecond)
	storeAllKeys(node1, "-two")
	time.Sleep(5000 * time.Millisecond)
	storeAllKeys(node2, "-two")
	time.Sleep(5000 * time.Millisecond)
	storeAllKeys(node3, "-two")

	//round 3
	time.Sleep(5000 * time.Millisecond)
	storeAllKeys(node1, "-three")
	time.Sleep(5000 * time.Millisecond)
	storeAllKeys(node2, "-three")
	time.Sleep(5000 * time.Millisecond)
	storeAllKeys(node3, "-three")

	//round 4: node1 and node2 only.
	time.Sleep(5000 * time.Millisecond)
	storeAllKeys(node1, "-four")
	time.Sleep(5000 * time.Millisecond)
	storeAllKeys(node2, "-four")

	//wait sufficient time for sync/propagation across the full network.
	time.Sleep(20000*time.Millisecond + ctx.Config.IndexSyncDelay)

	//expected final targets: node1 and node2 at version four, node3 at version three.
	expectedLatestTargets := map[string]bool{
		"super/" + node1.ID.String() + "-four":  true,
		"super/" + node2.ID.String() + "-four":  true,
		"super/" + node3.ID.String() + "-three": true,
	}

	//minimum event totals (summed across all 3 keys):
	//  Each co-publisher relies on the SYNC mechanism to learn the other two publishers' updates.
	//  With a 40 s sync interval and the post-storage wait, at least one sync cycle fires per
	//  co-publisher pair, so each publisher receives at least one event per key from each of the
	//  other two publishers → minimum 2 co-publishers × 3 keys = 6 events.
	type nodeExpectation struct {
		listener      *TestNodeEventListener
		node          *dht.Node
		minEventCount int
	}

	expectations := []nodeExpectation{
		{node1Listener, node1, 6},
		{node2Listener, node2, 6},
		{node3Listener, node3, 6},
	}

	for _, exp := range expectations {
		allEvents := exp.listener.GetReceivedIndexUpdateEvents()

		//1) Check the total event count across all keys meets the minimum.
		if len(allEvents) < exp.minEventCount {
			t.Fatalf("Node %s expected at least %d total IndexUpdateEvents but received: %d",
				exp.node.Addr, exp.minEventCount, len(allEvents))
		} else {
			t.Logf("Node %s received %d total IndexUpdateEvents (minimum expected: %d)",
				exp.node.Addr, len(allEvents), exp.minEventCount)
		}

		//2) For each key find the last event and verify its entries snapshot.
		for _, key := range keys {

			//collect all events for this specific key in the order they arrived.
			var keyEvents []events.IndexUpdateEvent
			for _, ev := range allEvents {
				if ev.GetKey() == key {
					keyEvents = append(keyEvents, ev)
				}
			}

			if len(keyEvents) == 0 {
				t.Fatalf("Node %s received no IndexUpdateEvents for key %s", exp.node.Addr, key)
			}

			//inspect the last (most recent) event for this key.
			lastEvent := keyEvents[len(keyEvents)-1]

			//the entries snapshot must contain exactly one entry per publisher.
			if len(lastEvent.GetEntries()) != 3 {
				t.Fatalf("Node %s last event for key %s: expected 3 entries but got %d",
					exp.node.Addr, key, len(lastEvent.GetEntries()))
			}

			//every expected latest target must be present in the entries snapshot.
			actualTargets := make(map[string]bool)
			entryPairs := make([]string, 0, len(lastEvent.GetEntries()))
			for _, entry := range lastEvent.GetEntries() {
				actualTargets[entry.GetTarget()] = true
				entryPairs = append(entryPairs, entry.GetSource()+" -> "+entry.GetTarget())
			}

			for expectedTarget := range expectedLatestTargets {
				if !actualTargets[expectedTarget] {
					t.Fatalf("Node %s last event for key %s: expected latest target %q but it was missing; actual targets: %v",
						exp.node.Addr, key, expectedTarget, actualTargets)
				}
			}

			t.Logf("Node %s key %s: last event ok — %d entries, all latest targets present",
				exp.node.Addr, key, len(lastEvent.GetEntries()))
			t.Logf("The last event on node %s for key %s contains the entries: %v",
				exp.node.Addr, key, entryPairs)
		}
	}

}

func Test_Full_Network_Create_Index_Entry_And_Validate_Sync_With_Auto_Sync_Replica_Conflict_Resolution_With_Partially_Disjoint_Replica_Set_On_Node_1(t *testing.T) {

	/*
		Here we extend the Test_Full_Network_Create_Index_Entry_And_Validate_Sync_With_Auto_Sync_Replica_Conflict_Resolution test
		a little further by verifying that nodes CAN STILL auto resolve the replica set/sync
		conflict that may arrise in smaller network when a node pick another as a replica
		that is also a first party publisher of the same index being stored. The main difference
		here is:UNLIKE THE AFOREMENTIONED PRIOR TEST, OF TWO NODES SELECTED FOR THE PURPOSES OF **THIS** TEST
		NODE 2 WILL **NOT** BE PART OF NODE 1'S, REPLICA SET. The implications of this change is two fold:

		1)When NODE 1 stores an entry, it will NOT be replicated to NODE 2 as it will not be in Node 1's replica set
		  as a DIRECT RESULT of this the LOCAL "FIND" lookup NODE 2 undertakes, before attempting to store IT'S entry
		  in the index WILL FAIL. It will therefore assume that its the ONLY node that references the index.
		2)Later when we store to NODE 2, NODE 1 **WILL** BE in NODE 2's replica set and thus it will initially attempt
		  to replicate the entry to NODE 1 ***AND THIS IS WHERE THE CONFLICT ARISES ON NODE 1 ***, as node 1, at this point, will be
		  part of node 1s replica set for the entry AND also a first party publisher of the entry. To resolve this
		  on receipt of the replication request NODE1 should:
		    a)Attempt to find the entry locally, in this case the entry WILL be found.
			b)Determine whether it has an existing entry in the index, again, it WILL in this case.
			c)Drop the replication request, it can be sure that NODE 2, will dispatch a subsequent SYNC INDEX message
			  a moment later once it discovers (VIA THE SYNC PROCESS) that NODE 1 is also a first party publisher, this will occur just
			  after the storage and replication process.

		3)Later, back on Node 2, it WILL learn of NODE 1's entry as part of the SYNC process which executes a network-wide find,
		at that point it will need to dispatch a SYNC INDEX request to NODE 1, ***AND THIS IS WHERE THE CONFLICT ARISES ON NODE 2 ***
		as node 2 at this point will have NODE 1 in its replica set AFTER having learned that its also a first party
		publisher to the same index. To resolve this, rather than dropping all nodes present in the replica set from
		being possible sync candidates, we should instead ad the condition to only do this when this node is NOT
		a first party publisher itself. Note: this conflict will only need to be resolved ONCE per entry as once the
		learn nodes about co-third party publisher, they will automatically be excluded from the replica set for
		that entry in any subsequent storage operations.

	*/

	//define node addresses for each set.
	bootstrapNodes := []string{":7401", ":7402", ":7403"}

	//init nodes
	ctx := NewConfigurableTestContextWithBootstrapAddresses(t, 0, nil, bootstrapNodes, 0, 0)

	//slightly alter the default config to extend the sync delay, this will allow us
	//to verify that the pair of nodes that we select are NOT in each others replica set
	//prior to the synchronization process.
	ctx.Config.IndexSyncDelay = 40 * time.Second

	//allow sufficient time for the bootstrap process to complete.
	time.Sleep(30000 * time.Millisecond)

	//chose any two nodes..
	node1 := ctx.BootstrapNodes[0]
	node2 := ctx.BootstrapNodes[2]

	//we explicitly blacklist node 2 from node 1
	node1.AddToBlacklist(node2.Addr)

	//define shared key
	key := "leaf/x"

	node1StoreErr := node1.StoreIndex(key, dht.RecordIndexEntry{Source: key, Target: "super/" + node1.ID.String()})
	if node1StoreErr != nil {
		t.Fatalf("Error occurred whilst Peer Node 1 was trying to store index entry: %v", node1StoreErr)
	}

	//pause to allow some time for the first storage operation to propergate, this is vital to this
	//specific test as the entry must be present on node 2 for the new conflict resolution logic
	//in the store method to lookup the entry locally and ensure its not included in the replica set.
	time.Sleep(5000 * time.Millisecond)

	node2StoreErr := node2.StoreIndex(key, dht.RecordIndexEntry{Source: key, Target: "super/" + node2.ID.String()})
	if node2StoreErr != nil {

		t.Fatalf("Error occurred whilst Peer Node 2 was trying to store index entry: %v", node2StoreErr)
	}

	//pause for a breif time to allow the second storage operation to propergate.
	time.Sleep(5000 * time.Millisecond)

	//after a short time we validate that node 2 still only contains a SINGLE entry: ITSELF; node 1
	//entry should not have propergated to node 2 (via replica set) as a result of the temporary
	// blacklist of node 1
	indexFromNode2BeforeSync, found := node2.FindIndexLocal(key)
	if len(indexFromNode2BeforeSync) != 1 {
		t.Fatalf("Expected node2 data store BEFORE SYNC to equal 1 actual length was: %d", len(indexFromNode2BeforeSync))
	} else {
		t.Logf("\n Node 2 data store entries BEFORE SYNC are: \n %v", indexFromNode2BeforeSync)
	}

	//next unblacklist node 2 from node 1, to allow subsequent SYNC messages to be exchanged between the nodes.
	node1.RemoveFromBlacklist(node2.Addr)

	//pause to allow some time for storage opp 2 to propergate, which SHOULD undertake the
	//Sync Index process once the ctx.Config.IndexSyncDelay time has elapsed. THIS will
	//propegate the storage to node 1 rather than via replica-set propergation, which would
	//have been detected and blocked by the conflict resolution logic in the store method.
	time.Sleep(20000*time.Millisecond + ctx.Config.IndexSyncDelay)

	indexFromNode1, found := node1.FindIndexLocal(key)
	if !found || len(indexFromNode1) != 2 {
		t.Fatalf("Expected node1 data store to equal 2 actual length was: %d", len(indexFromNode1))
	}

	//look uop entries in local store
	indexFromNode2, found := node2.FindIndexLocal(key)
	if !found || len(indexFromNode2) != 2 {
		t.Fatalf("Expected node2 data store to equal 2 actual length was: %d", len(indexFromNode2))
	}

	t.Logf("\n Node 1 data store entries are: \n %v", indexFromNode1)
	t.Logf("\n Node 2 data store entries are: \n %v", indexFromNode2)

}

func Test_Full_Network_Create_Index_Entry_And_Validate_Sync_With_Auto_Sync_Replica_Conflict_Resolution_with_Event_Publication_Enabled_With_Partially_Disjoint_Replica_Set_On_Node_1(t *testing.T) {

	/*
		Here we extend the
		Test_Full_Network_Create_Index_Entry_And_Validate_Sync_With_Auto_Sync_Replica_Conflict_Resolution
		test to ensure requisite event it also published.

	*/

	//define node addresses for each set, we choose 3 to ensure they are each in each others replica set to begin with; the default replica set size is 3.
	bootstrapNodes := []string{":7401", ":7402", ":7403"}

	//init nodes
	ctx := NewConfigurableTestContextWithBootstrapAddresses(t, 0, nil, bootstrapNodes, 0, 0)

	//slightly alter the default config to extend the sync delay, this will allow us
	//to verify that the pair of nodes that we select are NOT in each others replica set
	//prior to the synchronization process.
	ctx.Config.IndexSyncDelay = 40 * time.Second

	//allow sufficient time for the bootstrap process to complete.
	time.Sleep(30000 * time.Millisecond)

	//chose any two nodes..
	node1 := ctx.BootstrapNodes[0]
	node2 := ctx.BootstrapNodes[2]

	//lack list node 2 from node 1 to ensure its not in node 1's replica set.
	node1.AddToBlacklist(node2.Addr)

	//before storing the entry we attach listeners to both nodes in order to be notified
	//of the deletion, out TestListener will merely append all events it receives to
	//an internal collection which may then query later for validaton purposes.
	node1Listener := NewTestNodeEventListener()
	node2Listener := NewTestNodeEventListener()
	node1.AppendNodeEventListener("Node1Lsnr", node1Listener)
	node2.AppendNodeEventListener("Node2Lsnr", node2Listener)

	//define shared key
	key := "leaf/x"

	//store data via node 1 critically, for the purposes of this test, we ensure Update events are enabled.
	node1StoreErr := node1.StoreIndex(key, dht.RecordIndexEntry{Source: key, Target: "super/" + node1.ID.String(), EnableIndexUpdateEvents: true})
	if node1StoreErr != nil {
		t.Fatalf("Error occurred whilst Peer Node 1 was trying to store index entry: %v", node1StoreErr)
	}

	//pause to allow some time for the first storage operation to propergate, this is vital to this
	//specific test as the entry must be present on node 2 for the new conflict resolution logic
	//in the store method to lookup the entry locally and ensure its not included in the replica set.
	time.Sleep(5000 * time.Millisecond)

	//store data via node 2 critically, for the purposes of this test, we ensure Update events are enabled.
	node2StoreErr := node2.StoreIndex(key, dht.RecordIndexEntry{Source: key, Target: "super/" + node2.ID.String(), EnableIndexUpdateEvents: true})
	if node2StoreErr != nil {

		t.Fatalf("Error occurred whilst Peer Node 2 was trying to store index entry: %v", node2StoreErr)
	}

	//pause for a breif time to allow the second storage operation to propergate.
	time.Sleep(5000 * time.Millisecond)

	//after a short time we validate that node 1 still only contains a SINGLE entry: ITSELF; node 2
	//entry should not have propergated to node 1 as it is blacklisted.
	indexFromNode1BeforeSync, found := node1.FindIndexLocal(key)
	if len(indexFromNode1BeforeSync) != 1 {
		t.Fatalf("Expected node1 data store BEFORE SYNC to equal 1 actual length was: %d", len(indexFromNode1BeforeSync))
	} else {
		t.Logf("\n Node 1 data store entries BEFORE SYNC are: \n %v", indexFromNode1BeforeSync)
	}

	// validate that node 2 still only contains a SINGLE entry: ITSELF; node 1 entry should not have propergated to node 2 (via replica set) as a result of the temporary
	// blacklist of node 1
	indexFromNode2BeforeSync, found := node2.FindIndexLocal(key)
	if len(indexFromNode2BeforeSync) != 1 {
		t.Fatalf("Expected node2 data store BEFORE SYNC to equal 1 actual length was: %d", len(indexFromNode2BeforeSync))
	} else {
		t.Logf("\n Node 2 data store entries BEFORE SYNC are: \n %v", indexFromNode2BeforeSync)
	}

	//next unblacklist node 2 from node 1, to allow subsequent SYNC messages to be exchanged between the nodes.
	node1.RemoveFromBlacklist(node2.Addr)

	//pause to allow some time for storage opp 2 to propergate, which SHOULD undertake the
	//Sync Index process once the ctx.Config.IndexSyncDelay time has elapsed. THIS will
	//propegate the storage to node 1 rather than via replica-set propergation, which would
	//have been detected and blocked by the conflict resolution logic in the store method.
	time.Sleep(20000*time.Millisecond + ctx.Config.IndexSyncDelay)

	indexFromNode1, found := node1.FindIndexLocal(key)
	if !found || len(indexFromNode1) != 2 {
		t.Fatalf("Expected node1 data store to equal 2 actual length was: %d", len(indexFromNode1))
	}

	//look uop entries in local store
	indexFromNode2, found := node2.FindIndexLocal(key)
	if !found || len(indexFromNode2) != 2 {
		t.Fatalf("Expected node2 data store to equal 2 actual length was: %d", len(indexFromNode2))
	}

	//we expect node 1 to have received a single event from node 2
	if len(node1Listener.GetReceivedIndexUpdateEvents()) != 1 {
		t.Fatalf("Expected Node 1 to have received 1 index update event but it received: %d", len(node1Listener.GetReceivedIndexUpdateEvents()))

	} else {
		t.Logf("\n Node 1 received the following index update event: \n %v", node1Listener.GetReceivedIndexUpdateEvents()[0])
	}

	t.Logf("\n Node 1 data store entries are: \n %v", indexFromNode1)
	t.Logf("\n Node 2 data store entries are: \n %v", indexFromNode2)

}

/* NB: This test will always fail until the EnableIndexUpdateEvents flag inheritance mechanism is properly implemented
       currently there is not way to do this without executing a Find operation, to pull the IndexRecord
	   prior to storage, which will obviously introduce significant overhead.
func Test_Full_Network_Create_Index_Entry_And_Validate_Sync_And_Index_Update_Events_Enablement_Inheritance(t *testing.T) {


		Here we validate that where the EnableIndexUpdateEvents flag is set to true/false at the record level, for the
		the FIRST IndexEntry in a Record that value is inherited by any all subsequent IndexEntrys added to that record.
		This mechanism is intended to prevent inconsistent values being set for the flag across different nodes.
		In this context, "first" is determined by the IndexEntry that has the earliest "created" timestamp.


	//define node addresses for each set
	bootstrapNodes := []string{":7401", ":7402", ":7403", ":7404", ":7405"}

	//init nodes
	ctx := NewConfigurableTestContextWithBootstrapAddresses(t, 0, nil, bootstrapNodes, 0, 0)

	//slightly alter the default config to extend the sync delay, this will allow us
	//to verify that the pair of nodes that we select are NOT each others replica set
	//prior to the synchronization process.
	ctx.Config.IndexSyncDelay = 40 * time.Second

	//allow sufficient time for the bootstrap process to complete.
	time.Sleep(30000 * time.Millisecond)

	//chose any two nodes, we blacklist them from one another to prevent them
	//choosing each other as replica set peers. We want to ensure that the nodes
	//ONLY discover each others entries via the IndexSync mechanism.
	node1 := ctx.BootstrapNodes[0]
	node2 := ctx.BootstrapNodes[3]
	node1.AddToBlacklist(node2.Addr)
	node2.AddToBlacklist(node1.Addr)

	//register to receive events from both nodes, via our test node event listener which will simply
	//add any received events to an internal queue that we can later query to validate that
	//events were received to our nodes as expected.
	node1EventListener := NewTestNodeEventListener()
	node2EventListener := NewTestNodeEventListener()
	node1.AppendNodeEventListener("node1", node1EventListener)
	node2.AppendNodeEventListener("node2", node2EventListener)

	//define shared key
	key := "leaf/x"

	//store our test index record 1 taking care to set EnableIndexUpdateEvents to true, this will ensure events are published when this index is updated.
	node1StoreErr := node1.StoreIndex(key, dht.RecordIndexEntry{Source: key, Target: "super/" + node1.ID.String(), EnableIndexUpdateEvents: true})
	if node1StoreErr != nil {
		t.Fatalf("Error occurred whilst Peer Node 1 was trying to store index entry: %v", node1StoreErr)
	}

	//store our test index record 2 taking care to set EnableIndexUpdateEvents to true, note we explicitly set the flag to false here, which should take
	// no effect as the flag value SHOULD be inherited from the IndexEntry that was firstly stored, above.
	//NB: We allow a small amount of time to elapse before storing the second entry to ensure it's "created" timestamp is later than that of the first entry.
	time.Sleep(time.Millisecond * 1000)
	node2StoreErr := node2.StoreIndex(key, dht.RecordIndexEntry{Source: key, Target: "super/" + node2.ID.String(), EnableIndexUpdateEvents: false})
	if node2StoreErr != nil {

		t.Fatalf("Error occurred whilst Peer Node 2 was trying to store index entry: %v", node2StoreErr)
	}

	//wait for approx half of the sync delay before validating that the nodes are not in
	//each other replica set, post the storage operation.
	if slices.Contains(node1.ListPeerAddresses(), node2.Addr) {
		fmt.Printf("\nNode 1 peer list INSIDE: %v\n", node1.ListPeerAddresses())
		t.Fatalf("Node 1 peer list should NOT contain Node 2 but it does, peer list: %v", node1.ListPeerAddresses())
	}

	if slices.Contains(node2.ListPeerAddresses(), node1.Addr) {
		fmt.Printf("\nNode 2 peer list INSIDE: %v\n", node2.ListPeerAddresses())
		t.Fatalf("Node 2 peer list should NOT contain Node 1 but it does, peer list: %v", node2.ListPeerAddresses())
	}

	//now we have confirmed that the nodes ARE NOT in each others replica set (as a result of the blacklist)
	//we wait half the sync delay time and unblacklist the nodes so they are able to exchange
	//SYNC INDEX messages.
	time.Sleep(ctx.Config.IndexSyncDelay / 2)
	node1.RemoveFromBlacklist(node2.Addr)
	node2.RemoveFromBlacklist(node1.Addr)

	//pause to allow some time for storage opp 2 to propergate.
	time.Sleep(10000*time.Millisecond + ctx.Config.IndexSyncDelay)

	indexFromNode1, found := node1.FindIndexLocal(key)
	if !found || len(indexFromNode1) != 2 {
		t.Fatalf("Expected node1 data store to equal 2 actual length was: %d", len(indexFromNode1))
	}

	//look uop entries in local store
	indexFromNode2, found := node2.FindIndexLocal(key)
	if !found || len(indexFromNode2) != 2 {
		t.Fatalf("Expected node2 data store to equal 2 actual length was: %d", len(indexFromNode2))
	}

	time.Sleep(5000 * time.Millisecond)
	//Next to validate receipt of the events on both nodes we pull the event store
	//from their respectie listeners. We expect Node1 to have received an event from Node2
	//and vice versa, indicating that each node received an index update event as a result of
	//the other nodes store operation and subsequent synchronization.
	node1Events := node1EventListener.GetReceivedIndexUpdateEvents()
	node2Events := node2EventListener.GetReceivedIndexUpdateEvents()

	//verify node 1 received and Index Update Event from Node 2
	if len(node1Events) == 0 {
		t.Fatal("Node 1 did not receive any index update events but was expected to receive at least one.")
	} else {
		t.Logf("\nNode 1 received %d index update events\n", len(node1Events))
	}

	foundEventFromNode2 := false
	for _, curNode1Event := range node1Events {
		fmt.Printf("\n Node: %s Received event from node @: %s\n", node1.Addr, curNode1Event.GetPublisherAddress())
		if curNode1Event.GetPublisherAddress() == node2.Addr {
			t.Logf("Node 1 received expected index update event: %v", curNode1Event)
			foundEventFromNode2 = true
			break
		}
	}
	if !foundEventFromNode2 {
		t.Fatal("Node 1 did not receive expected index update event from Node 2")
	}

	//verify node 2 received and Index Update Event from Node 1
	if len(node2Events) == 0 {
		t.Fatal("Node 2 did not receive any index update events but was expected to receive at least one.")
	}

	foundEventFromNode1 := false
	for _, curNode2Event := range node2Events {
		fmt.Printf("\n Node: %s Received event from node @: %s\n", node2.Addr, curNode2Event.GetPublisherAddress())
		if curNode2Event.GetPublisherAddress() == node1.Addr {
			t.Logf("Node 2 received expected index update event: %v", curNode2Event)
			foundEventFromNode1 = true
			break
		}
	}
	if !foundEventFromNode1 {
		t.Fatal("Node 2 did not receive expected index update event from Node 1")
	}

	t.Logf("\n Node 1 data store entries are: \n %v", indexFromNode1)
	t.Logf("\n Node 2 data store entries are: \n %v", indexFromNode2)

}
*/

func Test_PeerHealth_ThresholdCrossing_PublishesEventAndDropsPeer(t *testing.T) {
	cfg := configurePeerHealthTest(t, 2, 200*time.Millisecond)
	ctx := NewConfigurableTestContext(t, 2, cfg, false)
	n1 := ctx.Nodes[0]
	n2 := ctx.Nodes[1]
	rt := n1.GetRoutingTable()
	waitForPeerInRoutingTable(t, rt, n2.ID, 3*time.Second)

	listener := newTestRoutingTableListener()
	rt.RegisterListener("peer-health-threshold", listener)
	defer rt.UnregisterListener("peer-health-threshold")

	for i := 0; i < cfg.MaxNodeConnectionFailureThreshold; i++ {
		rt.MarkPeerConnectionFailure(n2.ID)
	}

	listener.waitForEventCount(t, 1, time.Second)
	time.Sleep(cfg.UnhealthyPeerGracePeriod + 200*time.Millisecond)

	if _, ok := rt.GetPeer(n2.ID); ok {
		t.Fatalf("expected peer %s to be removed after grace period", n2.ID.String())
	}
}

func Test_PeerHealth_SuccessResetsFailureCountBeforeThreshold(t *testing.T) {
	cfg := configurePeerHealthTest(t, 3, 400*time.Millisecond)
	ctx := NewConfigurableTestContext(t, 2, cfg, false)
	n1 := ctx.Nodes[0]
	n2 := ctx.Nodes[1]
	rt := n1.GetRoutingTable()
	waitForPeerInRoutingTable(t, rt, n2.ID, 3*time.Second)

	rt.MarkPeerConnectionFailure(n2.ID)
	peer, ok := rt.GetPeer(n2.ID)
	if !ok {
		t.Fatalf("expected peer %s in routing table", n2.ID.String())
	}
	if peer.GetCumulativeConnectionFailures() != 1 {
		t.Fatalf("expected failure count to equal 1, got %d", peer.GetCumulativeConnectionFailures())
	}

	rt.MarkPeerConnectionSuccess(n2.ID)
	peer, ok = rt.GetPeer(n2.ID)
	if !ok {
		t.Fatalf("expected peer %s in routing table after success", n2.ID.String())
	}
	if peer.GetCumulativeConnectionFailures() != 0 {
		t.Fatalf("expected failure count reset to 0, got %d", peer.GetCumulativeConnectionFailures())
	}
	if !peer.IsHealthy() {
		t.Fatalf("expected peer %s to be healthy after success", n2.ID.String())
	}
}

func Test_PeerHealth_SuccessBeforeGracePeriodPreventsPeerRemoval(t *testing.T) {
	cfg := configurePeerHealthTest(t, 2, 300*time.Millisecond)
	ctx := NewConfigurableTestContext(t, 2, cfg, false)
	n1 := ctx.Nodes[0]
	n2 := ctx.Nodes[1]
	rt := n1.GetRoutingTable()
	waitForPeerInRoutingTable(t, rt, n2.ID, 3*time.Second)

	listener := newTestRoutingTableListener()
	rt.RegisterListener("peer-health-recovery", listener)
	defer rt.UnregisterListener("peer-health-recovery")

	for i := 0; i < cfg.MaxNodeConnectionFailureThreshold; i++ {
		rt.MarkPeerConnectionFailure(n2.ID)
	}
	listener.waitForEventCount(t, 1, time.Second)

	rt.MarkPeerConnectionSuccess(n2.ID)
	time.Sleep(cfg.UnhealthyPeerGracePeriod + 200*time.Millisecond)

	peer, ok := rt.GetPeer(n2.ID)
	if !ok {
		t.Fatalf("expected peer %s to remain in routing table after recovery", n2.ID.String())
	}
	if !peer.IsHealthy() {
		t.Fatalf("expected peer %s to be healthy after recovery", n2.ID.String())
	}
	if peer.GetCumulativeConnectionFailures() != 0 {
		t.Fatalf("expected failure count reset to 0 after recovery, got %d", peer.GetCumulativeConnectionFailures())
	}
}

func Test_PeerHealth_UnhealthyPeersExcludedFromClosestResults(t *testing.T) {
	cfg := configurePeerHealthTest(t, 1, 5*time.Second)
	ctx := NewConfigurableTestContext(t, 3, cfg, false)
	n1 := ctx.Nodes[0]
	n2 := ctx.Nodes[1]
	n3 := ctx.Nodes[2]
	rt := n1.GetRoutingTable()
	waitForPeerInRoutingTable(t, rt, n2.ID, 3*time.Second)
	waitForPeerInRoutingTable(t, rt, n3.ID, 3*time.Second)

	initial := rt.Closest(n1.ID, 10)
	if !peerSliceContains(initial, n2.ID) || !peerSliceContains(initial, n3.ID) {
		t.Fatalf("expected routing table to return both peers before marking unhealthy")
	}

	rt.MarkPeerConnectionFailure(n2.ID)
	afterFailure := rt.Closest(n1.ID, 10)
	if peerSliceContains(afterFailure, n2.ID) {
		t.Fatalf("expected unhealthy peer %s to be excluded from closest results", n2.ID.String())
	}
	if !peerSliceContains(afterFailure, n3.ID) {
		t.Fatalf("expected healthy peer %s to remain in closest results", n3.ID.String())
	}

	rt.MarkPeerConnectionSuccess(n2.ID)
	recovered := rt.Closest(n1.ID, 10)
	if !peerSliceContains(recovered, n2.ID) {
		t.Fatalf("expected recovered peer %s to reappear in closest results", n2.ID.String())
	}
}

type routingTableEvent struct {
	peerID   types.NodeID
	peerAddr string
}

type testRoutingTableListener struct {
	mu     sync.Mutex
	events []routingTableEvent
}

func newTestRoutingTableListener() *testRoutingTableListener {
	return &testRoutingTableListener{}
}

func (l *testRoutingTableListener) OnPeerUnhealthyStateChange(peerId types.NodeID, peerAddr string) {
	l.mu.Lock()
	l.events = append(l.events, routingTableEvent{peerID: peerId, peerAddr: peerAddr})
	l.mu.Unlock()
}

func (l *testRoutingTableListener) waitForEventCount(t *testing.T, expected int, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if l.eventCount() >= expected {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %d routing table events (received %d)", expected, l.eventCount())
		case <-ticker.C:
		}
	}
}

func (l *testRoutingTableListener) eventCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.events)
}

func waitForPeerInRoutingTable(t *testing.T, rt *routing.RoutingTable, peerID types.NodeID, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, ok := rt.GetPeer(peerID); ok {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for peer %s to appear in routing table", peerID.String())
		case <-ticker.C:
		}
	}
}

func peerSliceContains(peers []*routing.Peer, peerID types.NodeID) bool {
	for _, p := range peers {
		if p != nil && p.ID == peerID {
			return true
		}
	}
	return false
}

func configurePeerHealthTest(t *testing.T, threshold int, grace time.Duration) *config.Config {
	t.Helper()
	config.Reset()
	cfg := config.GetDefaultSingletonInstance()
	cfg.UseProtobuf = true
	cfg.RequestTimeout = 1000 * time.Millisecond
	cfg.DefaultEntryTTL = 30 * time.Second
	cfg.DefaultIndexEntryTTL = 30 * time.Second
	cfg.RefreshInterval = 5 * time.Second
	cfg.JanitorInterval = 10 * time.Second
	cfg.MaxNodeConnectionFailureThreshold = threshold
	cfg.UnhealthyPeerGracePeriod = grace
	t.Cleanup(func() {
		config.Reset()
	})
	return cfg
}

/*****************************************************************************************************************
 *                                     HELPER/UTILITY TYPES AND FUNCTIONS FOR E2E TESTS
 ******************************************************************************************************************/

// TestContext is used to hold context info for e2e tests
type TestContext struct {
	Nodes          []*dht.Node
	Config         *config.Config
	BootstrapNodes []*dht.Node
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
	n1, _ := dht.NewNode("node1", ":9321", netx.NewTCP(), cfg, dht.NT_CORE)
	n2, _ := dht.NewNode("node2", ":9322", netx.NewTCP(), cfg, dht.NT_CORE)
	Nodes := []*dht.Node{n1, n2}

	//we now bootstrap via the connect public interface method.
	if err := n1.Connect(n2.Addr); err != nil {
		t.Fatal("Error occurred whilst Peer Node 1 was trying to connect to Peer Node 2:", err)
	}

	//finally defer context cleanup
	t.Cleanup(func() {
		for _, n := range Nodes {
			n.Shutdown()
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
	Nodes := make([]*dht.Node, 0)
	startingIP := 8999
	for i := 0; i < nodeCount; i++ {
		startingIP++
		nodeIP := startingIP + 1
		nodeNameStr := "node" + strconv.Itoa(i+1)
		nodeIpStr := strconv.Itoa(nodeIP)
		node, _ := dht.NewNode(nodeNameStr, ":"+nodeIpStr, netx.NewTCP(), cfg, dht.NT_CORE)
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
			n.Shutdown()
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
		/*
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
		*/

	} else {
		cfg = conf
	}

	//first create and bootstrap the core network (bootstrap) nodes.
	bootstrapNodes := make([]*dht.Node, 0)
	for i, addr := range bootstrapAddresses {
		bootstrapNode, instantiationErr := dht.NewNode("bootstrapNode"+strconv.Itoa(i), addr, netx.NewTCP(), cfg, dht.NT_CORE)
		if instantiationErr != nil {
			t.Fatalf("Failed to create bootstrap node %d at address %s: %v", i+1, addr, instantiationErr)
		}
		bootstrapNode.Bootstrap(bootstrapAddresses, connectDelayMillis) //crucially bootstrap the node, witch the provided addresses. The node will ignore its own address.
		bootstrapNodes = append(bootstrapNodes, bootstrapNode)

	}

	//Next create some STANDARD test nodes.
	standardNodes := make([]*dht.Node, 0)
	startingIP := 8999
	for i := 0; i < standardNodeCount; i++ {
		startingIP++
		nodeIP := startingIP + 1
		nodeNameStr := "node" + strconv.Itoa(i+1)
		nodeIpStr := strconv.Itoa(nodeIP)
		node, instantiationErr := dht.NewNode(nodeNameStr, ":"+nodeIpStr, netx.NewTCP(), cfg, dht.NT_EXTERNAL)
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
			n.Shutdown()
		}

		//then clean up standard nodes
		for _, n := range standardNodes {
			n.Shutdown()
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

func ToSparsePairings(pairings []Pairing[*dht.Node], bootstrapNodes []*dht.Node) []Pairing[*dht.Node] {

	//forward-like, function declaration to allow us to call the fuction recursively.
	var selectRandomNode func([]*dht.Node, *dht.Node, int, int) (*dht.Node, error)
	selectRandomNode = func(allNodes []*dht.Node, callerNode *dht.Node, retryCount int, maxRetries int) (*dht.Node, error) {

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

func IsSparsePairing(pairings []Pairing[*dht.Node]) bool {

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

// TestNodeEventListener - A simple implementation of the NodeEventListener interface which
// will add any received IndexUpdateEvents to the receivedIndexUpdateEvents collection
// for later validation.
type TestNodeEventListener struct {
	receivedIndexUpdateEvents []events.IndexUpdateEvent
}

func (tl *TestNodeEventListener) OnIndexUpdated(event events.IndexUpdateEvent) {
	//fmt.Printf("\n++++++Appending event to collection: %v", event)
	tl.receivedIndexUpdateEvents = append(tl.receivedIndexUpdateEvents, event)
}
func (tl *TestNodeEventListener) GetReceivedIndexUpdateEvents() []events.IndexUpdateEvent {
	return tl.receivedIndexUpdateEvents
}
func NewTestNodeEventListener() *TestNodeEventListener {
	return &TestNodeEventListener{
		receivedIndexUpdateEvents: make([]events.IndexUpdateEvent, 0),
	}
}
