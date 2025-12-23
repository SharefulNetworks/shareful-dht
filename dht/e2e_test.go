package dht

import (
	"strconv"
	"testing"
	"time"

	"github.com/SharefulNetworks/shareful-dht/netx"
)

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
	peer1StoreErr := n1.StoreWithTTL("alpha", []byte("v"), 10*time.Second, n1.ID)
	if peer1StoreErr != nil {
		t.Fatal("Error occurred whilst Peer Node 1 was trying to store entry:", peer1StoreErr)
	}

	//store a standard entry to the DHT via node 2,we directly call the *WithTTL variant to set a short ttl
	peer2StoreErr := n2.StoreWithTTL("beta", []byte("w"), 10*time.Second, n2.ID)
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

// TestContext is used to hold context info for e2e tests
type TestContext struct {
	Nodes  []*Node
	Config *Config
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

	//if th user has requested a peer mapping, output it to the console.
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
