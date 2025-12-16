package dht

import (
	"testing"
	"time"

	"github.com/SharefulNetworks/shareful-dht/netx"
)

func Test_Create_And_Find_Standard_Entry_Value(t *testing.T) {

	//prepare config.
	cfg := DefaultConfig()
	cfg.UseProtobuf = true // set true after generating pb
	cfg.RequestTimeout = 2000 * time.Millisecond
	cfg.DefaultTTL = 30 * time.Second
	cfg.RefreshInterval = 5 * time.Second
	cfg.JanitorInterval = 10 * time.Second

	//prepare nodes.
	n1 := NewNode("node1", ":9321", netx.NewTCP(), cfg)
	n2 := NewNode("node2", ":9322", netx.NewTCP(), cfg)
	defer n1.Close()
	defer n2.Close()

	//we now bootstrap via the connect public interface method.
	if err := n1.Connect(n2.Addr); err != nil {
		t.Fatal("Error occurred whilst Peer Node 1 was trying to connect to Peer Node 2:", err)
	}

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

	//prepare config
	cfg := DefaultConfig()
	cfg.UseProtobuf = true
	cfg.RequestTimeout = 2000 * time.Millisecond
	cfg.DefaultTTL = 30 * time.Second
	cfg.RefreshInterval = 5 * time.Second
	cfg.JanitorInterval = 10 * time.Second

	//prepare nodes
	n1 := NewNode("node1", ":9321", netx.NewTCP(), cfg)
	n2 := NewNode("node2", ":9322", netx.NewTCP(), cfg)
	defer n1.Close()
	defer n2.Close()

	//we now bootstrap via the connect public interface method.
	if err := n1.Connect(n2.Addr); err != nil {
		t.Fatal("Error occurred whilst Peer Node 1 was trying to connect to Peer Node 2:", err)
	}

	//sote entries from both nodes under the same key
	key := "leaf/x"
	peer1StoreIndexErr := n1.StoreIndexValue(key, IndexEntry{Source: key, Target: "super/" + n1.ID.String(), UpdatedUnix: time.Now().UnixNano()}, 15*time.Second)
	if peer1StoreIndexErr != nil {
		t.Fatalf("Error occurred whilst Peer Node 1 was trying to store index entry: %v", peer1StoreIndexErr)
	}

	peer2IndexIndexStoreErr := n2.StoreIndexValue(key, IndexEntry{Source: key, Target: "super/" + n2.ID.String(), UpdatedUnix: time.Now().UnixNano()}, 15*time.Second)
	if peer2IndexIndexStoreErr != nil {
		t.Fatalf("Error occurred whilst Peer Node 2 was trying to store index entry: %v", peer2IndexIndexStoreErr)
	}

	//after a short delay attempt to retrieve the merged index FROM the DHT, via node 1
	time.Sleep(3000 * time.Millisecond)
	if ents, ok := n1.FindIndexRemote(key); !ok || len(ents) < 2 {
		t.Fatalf("expected merged index >=2")
	}
	time.Sleep(4000 * time.Millisecond)
	if ents, ok := n2.FindIndexRemote(key); !ok || len(ents) < 2 {
		t.Fatalf("index not refreshed")
	}
}
