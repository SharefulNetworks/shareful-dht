package dht

import (
	"testing"
	"time"

	"github.com/SharefulNetworks/shareful-dht/netx"
)

func Test_Remote_Find_Value(t *testing.T) {

	//prepare config.
	cfg := DefaultConfig()
	cfg.UseProtobuf = true // set true after generating pb
	cfg.RequestTimeout = 2000 * time.Millisecond
	cfg.DefaultTTL = 30 * time.Second
	cfg.RefreshInterval = 3 * time.Second
	cfg.JanitorInterval = 1 * time.Second

	//prepare nodes.
	n1 := NewNode("node1", ":9321", netx.NewTCP(), cfg)
	n2 := NewNode("node2", ":9322", netx.NewTCP(), cfg)
	defer n1.Close()
	defer n2.Close()
	n1.AddPeer(n2.Addr, n2.ID)
	n2.AddPeer(n1.Addr, n1.ID)

	//store test data TO the DHT via node 1.
	//if err := n1.Store("k", []byte("v")); err != nil {
	//	t.Fatal(err)
	//}

	_ = n1.Store("alpha", []byte("v"))

	//after a short delay attempt to retrieve the data FROM the DHT, via node 2.
	time.Sleep(1000 * time.Millisecond)
	if v, ok := n2.FindRemote("alpha"); !ok || string(v) != "v" {
		t.Fatalf("FindRemote failed %q", string(v))
	}
}
func Test_Remote_Find_Index_Merge_Refresh(t *testing.T) {
	cfg := DefaultConfig()
	cfg.UseProtobuf = false
	cfg.DefaultTTL = 1500 * time.Millisecond
	cfg.RequestTimeout = 1500 * time.Millisecond
	cfg.RefreshInterval = 700 * time.Millisecond
	cfg.JanitorInterval = 200 * time.Millisecond
	n1 := NewNode("node3", ":9331", netx.NewTCP(), cfg)
	n2 := NewNode("node4", ":9332", netx.NewTCP(), cfg)
	defer n1.Close()
	defer n2.Close()
	n1.AddPeer(n2.Addr, n2.ID)
	n2.AddPeer(n1.Addr, n1.ID)
	key := "leaf/x"
	_ = n1.StoreIndexValue(key, IndexEntry{Source: key, Target: "super/" + n1.ID.String(), UpdatedUnix: time.Now().UnixNano()}, 1500*time.Millisecond)
	_ = n2.StoreIndexValue(key, IndexEntry{Source: key, Target: "super/" + n2.ID.String(), UpdatedUnix: time.Now().UnixNano()}, 1500*time.Millisecond)
	time.Sleep(150 * time.Millisecond)
	if ents, ok := n1.FindIndexRemote(key); !ok || len(ents) < 2 {
		t.Fatalf("expected merged index >=2")
	}
	time.Sleep(2200 * time.Millisecond)
	if ents, ok := n2.FindIndexRemote(key); !ok || len(ents) < 2 {
		t.Fatalf("index not refreshed")
	}
}