package main

import (
	"fmt"
	"time"

	"github.com/SharefulNetworks/shareful-dht/dht"
	"github.com/SharefulNetworks/shareful-dht/netx"
)

func main() {

	//setup config.
	cfg := dht.DefaultConfig()
	cfg.UseProtobuf = true // set true after generating pb
	cfg.RequestTimeout = 1000 * time.Millisecond
	cfg.DefaultTTL = 30 * time.Second
	cfg.RefreshInterval = 10 * time.Second
	cfg.JanitorInterval = 1 * time.Second

	//create two nodes
	n1 := dht.NewNode("node1", ":9301", netx.NewTCP(), cfg)
	n2 := dht.NewNode("node2", ":9302", netx.NewTCP(), cfg)
	defer n1.Close()
	defer n2.Close()

	//for the purposes of this demo, manually add each pear to oneother as peers
	//n1.AddPeer(n2.Addr, n2.ID)
	//n2.AddPeer(n1.Addr, n1.ID)

	//we now boot strap via the connect public interface method.
	if err := n1.Connect(n2.Addr); err != nil {
		fmt.Println("Error occurred whilst Peer Node 1 was trying to connect to Peer Node 2:", err)
	}

	//TESTS

	//1)FIRST WE ATTEMPT TO STORE AND RETREIVE A STANDARD ENTRY FROM
	//  THE DHT WITH THE DEFAULT TTL. EACH PEER SETS A UNIQUE KEY/VALUE
	//  WHICH IS SUBSEQUENTLY RETRIEVED BY THE OTHER PEER, THEREBY
	//  CONFIRMING THAT EACH PEER HAS FULL VISIBILITY OF THE DHT.

	//store an STANDARD entry to the DHT via peer node 1.
	peer1StoreErr := n1.Store("alpha", []byte("A"))
	if peer1StoreErr != nil {
		fmt.Println("Error occurred whilst Peer Node 1 was trying to store entry:", peer1StoreErr)
	}

	//store an STANDARD entry to the DHT via peer node 2.
	peer2StoreErr := n2.Store("beta", []byte("B"))
	if peer2StoreErr != nil {
		fmt.Println("Error occurred whilst Peer Node 2 was trying to store entry:", peer2StoreErr)
	}

	//after a short delay, to allow store opp to propogate attempt to
	//retrieve the STANDARD entry, from the DHT, set by peer node 1 via peer node 2
	//and conversely the STANDARD entry set by peer node 2 via peer node 1.
	time.AfterFunc(time.Second*2, func() {

		//attempt to find entry set by peer node 1 via peer node 2
		if v, ok := n2.FindRemote("alpha"); ok {
			fmt.Println("Peer Node 2 found entry set by Peer Node 1", string(v))
		} else {
			fmt.Println("Error occurred whilst Peer Node 2 tried to find entry set by Peer Node 1")
		}

		//next attempt to find entry set by peer node 2 via peer node 1
		if v, ok := n1.FindRemote("beta"); ok {
			fmt.Println("Peer Node 1 found entry set by Peer Node 2", string(v))
		} else {
			fmt.Println("Error occurred whilst Peer Node 1 tried to find entry set by Peer Node 2")
		}

		fmt.Println()
		fmt.Println("NOW ATTEMPTING TO STORE AND RETREIVE INDEX:")
		fmt.Println()
	})

	//2)NEXT WE ATTEMPT TO STORE AND RETREIVE AN INDEXED ENTRY FROM
	//  THE DHT WITH A CUSTOM TTL. CRITICALLY THE SAME KEY IS STORED
	//  BY BOTH NODES, TO DEMONSTRATE MULTI-PEER INDEXING; EACH PEER
	//. USES ITS OWN NODE ID AS THE TARGET VALUE...

	//create the key
	key := "leaf/alpha"

	//store INDEX entry to the DHT via both nodes. note each node sets the target to be its own ID
	peer1StoreIndexErr := n1.StoreIndexValue(key, dht.IndexEntry{Source: key, Target: "super/" + n1.ID.String(), UpdatedUnix: time.Now().UnixNano()}, 10*time.Second)
	if peer1StoreIndexErr != nil {
		fmt.Println("Error occurred whilst Peer Node 1 was trying to store INDEX entry:", peer1StoreIndexErr)
	}

	peer2IndexIndexStoreErr := n2.StoreIndexValue(key, dht.IndexEntry{Source: key, Target: "super/" + n2.ID.String(), UpdatedUnix: time.Now().UnixNano()}, 10*time.Second)
	if peer2IndexIndexStoreErr != nil {
		fmt.Println("Error occurred whilst Peer Node 2 was trying to store INDEX entry:", peer2IndexIndexStoreErr)
	}

	//after a short delay, to allow index store opp to propogate attempt to reteive all
	//INDEX entries for the key from the DHT via peer node 1 and 2. Which should both return
	//an entry index of length 2.
	time.AfterFunc(2*time.Second, func() {

		if ents, ok := n1.FindIndexRemote(key); ok {
			fmt.Println("n1 FindIndexRemote entries:", len(ents))
		}
		if ents, ok := n2.FindIndexRemote(key); ok {
			fmt.Println("n2 FindIndexRemote entries:", len(ents))
		}
	})

	//first extended wait
	time.AfterFunc(time.Second*60, func() {

		fmt.Println("Waited. past TTL to observe refresh...")
		if ents, ok := n2.FindIndexRemote(key); ok {
			fmt.Println("n2 FindIndexRemote after refresh:", len(ents))
			fmt.Println(ents)
		} else {
			fmt.Println("index missing")
		}

	})

	//second extended wait
	time.AfterFunc(time.Second*120, func() {

		fmt.Println("Waited. past TTL to observe refresh 2...")
		if ents, ok := n1.FindIndexRemote(key); ok {
			fmt.Println("n1 FindIndexRemote after refresh 2:", len(ents))
			fmt.Println(ents)
		} else {
			fmt.Println("index missing")
		}

	})

	time.AfterFunc(time.Second*300, func() {

		fmt.Println("Waited. past TTL to observe refresh 3...")
		if ents, ok := n2.FindIndexRemote(key); ok {
			fmt.Println("n1 FindIndexRemote after refresh 2:", len(ents))
			fmt.Println(ents)
		} else {
			fmt.Println("index missing")
		}

	})

	//block main thread to allow async ops to complete
	select {}
}
