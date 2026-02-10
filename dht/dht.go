package dht

import (
	"crypto/rand"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SharefulNetworks/shareful-dht/netx"
	"github.com/SharefulNetworks/shareful-dht/proto/dhtpb"
	"github.com/SharefulNetworks/shareful-dht/routing"
	"github.com/SharefulNetworks/shareful-dht/types"
	"github.com/SharefulNetworks/shareful-dht/wire"
	"google.golang.org/protobuf/proto"
)

// op type enum-like
const (
	OP_STORE        = 1
	OP_FIND         = 2
	OP_STORE_INDEX  = 3
	OP_FIND_INDEX   = 4
	OP_PING         = 5
	OP_CONNECT      = 6
	OP_DELETE_INDEX = 7
	OP_FIND_NODE    = 8
	OP_FIND_VALUE   = 9 // NEW
)

// node type enum-like
const (
	NT_UNKNOWN  = 0
	NT_CORE     = 1
	NT_ENTRY    = 2
	NT_EXTERNAL = 3
)

func NewRandomID() types.NodeID {
	var id types.NodeID
	var randomData [32]byte
	io.ReadFull(rand.Reader, randomData[:])
	sum := sha1.Sum(randomData[:])
	copy(id[:], sum[:])
	return id
}

func HashKey(k string) types.NodeID {
	s := sha1.Sum([]byte(k))
	var id types.NodeID
	copy(id[:], s[:])
	return id
}

func ParseNodeID(s string) (types.NodeID, error) {
	var id types.NodeID
	b, err := hex.DecodeString(s)
	if err != nil {
		return id, err
	}
	if len(b) != types.IDBytes {
		return id, fmt.Errorf("invalid node id length: %d", len(b))
	}
	copy(id[:], b[:types.IDBytes])
	return id, nil
}

func xor(a, b types.NodeID) (o types.NodeID) {
	for i := 0; i < types.IDBytes; i++ {
		o[i] = a[i] ^ b[i]
	}
	return
}
func CompareDistance(a, b, t types.NodeID) int {
	da := xor(a, t)
	db := xor(b, t)
	for i := 0; i < types.IDBytes; i++ {
		if da[i] < db[i] {
			return -1
		}
		if da[i] > db[i] {
			return 1
		}
	}
	return 0
}

func CollectErrors(errs []error) error {
	if len(errs) == 0 {
		return nil
	}
	var sb strings.Builder
	for i, err := range errs {
		sb.WriteString(err.Error())
		if i < len(errs)-1 {
			sb.WriteString("\n") // Add a newline after each error except the last one
		}
	}
	return errors.New(sb.String())
}

func pickEvenDistribution(all []int, k int) []int {
	n := len(all)

	//if either the collection or the the number of nodes to select is less than or equal to zero, return nil
	if k <= 0 || n == 0 {
		return nil
	}

	//if the number of nodes to select is greater than or equal to the total number of available nodes, return all nodes
	if k >= n {
		out := make([]int, n)
		copy(out, all)
		return out
	}

	//otherwise set output array capacity equal to the number of requested nodes, K
	out := make([]int, 0, k)

	for i := 0; i < k; i++ {
		idx := int((float64(i) + 0.5) * float64(n) / float64(k))
		if idx >= n {
			idx = n - 1
		}
		out = append(out, all[idx])
	}
	return out
}

type Config struct {
	K                              int
	Alpha                          int
	DefaultEntryTTL                time.Duration
	DefaultIndexEntryTTL           time.Duration
	AllowPermanentDefault          bool
	RefreshInterval                time.Duration
	JanitorInterval                time.Duration
	UseProtobuf                    bool
	RequestTimeout                 time.Duration
	BatchRequestTimeout            time.Duration
	OutboundQueueWorkerCount       int
	ReplicationFactor              int
	EnableAuthorativeStorage       bool
	MaxLocalEntryRefreshCount      int //the max local stabndard entry refreshes (i.e refresh to known nodes) that the node can undertake before a network-wide refresh is required which may result in new k-nearest candidates.
	MaxLocalIndexEntryRefreshCount int //the max local index entry refreshes (i.e refresh to known nodes) that the node can undertake before a network-wide refresh is required which may result in new k-nearest candidates.
	BootstrapConnectDelayMillis    int //the delay, in milliseconds, that should elapse before the node attempts to connect to each bootstrap node. The delay is useful where all or multiple nodes in the list are being started in tandem as it allows sufficient time for each respective node to spin up and drop into a ready state.
}

func DefaultConfig() Config {
	return Config{K: 20, Alpha: 3, DefaultEntryTTL: 10 * time.Minute, DefaultIndexEntryTTL: 10 * time.Minute, RefreshInterval: 2 * time.Minute, JanitorInterval: time.Minute, UseProtobuf: true, RequestTimeout: 1500 * time.Millisecond, BatchRequestTimeout: 4500 * time.Millisecond, OutboundQueueWorkerCount: 4, ReplicationFactor: 3, EnableAuthorativeStorage: false, MaxLocalEntryRefreshCount: 50, MaxLocalIndexEntryRefreshCount: 50, BootstrapConnectDelayMillis: 20000}
}

type IndexEntry struct {
	Source            string       `json:"source"`
	Target            string       `json:"target"`
	Meta              []byte       `json:"meta"`
	UpdatedUnix       int64        `json:"updated_unix"`
	Publisher         types.NodeID `json:"publisher"`
	TTL               int64        `json:"ttl"`
	LocalRefreshCount uint64       `json:"local_refresh_count"` //record level refresh count doesn't make sense for IndexEntry as each entry in the record will be maintained by different nodes, each of which will have their own respective refresh intervals.
}

type record struct {
	Key               string
	Value             []byte
	Expiry            time.Time
	IsIndex           bool
	Replicas          []string
	TTL               time.Duration
	Publisher         types.NodeID
	LocalRefreshCount uint64
	mu                sync.RWMutex
}

type Node struct {
	ID           types.NodeID
	Addr         string
	transport    netx.Transport
	cfg          Config
	nodeType     int
	cd           wire.Codec
	mu           sync.RWMutex
	dataStore    map[string]*record
	routingTable *routing.RoutingTable
	stop         chan struct{}
	reqSeq       uint64
	pending      sync.Map // map[uint64]chan []byte
	refreshCount uint64
	wg           sync.WaitGroup
	closeOnce    sync.Once
}

func NewNode(id string, addr string, transport netx.Transport, cfg Config, nodeType int) (*Node, error) {
	var codec wire.Codec
	codec = wire.JSONCodec{}
	if cfg.UseProtobuf {
		codec = wire.ProtobufCodec{}
	}

	//NB: Don't forget to increment this if/when new types are added.
	if nodeType <= 0 || nodeType >= 4 {
		return nil, fmt.Errorf("An unsupported node type was provided: %d", nodeType)
	}

	//instantiate the new DHT node
	n := &Node{
		ID:           HashKey(id),
		Addr:         addr,
		transport:    transport,
		cfg:          cfg,
		nodeType:     nodeType,
		cd:           codec,
		mu:           sync.RWMutex{},
		dataStore:    map[string]*record{},
		routingTable: routing.NewRoutingTable(HashKey(id), cfg.K),
		stop:         make(chan struct{}),
		refreshCount: 0,
	}

	//start listening for incoming messages
	_ = n.transport.Listen(addr, n.onMessage)

	//spin up refresher and janitor background tasks and append them to the waitist.
	n.wg.Add(2)
	go n.janitor()
	go n.refresher()

	return n, nil
}

func (n *Node) Bootstrap(bootstrapAddrs []string, connectDelayMillis int) error {

	if len(bootstrapAddrs) == 0 {
		return fmt.Errorf("no bootstrap addresses provided")
	}

	//if a connect delay has not been specified use the default value from the prevailing config.
	if connectDelayMillis <= 0 {
		connectDelayMillis = n.cfg.BootstrapConnectDelayMillis
	}

	//var to hold any errors that occur during the bootstrap process.
	var bootstapConnErrors []error

	//once the connect delay period has elapsed, attempt to connect to each bootstrap node.,
	time.AfterFunc(time.Duration(connectDelayMillis)*time.Millisecond, func() {
		for i := 0; i < len(bootstrapAddrs); i++ {
			if bootstrapAddrs[i] == n.Addr {
				continue //skip connecting to self
			}
			if err := n.Connect(bootstrapAddrs[i]); err != nil {
				bootstapConnErrors = append(bootstapConnErrors, fmt.Errorf("error occurred whilst trying to connect to bootstrap address %s: %v", bootstrapAddrs[i], err))
			}
		}
	})

	//we capture any and all errors, that occur during the process and return them as a single error object.
	if len(bootstapConnErrors) > 0 {
		collectiveErr := CollectErrors(bootstapConnErrors)
		return collectiveErr
	}

	fmt.Println()
	fmt.Printf("Node: %s Successfully boottrapped to all %d nodes provided. ", n.Addr, len(bootstrapAddrs))
	fmt.Println()

	return nil
}

func (n *Node) Close() {
	n.closeOnce.Do(func() {
		close(n.stop)
		n.wg.Wait() // wait for janitor and refresher to finish
		n.transport.Close()
	})
}

func (n *Node) AddPeer(addr string, id types.NodeID) {
	n.routingTable.Update(id, addr)
}

func (n *Node) DropPeer(id types.NodeID) bool {
	return n.routingTable.Remove(id)
}

func (n *Node) lookupAddrForId(id types.NodeID) (string, error) {
	if id == n.ID {
		return n.Addr, nil
	}
	if addr, ok := n.routingTable.GetAddr(id); ok {
		return addr, nil
	}
	return "", fmt.Errorf("unknown peer ID: %s", id.String())
}

func (n *Node) nearestK(key string) []string {
	t := HashKey(key)
	closest := n.routingTable.Closest(t, n.cfg.K)

	addrs := make([]string, 0, len(closest)+1)

	// include self as candidate
	addrs = append(addrs, n.Addr)

	for _, c := range closest {
		if c.ID == n.ID {
			continue
		}
		addrs = append(addrs, c.Addr)
	}

	// clamp to K
	if len(addrs) > n.cfg.K {
		addrs = addrs[:n.cfg.K]
	}
	return addrs
}

func (n *Node) nearestKByID(target types.NodeID) []*routing.Peer {
	closest := n.routingTable.Closest(target, n.cfg.K)

	out := make([]*routing.Peer, 0, len(closest))
	for _, p := range closest {
		if p == nil || p.Addr == "" || p.ID == n.ID {
			continue
		}
		out = append(out, p)
		if len(out) == n.cfg.K {
			break
		}
	}
	return out
}

func (n *Node) nextReqID() uint64 {
	return atomic.AddUint64(&n.reqSeq, 1)
}

// -----------------------------------------------------------------------------
// Central encoding helpers
// -----------------------------------------------------------------------------

func (n *Node) encode(v any) ([]byte, error) {
	if n.cfg.UseProtobuf {
		msg, ok := v.(proto.Message)
		if !ok {
			return nil, fmt.Errorf("expected proto.Message when UseProtobuf enabled: %T", v)
		}
		return proto.Marshal(msg)
	}
	return json.Marshal(v)
}

func (n *Node) decode(b []byte, v any) error {
	if n.cfg.UseProtobuf {
		msg, ok := v.(proto.Message)
		if !ok {
			return fmt.Errorf("expected proto.Message when UseProtobuf enabled: %T", v)
		}
		return proto.Unmarshal(b, msg)
	}
	return json.Unmarshal(b, v)
}

// makeMessage returns an empty request/response pair appropriate for the op and codec.
func (n *Node) makeMessage(op int) (req any, resp any) {
	if n.cfg.UseProtobuf {
		switch op {
		case OP_STORE:
			return &dhtpb.StoreRequest{}, &dhtpb.StoreResponse{}
		case OP_FIND:
			return &dhtpb.FindRequest{}, &dhtpb.FindResponse{}
		case OP_STORE_INDEX:
			return &dhtpb.StoreIndexRequest{}, &dhtpb.StoreIndexResponse{}
		case OP_FIND_INDEX:
			return &dhtpb.FindIndexRequest{}, &dhtpb.FindIndexResponse{}
			//case OP_PING:
			//		panic("Ping Messages are currently ")
		case OP_CONNECT:
			return &dhtpb.ConnectRequest{}, &dhtpb.ConnectResponse{}
		case OP_DELETE_INDEX:
			return &dhtpb.DeleteIndexRequest{}, &dhtpb.DeleteIndexResponse{}
		case OP_FIND_NODE:
			return &dhtpb.FindNodeRequest{}, &dhtpb.FindNodeResponse{}
		case OP_FIND_VALUE:
			return &dhtpb.FindValueRequest{}, &dhtpb.FindValueResponse{}

		default:
			return nil, nil
		}
	}

	// JSON fallback shapes
	switch op {
	case OP_STORE:
		return &struct {
				Key      string   `json:"key"`
				Value    []byte   `json:"value"`
				TTLms    int64    `json:"ttl_ms"`
				Replicas []string `json:"replicas"`
			}{}, &struct {
				Ok  bool   `json:"ok"`
				Err string `json:"err"`
			}{}
	case OP_FIND:
		return &struct {
				Key string `json:"key"`
			}{}, &struct {
				Ok    bool   `json:"ok"`
				Value []byte `json:"value"`
				Err   string `json:"err"`
			}{}
	case OP_STORE_INDEX:
		return &struct {
				Key      string     `json:"key"`
				Entry    IndexEntry `json:"entry"`
				TTLms    int64      `json:"ttl_ms"`
				Replicas []string   `json:"replicas"`
			}{}, &struct {
				Ok  bool   `json:"ok"`
				Err string `json:"err"`
			}{}
	case OP_FIND_INDEX:
		return &struct {
				Key string `json:"key"`
			}{}, &struct {
				Ok      bool         `json:"ok"`
				Entries []IndexEntry `json:"entries"`
				Err     string       `json:"err"`
			}{}
	case OP_PING:
		return &struct{}{}, &struct {
			Ok bool `json:"ok"`
		}{}
	}

	return nil, nil
}

// -----------------------------------------------------------------------------
// Core DHT operations
// -----------------------------------------------------------------------------

// Connect sends an OP_CONNECT request to the given address, asking that
// remote node to register this node (ID + Addr) in its peer table.
func (n *Node) Connect(remoteAddr string) error {
	fmt.Println("Node: " + n.ID.String() + " is connecting to node @ " + remoteAddr + " standby...")
	req := &dhtpb.ConnectRequest{
		NodeId: n.ID[:], // types.NodeID is [20]byte, cast to slice
		Addr:   n.Addr,
	}

	// sendRequest(address, op, payload) already exists in your code
	respBytes, err := n.sendRequest(remoteAddr, OP_CONNECT, req)
	if err != nil {
		return err
	}

	var resp dhtpb.ConnectResponse
	if err := n.decode(respBytes, &resp); err != nil {
		return err
	}
	if !resp.Ok {
		return fmt.Errorf("connect rejected: %s", resp.Err)
	}

	//otherwise where the connection was accepted, parse the returned Node ID and add the remote peer to our peer table.
	var remoteID types.NodeID
	copy(remoteID[:], resp.NodeId)
	n.AddPeer(remoteAddr, remoteID)

	return nil
}

func (n *Node) Store(key string, val []byte) error {
	ttl := n.cfg.DefaultEntryTTL
	if n.cfg.AllowPermanentDefault {
		ttl = 0
	}
	return n.StoreWithTTL(key, val, ttl, n.ID, true)
}

func (n *Node) StoreWithTTL(key string, val []byte, ttl time.Duration, publisherId types.NodeID, recomputeReplicas bool) error {

	//holds our collection of replica addresses
	var reps []string

	//where recomputeReplicas is set to false we only store to nearest nodes in our LOCAL routing table
	//this is primarily used by the refresh routine, to periodically refresh entries on replicas
	//ALREADY KNOWN to this node.

	//Then, at some less frequent interval, it will call StoreWithTTL with
	//this value set to true in order to prompt the network wide, gathering of nearest K nodes
	//to refresh the entries to which may, in turn, result in new nearest candidates being identified
	//and thereby ensure that we account for node churn in the wider network and always attempt to
	//store to the prevailing nearest K nodes.
	if !recomputeReplicas {
		fmt.Println("Note: local only flag provided: storage will only be propagated to known nodes in the local routing table.")
		reps = n.nearestK(key)
	} else {

		//look up k nearest nodes over network, only fall back to localised search
		//in the event of an error.
		peers, err := n.lookupK(HashKey(key))
		if err != nil {
			fmt.Printf("An error occurred whilst attemptiing to lookup nearest nodes over the network, falling back to local search. the error was: %v", err)
			reps = n.nearestK(key)
		} else {
			reps = n.peersToAddrs(peers)
		}

	}

	//clamp replicas to the replication factor defined in the prevailing config
	reps = reps[:min(len(reps), n.cfg.ReplicationFactor)]

	var exp time.Time
	switch {
	case ttl < 0:
		exp = time.Now().Add(-time.Second) // negative ttl = force-expire (delete)
	case ttl == 0:
		exp = time.Time{} // zero tll = permanent
	default:
		exp = time.Now().Add(ttl) // positive ttl = normal expiry
	}

	//obtained previously stored record's local refresh count
	var prevLocalRefresh uint64
	n.mu.RLock()
	if old, ok := n.dataStore[key]; ok && old != nil {
		prevLocalRefresh = old.LocalRefreshCount
	}
	n.mu.RUnlock()

	n.mu.Lock()
	n.dataStore[key] = &record{Key: key, Value: val, Expiry: exp, IsIndex: false, Replicas: reps, TTL: ttl, Publisher: publisherId, LocalRefreshCount: prevLocalRefresh}
	n.mu.Unlock()

	reqAny, _ := n.makeMessage(OP_STORE)
	switch r := reqAny.(type) {
	case *dhtpb.StoreRequest:
		r.Key = key
		r.Value = val
		r.Replicas = reps
		r.PublisherId = publisherId[:]
		if ttl < 0 {
			r.TtlMs = -1
		} else {
			r.TtlMs = int64(ttl / time.Millisecond)
		}
	case *struct {
		Key      string   `json:"key"`
		Value    []byte   `json:"value"`
		TTLms    int64    `json:"ttl_ms"`
		Replicas []string `json:"replicas"`
	}:
		r.Key = key
		r.Value = val
		r.Replicas = reps
		if ttl < 0 {
			r.TTLms = -1
		} else {
			r.TTLms = int64(ttl / time.Millisecond)
		}
	}

	var errorsList []error
	for _, a := range reps {
		if a == n.Addr {
			continue
		}
		if _, err := n.sendRequest(a, OP_STORE, reqAny); err != nil {
			errorsList = append(errorsList, fmt.Errorf("Node: %s encountered an error when attempting to propagate storage of STANDARD entry to node @ %s", n.Addr, a))
		}
	}

	//where at least one error occurred call into our utility to compile
	//the errors into a single error object and return it.
	if len(errorsList) > 0 {
		compiledErr := CollectErrors(errorsList)
		return compiledErr
	} else {
		return nil
	}
}

func (n *Node) FindLocal(key string) ([]byte, bool) {
	n.mu.RLock()
	rec, ok := n.dataStore[key]
	n.mu.RUnlock()
	if ok && (rec.Expiry.IsZero() || time.Now().Before(rec.Expiry)) {
		return append([]byte(nil), rec.Value...), true
	}
	return nil, false
}

func (n *Node) DataStoreLength() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return len(n.dataStore)
}

func (n *Node) OldFind(key string) ([]byte, bool) {

	//holds our collection of replica addresses
	var reps []string

	//look up k nearest nodes over network, only fall back to localised search
	//in the event of an error.
	peers, err := n.lookupK(HashKey(key))
	if err != nil {
		fmt.Printf("An error occurred whilst attemptiing to lookup nearest nodes over the network, falling back to local search. the error was: %v", err)
		reps = n.nearestK(key)
	} else {
		reps = n.peersToAddrs(peers)
	}

	fmt.Println("[[[[[[[Returned NEAREST XOR DISTANCE FROM KEY]]]]]]]]]]]")
	for i, peer := range peers {
		fmt.Println()
		fmt.Printf("nearest peer no: %d is XOR distance: %d from key.", i, n.routingTable.XORDistanceRank(peer.ID, HashKey(key)))
		fmt.Println()
		if i >= 3 {
			break
		}
	}

	fmt.Println()
	fmt.Println("[[[[[[[ALL DISTANCE FROM KEY]]]]]]]]]]]")
	for i, peer := range n.ListPeers() {
		fmt.Println()
		fmt.Printf("peer no: %d is XOR distance: %d from key.", i, n.routingTable.XORDistanceRank(peer.ID, HashKey(key)))
		fmt.Println()

	}

	//clamp replicas to the replication factor defined in the prevailing config
	reps = reps[:min(len(reps), n.cfg.ReplicationFactor)]
	if v, ok := n.FindLocal(key); ok {
		return v, true
	}

	//channel to collate responses from concurrent find requests
	ch := make(chan struct {
		v  []byte
		ok bool
	}, len(reps))
	for _, a := range reps {
		if a == n.Addr {
			continue
		}
		go func(addr string) {
			reqAny, _ := n.makeMessage(OP_FIND)
			switch r := reqAny.(type) {
			case *dhtpb.FindRequest:
				r.Key = key
			case *struct {
				Key string `json:"key"`
			}:
				r.Key = key
			}

			b, err := n.sendRequest(addr, OP_FIND, reqAny)
			fmt.Println(err)
			if err != nil {
				ch <- struct {
					v  []byte
					ok bool
				}{nil, false}
				return
			}
			_, respAny := n.makeMessage(OP_FIND)
			if err := n.decode(b, respAny); err != nil {
				ch <- struct {
					v  []byte
					ok bool
				}{nil, false}
				return
			}

			switch resp := respAny.(type) {
			case *dhtpb.FindResponse:
				if resp.Ok {
					ch <- struct {
						v  []byte
						ok bool
					}{resp.Value, true}
					return
				}
			case *struct {
				Ok    bool   `json:"ok"`
				Value []byte `json:"value"`
				Err   string `json:"err"`
			}:
				if resp.Ok {
					ch <- struct {
						v  []byte
						ok bool
					}{resp.Value, true}
					return
				}
			}
			ch <- struct {
				v  []byte
				ok bool
			}{nil, false}
		}(a)
	}
	deadline := time.After(n.cfg.RequestTimeout)
	//for i := 0; i < len(reps)-1; i++ {
	for i := 0; i < len(reps); i++ {
		select {
		case r := <-ch:
			if r.ok {
				return r.v, true
			}
		case <-deadline:
			return nil, false
		}
	}
	return nil, false
}

/*
func (n *Node) Find(key string) ([]byte, bool) {
	// 0) local fast-path
	if v, ok := n.FindLocal(key); ok {
		return v, true
	}

	target := HashKey(key)
	K := n.cfg.K
	alpha := n.cfg.Alpha
	timeout := n.cfg.BatchRequestTimeout

	shortlist := n.nearestKByID(target)
	seen := make(map[types.NodeID]*routing.Peer, len(shortlist))
	queried := make(map[types.NodeID]bool, len(shortlist))

	for _, p := range shortlist {
		if p == nil || p.Addr == "" || p.ID == n.ID {
			continue
		}
		seen[p.ID] = p
	}

	rebuild := func() []*routing.Peer {
		all := make([]*routing.Peer, 0, len(seen))
		for _, p := range seen {
			all = append(all, p)
		}
		sort.Slice(all, func(i, j int) bool {
			return CompareDistance(all[i].ID, all[j].ID, target) < 0
		})
		if len(all) > K {
			all = all[:K]
		}
		return all
	}

	//compares the two provided snapshots arrays to determine if they are identical
	sameShortList := func(a, b []*routing.Peer) bool {
		if len(a) != len(b) {
			return false
		}
		for i := range a {
			if a[i] == nil || b[i] == nil {
				if a[i] != b[i] {
					return false
				}
				continue
			}
			if a[i].ID != b[i].ID {
				return false
			}
		}
		return true
	}

	shortlist = rebuild()

	for {
		// pick α closest unqueried
		batch := make([]*routing.Peer, 0, alpha)
		for _, p := range shortlist {
			if p == nil || p.ID == n.ID {
				continue
			}
			if !queried[p.ID] {
				queried[p.ID] = true
				batch = append(batch, p)
				if len(batch) == alpha {
					break
				}
			}
		}
		if len(batch) == 0 {
			break
		}

		type res struct {
			value []byte
			ok    bool
			peers []*routing.Peer
			err   error
			from  *routing.Peer
		}
		ch := make(chan res, len(batch))

		for _, p := range batch {
			go func(peer *routing.Peer) {
				req := &dhtpb.FindValueRequest{Key: key}

				b, err := n.sendRequest(peer.Addr, OP_FIND_VALUE, req)
				if err != nil {
					ch <- res{err: err, from: peer}
					return
				}

				resp := &dhtpb.FindValueResponse{}
				if err := n.decode(b, resp); err != nil {
					ch <- res{err: err, from: peer}
					return
				}

				if resp.Ok {
					ch <- res{ok: true, value: resp.Value, from: peer}
					return
				}

				out := make([]*routing.Peer, 0, len(resp.Peers))
				for _, rp := range resp.Peers {
					if rp == nil || rp.Addr == "" || len(rp.NodeId) != len(target) {
						continue
					}
					var id types.NodeID
					copy(id[:], rp.NodeId)
					if id == n.ID {
						continue
					}
					out = append(out, &routing.Peer{ID: id, Addr: rp.Addr})
				}
				ch <- res{ok: false, peers: out, from: peer}
			}(p)
		}

		timer := time.NewTimer(timeout)
		responded := 0

		for responded < len(batch) {
			select {
			case r := <-ch:
				responded++

				// always learn responders + their contacts
				if r.from != nil {
					n.routingTable.Update(r.from.ID, r.from.Addr)
				}

				if r.ok {
					// OPTIONAL: cache value at closest node you queried that didn’t have it
					// (classic Kademlia: store at the closest node seen that didn’t return it)
					timer.Stop()
					return r.value, true
				}

				for _, np := range r.peers {
					if np == nil || np.Addr == "" || np.ID == n.ID {
						continue
					}
					n.routingTable.Update(np.ID, np.Addr)
					if _, ok := seen[np.ID]; !ok {
						seen[np.ID] = np
					}
				}

			case <-timer.C:
				responded = len(batch)
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}

		// rebuild shortlist
		old := shortlist
		shortlist = rebuild()

		// termination: if shortlist didn’t change and nothing left to query, stop
		if sameShortList(old, shortlist) {
			done := true
			for _, p := range shortlist {
				if p != nil && !queried[p.ID] {
					done = false
					break
				}
			}
			if done {
				break
			}
		}
	}

	return nil, false
}
*/

func (n *Node) Find(key string) ([]byte, bool) {
	// 0) local fast-path
	if v, ok := n.FindLocal(key); ok {
		return v, true
	}

	target := HashKey(key)
	K := n.cfg.K
	alpha := n.cfg.Alpha
	batchTimeout := n.cfg.BatchRequestTimeout

	shortlist := n.nearestKByID(target)

	seen := make(map[types.NodeID]*routing.Peer, len(shortlist))
	queried := make(map[types.NodeID]bool, len(shortlist))

	// NEW: track requests in-flight so we don't schedule duplicates
	inflight := make(map[types.NodeID]bool, len(shortlist))

	// NEW: optional retry budget per peer for transient failures
	const maxRetries = 1 // set to 0 to disable retries, 1 is usually enough for tests
	failCount := make(map[types.NodeID]int, len(shortlist))

	for _, p := range shortlist {
		if p == nil || p.Addr == "" || p.ID == n.ID {
			continue
		}
		seen[p.ID] = p
	}

	rebuild := func() []*routing.Peer {
		all := make([]*routing.Peer, 0, len(seen))
		for _, p := range seen {
			all = append(all, p)
		}
		sort.Slice(all, func(i, j int) bool {
			return CompareDistance(all[i].ID, all[j].ID, target) < 0
		})
		if len(all) > K {
			all = all[:K]
		}
		return all
	}

	sameShortList := func(a, b []*routing.Peer) bool {
		if len(a) != len(b) {
			return false
		}
		for i := range a {
			if a[i] == nil || b[i] == nil {
				if a[i] != b[i] {
					return false
				}
				continue
			}
			if a[i].ID != b[i].ID {
				return false
			}
		}
		return true
	}

	shortlist = rebuild()

	type res struct {
		value []byte
		ok    bool
		peers []*routing.Peer
		err   error
		from  *routing.Peer
	}

	for {
		// pick α closest not yet queried and not already in-flight
		batch := make([]*routing.Peer, 0, alpha)
		for _, p := range shortlist {
			if p == nil || p.ID == n.ID || p.Addr == "" {
				continue
			}
			if queried[p.ID] || inflight[p.ID] {
				continue
			}

			// if we exceeded retry budget, treat as queried (dead for this lookup)
			if maxRetries >= 0 && failCount[p.ID] > maxRetries {
				queried[p.ID] = true
				continue
			}

			inflight[p.ID] = true
			batch = append(batch, p)
			if len(batch) == alpha {
				break
			}
		}

		if len(batch) == 0 {
			break
		}

		ch := make(chan res, len(batch))

		// send requests concurrently
		for _, p := range batch {
			go func(peer *routing.Peer) {
				req := &dhtpb.FindValueRequest{Key: key}

				b, err := n.sendRequest(peer.Addr, OP_FIND_VALUE, req)
				if err != nil {
					ch <- res{err: err, from: peer}
					return
				}

				resp := &dhtpb.FindValueResponse{}
				if err := n.decode(b, resp); err != nil {
					ch <- res{err: err, from: peer}
					return
				}

				if resp.Ok {
					ch <- res{ok: true, value: resp.Value, from: peer}
					return
				}

				out := make([]*routing.Peer, 0, len(resp.Peers))
				for _, rp := range resp.Peers {
					if rp == nil || rp.Addr == "" || len(rp.NodeId) != len(target) {
						continue
					}
					var id types.NodeID
					copy(id[:], rp.NodeId)
					if id == n.ID {
						continue
					}
					out = append(out, &routing.Peer{ID: id, Addr: rp.Addr})
				}
				ch <- res{ok: false, peers: out, from: peer}
			}(p)
		}

		timer := time.NewTimer(batchTimeout)
		responded := 0

		for responded < len(batch) {
			select {
			case r := <-ch:
				responded++

				// request is no longer in-flight
				if r.from != nil {
					delete(inflight, r.from.ID)
				}

				// Mark as queried only once we have a result (success OR error)
				if r.from != nil {
					// If error, maybe allow retry (don't mark queried yet if retrying)
					if r.err != nil {
						failCount[r.from.ID]++
						// keep it unqueried so it may be retried, unless we've exceeded retry budget
						if failCount[r.from.ID] > maxRetries {
							queried[r.from.ID] = true
						}
						continue
					}

					// success response: mark queried
					queried[r.from.ID] = true

					// learn responder (alive)
					n.routingTable.Update(r.from.ID, r.from.Addr)
				}

				// value found -> return immediately
				if r.ok {
					timer.Stop()

					/* learn everything that already got scheduled in this batch
					go func(expected int, ch <-chan res) {
						for i := responded; i < expected; i++ {
							rr := <-ch
							if rr.from != nil && rr.err == nil {
								n.routingTable.Update(rr.from.ID, rr.from.Addr)
							}
							for _, np := range rr.peers {
								if np != nil && np.Addr != "" && np.ID != n.ID {
									n.routingTable.Update(np.ID, np.Addr)
								}
							}
						}
					}(len(batch), ch)
					*/

					return r.value, true
				}

				// incorporate returned peers
				for _, np := range r.peers {
					if np == nil || np.Addr == "" || np.ID == n.ID {
						continue
					}
					n.routingTable.Update(np.ID, np.Addr)
					if _, ok := seen[np.ID]; !ok {
						seen[np.ID] = np
					}
				}

			case <-timer.C:
				// batch timed out: mark inflight peers as no longer inflight and apply retry logic
				for _, p := range batch {
					delete(inflight, p.ID)
					failCount[p.ID]++
					if failCount[p.ID] > maxRetries {
						queried[p.ID] = true
					}
				}
				responded = len(batch)
			}
		}

		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}

		// rebuild shortlist
		old := shortlist
		shortlist = rebuild()

		// termination: shortlist stable AND nothing left worth querying
		if sameShortList(old, shortlist) {
			done := true
			for _, p := range shortlist {
				if p == nil || p.ID == n.ID {
					continue
				}
				// not done if there's an unqueried peer we can still try
				if !queried[p.ID] && !inflight[p.ID] && failCount[p.ID] <= maxRetries {
					done = false
					break
				}
			}
			if done {
				break
			}
		}
	}

	return nil, false
}

func (n *Node) StoreIndex(indexKey string, e IndexEntry) error {
	ttl := n.cfg.DefaultIndexEntryTTL
	return n.StoreIndexWithTTL(indexKey, e, ttl, n.ID, true)
}

func (n *Node) StoreIndexWithTTL(indexKey string, e IndexEntry, ttl time.Duration, publisherId types.NodeID, recomputeReplicas bool) error {

	//holds our collection of replica addresses
	var reps []string

	if !recomputeReplicas {
		fmt.Println("Note: local only flag provided: index storage will only be propagated to known nodes in the local routing table.")
		reps = n.nearestK(indexKey)
	} else {

		fmt.Println("Note: recomputeReplicas flag provided: node will attempt to lookup nearest nodes over the network to propagate index storage to, only falling back to localised search in the event of an error.")
		//look up k nearest nodes over network, only fall back to localised search
		//in the event of an error.
		peers, err := n.lookupK(HashKey(indexKey))
		if err != nil {
			fmt.Printf("An error occurred whilst attemptiing to lookup nearest nodes over the network, falling back to local search. the error was: %v", err)
			reps = n.nearestK(indexKey)
		} else {
			reps = n.peersToAddrs(peers)
		}

	}

	//clamp replicas to the replication factor defined in the prevailing config
	reps = reps[:min(len(reps), n.cfg.ReplicationFactor)]

	//NB: where the call to the merge function is being made as a direct result
	//of a call to StoreIndex we can safely pass in the id of THIS node as the publisher.
	e.Publisher = publisherId
	e.TTL = ttl.Milliseconds()
	e.UpdatedUnix = time.Now().UnixMilli()
	mergeIndexError := n.mergeIndexLocal(indexKey, e, reps, publisherId)
	if mergeIndexError != nil {
		return fmt.Errorf("an error occurred whilst attempting to store index value %s", mergeIndexError.Error())
	}

	reqAny, _ := n.makeMessage(OP_STORE_INDEX)
	switch r := reqAny.(type) {
	case *dhtpb.StoreIndexRequest:
		r.Key = indexKey
		r.Entry = &dhtpb.IndexEntry{
			Source:      e.Source,
			Target:      e.Target,
			Meta:        e.Meta,
			UpdatedUnix: e.UpdatedUnix,
			PublisherId: e.Publisher[:],
			Ttl:         e.TTL,
		}
		r.TtlMs = int64(ttl / time.Millisecond)
		r.Replicas = reps
	case *struct {
		Key      string     `json:"key"`
		Entry    IndexEntry `json:"entry"`
		TTLms    int64      `json:"ttl_ms"`
		Replicas []string   `json:"replicas"`
	}:
		r.Key = indexKey
		r.Entry = e
		r.TTLms = int64(ttl / time.Millisecond)
		r.Replicas = reps
	}

	var errorsList []error
	for _, a := range reps {
		if a == n.Addr {
			continue
		}
		if _, err := n.sendRequest(a, OP_STORE_INDEX, reqAny); err != nil {
			errorsList = append(errorsList, fmt.Errorf("Node: %s encountered an error when attempting to propagate storage of INDEX entry to node @ %s", n.Addr, a))
		}
	}

	//where at least one error occurred call into our utility to compile
	//the errors into a single error object and return it.
	if len(errorsList) > 0 {
		compiledErr := CollectErrors(errorsList)
		return compiledErr
	} else {
		return nil
	}

}

func (n *Node) FindIndexLocal(key string) ([]IndexEntry, bool) {
	n.mu.RLock()
	rec, ok := n.dataStore[key]
	n.mu.RUnlock()
	if !ok {
		return nil, false
	}
	var entries []IndexEntry
	_ = json.Unmarshal(rec.Value, &entries)
	return entries, true
}

/*
func (n *Node) FindIndex(key string) ([]IndexEntry, bool) {
	reps := n.nearestK(key)
	if ents, ok := n.FindIndexLocal(key); ok {
		return ents, true
	}
	type res struct {
		ents []IndexEntry
		ok   bool
	}
	ch := make(chan res, len(reps))
	for _, a := range reps {
		if a == n.Addr {
			continue
		}
		go func(addr string) {
			reqAny, _ := n.makeMessage(OP_FIND_INDEX)
			switch r := reqAny.(type) {
			case *dhtpb.FindIndexRequest:
				r.Key = key
			case *struct {
				Key string `json:"key"`
			}:
				r.Key = key
			}

			b, err := n.sendRequest(addr, OP_FIND_INDEX, reqAny)
			fmt.Println(err)
			if err != nil {
				ch <- res{nil, false}
				return
			}
			_, respAny := n.makeMessage(OP_FIND_INDEX)
			if err := n.decode(b, respAny); err != nil {
				ch <- res{nil, false}
				return
			}

			switch resp := respAny.(type) {
			case *dhtpb.FindIndexResponse:
				if resp.Ok {
					out := make([]IndexEntry, 0, len(resp.Entries))
					for _, ie := range resp.Entries {
						e := IndexEntry{
							Source:      ie.Source,
							Target:      ie.Target,
							Meta:        ie.Meta,
							UpdatedUnix: ie.UpdatedUnix,
							TTL:         ie.Ttl,
						}
						copy(e.Publisher[:], ie.PublisherId[:])
						out = append(out, e)
					}
					ch <- res{out, true}
					return
				}
			case *struct {
				Ok      bool         `json:"ok"`
				Entries []IndexEntry `json:"entries"`
				Err     string       `json:"err"`
			}:
				if resp.Ok {
					ch <- res{resp.Entries, true}
					return
				}
			}
			ch <- res{nil, false}
		}(a)
	}
	merged := map[string]IndexEntry{}
	deadline := time.After(n.cfg.RequestTimeout)
	for i := 0; i < len(reps)-1; i++ {
		select {
		case r := <-ch:
			if r.ok {
				for _, e := range r.ents {
					merged[e.Source+"\x1f"+e.Target] = e
				}
			}
		case <-deadline:
			break
		}
	}
	if len(merged) == 0 {
		return nil, false
	}
	out := make([]IndexEntry, 0, len(merged))
	for _, e := range merged {
		out = append(out, e)
	}
	return out, true
}
*/

func (n *Node) FindIndex(key string) ([]IndexEntry, bool) {
	// 0) local fast-path
	if ents, ok := n.FindIndexLocal(key); ok {
		return ents, true
	}

	target := HashKey(key)
	K := n.cfg.K
	alpha := n.cfg.Alpha
	timeout := n.cfg.BatchRequestTimeout

	shortlist := n.nearestKByID(target)
	seen := make(map[types.NodeID]*routing.Peer, len(shortlist))
	queried := make(map[types.NodeID]bool, len(shortlist))

	for _, p := range shortlist {
		if p == nil || p.Addr == "" || p.ID == n.ID {
			continue
		}
		seen[p.ID] = p
	}

	rebuild := func() []*routing.Peer {
		all := make([]*routing.Peer, 0, len(seen))
		for _, p := range seen {
			all = append(all, p)
		}
		sort.Slice(all, func(i, j int) bool {
			return CompareDistance(all[i].ID, all[j].ID, target) < 0
		})
		if len(all) > K {
			all = all[:K]
		}
		return all
	}

	sameShortList := func(a, b []*routing.Peer) bool {
		if len(a) != len(b) {
			return false
		}
		for i := range a {
			if a[i] == nil || b[i] == nil {
				if a[i] != b[i] {
					return false
				}
				continue
			}
			if a[i].ID != b[i].ID {
				return false
			}
		}
		return true
	}

	shortlist = rebuild()

	// aggregated index entries (deduped)
	merged := make(map[string]IndexEntry)

	for {
		// pick α closest unqueried
		batch := make([]*routing.Peer, 0, alpha)
		for _, p := range shortlist {
			if p == nil || p.ID == n.ID {
				continue
			}
			if !queried[p.ID] {
				queried[p.ID] = true
				batch = append(batch, p)
				if len(batch) == alpha {
					break
				}
			}
		}

		if len(batch) == 0 {
			break
		}

		type res struct {
			ents  []IndexEntry
			peers []*routing.Peer
		}

		ch := make(chan res, len(batch))

		for _, p := range batch {
			go func(peer *routing.Peer) {
				reqAny, _ := n.makeMessage(OP_FIND_INDEX)

				switch r := reqAny.(type) {
				case *dhtpb.FindIndexRequest:
					r.Key = key
				case *struct {
					Key string `json:"key"`
				}:
					r.Key = key
				}

				b, err := n.sendRequest(peer.Addr, OP_FIND_INDEX, reqAny)
				if err != nil {
					ch <- res{}
					return
				}

				_, respAny := n.makeMessage(OP_FIND_INDEX)
				if err := n.decode(b, respAny); err != nil {
					ch <- res{}
					return
				}

				switch resp := respAny.(type) {
				case *dhtpb.FindIndexResponse:
					out := make([]IndexEntry, 0, len(resp.Entries))
					for _, ie := range resp.Entries {
						e := IndexEntry{
							Source:      ie.Source,
							Target:      ie.Target,
							Meta:        ie.Meta,
							UpdatedUnix: ie.UpdatedUnix,
							TTL:         ie.Ttl,
						}
						copy(e.Publisher[:], ie.PublisherId[:])
						out = append(out, e)
					}
					ch <- res{out, n.nodeContactsToPeers(resp.GetPeers())}
					return

					//TODO:GT Remove JSON support its not used.
				case *struct {
					Ok      bool         `json:"ok"`
					Entries []IndexEntry `json:"entries"`
					Peers   []string     `json:"peers"`
				}:
					if resp.Ok {
						//	ch <- res{resp.Entries, n.nodeContactToPeer(resp.GetPeers())}
						return
					}
				}

				ch <- res{}
			}(p)
		}

		prev := shortlist

		deadline := time.After(timeout)
		for i := 0; i < len(batch); i++ {
			select {
			case r := <-ch:
				for _, e := range r.ents {
					merged[e.Source+"\x1f"+e.Target] = e
				}
				for _, np := range r.peers {
					if np != nil && np.ID != n.ID {
						seen[np.ID] = np
					}
				}
			case <-deadline:
				break
			}
		}

		shortlist = rebuild()
		if sameShortList(prev, shortlist) {
			break
		}
	}

	if len(merged) == 0 {
		return nil, false
	}

	out := make([]IndexEntry, 0, len(merged))
	for _, e := range merged {
		out = append(out, e)
	}

	return out, true
}

func (n *Node) Delete(key string) error {
	n.mu.RLock()
	rec, ok := n.dataStore[key]
	n.mu.RUnlock()

	if !ok {
		return fmt.Errorf("no record was found for the given key: %s the record may have expired or have already been deleted", key)
	}

	if rec.Publisher != n.ID {
		return fmt.Errorf("cannot delete record for key: %s as this node is not the original publisher", key)
	}

	return n.StoreWithTTL(key, nil, -1, n.ID, false) //store with negative TTL to indicate deletion
}

func (n *Node) DeleteIndex(indexKey string, indexEntrySource string) error {

	//attempt to delete the index entry locally.
	deleted, deletionErr := n.deleteIndexLocal(indexKey, n.ID, indexEntrySource)
	if deletionErr != nil {
		return fmt.Errorf("an error occurred whilst attempting to deleted the specified Index: %o", deletionErr)
	}

	//where the index entry WAS successfully deleted locally, propagate the delete to nearest K nodes.
	if deleted {
		reps := n.nearestK(indexKey)

		reqAny, _ := n.makeMessage(OP_DELETE_INDEX)
		switch r := reqAny.(type) {
		case *dhtpb.DeleteIndexRequest:
			r.PublisherId = n.ID[:]
			r.Key = indexKey
			r.Source = indexEntrySource
		case *struct {
			PublisherId string `json:"publisherId"`
			Key         string `json:"key"`
			Source      string `json:"source"`
		}:

			r.PublisherId = n.ID.String()
			r.Key = indexKey
			r.Source = indexEntrySource
		}

		for _, a := range reps {
			if a == n.Addr {
				continue
			}
			if _, err := n.sendRequest(a, OP_DELETE_INDEX, reqAny); err != nil {
				return err
			}
		}

		return nil
	} else {
		return fmt.Errorf("entry associated with key: %s could not be deleted; either no entry was found with the specified source OR this node was NOT the original publisher of the entry  ", indexKey)
	}

}

func (n *Node) ListPeersAsString() []string {
	peerDetails := make([]string, 0)
	allPeers := n.ListPeers()
	for _, p := range allPeers {
		peerDetails = append(peerDetails, fmt.Sprintf("Peer ID: %s @ Address: %s", p.ID.String(), p.Addr))
	}
	return peerDetails
}

func (n *Node) ListPeers() []routing.Peer {
	return n.routingTable.ListKnownPeers()
}

func (n *Node) ListPeerIds() []types.NodeID {
	allPeers := n.ListPeers()
	allPeerIds := make([]types.NodeID, 0)
	for _, curPeer := range allPeers {
		allPeerIds = append(allPeerIds, curPeer.ID)
	}
	return allPeerIds
}

func (n *Node) ListPeerIdsAsStrings() []string {
	allPeerIds := n.ListPeerIds()
	allPeerIdStrings := make([]string, 0)
	for _, curPeerId := range allPeerIds {
		allPeerIdStrings = append(allPeerIdStrings, curPeerId.String())
	}
	return allPeerIdStrings
}

func (n *Node) PeerCount() int {
	return len(n.ListPeers())
}

// -----------------------------------------------------------------------------
// Incoming Message Handler (uses makeMessage + encode/decode)
// -----------------------------------------------------------------------------

func (n *Node) onMessage(from string, data []byte) {
	op, reqID, isResp, fromID, fromAddr, nodeType, payload, err := n.cd.Unwrap(data)

	//if an error occurred during the unwrap, log and exit
	if err != nil {
		fmt.Println("Failed to Unwrap message received from node @:", fromAddr, "of type:", nodeType, "failed on node:", n.Addr, "ERROR:", err)
		return
	}

	//the node that this message was received from MUST have published its address
	if fromAddr == "" {
		fmt.Printf("Received message from node: %s with no published address, dropping message.", fromID)
		return
	}

	//parse the (hex encoded) ID of the sender from the message envolope, to its equivilant (binary) types.NodeID valye
	var senderNodeID types.NodeID
	var senderNodeIDParseErr error
	senderNodeID, senderNodeIDParseErr = ParseNodeID(fromID)
	if senderNodeIDParseErr != nil {
		fmt.Println("An error occurred whilst attempting to parse sender Node ID from inbound request, the request will therefore be dropped.:", fromID)
		fmt.Println(senderNodeIDParseErr.Error())
		return
	}

	//crucially, update our routing table with the sender's details
	n.routingTable.Update(senderNodeID, fromAddr)

	//if this is a response message, look up the pending request channel and queue the payload for processing
	if isResp {
		//fmt.Println("response received")
		if chI, ok := n.pending.Load(reqID); ok {
			ch := chI.(chan []byte)
			select {
			case ch <- payload:
			default:
			}
		}
		return
	}

	switch op {
	case OP_STORE:
		reqAny, respAny := n.makeMessage(OP_STORE)
		if err := n.decode(payload, reqAny); err != nil {
			fmt.Println(err)
			return
		}

		// Apply request
		var key string
		var val []byte
		var ttlms int64
		var reps []string
		var pubId []byte

		switch r := reqAny.(type) {
		case *dhtpb.StoreRequest:
			key = r.Key
			val = r.Value
			ttlms = r.TtlMs
			reps = r.Replicas
			pubId = r.PublisherId
		case *struct {
			Key         string   `json:"key"`
			Value       []byte   `json:"value"`
			TTLms       int64    `json:"ttl_ms"`
			Replicas    []string `json:"replicas"`
			PublisherId []byte   `json:"publisher_id"`
		}:
			key = r.Key
			val = r.Value
			ttlms = r.TTLms
			reps = r.Replicas
			pubId = r.PublisherId
		}

		var exp time.Time
		switch {
		case ttlms < 0:
			//fmt.Println("Received TTL: " + fmt.Sprint(ttlms) + " indicating deletion request for key: " + key)
			exp = time.Now().Add(-time.Second) // negative ttl = force-expire (delete)
		case ttlms == 0:
			exp = time.Time{} // zero tll = permanent
		default:
			exp = time.Now().Add(time.Duration(ttlms) * time.Millisecond) // positive ttl = normal expiry
		}

		n.mu.Lock()
		var publisherId types.NodeID
		copy(publisherId[:], pubId)
		n.dataStore[key] = &record{Key: key, Value: val, Expiry: exp, IsIndex: false, Replicas: reps, TTL: time.Duration(ttlms) * time.Millisecond, Publisher: publisherId}
		n.mu.Unlock()

		fmt.Println("Node: " + n.Addr + "Succesfully handled request to store/update standard entry for key:" + key)
		senderNodeID, parseNodeErr := ParseNodeID(fromID)
		if parseNodeErr != nil {
			fmt.Println("An error occurred whilst attempting to parse sender Node ID:", fromID)
			fmt.Println(parseNodeErr.Error())
			return
		}
		fmt.Println("Request was sent from node: " + senderNodeID.String())
		fmt.Println("Request was received to node: " + n.ID.String())
		fmt.Println("")
		fmt.Println("")

		// Build response
		switch r := respAny.(type) {
		case *dhtpb.StoreResponse:
			r.Ok = true
		case *struct {
			Ok  bool   `json:"ok"`
			Err string `json:"err"`
		}:
			r.Ok = true
			r.Err = ""
		}
		b, _ := n.encode(respAny)
		msg, _ := n.cd.Wrap(OP_STORE, reqID, true, n.ID.String(), n.Addr, n.nodeType, b)
		//fmt.Println("Sending response to: ")
		//fmt.Println(from)
		_ = n.transport.Send(fromAddr, msg)

	case OP_FIND:
		reqAny, respAny := n.makeMessage(OP_FIND)
		if err := n.decode(payload, reqAny); err != nil {
			fmt.Println(err)
			return
		}

		var key string
		switch r := reqAny.(type) {
		case *dhtpb.FindRequest:
			key = r.Key
		case *struct {
			Key string `json:"key"`
		}:
			key = r.Key
		}

		val, ok := n.FindLocal(key)
		switch r := respAny.(type) {
		case *dhtpb.FindResponse:
			r.Ok = ok
			r.Value = val
		case *struct {
			Ok    bool   `json:"ok"`
			Value []byte `json:"value"`
			Err   string `json:"err"`
		}:
			r.Ok = ok
			r.Value = val
			r.Err = ""
		}
		b, _ := n.encode(respAny)
		msg, _ := n.cd.Wrap(OP_FIND, reqID, true, n.ID.String(), n.Addr, n.nodeType, b)
		//fmt.Println("Sending response to: ")
		//fmt.Println(from)
		_ = n.transport.Send(fromAddr, msg)

	case OP_STORE_INDEX:
		reqAny, respAny := n.makeMessage(OP_STORE_INDEX)
		if err := n.decode(payload, reqAny); err != nil {
			fmt.Println(err)
			return
		}

		var key string
		var entry IndexEntry
		var reps []string

		switch r := reqAny.(type) {
		case *dhtpb.StoreIndexRequest:
			key = r.Key
			if r.Entry != nil {
				entry = IndexEntry{
					Source:      r.Entry.Source,
					Target:      r.Entry.Target,
					Meta:        r.Entry.Meta,
					UpdatedUnix: r.Entry.UpdatedUnix,
					TTL:         r.Entry.Ttl,
				}
				copy(entry.Publisher[:], r.Entry.PublisherId[:])
			}
			reps = r.Replicas
		case *struct {
			Key      string     `json:"key"`
			Entry    IndexEntry `json:"entry"`
			TTLms    int64      `json:"ttl_ms"`
			Replicas []string   `json:"replicas"`
		}:
			key = r.Key
			entry = r.Entry
			reps = r.Replicas
		}

		//fmt.Printf("Node @ %s received request from publisher: %s to merge entry with key: %s at: %s", n.Addr, sendertypes.NodeID, key, time.Now())
		//fmt.Println()

		//pass in the id of the SENDER as the publisher of this index entry, the mergeIndexLocal
		//will take care of ensuring only the original publisher can store/update their own entries.
		mergeIndexErr := n.mergeIndexLocal(key, entry, reps, senderNodeID)
		if mergeIndexErr != nil {
			fmt.Println("an error occured whilst attempting to handle request to store index entry:", mergeIndexErr)
		}
		//fmt.Println("Node: " + n.Addr + "Succesfully handled request to store/update index entry for key:" + key)

		switch r := respAny.(type) {
		case *dhtpb.StoreIndexResponse:
			r.Ok = true
		case *struct {
			Ok  bool   `json:"ok"`
			Err string `json:"err"`
		}:
			r.Ok = true
			r.Err = ""
		}
		b, _ := n.encode(respAny)
		msg, _ := n.cd.Wrap(OP_STORE_INDEX, reqID, true, n.ID.String(), n.Addr, n.nodeType, b)
		//fmt.Println("Sending response to: ")
		//fmt.Println(from)
		_ = n.transport.Send(fromAddr, msg)

	case OP_FIND_INDEX:
		reqAny, respAny := n.makeMessage(OP_FIND_INDEX)
		if err := n.decode(payload, reqAny); err != nil {
			fmt.Println(err)
			return
		}

		req := reqAny.(*dhtpb.FindIndexRequest)
		key := req.Key
		target := HashKey(key)

		resp := respAny.(*dhtpb.FindIndexResponse)
		if ents, ok := n.FindIndexLocal(key); ok {
			resp.Ok = ok
			resp.Entries = make([]*dhtpb.IndexEntry, 0, len(ents))
			for _, e := range ents {
				resp.Entries = append(resp.Entries, &dhtpb.IndexEntry{
					Source:      e.Source,
					Target:      e.Target,
					Meta:        e.Meta,
					UpdatedUnix: e.UpdatedUnix,
					PublisherId: e.Publisher[:],
					Ttl:         e.TTL,
				})
			}
		} else {
			//otherwise where the entry is NOT found locally,we return the
			//nearest peers to the target key that we know about from our rooting table.
			//Ok is also set to false to indicate that this node does not directly have reference to the target entry.
			resp.Ok = false
			resp.Entries = make([]*dhtpb.IndexEntry, 0) //an empty array of entries is returned.
			nearestPeersToKey := n.nearestKByID(target)
			resp.Peers = make([]*dhtpb.NodeContact, 0, len(nearestPeersToKey))
			for _, p := range nearestPeersToKey {
				resp.Peers = append(resp.Peers, &dhtpb.NodeContact{
					NodeId: p.ID[:],
					Addr:   p.Addr,
				})
			}
		}

		b, _ := n.encode(respAny)
		msg, _ := n.cd.Wrap(OP_FIND_INDEX, reqID, true, n.ID.String(), n.Addr, n.nodeType, b)
		//fmt.Println("Sending response to: ")
		//fmt.Println(from)
		_ = n.transport.Send(fromAddr, msg)

	case OP_CONNECT:

		//NB: this is just a signalling message to request that this node adds the sender to its routing table
		//which will already have been done at the top of this onMessage function. Thus here we simply
		//parse the request and send back an acknowledgement.
		req, resp := &dhtpb.ConnectRequest{}, &dhtpb.ConnectResponse{}
		_ = n.decode(payload, req)

		resp.Ok = true
		resp.NodeId = n.ID[:]
		encoded, _ := n.encode(resp)
		reply, _ := n.cd.Wrap(OP_CONNECT, reqID, true, n.ID.String(), n.Addr, n.nodeType, encoded)

		// respond directly to TCP connection origin
		_ = n.transport.Send(fromAddr, reply)

	case OP_DELETE_INDEX:

		req, resp := n.makeMessage(OP_DELETE_INDEX)
		if err := n.decode(payload, req); err != nil {
			fmt.Println(err)
			return
		}

		var indexKey string
		var source string
		var publisherId types.NodeID

		switch r := req.(type) {
		case *dhtpb.DeleteIndexRequest:
			indexKey = r.Key
			source = r.Source
			copy(publisherId[:], r.PublisherId)
		case *struct {
			PublisherId string `json:"publisherId"`
			Key         string `json:"key"`
			Source      string `json:"source"`
		}:
			indexKey = r.Key
			source = r.Source
			copy(publisherId[:], r.PublisherId)
		}

		//the id of the sender must match the publisher id of the entry they are attempting to update
		if senderNodeID != publisherId {
			fmt.Println("An error occurred whilst processing received request to delete index entry for key:" + indexKey + ". Publisher Id Missmatch. ")
			return
		}

		_, deletionErr := n.deleteIndexLocal(indexKey, publisherId, source)
		if deletionErr != nil {
			fmt.Println("An error occurred whilst a processing received request to delete index entry for key: " + indexKey + " on this node: " + n.ID.String())
			fmt.Println(deletionErr.Error())
		}

		switch r := resp.(type) {
		case *dhtpb.DeleteIndexResponse:
			r.Ok = true
		case *struct {
			Ok  bool   `json:"ok"`
			Err string `json:"err"`
		}:
			r.Ok = true
			r.Err = ""
		}
		b, _ := n.encode(resp)
		msg, _ := n.cd.Wrap(OP_DELETE_INDEX, reqID, true, n.ID.String(), n.Addr, n.nodeType, b)
		_ = n.transport.Send(fromAddr, msg)

	case OP_FIND_NODE:
		req, _ := n.makeMessage(OP_FIND_NODE)
		_ = n.decode(payload, req)

		//parse the target node id from the request
		r := req.(*dhtpb.FindNodeRequest)
		var target types.NodeID
		copy(target[:], r.NodeId)

		//find nodes closest to the target id from our LOCAL routing table.
		peers := n.nearestKByID(target) //n.routingTable.Closest(target, n.cfg.K) // K shortlist from local RT

		_, resp := n.makeMessage(OP_FIND_NODE)
		rsp := resp.(*dhtpb.FindNodeResponse)
		rsp.Peers = make([]*dhtpb.NodeContact, 0, len(peers))
		rsp.Ok = true
		for _, p := range peers {
			rsp.Peers = append(rsp.Peers, &dhtpb.NodeContact{
				NodeId: p.ID[:],
				Addr:   p.Addr,
			})
		}

		b, _ := n.encode(resp)
		msg, _ := n.cd.Wrap(OP_FIND_NODE, reqID, true, n.ID.String(), n.Addr, nodeType, b)
		_ = n.transport.Send(fromAddr, msg)

	case OP_FIND_VALUE:
		req := &dhtpb.FindValueRequest{}
		if err := n.decode(payload, req); err != nil {
			return
		}

		key := req.Key
		target := HashKey(key)

		// If node has value, return it
		if v, ok := n.FindLocal(key); ok {
			resp := &dhtpb.FindValueResponse{Ok: true, Value: v}
			b, _ := n.encode(resp)
			msg, _ := n.cd.Wrap(OP_FIND_VALUE, reqID, true, n.ID.String(), n.Addr, n.nodeType, b)
			_ = n.transport.Send(fromAddr, msg)
			return
		}

		// Otherwise return closest peers (Kademlia-compliant)
		peers := n.nearestKByID(target) //n.routingTable.Closest(target, n.cfg.K)
		resp := &dhtpb.FindValueResponse{Ok: false, Peers: make([]*dhtpb.NodeContact, 0, len(peers))}
		for _, p := range peers {
			resp.Peers = append(resp.Peers, &dhtpb.NodeContact{NodeId: p.ID[:], Addr: p.Addr})
		}

		b, _ := n.encode(resp)
		msg, _ := n.cd.Wrap(OP_FIND_VALUE, reqID, true, n.ID.String(), n.Addr, n.nodeType, b)
		_ = n.transport.Send(fromAddr, msg)

	default:
		return
	}

}

func (n *Node) lookupK(target types.NodeID) ([]*routing.Peer, error) {

	//set core lookup parameters from config.
	K := n.cfg.K
	alpha := n.cfg.Alpha
	timeout := n.cfg.BatchRequestTimeout

	//populate initial shortlist from the CLOSEST nodes in our LOCAL routing table.
	shortlist := n.routingTable.Closest(target, K)

	//instantiate our seen and queried maps.
	seen := make(map[types.NodeID]*routing.Peer, len(shortlist))
	queried := make(map[types.NodeID]bool, len(shortlist))

	//seed seen with initial shortlist
	for _, p := range shortlist {
		if p == nil || p.Addr == "" || p.ID == n.ID {
			continue
		}
		seen[p.ID] = p
	}

	//called to rebuild the shortlist and the wider seen candidates pool from our discovered nodes.
	rebuild := func() {
		all := make([]*routing.Peer, 0, len(seen))
		for _, p := range seen {
			all = append(all, p)
		}
		sort.Slice(all, func(i, j int) bool {
			return CompareDistance(all[i].ID, all[j].ID, target) < 0
		})

		// cap candidate pool
		if len(all) > K*8 {
			all = all[:K*8]
		}

		// prune seen to match the cap (THIS is the important part)
		newSeen := make(map[types.NodeID]*routing.Peer, len(all))
		for _, p := range all {
			newSeen[p.ID] = p
		}
		seen = newSeen

		// shortlist is always top K from the bounded pool
		if len(all) > K {
			shortlist = all[:K]
		} else {
			shortlist = all
		}
	}

	// helper: are there unqueried nodes in best K?
	hasUnqueriedInBestK := func() bool {
		for _, p := range shortlist {
			if p == nil || p.ID == n.ID {
				continue
			}
			if !queried[p.ID] {
				return true
			}
		}
		return false
	}

	rebuild()

	// snapshot best K IDs to detect progress
	snapshotBest := func() []types.NodeID {
		out := make([]types.NodeID, 0, len(shortlist))
		for _, p := range shortlist {
			out = append(out, p.ID)
		}
		return out
	}

	//compares the two provided snapshots arrays to determine if they are identical
	sameSnapshot := func(a, b []types.NodeID) bool {
		if len(a) != len(b) {
			return false
		}
		for i := range a {
			if a[i] != b[i] {
				return false
			}
		}
		return true
	}

	//we take a snapshot of the best K nodes before starting each iteration
	bestBefore := snapshotBest()

	for {
		// pick alpha closest unqueried, alopha here is the max node we will query in parallel
		batch := make([]*routing.Peer, 0, alpha)
		for _, p := range shortlist {
			if p == nil || p.ID == n.ID {
				continue
			}
			if !queried[p.ID] {
				batch = append(batch, p)
				if len(batch) == alpha {
					break
				}
			}
		}
		if len(batch) == 0 {
			break
		}

		//struct return on result of query of operation.
		type res struct {
			peers []*routing.Peer
			err   error
		}

		//prepare channel to collect results from batch query
		ch := make(chan res, len(batch))

		//begin quering nodes in batch, in parallel
		for _, p := range batch {
			queried[p.ID] = true
			go func(addr string) {

				req, _ := n.makeMessage(OP_FIND_NODE)
				r := req.(*dhtpb.FindNodeRequest)
				r.NodeId = target[:]

				b, err := n.sendRequest(addr, OP_FIND_NODE, req)
				if err != nil {
					ch <- res{nil, err}
					return
				}

				var resp dhtpb.FindNodeResponse
				if err := n.decode(b, &resp); err != nil {
					ch <- res{nil, err}
					return
				}

				out := make([]*routing.Peer, 0, len(resp.Peers))
				for _, rp := range resp.Peers {
					if rp == nil || len(rp.NodeId) != len(target) || rp.Addr == "" {
						continue
					}
					var id types.NodeID
					copy(id[:], rp.NodeId)
					if id == n.ID {
						continue
					}
					out = append(out, &routing.Peer{ID: id, Addr: rp.Addr})
				}
				ch <- res{out, nil}
			}(p.Addr)
		}

		// wait up to timeout for this batch
		timer := time.NewTimer(timeout)
		for i := 0; i < len(batch); i++ {
			select {
			case r := <-ch:
				if r.err != nil {
					continue
				}
				for _, np := range r.peers {
					if np == nil || np.Addr == "" || np.ID == n.ID {
						continue
					}
					// learn them
					n.routingTable.Update(np.ID, np.Addr)

					if _, ok := seen[np.ID]; !ok {
						seen[np.ID] = np
					}
				}
			case <-timer.C:
				i = len(batch) // stop waiting for remaining responses
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}

		//rebuild shortlist and wider seen candidate pool from nodes discovered in this iteration
		rebuild()

		// check for progress, by determining if best k has changed
		bestAfter := snapshotBest()
		progressed := !sameSnapshot(bestBefore, bestAfter)
		bestBefore = bestAfter

		//where there is no change and no further nodes to query we return.
		if !progressed && !hasUnqueriedInBestK() {
			break
		}
	}

	return shortlist, nil
}

func (n *Node) sendRequest(to string, op int, payload any) ([]byte, error) {
	//fmt.Println("Send Request Executed...")
	b, err := n.encode(payload)
	if err != nil {
		return nil, err
	}
	reqID := n.nextReqID()
	msg, err := n.cd.Wrap(op, reqID, false, n.ID.String(), n.Addr, n.nodeType, b)
	if err != nil {
		return nil, err
	}
	ch := make(chan []byte, 1)
	n.pending.Store(reqID, ch)
	defer n.pending.Delete(reqID)
	if err := n.transport.Send(to, msg); err != nil {
		return nil, err
	}
	select {
	case resp := <-ch:
		//fmt.Println("response from remote node was: ")
		//fmt.Println(resp)
		return resp, nil
	case <-time.After(n.cfg.RequestTimeout):
		return nil, fmt.Errorf("node request timeout")
	}
}

func (n *Node) mergeIndexLocal(key string, e IndexEntry, reps []string, publisherId types.NodeID) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	//only allow the original publisher to update their own index entries
	if e.Publisher != publisherId {
		return fmt.Errorf("only the original publisher can update their own index entries")
	}

	rec, ok := n.dataStore[key]
	if !ok {
		//PublisherId is not used for index records, it will be stored per enry instead
		//TTL is also stored per entry.
		rec = &record{Key: key, IsIndex: true, Replicas: reps, Publisher: types.NodeID{}}
	}
	var entries []IndexEntry
	_ = json.Unmarshal(rec.Value, &entries)
	found := false
	for i := range entries {
		if entries[i].Publisher == e.Publisher && entries[i].Source == e.Source {
			entries[i] = e
			found = true
			break
		}
	}
	if !found {
		entries = append(entries, e)
	}
	b, _ := json.Marshal(entries)
	rec.Value = b
	rec.Replicas = reps
	n.dataStore[key] = rec
	return nil
}

func (n *Node) peersToAddrs(peers []*routing.Peer) []string {
	addrs := make([]string, 0, len(peers))
	for _, p := range peers {
		addrs = append(addrs, p.Addr)
	}
	return addrs
}

func (n *Node) nodeContactsToPeers(nodeContacts []*dhtpb.NodeContact) []*routing.Peer {
	peers := make([]*routing.Peer, 0, len(nodeContacts))
	for _, nc := range nodeContacts {
		if nc == nil || nc.Addr == "" {
			continue
		}
		var id types.NodeID
		copy(id[:], nc.NodeId)
		peers = append(peers, &routing.Peer{ID: id, Addr: nc.Addr})
	}
	return peers
}

func (n *Node) janitor() {
	defer n.wg.Done()
	t := time.NewTicker(n.cfg.JanitorInterval)
	defer t.Stop()
	for {
		select {
		case <-n.stop:
			return
		case <-t.C:
			n.mu.Lock()
			now := time.Now()
			expiredEntries := make([]string, 0)
			for k, rec := range n.dataStore {

				if rec.IsIndex {

					//for index records we need to check each individual entry for expiry
					var entries []IndexEntry
					if err := json.Unmarshal(rec.Value, &entries); err != nil {
						continue
					}

					filtered := entries[:0]
					for _, e := range entries {

						//check if the entry has expired
						if e.TTL > 0 {
							entryExpiryTime := time.UnixMilli(e.UpdatedUnix).Add(time.Duration(e.TTL) * time.Millisecond)
							if now.After(entryExpiryTime) {
								fmt.Printf("IndexEntry on node: %s with key: %s by publisher: %s expired at: %s the time is now: %s", n.Addr, k, e.Publisher.String(), entryExpiryTime.String(), now.String())
								fmt.Println()
								continue //skip adding this entry to the filtered list
							}
						}
						//otherwise retain the entry
						filtered = append(filtered, e)
					}

					//if no entries remain, mark the whole record for deletion
					if len(filtered) == 0 {
						expiredEntries = append(expiredEntries, k)
					} else {
						//otherwise update the record with the filtered entries
						rec.Value, _ = json.Marshal(filtered)
						n.dataStore[k] = rec
					}

				} else {

					//otherwise for standard records just check the record expiry
					if !rec.Expiry.IsZero() && now.After(rec.Expiry) {
						fmt.Println("Record with key: " + k + " has expired will be deleted from node: " + n.Addr)
						fmt.Println("Expiry:" + rec.Expiry.String())
						fmt.Println("Now:" + now.String())
						expiredEntries = append(expiredEntries, k)
					}

				}

			}
			//now actually delete the expired records
			for _, exp := range expiredEntries {
				delete(n.dataStore, exp)
			}

			n.mu.Unlock()
		}
	}
}

func (n *Node) deleteIndexLocal(key string, publisher types.NodeID, source string) (bool, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	var deleted bool = false

	rec, ok := n.dataStore[key]
	if !ok || !rec.IsIndex {
		return false, fmt.Errorf("no index record found for key: %s", key)
	}

	var entries []IndexEntry
	if err := json.Unmarshal(rec.Value, &entries); err != nil {
		return false, fmt.Errorf("failed to unmarshal index entries for key: %s", key)
	}

	filtered := entries[:0]

	for _, e := range entries {

		// publisher AND source must match for the deletion to be actioned
		if e.Publisher != publisher {
			filtered = append(filtered, e)
			continue
		}
		if e.Source != source {
			filtered = append(filtered, e)
			continue
		}

		// Otherwise: matched → delete (skip adding the entry to our filtered list)
		deleted = true
		fmt.Println("Index entry with source: " + source + " has been deleted from index with key: " + key + " on node: " + string(n.ID[:]))
	}

	// If nothing remains, remove the whole index record for the specified key
	if len(filtered) == 0 {
		delete(n.dataStore, key)
		fmt.Println("All index entries have been deleted for index with key: " + key + " on node: " + string(n.ID[:]))
		return deleted, nil
	}

	rec.Value, _ = json.Marshal(filtered)
	n.dataStore[key] = rec

	return deleted, nil
}

func (n *Node) refresher() {
	defer n.wg.Done()
	t := time.NewTicker(n.cfg.RefreshInterval)
	defer t.Stop()

	for {
		select {
		case <-n.stop:
			return
		case <-t.C:
			func() {
				defer func() {
					if r := recover(); r != nil {
						fmt.Println("Recovered from panic in refresher tick:", r)
					}
				}()
				n.refresh()
			}()

		}
	}
}

func (n *Node) refresh() {

	//if refresh count has reached max uint64, reset to zero to avoid overflow
	if n.refreshCount == math.MaxUint64 {
		n.refreshCount = 0
	}

	//increment the refresh count
	n.refreshCount++

	n.mu.RLock()
	keys := make([]string, 0, len(n.dataStore))
	recs := make([]*record, 0, len(n.dataStore))
	for k, r := range n.dataStore {
		keys = append(keys, k)
		recs = append(recs, r)
	}
	n.mu.RUnlock()

	//if there are no stored recrods log this edge case
	//once full logging is implemented this will be a low-level DEBUF log entry
	if len(keys) == 0 {
		fmt.Println("No records to refresh on node: " + n.Addr)
		return
	}

	for i, k := range keys {
		rec := recs[i]

		if rec.IsIndex {

			// Decode existing entries
			var entries []IndexEntry
			if err := json.Unmarshal(rec.Value, &entries); err != nil {
				continue
			}

			// Re-announce each entry with updated timestamp
			for _, e := range entries {

				//skip the refresh where this node is not the original publisher
				if e.Publisher != n.ID {
					continue
				}

				//if the entry has not yet expired ...
				timeElapsedSinceUpdate := time.Since(time.UnixMilli(e.UpdatedUnix))
				if timeElapsedSinceUpdate.Milliseconds() < e.TTL {

					//...and is close to expiry, refresh it
					if (e.TTL - timeElapsedSinceUpdate.Milliseconds()) <= (n.cfg.RefreshInterval * 2).Milliseconds() {

						//increament the IndexEntry's local refresh count
						if e.LocalRefreshCount == math.MaxUint64 {
							e.LocalRefreshCount = 0
						}
						e.LocalRefreshCount++

						//if we have hit the max number of local refresh attempts set "recomputeReplicas" parameter to true
						//to prompt the StoreIndex function to recompute the network-wide, k nearest nodes afresh
						recomputeReplicas := e.LocalRefreshCount > uint64(n.cfg.MaxLocalIndexEntryRefreshCount) && e.LocalRefreshCount%uint64(n.cfg.MaxLocalIndexEntryRefreshCount) == 0

						ttlDuration := time.Duration(e.TTL) * time.Millisecond
						e.UpdatedUnix = time.Now().UnixMilli()
						indexRefreshErr := n.StoreIndexWithTTL(k, e, ttlDuration, e.Publisher, recomputeReplicas)
						if indexRefreshErr != nil {
							fmt.Println("An error occurred whilst attempting to refresh index entry with key: " + k + " the error was: " + indexRefreshErr.Error())
						}
					}
				}

			}

			//fmt.Println("SUCCESSFULLY REFRESHED INDEX KEY: "+k+" Refresh Count: ", n.refreshCount)
		} else {

			//only original publisher should refresh
			if rec.Publisher != n.ID {
				continue
			}

			//if this is permanent record, skip the refresh
			if rec.Expiry.IsZero() {
				continue
			}

			//if the record is not close to expiry, skip the refresh
			left := time.Until(rec.Expiry)
			if left > (n.cfg.RefreshInterval * 2) {
				//fmt.Println("Skipping refresh for Key: "+rec.Key+" as it not near expiry.")
				continue
			}

			//otherwise, refresh the record...

			//set the TTL to be used for the refresh
			left = rec.TTL

			//increament the local refresh count for this record
			rec.mu.Lock()
			if rec.LocalRefreshCount == math.MaxUint64 {
				rec.LocalRefreshCount = 0
			}
			rec.LocalRefreshCount++
			rec.mu.Unlock()

			//if we have hit the max number of local refresh attempts set "recomputeReplicas" parameter to true
			//to prompt the StoreWithTTL function to recompute the network-wide, k nearest nodes afresh
			rec.mu.RLock()
			recomputeReplicas := rec.LocalRefreshCount > uint64(n.cfg.MaxLocalEntryRefreshCount) && rec.LocalRefreshCount%uint64(n.cfg.MaxLocalEntryRefreshCount) == 0
			rec.mu.RUnlock()

			entryRefreshErr := n.StoreWithTTL(k, rec.Value, left, rec.Publisher, recomputeReplicas)
			if entryRefreshErr != nil {
				fmt.Println("An error occurred whilst attempting to refresh entry with key: " + k + " the error was: " + entryRefreshErr.Error())
			}
			//fmt.Println("SUCCESSFULLY REFRESHED ENTRY KEY: "+k+" Refresh Count: ", n.refreshCount)
		}
	}
}
