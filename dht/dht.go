package dht

import (
	"crypto/rand"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SharefulNetworks/shareful-dht/netx"
	"github.com/SharefulNetworks/shareful-dht/proto/dhtpb"
	"github.com/SharefulNetworks/shareful-dht/wire"
	"google.golang.org/protobuf/proto"
)

const IDBytes = 20

const (
	OP_STORE       = 1
	OP_FIND        = 2
	OP_STORE_INDEX = 3
	OP_FIND_INDEX  = 4
	OP_PING        = 5
	OP_CONNECT     = 6
)

type NodeID [IDBytes]byte

func NewRandomID() NodeID {
	var id NodeID
	var randomData [32]byte
	io.ReadFull(rand.Reader, randomData[:])
	sum := sha1.Sum(randomData[:])
	copy(id[:], sum[:])
	return id
}
func HashKey(k string) NodeID    { s := sha1.Sum([]byte(k)); var id NodeID; copy(id[:], s[:]); return id }
func (id NodeID) String() string { return hex.EncodeToString(id[:]) }

func ParseNodeID(s string) (NodeID, error) {
	var id NodeID
	b, err := hex.DecodeString(s)
	if err != nil {
		return id, err
	}
	if len(b) != IDBytes {
		return id, fmt.Errorf("invalid node id length: %d", len(b))
	}
	copy(id[:], b[:IDBytes])
	return id, nil
}

func xor(a, b NodeID) (o NodeID) {
	for i := 0; i < IDBytes; i++ {
		o[i] = a[i] ^ b[i]
	}
	return
}
func CompareDistance(a, b, t NodeID) int {
	da := xor(a, t)
	db := xor(b, t)
	for i := 0; i < IDBytes; i++ {
		if da[i] < db[i] {
			return -1
		}
		if da[i] > db[i] {
			return 1
		}
	}
	return 0
}

type Config struct {
	K                        int
	DefaultTTL               time.Duration
	AllowPermanentDefault    bool
	RefreshInterval          time.Duration
	JanitorInterval          time.Duration
	UseProtobuf              bool
	RequestTimeout           time.Duration
	OutboundQueueWorkerCount int
}

func DefaultConfig() Config {
	return Config{K: 3, DefaultTTL: 10 * time.Minute, RefreshInterval: 2 * time.Minute, JanitorInterval: time.Minute, UseProtobuf: true, RequestTimeout: 1500 * time.Millisecond, OutboundQueueWorkerCount: 4}
}

type IndexEntry struct {
	Source      string `json:"source"`
	Target      string `json:"target"`
	Meta        []byte `json:"meta"`
	UpdatedUnix int64  `json:"updated_unix"`
}
type record struct {
	Key      string
	Value    []byte
	Expiry   time.Time
	IsIndex  bool
	Replicas []string
}

type Node struct {
	ID           NodeID
	Addr         string
	cfg          Config
	transport    netx.Transport
	cd           wire.Codec
	mu           sync.RWMutex
	store        map[string]*record
	peers        map[NodeID]string
	stop         chan struct{}
	reqSeq       uint64
	pending      sync.Map // map[uint64]chan []byte
	refreshCount uint64
}

func NewNode(id string, addr string, transport netx.Transport, cfg Config) *Node {
	var codec wire.Codec
	codec = wire.JSONCodec{}
	if cfg.UseProtobuf {
		codec = wire.ProtobufCodec{}
	}
	n := &Node{
		ID:           HashKey(id),
		Addr:         addr,
		cfg:          cfg,
		transport:    transport,
		cd:           codec,
		mu:           sync.RWMutex{},
		store:        map[string]*record{},
		peers:        map[NodeID]string{},
		stop:         make(chan struct{}),
		refreshCount: 0,
	}
	_ = n.transport.Listen(addr, n.onMessage)
	go n.janitor()
	go n.refresher()
	return n
}
func (n *Node) Close() {
	close(n.stop)
	n.transport.Close()
}
func (n *Node) AddPeer(addr string, id NodeID) {
	n.mu.Lock()
	n.peers[id] = addr
	n.mu.Unlock()
}

func (n *Node) idFor(addr string) NodeID {
	if addr == n.Addr {
		return n.ID
	}
	var zero NodeID
	n.mu.RLock()
	defer n.mu.RUnlock()
	for id, a := range n.peers {
		if a == addr {
			return id
		}
	}
	return zero
}

func (n *Node) lookupAddrForId(id NodeID) (string, error) {
	if id == n.ID {
		return n.Addr, nil
	}
	n.mu.RLock()
	defer n.mu.RUnlock()
	peerAddr, ok := n.peers[id]
	if !ok {
		return "", fmt.Errorf("unknown peer ID: %s", id.String())
	}
	return peerAddr, nil
}

func (n *Node) nearestK(key string) []string {
	t := HashKey(key)
	// collect candidate node IDs (peers + self)
	n.mu.RLock()
	ids := make([]NodeID, 0, len(n.peers)+1)
	for id := range n.peers {
		ids = append(ids, id)
	}
	n.mu.RUnlock()
	ids = append(ids, n.ID)
	sort.Slice(ids, func(i, j int) bool {
		return CompareDistance(ids[i], ids[j], t) < 0
	})
	k := n.cfg.K
	if k > len(ids) {
		k = len(ids)
	}
	ids = ids[:k]
	addrs := make([]string, 0, len(ids))
	for _, id := range ids {
		if id == n.ID {
			addrs = append(addrs, n.Addr)
		} else {
			n.mu.RLock()
			addr, ok := n.peers[id]
			n.mu.RUnlock()
			if ok {
				addrs = append(addrs, addr)
			}
		}
	}
	return addrs
}

func (n *Node) nextReqID() uint64 { return atomic.AddUint64(&n.reqSeq, 1) }

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

func (n *Node) sendRequest(to string, op int, payload any) ([]byte, error) {
	//fmt.Println("Send Request Executed...")
	b, err := n.encode(payload)
	if err != nil {
		return nil, err
	}
	reqID := n.nextReqID()
	msg, err := n.cd.Wrap(op, reqID, false, n.ID.String(), b)
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

func (n *Node) Store(key string, val []byte) error {
	ttl := n.cfg.DefaultTTL
	if n.cfg.AllowPermanentDefault {
		ttl = 0
	}
	return n.StoreWithTTL(key, val, ttl)
}

func (n *Node) StoreWithTTL(key string, val []byte, ttl time.Duration) error {
	reps := n.nearestK(key)
	exp := time.Time{}
	if ttl > 0 {
		exp = time.Now().Add(ttl)
	}
	n.mu.Lock()
	n.store[key] = &record{Key: key, Value: val, Expiry: exp, IsIndex: false, Replicas: reps}
	n.mu.Unlock()

	reqAny, _ := n.makeMessage(OP_STORE)
	switch r := reqAny.(type) {
	case *dhtpb.StoreRequest:
		r.Key = key
		r.Value = val
		r.TtlMs = int64(ttl / time.Millisecond)
		r.Replicas = reps
	case *struct {
		Key      string   `json:"key"`
		Value    []byte   `json:"value"`
		TTLms    int64    `json:"ttl_ms"`
		Replicas []string `json:"replicas"`
	}:
		r.Key = key
		r.Value = val
		r.TTLms = int64(ttl / time.Millisecond)
		r.Replicas = reps
	}

	for _, a := range reps {
		if a == n.Addr {
			continue
		}
		if _, err := n.sendRequest(a, OP_STORE, reqAny); err != nil {
			return err
		}
	}
	return nil
}

func (n *Node) Find(key string) ([]byte, bool) {
	n.mu.RLock()
	rec, ok := n.store[key]
	n.mu.RUnlock()
	if ok && (rec.Expiry.IsZero() || time.Now().Before(rec.Expiry)) {
		return append([]byte(nil), rec.Value...), true
	}
	return nil, false
}

func (n *Node) FindRemote(key string) ([]byte, bool) {
	reps := n.nearestK(key)
	if v, ok := n.Find(key); ok {
		return v, true
	}
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
	for i := 0; i < len(reps)-1; i++ {
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

func (n *Node) StoreIndexValue(indexKey string, e IndexEntry, ttl time.Duration) error {
	reps := n.nearestK(indexKey)
	exp := time.Time{}
	if ttl > 0 {
		exp = time.Now().Add(ttl)
	}
	n.mergeIndexLocal(indexKey, e, exp, reps)

	reqAny, _ := n.makeMessage(OP_STORE_INDEX)
	switch r := reqAny.(type) {
	case *dhtpb.StoreIndexRequest:
		r.Key = indexKey
		r.Entry = &dhtpb.IndexEntry{
			Source:      e.Source,
			Target:      e.Target,
			Meta:        e.Meta,
			UpdatedUnix: e.UpdatedUnix,
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

	for _, a := range reps {
		if a == n.Addr {
			continue
		}
		if _, err := n.sendRequest(a, OP_STORE_INDEX, reqAny); err != nil {
			return err
		}
	}
	return nil
}

func (n *Node) FindIndex(key string) ([]IndexEntry, bool) {
	n.mu.RLock()
	rec, ok := n.store[key]
	n.mu.RUnlock()
	if !ok || (!rec.Expiry.IsZero() && time.Now().After(rec.Expiry)) {
		return nil, false
	}
	var entries []IndexEntry
	_ = json.Unmarshal(rec.Value, &entries)
	return entries, true
}

func (n *Node) FindIndexRemote(key string) ([]IndexEntry, bool) {
	reps := n.nearestK(key)
	if ents, ok := n.FindIndex(key); ok {
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
						out = append(out, IndexEntry{
							Source:      ie.Source,
							Target:      ie.Target,
							Meta:        ie.Meta,
							UpdatedUnix: ie.UpdatedUnix,
						})
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

// Connect sends an OP_CONNECT request to the given address, asking that
// remote node to register this node (ID + Addr) in its peer table.
func (n *Node) Connect(remoteAddr string) error {
	fmt.Println("Node: " + n.ID.String() + " is connecting to node @ " + remoteAddr + " standby...")
	req := &dhtpb.ConnectRequest{
		NodeId: n.ID[:], // NodeID is [20]byte, cast to slice
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
	var remoteID NodeID
	copy(remoteID[:], resp.NodeId)
	n.AddPeer(remoteAddr, remoteID)

	return nil
}

// -----------------------------------------------------------------------------
// Incoming Message Handler (uses makeMessage + encode/decode)
// -----------------------------------------------------------------------------

func (n *Node) onMessage(from string, data []byte) {
	op, reqID, isResp, fromID, payload, err := n.cd.Unwrap(data)

	//if an error occurred during the unwrap, log and exit
	if err != nil {
		fmt.Println("Unwrap failed on node:", n.Addr, "ERROR:", err)
		return
	}

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

	//fmt.Println("the from id was: " + fromID)

	//if this is not an initial CONNECT request attempt to parse the sender Node ID and lookup its corresponding Address
	//obviously where THIS IS a CONNECT message we cannot guarentee the sender will be
	//known to us at this point (unless it connected prior) and thus we omit the address lookup here.
	var senderAddr string
	var senderAddrLookupErr error
	if op != OP_CONNECT {

		//parse the senders, hex encoded node ID, to its equivilant (byte) NodeID type value.
		senderNodeID, parseNodeErr := ParseNodeID(fromID)
		if parseNodeErr != nil {
			fmt.Println("An error occurred whilst attempting to parse sender Node ID:", fromID)
			fmt.Println(parseNodeErr.Error())
			return
		}

		//use the senders nodeId to look up its address, this will be used to send responses.
		senderAddr, senderAddrLookupErr = n.lookupAddrForId(senderNodeID)
		if senderAddrLookupErr != nil {
			fmt.Println("An error occurred whilst attempting to lookup sender address for Node ID:", fromID)
			fmt.Println(senderAddrLookupErr.Error())
		}

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

		switch r := reqAny.(type) {
		case *dhtpb.StoreRequest:
			key = r.Key
			val = r.Value
			ttlms = r.TtlMs
			reps = r.Replicas
		case *struct {
			Key      string   `json:"key"`
			Value    []byte   `json:"value"`
			TTLms    int64    `json:"ttl_ms"`
			Replicas []string `json:"replicas"`
		}:
			key = r.Key
			val = r.Value
			ttlms = r.TTLms
			reps = r.Replicas
		}

		exp := time.Time{}
		if ttlms > 0 {
			exp = time.Now().Add(time.Duration(ttlms) * time.Millisecond)
		}
		n.mu.Lock()
		n.store[key] = &record{Key: key, Value: val, Expiry: exp, IsIndex: false, Replicas: reps}
		n.mu.Unlock()

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
		msg, _ := n.cd.Wrap(OP_STORE, reqID, true, n.ID.String(), b)
		//fmt.Println("Sending response to: ")
		//fmt.Println(from)
		_ = n.transport.Send(senderAddr, msg)

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

		val, ok := n.Find(key)
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
		msg, _ := n.cd.Wrap(OP_FIND, reqID, true, n.ID.String(), b)
		//fmt.Println("Sending response to: ")
		//fmt.Println(from)
		_ = n.transport.Send(senderAddr, msg)

	case OP_STORE_INDEX:
		reqAny, respAny := n.makeMessage(OP_STORE_INDEX)
		if err := n.decode(payload, reqAny); err != nil {
			fmt.Println(err)
			return
		}

		var key string
		var entry IndexEntry
		var ttlms int64
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
				}
			}
			ttlms = r.TtlMs
			reps = r.Replicas
		case *struct {
			Key      string     `json:"key"`
			Entry    IndexEntry `json:"entry"`
			TTLms    int64      `json:"ttl_ms"`
			Replicas []string   `json:"replicas"`
		}:
			key = r.Key
			entry = r.Entry
			ttlms = r.TTLms
			reps = r.Replicas
		}

		exp := time.Time{}
		if ttlms > 0 {
			exp = time.Now().Add(time.Duration(ttlms) * time.Millisecond)
		}
		n.mergeIndexLocal(key, entry, exp, reps)

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
		msg, _ := n.cd.Wrap(OP_STORE_INDEX, reqID, true, n.ID.String(), b)
		//fmt.Println("Sending response to: ")
		//fmt.Println(from)
		_ = n.transport.Send(senderAddr, msg)

	case OP_FIND_INDEX:
		reqAny, respAny := n.makeMessage(OP_FIND_INDEX)
		if err := n.decode(payload, reqAny); err != nil {
			fmt.Println(err)
			return
		}

		var key string
		switch r := reqAny.(type) {
		case *dhtpb.FindIndexRequest:
			key = r.Key
		case *struct {
			Key string `json:"key"`
		}:
			key = r.Key
		}

		ents, ok := n.FindIndex(key)
		switch r := respAny.(type) {
		case *dhtpb.FindIndexResponse:
			r.Ok = ok
			if ok {
				r.Entries = make([]*dhtpb.IndexEntry, 0, len(ents))
				for _, e := range ents {
					r.Entries = append(r.Entries, &dhtpb.IndexEntry{
						Source:      e.Source,
						Target:      e.Target,
						Meta:        e.Meta,
						UpdatedUnix: e.UpdatedUnix,
					})
				}
			}
		case *struct {
			Ok      bool         `json:"ok"`
			Entries []IndexEntry `json:"entries"`
			Err     string       `json:"err"`
		}:
			r.Ok = ok
			if ok {
				r.Entries = ents
			}
			r.Err = ""
		}
		b, _ := n.encode(respAny)
		msg, _ := n.cd.Wrap(OP_FIND_INDEX, reqID, true, n.ID.String(), b)
		//fmt.Println("Sending response to: ")
		//fmt.Println(from)
		_ = n.transport.Send(senderAddr, msg)

	case OP_CONNECT:

		req, resp := &dhtpb.ConnectRequest{}, &dhtpb.ConnectResponse{}
		_ = n.decode(payload, req)

		// register their address + ID
		var peerID NodeID
		copy(peerID[:], req.NodeId)
		n.AddPeer(req.Addr, peerID)

		resp.Ok = true
		resp.NodeId = n.ID[:]
		encoded, _ := n.encode(resp)
		reply, _ := n.cd.Wrap(OP_CONNECT, reqID, true, n.ID.String(), encoded)

		// respond directly to TCP connection origin
		_ = n.transport.Send(req.GetAddr(), reply)
		return

	default:
		return
	}
}

func (n *Node) mergeIndexLocal(key string, e IndexEntry, exp time.Time, reps []string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	rec, ok := n.store[key]
	if !ok {
		rec = &record{Key: key, IsIndex: true, Replicas: reps}
	}
	var entries []IndexEntry
	_ = json.Unmarshal(rec.Value, &entries)
	found := false
	for i := range entries {
		if entries[i].Source == e.Source && entries[i].Target == e.Target {
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
	rec.Expiry = exp
	rec.Replicas = reps
	n.store[key] = rec
}

func (n *Node) janitor() {
	t := time.NewTicker(n.cfg.JanitorInterval)
	defer t.Stop()
	for {
		select {
		case <-n.stop:
			return
		case <-t.C:
			n.mu.Lock()
			now := time.Now()
			for k, rec := range n.store {
				if !rec.Expiry.IsZero() && now.After(rec.Expiry) {
					delete(n.store, k)
				}
			}
			n.mu.Unlock()
		}
	}
}

func (n *Node) refresher() {
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
	n.refreshCount++
	n.mu.RLock()
	keys := make([]string, 0, len(n.store))
	recs := make([]*record, 0, len(n.store))
	for k, r := range n.store {
		keys = append(keys, k)
		recs = append(recs, r)
	}
	n.mu.RUnlock()

	for i, k := range keys {
		rec := recs[i]
		if rec.Expiry.IsZero() {
			continue
		}

		left := rec.Expiry.Sub(time.Now())
		if left < n.cfg.RefreshInterval {
			left = n.cfg.RefreshInterval
		}

		if rec.IsIndex {
			// Only leader should refresh
			if !n.isLeader(rec.Replicas) {
				continue
			}

			// Decode existing entries
			var entries []IndexEntry
			if err := json.Unmarshal(rec.Value, &entries); err != nil {
				continue
			}

			// Re-announce each entry with updated timestamp
			for _, e := range entries {
				e.UpdatedUnix = time.Now().UnixNano()
				indexRefreshErr := n.StoreIndexValue(k, e, left)
				if indexRefreshErr != nil {
					fmt.Println("An error occurred whilst attempting to refresh index entry with key: " + k + " the error was: " + indexRefreshErr.Error())
				}
			}
			fmt.Println("SUCCESSFULLY REFRESHED INDEX KEY: "+k+" Refresh Count: ", n.refreshCount)
		} else {
			entryRefreshErr := n.StoreWithTTL(k, rec.Value, left)
			if entryRefreshErr != nil {
				fmt.Println("An error occurred whilst attempting to refresh entry with key: " + k + " the error was: " + entryRefreshErr.Error())
			}
		}
	}
}

func (n *Node) isLeader(reps []string) bool {
	if len(reps) == 0 {
		return false
	}
	t := make([]string, len(reps))
	copy(t, reps)
	sort.Slice(t, func(i, j int) bool {
		a := n.idFor(t[i])
		b := n.idFor(t[j])
		for k := 0; k < IDBytes; k++ {
			if a[k] < b[k] {
				return true
			}
			if a[k] > b[k] {
				return false
			}
		}
		return false
	})
	return t[0] == n.Addr
}
