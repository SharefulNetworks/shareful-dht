package dht

import (
	"crypto/rand"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"runtime/debug"

	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SharefulNetworks/shareful-dht/commons"
	"github.com/SharefulNetworks/shareful-dht/config"
	"github.com/SharefulNetworks/shareful-dht/events"
	"github.com/SharefulNetworks/shareful-dht/net"
	"github.com/SharefulNetworks/shareful-dht/proto/dhtpb"
	"github.com/SharefulNetworks/shareful-dht/routing"
	"github.com/SharefulNetworks/shareful-dht/types"
	"github.com/SharefulNetworks/shareful-dht/wire"
	"github.com/SharefulNetworks/shareful-utils-slog/slog"
	"github.com/SharefulNetworks/shareful-utils-unpanicked/unpanicked"

	"google.golang.org/protobuf/proto"
)

// Node - represents a single DHT node instance. It maintains the node's unique ID and address,
// a reference to a Transport implemention that, in turn, handles low-level comms between this
// node and the wider network, a real-time routing table of known peers, and various collections,
// channels and synchronization primitives for managing networking,background and lifecycle tasks.
type Node struct {
	ID                   types.NodeID
	plainTextId          string //some client facing ops, like event prop, may require reference to the user originally specified id to enable the parent app to correlate received events with application-specific/level node id assignment.
	Addr                 string
	transport            net.Transport
	cfg                  *config.Config
	nodeType             int
	cd                   wire.Codec
	mu                   sync.RWMutex
	dataStore            map[string]*Record
	routingTable         *routing.RoutingTable
	stop                 chan struct{}
	reqSeq               uint64
	pending              sync.Map // map[uint64]chan []byte
	refreshCount         uint64
	wg                   sync.WaitGroup
	closeOnce            sync.Once
	blacklist            sync.Map
	indexUpdateEventKeys sync.Map // map[string]struct{} used to track index keys that, on mutation, will trigger the production of a SyncIndexUpdatedEvent which the parent applicaton can use to be promptly notified of any changes and undertake any application level operations, as necessary.
	nodeEventListeners   sync.Map
	logger               *slog.Logger
}

// NewNode - creates and initializes a new DHT node instance.
// It takes as input a string ID, a string address, a transport implementation,
// a config struct and an integer node type. It returns a pointer to the
// newly created Node instance or non nil error object, where an error occurred
// during the creation process. The new node will immediately begin listening for
// incoming messages and will spin up various background network maintanence tasks.
func NewNode(plainTextId string, addr string, transport net.Transport, cfg *config.Config, nodeType int) (*Node, error) {
	var codec wire.Codec
	codec = wire.JSONCodec{}
	if cfg.UseProtobuf {
		codec = wire.ProtobufCodec{}
	} else {
		return nil, fmt.Errorf("An unsupported wire codec was specified, Protobuf is currently the only supported codec. Please set UseProtobuf to TRUE in the node config.")
	}

	//NB: Don't forget to increment this if/when new types are added.
	if nodeType <= 0 || nodeType >= 4 {
		return nil, fmt.Errorf("An unsupported node type was provided: %d", nodeType)
	}

	if addr == "" || !strings.Contains(addr, ":") {
		return nil, fmt.Errorf("A valid network address must be provided")
	}

	//instantiate the new DHT node
	n := &Node{
		ID:           HashKey(plainTextId),
		plainTextId:  plainTextId,
		Addr:         addr,
		transport:    transport,
		cfg:          cfg,
		nodeType:     nodeType,
		cd:           codec,
		mu:           sync.RWMutex{},
		dataStore:    map[string]*Record{},
		stop:         make(chan struct{}),
		refreshCount: 0,
		logger:       slog.NewLogger("shareful.dht.Node", nil),
	}

	//set the nodes globally reachable address if it hasn't been set already
	if n.cfg.NodeAddress == "" {
		n.cfg.NodeAddress = addr
	}

	//set global log-level, this will apply to all loggers in the DHT.
	slog.SetGlobalMinLevel(n.cfg.GlobalLogLevel)

	//instantiate the nodes routing table, we have to do this outside of the
	//constructor as the routing table requires reference to the node being constructed.
	n.routingTable = routing.NewRoutingTable(n.ID, cfg.K, n)

	//append ourself as a RoutingTableListener to the routing table.
	n.routingTable.RegisterListener("Node", n)

	//start listening for incoming messages, we bind to all TCP interfaces (:PORT), regardless of
	//the actual address that was passed into the node. As per the Go net.Listen documentation
	//specifying a globally reachable HOST NAME has the two following implications:
	//1)An internal DNS lookup will need to be undertaken in order for the node to start listening.
	//2)The node will ONLY accept incoming connection on a single interface on a single IP address.
	var listenAddr string
	if strings.HasPrefix(addr, ":") {
		listenAddr = addr //if the address already begins with ":" we may use it as is.
	} else {

		//take a substring from : onwards
		_, after, _ := strings.Cut(addr, ":")
		listenAddr = fmt.Sprintf(":%s", after)

	}
	_ = n.transport.Listen(listenAddr, n.onMessage)

	n.logger.Debug("Node started listening for incoming connections on: %s", listenAddr)

	//spin up refresher and janitor background tasks and append them to the waitist.
	n.wg.Add(2)
	go unpanicked.RunSafe(n.janitor, func(rec any, stacktrace []byte) {
		n.logger.Error("Janitor panicked with error: %v, stacktrace: %s", rec, string(stacktrace))
	})
	go unpanicked.RunSafe(n.refresher, func(rec any, stacktrace []byte) {
		n.logger.Error("Refresher panicked with error: %v, stacktrace: %s", rec, string(stacktrace))
	})

	n.logger.Info("Node: %s @ address %s was succesfully started up.", n.ID, n.Addr)

	return n, nil
}

// -----------------------------------------------------------------------------
// Core DHT public interface methods.
// -----------------------------------------------------------------------------
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
	//using time.AfterFunc
	bootstrapWaitPeriod := time.Duration(connectDelayMillis) * time.Millisecond
	time.AfterFunc(bootstrapWaitPeriod, func() {

		for i := 0; i < len(bootstrapAddrs); i++ {
			if bootstrapAddrs[i] == n.Addr {
				continue //skip connecting to self
			}
			if err := n.Connect(bootstrapAddrs[i]); err != nil {
				bootstapConnErrors = append(bootstapConnErrors, fmt.Errorf("error occurred whilst trying to connect to bootstrap address %s: %v", bootstrapAddrs[i], err))
			}
		}

		//we capture any and all errors, that occur during the process and return them as a single error object.
		if len(bootstapConnErrors) > 0 {
			collectiveErr := CollectErrors(bootstapConnErrors)
			n.logger.Error("An error occurred whilst attempting to bootstrap to one or more nodes: %v", collectiveErr)
		}

		n.logger.Info("Node: %s Successfully bootstrapped to all %d nodes provided.", n.Addr, len(bootstrapAddrs))

		//schedule the firing of the onBootstrapComplete event, we do this in a time thread to avoid tieing up the main thread
		time.AfterFunc(500*time.Millisecond, n.publishOnBootstrapCompleteEvent)

	})

	// discover neighbouring peers after a delay post bootstrap, to allow time for the bootstrap connections
	// to be established and for the routing tables to be accordingly updated before attempting to discover
	// neighbouring peers.
	time.AfterFunc(bootstrapWaitPeriod+n.cfg.PostBootstrapNeighboorhoodResolutionDelay, func() {
		n.resolveNeighbouringNodes()
	})

	return nil
}

func (n *Node) Shutdown() {
	n.closeOnce.Do(func() {
		n.logger.Info("Node: %s is shutting down, waiting for background tasks to complete and connections to close...", n.Addr)
		close(n.stop)
		n.wg.Wait() // wait for janitor and refresher to finish
		n.transport.Close()
		n.routingTable.UnregisterListener("Node")
		n.routingTable.Destroy()
		n.logger.Info("Node: %s has shutdown successfully.", n.Addr)
	})
}

func (n *Node) AddPeer(addr string, id types.NodeID) {

	if n.IsBlacklisted(addr) {
		n.logger.Warn("Unable to add peer, its address: %s is currently black listed", addr)
		return
	}
	n.routingTable.Update(id, addr)
}

func (n *Node) DropPeer(id types.NodeID) bool {

	//we'll need the peer address to close any active connections
	//currently being held to it. OK will be false where no active connection exist to the peer.
	peerAddr, ok := n.routingTable.GetPeerAddr(id)
	if ok {
		//where an active connection DOES exist to the peer close it.
		closeErr := n.transport.CloseConnection(peerAddr)
		if closeErr != nil {
			n.logger.Error("An error occurred whilst attempting to close active connection to the peer @: %s the error was %v", peerAddr, closeErr)
		}
	}
	return n.routingTable.Remove(id)
}

// ResolvePeer - attempts to resolve the network address of a peer with the given id,
// returning the address and a boolean value of TRUE where resolution is successful and an
// empty string with a boolean value of FALSE where resolution is unsuccessful.
// Whilst this can be utilised directly as necessary, it is also used internally by the
// node to resolve peer addresses for the purpose of sending messages and undertaking other
// operations where only the peer ID is known.
func (n *Node) ResolvePeerAddr(peerPlainTextId string) (string, bool) {

	//first attempt to resolve the peers address from our local routing table.
	id := HashKey(peerPlainTextId)
	if addr, ok := n.routingTable.GetPeerAddr(id); ok {
		return addr, true
	} else {
		//otherwise we attempt to lookup the peer over the network
		if peers, err := n.lookupK(id); err == nil {
			for _, p := range peers {
				if p.ID == id {
					return p.Addr, true
				}
			}
		}
	}

	return "", false
}

// Connect sends an OP_CONNECT request to the given address, asking that
// remote node to register this node (ID + Addr) in its peer table.
func (n *Node) Connect(remoteAddr string) error {

	n.logger.Info("Node: %s is connecting to node @ %s standby...", n.ID.String(), remoteAddr)

	if len(remoteAddr) == 0 {
		return errors.New("a non empty remote address must be provided")
	}

	req := &dhtpb.ConnectRequest{
		NodeId: n.ID[:], // types.NodeID is [20]byte, cast to slice
		Addr:   n.Addr,
	}

	// sendRequest(address, op, payload)
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

	if len(key) == 0 {
		return errors.New("a non empty key must be provided")
	}

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
		n.logger.Debug("Note: local only flag provided: storage will only be propagated to known nodes in the local routing table.")
		reps = n.nearestK(key)
	} else {

		//look up k nearest nodes over network, only fall back to localised search
		//in the event of an error.
		peers, err := n.lookupK(HashKey(key))
		if err != nil {
			n.logger.Error("An error occurred whilst attemptiing to lookup nearest nodes over the network, falling back to local search. the error was: %v", err)
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
	n.dataStore[key] = &Record{Key: key, Value: val, Expiry: exp, IsIndex: false, Replicas: reps, TTL: ttl, Publisher: publisherId, LocalRefreshCount: prevLocalRefresh}
	n.mu.Unlock()

	reqAny, _ := n.makeMessage(OP_STORE)
	r := reqAny.(*dhtpb.StoreRequest)

	r.Key = key
	r.Value = val
	r.Replicas = reps
	r.PublisherId = publisherId[:]
	if ttl < 0 {
		r.TtlMs = -1
	} else {
		r.TtlMs = int64(ttl / time.Millisecond)
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

// Find - Attempts to lookup value with the provided key in the DHT. The found value and a boolean value
// of TRUE is returned where the lookup is successful otherwise, a nil value is returned with a
// boolean value of FALSE to indicate that the lookup was unsuccessful.
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
		if p == nil || p.Addr == "" || p.ID == n.ID || n.IsBlacklisted(p.Addr) {
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

		// send requests to each node in the batch concurrently
		findValReq := func(peer *routing.Peer) {
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
		}
		for _, p := range batch {

			//wrapper to allow us to (statically) pass the current peer as a parameter, this is necessary to allow the function to be passed to unpanicked.RunSafe which accepts a pointer to a function with no parameters
			findValReqWrapper := func() {
				findValReq(p)
			}
			go unpanicked.RunSafe(findValReqWrapper, func(rec any, stacktrace []byte) {
				n.logger.Error("FindValue request to peer @ %s panicked with error: %v, stacktrace: %s", p.Addr, rec, string(stacktrace))
			})
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
					if np == nil || np.Addr == "" || np.ID == n.ID || np.Addr == n.Addr { //|| n.IsBlacklisted(np.Addr) {
						continue
					}
					if n.IsBlacklisted(np.Addr) {
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

// AppendTargetValueToIndex - appends a given target value to the set of values in the index record
// associated with the provided index key. This is used to allow a single publisher to associate
// multiple values with a single index key. Where an IndexRecord and entry already exists for the provided
// key, the provided value is simply appended to the existing set of values. Where no such record
// and entry exist, a new IndexRecord and entry will be created with the provided value as the
// initial and only value in the set.
// NOTE: The enableIndexUpdateEvents boolean parameter will ONLY take effect where a new IndexEntry is being created
// .     where the IndexEntry already exists then the parameter will be ignored and the value that was initially used
// .     when the IndexEntry was created will be used to determine whether or not to trigger index update events
//
//	on mutation of the IndexEntry.
func (n *Node) AppendTargetValueToIndex(IndexKey string, targetValue string, enableIndexUpdateEvents bool) error {

	if len(IndexKey) == 0 {
		return errors.New("a non empty index key must be provided")
	}

	if len(targetValue) == 0 {
		return errors.New("a non empty target value must be provided")
	}

	//first attempt to find the relevant index entry locally, if it exists,
	//where it does we will just use its append method to add the new target value
	//and proceed to call the public interface StoreIndex method to handle the propagation
	// of the updated index record to the wider network.
	existingIndexEntries, found := n.FindIndexLocal(IndexKey)
	if found {

		//where one or more existing entries EXIST we attempt to find an entry published by THIS
		// node, where one is found we append the new target value to it and then call
		// StoreIndex to handle the propagation of the updated record to the wider network.
		for i, curEntry := range existingIndexEntries {
			if curEntry.Publisher == n.ID {

				//append the new target value to the existing entry
				appendErr := existingIndexEntries[i].AppendTargetValue(targetValue)
				if appendErr != nil {
					return fmt.Errorf("An error occurred whilst attempting to append the new target value: %s to the existing index record with key: %s, the error was: %v", targetValue, IndexKey, appendErr)
				}

				//call StoreIndex to handle the propagation of the updated record to the wider network.
				storeErr := n.StoreIndex(IndexKey, existingIndexEntries...)
				if storeErr != nil {
					return fmt.Errorf("An error occurred whilst attempting to store the updated index record with key: %s to the wider network after appending a new target value: %s to it, the error was: %v", IndexKey, targetValue, storeErr)
				}
				return nil
			}
		}

		//where no existing entry published by THIS node exists we create a new one with the provided target value
		//as its initial and only target value and then call StoreIndex to handle the propagation of the new record
		//to the wider network.
		newEntry := RecordIndexEntry{
			Source:                  IndexKey,
			Target:                  targetValue,
			EnableIndexUpdateEvents: enableIndexUpdateEvents,
		}

		storeErr := n.StoreIndex(IndexKey, append(existingIndexEntries, newEntry)...)
		if storeErr != nil {
			return fmt.Errorf("An error occurred whilst attempting to store the updated index record with key: %s to the wider network after appending a new target value: %s to it, the error was: %v", IndexKey, targetValue, storeErr)
		}

		return nil
	} else {
		//where no existing entries exist we create a new one with the provided target value
		//as its initial and only target value and then call StoreIndex to handle the propagation of the new record
		//to the wider network.
		newEntry := RecordIndexEntry{
			Source:                  IndexKey,
			Target:                  targetValue,
			EnableIndexUpdateEvents: enableIndexUpdateEvents,
		}
		storeErr := n.StoreIndex(IndexKey, newEntry)
		if storeErr != nil {
			return fmt.Errorf("An error occurred whilst attempting to store the new index record with key: %s to the wider network after appending a new target value: %s to it, the error was: %v", IndexKey, targetValue, storeErr)
		}
		return nil
	}

}

func (n *Node) AppendTargetValueToIndexWithMetadata(IndexKey string, targetValue string, enableIndexUpdateEvents bool, metadata []byte) error {

	if len(IndexKey) == 0 {
		return errors.New("a non empty index key must be provided")
	}
	if len(targetValue) == 0 {
		return errors.New("a non empty target value must be provided")
	}
	if len(metadata) == 0 {
		return errors.New("a non empty metadata must be provided")
	}

	//first attempt to find the relevant index entry locally, if it exists,
	//where it does we will just use its append method to add the new target value
	//and proceed to call the public interface StoreIndex method to handle the propagation
	// of the updated index record to the wider network.
	existingIndexEntries, found := n.FindIndexLocal(IndexKey)
	if found {

		//where one or more existing entries EXIST we attempt to find an entry published by THIS
		// node, where one is found we append the new target value to it and then call
		// StoreIndex to handle the propagation of the updated record to the wider network.
		for i, curEntry := range existingIndexEntries {
			if curEntry.Publisher == n.ID {

				//append the new target value to the existing entry
				appendErr := existingIndexEntries[i].AppendTargetValue(targetValue)
				if appendErr != nil {
					return fmt.Errorf("An error occurred whilst attempting to append the new target value: %s to the existing index record with key: %s, the error was: %v", targetValue, IndexKey, appendErr)
				}

				//call StoreIndex to handle the propagation of the updated record to the wider network.
				storeErr := n.StoreIndex(IndexKey, existingIndexEntries...)
				if storeErr != nil {
					return fmt.Errorf("An error occurred whilst attempting to store the updated index record with key: %s to the wider network after appending a new target value: %s to it, the error was: %v", IndexKey, targetValue, storeErr)
				}
				return nil
			}
		}

		//where no existing entry published by THIS node exists we create a new one with the provided target value
		//as its initial and only target value and then call StoreIndex to handle the propagation of the new record
		//to the wider network.
		newEntry := RecordIndexEntry{
			Source:                  IndexKey,
			Target:                  targetValue,
			Meta:                    metadata,
			EnableIndexUpdateEvents: enableIndexUpdateEvents,
		}

		storeErr := n.StoreIndex(IndexKey, append(existingIndexEntries, newEntry)...)
		if storeErr != nil {
			return fmt.Errorf("An error occurred whilst attempting to store the updated index record with key: %s to the wider network after appending a new target value: %s to it, the error was: %v", IndexKey, targetValue, storeErr)
		}

		return nil
	} else {
		//where no existing entries exist we create a new one with the provided target value
		//as its initial and only target value and then call StoreIndex to handle the propagation of the new record
		//to the wider network.
		newEntry := RecordIndexEntry{
			Source:                  IndexKey,
			Target:                  targetValue,
			Meta:                    metadata,
			EnableIndexUpdateEvents: enableIndexUpdateEvents,
		}
		storeErr := n.StoreIndex(IndexKey, newEntry)
		if storeErr != nil {
			return fmt.Errorf("An error occurred whilst attempting to store the new index record with key: %s to the wider network after appending a new target value: %s to it, the error was: %v", IndexKey, targetValue, storeErr)
		}
		return nil
	}

}

// RemoveTargetValueFromIndex - removes a given target value from the set of values in the index record
// associated with the IndexEntry with the provided key.
func (n *Node) RemoveTargetValueFromIndex(indexKey string, targetValue string) error {

	if len(indexKey) == 0 {
		return errors.New("a non empty index key must be provided")
	}

	if len(targetValue) == 0 {
		return errors.New("a non empty target value must be provided")
	}

	//first attempt to find the relevant index entry locally, if it exists,
	//where it does we will just use its remove method to remove the target value
	//and proceed to call the public interface StoreIndex method to handle the propagation
	// of the updated index record to the wider network.
	existingIndexEntries, found := n.FindIndexLocal(indexKey)
	if found {
		//where one or more existing entries EXIST we attempt to find an entry published by THIS
		// node, where one is found we remove the target value from it and then call
		// StoreIndex to handle the propagation of the updated record to the wider network.
		var targetIndexEntry *RecordIndexEntry
		for i, curEntry := range existingIndexEntries {
			if curEntry.Publisher == n.ID {
				targetIndexEntry = &existingIndexEntries[i]
				break
			}
		}

		//if we've found the target index entry...
		if targetIndexEntry != nil {

			//remove the target value from the existing entry
			removalErr := targetIndexEntry.RemoveTargetValue(targetValue)
			if removalErr != nil {
				return fmt.Errorf("An error occurred whilst attempting to remove target value: %s from index record with key: %s, the error was: %v", targetValue, indexKey, removalErr)
			}

			//Otherwise, where the target value WAS successfully removed..
			//CRITICALLY, we take one of two paths following the removal of the target value
			//1)Where the entry still contains at least one target value we simply call StoreIndex
			// to propagate the updated record to the wider network.
			//2)Where the entry no longer contains any target values we remove the entire entry
			//  from the index by calling DeleteIndexEntry which will in turn propegate the
			//. deletion to the wider network and ensure that all nodes remove the entry from
			// their respective local index records. This is important as an index entry with no
			// target values has no utility and would simply be taking up unnecessary space.
			remainingTargetValues, _ := targetIndexEntry.ListTargetValues()
			if len(remainingTargetValues) > 0 {

				storeErr := n.StoreIndex(indexKey, existingIndexEntries...)
				if storeErr != nil {
					return fmt.Errorf("An error occurred whilst attempting to store the updated index record with key: %s to the wider network after removing a target value: %s from it, the error was: %v", indexKey, targetValue, storeErr)
				}
				n.logger.Debug("Successfully removed target value: %s from index record with key: %s, the entry still contains target values so we propagated the updated record to the wider network.", targetValue, indexKey)
				return nil

			} else {

				deleteErr := n.DeleteIndex(indexKey, targetIndexEntry.Source, false)
				if deleteErr != nil {
					return fmt.Errorf("An error occurred whilst attempting to delete index entry from index record with key: %s after removing its last remaining target value: %s, the error was: %v", indexKey, targetValue, deleteErr)
				}
				n.logger.Debug("Successfully removed target value: %s from index record with key: %s, the entry no longer contains any target values so we removed the entry and propagated the deletion to the wider network.", targetValue, indexKey)
				return nil
			}

		}
		//otherwise where no exsting index entry value was found
		return fmt.Errorf("An error occurred whilst attempting to remove target value: %s from index record with key: %s target value does not exist.", targetValue, indexKey)
	}
	//where no existing entries exist we return an error to indicate that specfied IndexEntry
	//could not be found and thus the target value could not be removed from it.
	return fmt.Errorf("An error occurred whilst attempting to remove target value: %s from index record with key: %s as no existing index entries were found for the specified key.", targetValue, indexKey)
}

// ListIndexEntryTargetValues - lists the target values associated with the IndexEntry with the provided key. Where locallyOnly is set to TRUE,
// we will only attempt to find the relevant index entry locally and return its target values, where it is set to FALSE (it is by default) we
// will attempt to lookup the index entry over the network and return an aggregated deduped, UNION set of target values from all returned entries.
func (n *Node) ListIndexEntryTargetValues(indexKey string, locallyOnly bool) ([]string, error) {

	if len(indexKey) == 0 {
		return nil, errors.New("a non empty index key must be provided")
	}
	//if the user has specified locallyOnly then we only attempt to find the relevant index entry locally, if it exists,
	//where it does we will just use its list method to return the set of target values it contains.
	if locallyOnly {
		existingIndexEntries, found := n.FindIndexLocal(indexKey)
		if found {
			for _, curEntry := range existingIndexEntries {
				if curEntry.Publisher == n.ID {
					return curEntry.ListTargetValues()
				}
			}
			return nil, fmt.Errorf("An error occurred whilst attempting to list target values for index record with key: %s as no index entry (published by this node) was found for the specified key.", indexKey)
		} else {
			return nil, fmt.Errorf("An error occurred whilst attempting to list target values for index record with key: %s as no index entries were found for the specified key.", indexKey)
		}
	} else {

		//otherwise we attempt to lookup the index entry over the network,
		//this may obviously return multiple IndexEntries published by different nodes,
		//thus where multiple entries are returned we build a UNION set comprising of all
		//target values from all returned entries and return that to the user,
		// where only a single entry is returned we simply return the target values from that entry.
		indexEntries, found := n.FindIndex(indexKey)
		if found {

			//we use a map to produce the set of target values that way we ensure
			//that values are automatically deduped.
			targetValuesSet := make(map[string]struct{})
			for _, entry := range indexEntries {
				targetValues, listErr := entry.ListTargetValues()
				if listErr != nil {
					n.logger.Error("An error occurred whilst attempting to list target values from an index entry returned from the network lookup for index record with key: %s, the error was: %v. Proceeding to attempt to list target values from any remaining entries.", indexKey, listErr)
					continue
				}

				for _, v := range targetValues {
					targetValuesSet[v] = struct{}{}
				}
			}

			//finally we then convert the set to a slice to return to the user.
			targetValuesList := make([]string, 0, len(targetValuesSet))
			for v := range targetValuesSet {
				targetValuesList = append(targetValuesList, v)
			}
			return targetValuesList, nil

		} else {
			return nil, fmt.Errorf("An error occurred whilst attempting to list target values for index record with key: %s as no index entries were found for the specified key.", indexKey)
		}
	}

}

func (n *Node) StoreIndex(indexKey string, entries ...RecordIndexEntry) error {
	if len(indexKey) == 0 {
		return errors.New("a non empty index key must be provided")
	}
	ttl := n.cfg.DefaultIndexEntryTTL
	return n.StoreIndexWithTTL(indexKey, entries, ttl, n.ID, true, false)
}

func (n *Node) StoreIndexWithTTL(indexKey string, entries []RecordIndexEntry, ttl time.Duration, publisherId types.NodeID, recomputeReplicas bool, isIndexSyncRelated bool) error {

	n.logger.Fine("Node @ %s reveidd request to store index record with key: %s. The call graph looks as follows: %s", n.Addr, indexKey, debug.Stack())

	//chec that at least one entry has been provided.
	if len(entries) == 0 {
		return errors.New("At least one index entry must be provided")
	}

	//holds our collection of replica addresses
	var reps []string
	var blErr error
	var storeIndexErr error

	if !recomputeReplicas {
		n.logger.Debug("Note: local only flag provided: index storage will only be propagated to known nodes in the local routing table.")
		reps = n.excludeBlackListedPeerAddresses(n.nearestK(indexKey))
	} else {

		n.logger.Debug("Note: recomputeReplicas flag provided: node will attempt to lookup nearest nodes over the network to propagate index storage to, only falling back to localised search in the event of an error.")
		//look up k nearest nodes over network, only fall back to localised search
		//in the event of an error.
		peers, err := n.lookupK(HashKey(indexKey))
		if err != nil {
			n.logger.Error("An error occurred whilst attemptiing to lookup nearest nodes over the network, falling back to local search. the error was: %v", err)
			reps = n.excludeBlackListedPeerAddresses(n.nearestK(indexKey))
		} else {
			//exclude any blacklisted addresses before clampinng, to ensure we don't
			//clamp out valid replicas in place of blacklisted ones.
			reps, blErr = n.excludeBlackListedPeers(peers)
			if blErr != nil {
				n.logger.Error("An error occurred whilst attempting to exclude blacklisted addresses from the replica set obtained from the network lookup, the error was: %v. Proceeding with the unfiltered replica set.", blErr)
				reps = n.peersToAddrs(peers)
			}

		}

	}

	//next we must exclude co-publishers, i.e other first-party publishers to this
	//index (that we know about) from the replica set as they will be updated via the SyncIndex
	//mechanism and the two data propogation strategies are mutually exclusive.
	//NOTE: This will only apply to NON SYNC operations.
	//if !isIndexSyncRelated {

	//check our local store to determine if we have existing knowledge of the index
	//any associated co-publishers.
	locallyStoredIndexEntries, found := n.FindIndexLocal(indexKey)
	if found {
		reps = n.excludeCoPublisherAddresses(reps, locallyStoredIndexEntries, indexKey)
	} else {
		n.logger.Debug("No co-publishers related to index with key: %s was found", indexKey)
	}
	//}

	//if this call is sync related then we additionally want to exclude any known co-publishes
	//in the provided entries from the replica set, as they will be updated via the SyncIndex
	// mechanism and the two data propogation strategies are mutually exclusive.
	if isIndexSyncRelated {
		reps = n.excludeCoPublisherAddresses(reps, entries, indexKey)
	}

	//clamp replicas to the replication factor defined in the prevailing config
	reps = reps[:min(len(reps), n.cfg.ReplicationFactor)]

	//NB: where the call to the merge function is being made as a direct result
	//of a call to StoreIndex we can safely pass in the id of THIS node as the publisher.
	//IMPORTANT: WE NOW ACCEPT MULTIPLE IndexEntry OBJECTS!! Some of which may well have been
	//           published by other peers/nodes. Typically a node will only store a single
	//           IndexEntry in respect of a single key, thus where a record has multiple
	//           IndexEntries they WILL belong to other peer/nodes and thus we ONLY append
	//           this nodes publisher details, new TTL,etc to the IndexEntries IT has created
	//           as denoted by the IndexEntry not having an assigned PublisherId
	for i := range entries {
		e := &entries[i]

		//if the IndexEntry does not have an assigned publisher id then this indicates that
		//its a NEW IndexEntry (i.e not one received to this node over the network) thus
		//we provide initial values for its mandatory attributes.
		if e.Publisher == (types.NodeID{}) {
			e.Publisher = publisherId
			e.PublisherAddr = n.Addr
			e.TTL = ttl.Milliseconds()
			e.UpdatedUnix = time.Now().UnixMilli()
			e.CreatedUnix = time.Now().UnixMilli()

			//ensure that the provided key is set as the source of the IndexEntry to account for cases
			//where the caller may have specified an empty or incorrect source value.
			if e.Source == "" || e.Source != indexKey {
				e.Source = indexKey
				n.logger.Warn("The IndexEntry \"source\" WAS NOT specified to be equal to the IndexKey its criticial that this invarient holds and thus the \"source\" value has been duly updated to match the IndexKey.")
			}

			//set the new entries id based on existing entries where they exist.
			//TODO:GT: WE WILL HAVE TO RE-THINK THIS, THE MECHANISM FOR AUTO SDETTING THE ENABLEMENT
			//.        FLAG BASED ON PRIOR ENTRIES WILL NOT WORK BECAUSE WE DON'T LOOK UP THE INDEX
			//         RECORD PRIOR TO STORAGE THUS, AT THIS POINT, THE ENTRIES ARRAY WILL ONLY CONTAIN
			//         THE NEW ENTRY/ENTRIES BEING STORED. NOW, WHEN THIS METHOD IS CALLED FROM THE
			//         SYNC ROUTINE **IT WILL** CONTAIN THE EXISTING ENTRIES BUT BY THAT TIME IT'S TOO
			//         LATE THE ENABLEMENT FLAG WOULD HAVE ALREADY BEEN SET ON THE NEW ENTRY AND
			//.        PROPEGATED TO THE REPLIA SET. FOR NOW: WE JUST ACCEPT THE VALUE PROVIDED IN THE ENTRY.
			//n.setUpdateEventsEnabled(e, entries)

			//where update events are enabled for the IndexEntry(as determined by the call to
			// setUpdateEventEnsbled in the immediately preceeding instuction) store its
			// key to our indexUpdateEventKeys collection.
			if e.EnableIndexUpdateEvents {
				n.indexUpdateEventKeys.Store(e.Source, struct{}{})
			}
		}
	}

	//mergeIndexError := n.mergeIndexLocal(indexKey, e, reps, publisherId)
	mergeIndexError := n.mergeIndexEntriesLocal(indexKey, entries, reps, publisherId)
	if mergeIndexError != nil {
		return fmt.Errorf("Fatal: an error occurred whilst attempting to store index value %s", mergeIndexError.Error())
	}

	reqAny, _ := n.makeMessage(OP_STORE_INDEX)
	r := reqAny.(*dhtpb.StoreIndexRequest)

	var protoBuffEntries []*dhtpb.IndexEntry
	for i := range entries {
		e := &entries[i]
		protoBuffEntry := &dhtpb.IndexEntry{
			Source:                  e.Source,
			Target:                  e.Target,
			Meta:                    e.Meta,
			UpdatedUnix:             e.UpdatedUnix,
			PublisherId:             e.Publisher[:],
			Ttl:                     e.TTL,
			PublisherAddr:           e.PublisherAddr,
			CreatedUnix:             e.CreatedUnix,
			EnableIndexUpdateEvents: e.EnableIndexUpdateEvents,
		}
		protoBuffEntries = append(protoBuffEntries, protoBuffEntry)
	}
	r.Key = indexKey
	r.Entries = protoBuffEntries
	r.TtlMs = int64(ttl / time.Millisecond)
	r.Replicas = reps

	var errorsList []error
	for _, a := range reps {
		if a == n.Addr {
			continue
		}
		if _, err := n.sendRequest(a, OP_STORE_INDEX, reqAny); err != nil {
			errorsList = append(errorsList, fmt.Errorf("Node: %s encountered an error when attempting to propagate storage of INDEX entry to node @ %s", n.Addr, a))
		}
	}

	n.logger.Debug("Node: %s replicated index entry with key: %s to following nodes %s", n.Addr, indexKey, reps)

	//where at least one error occurred call into our utility to compile
	//the errors into a single error object and return it.
	if len(errorsList) > 0 {
		allReplicationErrs := CollectErrors(errorsList)

		//if the error list is of equal length to the number of replicas then this
		//would indicate that no data in respect of this storage operation wass propergated
		//to any relica nodes and therefore this is deemed to be a fatal error.
		if len(errorsList) == len(reps) {
			storeIndexErr = fmt.Errorf("Fatal: an error occurred whilst attempting to propagate storage of index entry to ALL replica nodes, the error was: %v", allReplicationErrs)
		} else {
			n.logger.Error("An error occurred whilst attempting to propagate storage of index entry to one or more replica nodes, the error was: %v. However, as at least one replica node successfully received the update this is not deemed to be a fatal error and thus we proceed without returning an error.", allReplicationErrs)
		}
	}

	//next, as index records may be shared between multiple nodes we dispatch a request
	//to notify the other peers that the index has been updated which will prompt them to pull
	//the latest version of the record.

	//we ONLY dispatch this notification where the update is NOT related to an index sync operation.
	//and providing a fatal error did not occur during the store operation.
	if !isIndexSyncRelated && !n.isFatalError(storeIndexErr) {

		n.logger.Fine("Index Record Sync delay set to: %s time scheduled: %s", n.cfg.IndexSyncDelay, time.Now().Format("hh:mm"))

		//wait for a short delay to allow the storage operation to propergate..
		time.AfterFunc(n.cfg.IndexSyncDelay, func() {

			n.logger.Debug("Index Record Sync time executed: %s", time.Now().Format("hh:mm"))

			//dispatch sync index request to applicable peers.(i.e. not ourself or those that are part of ths nodes replica set for this record.)
			n.dispatchSyncIndexRequest(publisherId, indexKey, reps, time.Now(), false, "", nil)

		})

	} else {
		n.logger.Debug("SYNC RELATED StoreIndexWithTTL CALL, SKIPPING DISPATCH OF SYNC REQUEST.")

	}

	return storeIndexErr

}

func (n *Node) FindIndexLocal(key string) ([]RecordIndexEntry, bool) {
	n.mu.RLock()
	rec, ok := n.dataStore[key]
	n.mu.RUnlock()
	if !ok {
		return nil, false
	}
	var entries []RecordIndexEntry
	_ = json.Unmarshal(rec.Value, &entries)
	return entries, true
}

func (n *Node) FindIndex(key string) ([]RecordIndexEntry, bool) {
	// 0) local fast-path
	localFindIndexEntries, localFindIndexOk := n.FindIndexLocal(key)
	n.logger.Debug("***LOCAL ENTRIES COUNT : %d on Node: %x", len(localFindIndexEntries), n.ID)

	//1) to account for that fact that a single index record may be shared between multiple nodes/peers we must always go out to the network to pull any changes
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
	merged := make(map[string]RecordIndexEntry)

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
			ents  []RecordIndexEntry
			peers []*routing.Peer
		}

		ch := make(chan res, len(batch))

		//send request to each node in the batch concurrently
		sendFindIndexReq := func(peer *routing.Peer) {
			reqAny, _ := n.makeMessage(OP_FIND_INDEX)
			r := reqAny.(*dhtpb.FindIndexRequest)
			r.Key = key

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

			resp := respAny.(*dhtpb.FindIndexResponse)
			out := make([]RecordIndexEntry, 0, len(resp.Entries))
			for _, ie := range resp.Entries {
				e := RecordIndexEntry{
					Source:                  ie.Source,
					Target:                  ie.Target,
					Meta:                    ie.Meta,
					UpdatedUnix:             ie.UpdatedUnix,
					TTL:                     ie.Ttl,
					PublisherAddr:           ie.PublisherAddr,
					CreatedUnix:             ie.CreatedUnix,
					EnableIndexUpdateEvents: ie.EnableIndexUpdateEvents,
				}
				copy(e.Publisher[:], ie.PublisherId[:])
				out = append(out, e)
			}
			ch <- res{out, n.nodeContactsToPeers(resp.GetPeers())}

		}

		for _, p := range batch {

			//wrapper to allow us to (statically) pass the current peer as a parameter, this is necessary to allow the function to be passed to unpanicked.RunSafe which accepts a pointer to a function with no parameters
			sendFindIndexReqWrapper := func() {
				sendFindIndexReq(p)
			}
			go unpanicked.RunSafe(sendFindIndexReqWrapper, func(rec any, stacktrace []byte) {
				n.logger.Error("FindIndex request to peer @ %s panicked with error: %v, stacktrace: %s", p.Addr, rec, string(stacktrace))
			})
		}

		prev := shortlist

		deadline := time.After(timeout)
	readLoop:
		for i := 0; i < len(batch); i++ {
			select {
			case r := <-ch:
				for _, e := range r.ents {
					entryKey := e.Source + "\x1f" + e.Publisher.String()
					existing, exists := merged[entryKey]
					if !exists || e.UpdatedUnix > existing.UpdatedUnix {
						merged[entryKey] = e
					}
				}
				for _, np := range r.peers {
					if np != nil && np.ID != n.ID {
						_, exists := seen[np.ID]
						if !exists {
							seen[np.ID] = np
						}
					}
				}
			case <-deadline:
				break readLoop
			}
		}

		shortlist = rebuild()
		if sameShortList(prev, shortlist) {
			break
		}
	}

	//if local find suceeded append entries to our merged collection
	if localFindIndexOk {
		for _, e := range localFindIndexEntries {
			if e.Publisher == n.ID {
				//always merge this nodes current local value over any value for the key that was discovered over the network, a node will always have the most up to date values of its own entries.
				merged[e.Source+"\x1f"+e.Publisher.String()] = e
			} else {
				//where the entry belongs to another publisher we only merge it
				// where we don't already have an entry for that publisher in our
				// merged collection, OR the update timestamp of our local
				// entry is more recent than the one discovered over the network,
				// this is to ensure we don't accidentally merge stale data from
				//our local record over fresher data discovered over the network.
				existing, exists := merged[e.Source+"\x1f"+e.Publisher.String()]
				if !exists || e.UpdatedUnix > existing.UpdatedUnix {
					merged[e.Source+"\x1f"+e.Publisher.String()] = e
				}
			}

		}
	}

	if len(merged) == 0 {
		return nil, false
	}

	out := make([]RecordIndexEntry, 0, len(merged))
	for _, e := range merged {
		out = append(out, e)
	}

	shouldMergeLocal := false
	if localFindIndexOk {
		localByKey := make(map[string]RecordIndexEntry, len(localFindIndexEntries))
		for _, e := range localFindIndexEntries {
			localByKey[e.Source+"\x1f"+e.Publisher.String()] = e
		}

		if len(out) > len(localByKey) {
			shouldMergeLocal = true
		} else if len(out) == len(localByKey) {
			//only run freshness arbitration for equal key-count sets; this path does not
			//model deletions, so where out < localByKey we intentionally do not merge/prune.
			for _, mergedEntry := range out {
				entryKey := mergedEntry.Source + "\x1f" + mergedEntry.Publisher.String()
				localEntry, exists := localByKey[entryKey]
				if !exists || mergedEntry.UpdatedUnix > localEntry.UpdatedUnix {
					shouldMergeLocal = true
					break
				}
			}
		}
	}

	//where this node has local index data and the merged result is newer/different,
	//attempt to merge the freshest entries back to local storage.
	if shouldMergeLocal {

		//we call into our local merge function to attempt to merge the new entries
		// with our local record, where applicable.
		mergeErr := n.mergeIndexEntriesLocal(key, out, []string{}, n.ID)
		if mergeErr != nil {
			n.logger.Error("Error merging index entries locally: %v", mergeErr)
		} else {
			n.logger.Debug("Successfully merged freshest index entries to local record for key: %s", key)
		}
	}

	return out, true
}

func (n *Node) Delete(key string) error {
	if len(key) == 0 {
		return errors.New("a non empty key must be provided")
	}
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

func (n *Node) DeleteIndex(indexKey string, indexEntrySource string, isIndexSyncRelated bool, optionalPublisherId ...byte) error {
	if len(indexKey) == 0 {
		return errors.New("a non empty index key must be provided")
	}

	//NB: A publisher id will only be explicitly provided where this DeleteIndex call is being made as part of an index sync operation,
	// that is, some publisher has deleted their index entry and is notifying us, as another peer that shares the same index record,
	// of the deletion as part of the sync process. In this scenario we want to ensure that we only delete the entry where the
	// publisher id matches that of the entry in our local record, this is to avoid us accidentally deleting entries that belong
	// to other publishers which may be sharing the same index record. For ALL other operations the array
	// will be nil or empty and the ID of this node will be used as the publisher id.
	var publisherId types.NodeID
	var deleteIndexErr error
	var coPublisherAddresses []string
	if isIndexSyncRelated {

		if len(optionalPublisherId) <= 0 {
			return fmt.Errorf("a publisher id MUST be provided where the DeleteIndex operation is related to an index sync operation, as this allows us to ensure that we only delete the entry associated with the publisher that initiated the sync process and avoid accidentally deleting entries belonging to other publishers that share the same index record.")
		}

		//call into our NodeID function to parse and validate the provided id
		var parseErr error
		publisherId, parseErr = types.NodeIDFromBytes(optionalPublisherId)
		if parseErr != nil {
			return fmt.Errorf("an error occurred whilst attempting to parse the publisher id: %o", parseErr)
		}

	} else {
		//otherwise for non-sync related request set the publisher id to the id of this node.
		publisherId = n.ID

		//where this method has NOT been called as a result of SYNC operation then we
		//WILL want to trigger a sync process. Delete is a special case since all data
		//related to the deleted entry may well have been removed from the network BEFORE
		//the SYNC request is dispatched. Thus here we take a snapshot of all co-publishers
		//that are associated with the IndexEntry to enable us to dispatch SYNC requests
		//after the fact!
		existingIndexEntries, found := n.FindIndex(indexKey)
		if found {
			for _, entry := range existingIndexEntries {
				if entry.GetPublisher().String() == n.ID.String() {
					continue
				}
				coPublisherAddresses = append(coPublisherAddresses, entry.PublisherAddr)
			}
		}
	}

	//attempt to delete the index entry locally.
	deleted, deletionErr := n.deleteIndexLocal(indexKey, publisherId, indexEntrySource)
	if deletionErr != nil {
		return fmt.Errorf("an error occurred whilst attempting to deleted the specified Index: %o", deletionErr)
	}

	n.logger.Debug("LOCAL DELETE SUCCEEDED: %t\n", deleted)

	//where the index entry WAS successfully deleted locally, propagate the delete to nearest K nodes.
	if deleted {
		reps := n.nearestK(indexKey)

		reqAny, _ := n.makeMessage(OP_DELETE_INDEX)
		r := reqAny.(*dhtpb.DeleteIndexRequest)
		r.PublisherId = publisherId[:]
		r.Key = indexKey
		r.Source = indexEntrySource
		r.IsSyncRelated = isIndexSyncRelated

		var errorsList []error
		for _, a := range reps {
			if a == n.Addr {
				continue
			}
			if _, err := n.sendRequest(a, OP_DELETE_INDEX, reqAny); err != nil {
				errorsList = append(errorsList, fmt.Errorf("Node: %s encountered an error when attempting to propagate (via repliction) deletion of INDEX entry to node @ %s", n.Addr, a))
			}
		}

		//where at least one error occurred call into our utility to compile
		//the errors into a single error object and return it.
		if len(errorsList) > 0 {
			allReplicationErrs := CollectErrors(errorsList)

			//if the error list is of equal length to the number of replicas then this
			//would indicate that no data in respect of this storage operation wass propergated
			//to any relica nodes and therefore this is deemed to be a fatal error.
			if len(errorsList) == len(reps) {
				deleteIndexErr = fmt.Errorf("Fatal: an error occurred whilst attempting to propagate deletion of index entry to ALL replica nodes, the error was: %v", allReplicationErrs)
			} else {
				n.logger.Error("An error occurred whilst attempting to propagate (via repliction) deletion of index entry to one or more replica nodes, the error was: %v. However, as at least one replica node successfully received the update this is not deemed to be a fatal error and thus we proceed without returning an error.", allReplicationErrs)
			}
		}

		//if this is NOT a sync related DeleteIndex operation, we dispatch a SyncIndexRequest to prompt
		//other peers/publisher to this index to pull the latest version of the record which will
		//reflect the deletion. We wait a nominal amount of time to allow the delete operation to propergate
		//across the network.
		if !isIndexSyncRelated && !n.isFatalError(deleteIndexErr) {

			n.logger.Fine(">>>>>>>>>>INDEX DELETION SYNC REQUEST SCHEDULED<<<<<<<<<<<<<<<<<")

			//wait for a short delay to allow the delete operation to propergate..
			time.AfterFunc(n.cfg.IndexSyncDelay, func() {

				n.logger.Fine(">>>>>>>>>>INDEX DELETION SYNC REQUEST RUNNING<<<<<<<<<<<<<<<<<")
				//dispatch sync index request to applicable peers.(i.e. not ourself or those that are part of ths nodes replica set for this record.)
				n.dispatchSyncIndexRequest(publisherId, indexKey, reps, time.Now(), true, indexEntrySource, coPublisherAddresses)

			})

		} else {
			n.logger.Info("SYNC RELATED DeleteIndex CALL, SKIPPING DISPATCH OF SYNC REQUEST.")

		}

		return deleteIndexErr
	} else {
		return fmt.Errorf("entry associated with key: %s could not be deleted; either no entry was found with the specified source OR this node was NOT the original publisher of the entry ,", indexKey)
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

func (n *Node) ListPeerAddresses() []string {
	allPeers := n.ListPeers()
	allPeerAddrs := make([]string, 0)
	for _, curPeer := range allPeers {
		allPeerAddrs = append(allPeerAddrs, curPeer.Addr)
	}
	return allPeerAddrs
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

// FindRaw - Attempts to look value associated with the provided raw NodeID Key in the DHT,
// this varirant of the Find opertion is used by the internal RoutingTable to periodically
// refresh is various buckets, as necessary. The accepteance of a raw key allows the
// routing table to specially craft the id's such that they fall within the scope of the
// specific bucket(s) that are required to be refreshed.
func (n *Node) FindRaw(key types.NodeID) ([]byte, bool) {

	K := n.cfg.K
	alpha := n.cfg.Alpha
	batchTimeout := n.cfg.BatchRequestTimeout

	shortlist := n.nearestKByID(key)

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
			return CompareDistance(all[i].ID, all[j].ID, key) < 0
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

		// send requests concurrently to each peer in the batch
		sendFindValueReq := func(peer *routing.Peer) {
			req := &dhtpb.FindValueRequest{Key: "", EncryptedKey: key[:]} // Ensure encrypted key is properly copied

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
				if rp == nil || rp.Addr == "" || len(rp.NodeId) != len(key) {
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
		}
		for _, p := range batch {

			// wrapper to allow us to (statically) pass the current peer as a parameter, this is necessary to allow the function to be passed to unpanicked.RunSafe which accepts a pointer to a function with no parameters
			sendFindValueReqWrapper := func() {
				sendFindValueReq(p)
			}
			go unpanicked.RunSafe(sendFindValueReqWrapper, func(rec any, stacktrace []byte) {
				n.logger.Error("FindValue request to peer @ %s panicked with error: %v, stacktrace: %s", p.Addr, rec, string(stacktrace))
			})
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
					if np == nil || np.Addr == "" || np.ID == n.ID { //|| n.IsBlacklisted(np.Addr) {
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

// GetRoutingTable - Returns reference to the nodes underlying routing table.
//
//	NB: Direct access to the routing table will generally not be required
//	    for typical use cases of the DHT, it may however be useful within the
//	    context of a test environment.
func (n *Node) GetRoutingTable() *routing.RoutingTable {
	return n.routingTable
}

func (n *Node) GetAddress() string {
	return n.Addr
}

func (n *Node) GetID() types.NodeID {
	return n.ID
}

func (n *Node) AddToBlacklist(addr ...string) {
	for _, a := range addr {
		n.blacklist.Store(a, struct{}{})
	}
}

func (n *Node) IsBlacklisted(addr string) bool {
	_, ok := n.blacklist.Load(addr)
	return ok
}

// AggregateIndexEntryTargetValues - This function is used to aggregate the target values across a list of
// RecordIndexEntryLike such that they are deduped and unique. This is intended for use where
// a collection of IndexEntries are received to a node via the "IndexUpdateEvent" mechanism and the node
// needs to extract the unique target values across the entries. As the Updated Index entries are actually
// INCLUDED in the received event. this negates the need to make a call to "ListIndexEntryTargetValues" which
// internally would make an additional network call to pull the latest version of the Index record.
func (n *Node) AggregateIndexEntryTargetValues(indexEntries []commons.RecordIndexEntryLike) []string {
	uniqueValues := make(map[string]struct{})
	for _, entry := range indexEntries {
		targetValsArr, listErr := entry.ListTargetValues()
		if listErr != nil {
			continue
		}
		for _, targetVal := range targetValsArr {
			uniqueValues[targetVal] = struct{}{}
		}
	}

	out := make([]string, 0, len(uniqueValues))
	for val := range uniqueValues {
		out = append(out, val)
	}

	return out
}

func (n *Node) RemoveFromBlacklist(addr ...string) {
	for _, a := range addr {
		n.blacklist.Delete(a)
	}
}

// SendMessage - Provides mechanism for peers on the network to directly exchange
//
//	arbitrary messages with one another, without the need for the message to be associated
//	with a specific DHT operation such as Find or Store. Dynamic node resolution is implicitly
//	supported which enables nodes to send messages to peers that they may not be directly connected
//	to at the time SendMessage is called, as long as the node is on the network, attempts will be made to firstly
//	resolve the recipients nodes address and where successful the message will be dispatched, in the usual way.
func (n *Node) SendMessage(peerPlaintextId string, message *Message) error {

	//basic input validation.
	if len(peerPlaintextId) == 0 {
		return fmt.Errorf("peerPlaintextId cannot be empty")
	}

	if message == nil {
		return fmt.Errorf("message cannot be nil")
	}

	/*
		NB: next we attempt to resolve the target peers address, the resolve  function will implicitly
		hash the provided plaintext id and attempt to directly resolver the peer address from our own routing table,
		only falling back to a network-wide resolution in the event that the peer is not found locally.
	*/
	peerAddr, peerResolved := n.ResolvePeerAddr(peerPlaintextId)
	if !peerResolved {
		return fmt.Errorf("\nERROR: an error occurred whilst attempting to resolve the target peers address with (plaintext) id: %s\n", peerPlaintextId)
	}

	//convert high-level Message model headers to lower level protobuf send message headers
	var pbMessageHeaders []*dhtpb.MessageHeader
	for _, curHeader := range message.headers {
		pbMessageHeaders = append(pbMessageHeaders, &dhtpb.MessageHeader{
			Key:   curHeader.Key,
			Value: curHeader.Value,
		})
	}

	//build request.
	reqAny, _ := n.makeMessage(OP_SEND_MESSAGE)
	r := reqAny.(*dhtpb.SendMessageRequest)
	r.Headers = pbMessageHeaders
	r.Body = message.body
	r.SenderPlaintextId = n.plainTextId
	r.RecipientPlaintextId = peerPlaintextId

	//dispatch request to the target peer using our existing sendRequest function which will handle all of the underlying logic related to peer resolution, message encoding, etc.
	if _, err := n.sendRequest(peerAddr, OP_SEND_MESSAGE, reqAny); err != nil {
		return fmt.Errorf("\nERROR: an error occurred whilst attempting to send message to target peer @ %s, the error was: %v\n", peerAddr, err)
	}

	return nil
}

// onPeerUnhealthyStateChange - This function is called by the routing table when a peer is marked as unhealthy,
// this allows the node to undertake any necessary operations in response to this event, such as removing the
// peer from the routing table, adding it to a blacklist, etc.
// NOTE: At this point the RoutingTable will have already marked the peer as unhealthy
//
//	and will exclude it from future lookups.
func (n *Node) OnPeerUnhealthyStateChange(peerId types.NodeID, peerAddr string) {
	n.logger.Debug("Received event to indicate that Peer has been marked as unhealthy: %s @ %s", peerId.String(), peerAddr)

	//after a short grace period, to allow any in-flight requests to complete, we drop the
	//unhealthy peer from the routing table AND issue a call to the Transport
	//layer to drop any existing connections to the peer.
	time.AfterFunc(n.cfg.UnhealthyPeerGracePeriod, func() {
		select {
		case <-n.stop:
			return // node is shutting down, skip deferred drop actions
		default:
		}

		//if peer is STILL unhealthy after the grace period we proceed to drop it.
		existingTargetPeer, peerExists := n.routingTable.GetPeer(peerId)
		if peerExists &&
			existingTargetPeer.Addr == peerAddr &&
			!existingTargetPeer.IsHealthy() {
			n.DropPeer(peerId)
			n.logger.Debug("UnhealthyPeer with ID %s @ %s was dropped from this node.", peerId.String(), peerAddr)
		}

	})
}

// IndexToUniqueValueMap - This function is used to transform a list of RecordIndexEntry items into an inverse map
// where the key represent unique values across all entries. The DHT considers two entries with the SAME value to
// be distinct as long as they differ in their source (key) and/or publisher. Thus it is entirely possible for two
// publishers to an index to store identical values under the same key. This method detect these duplicate value entries
// and stores the value once as the "key" component of the map, the "value" component of the map is then set to all IndexEntries
// that share this value which likely entries created by differing publishers.
func (n *Node) IndexToUniqueValueMap(indexEntries []RecordIndexEntry) map[string][]RecordIndexEntry {

	uniqueValueMap := make(map[string][]RecordIndexEntry)
	for _, entry := range indexEntries {
		curEntryValue := entry.Target
		uniqueValueMap[curEntryValue] = append(uniqueValueMap[curEntryValue], entry)
	}

	return uniqueValueMap
}

// RegisterNodeEventListener - Provides a mechanism by which external subscribers can
// listen for various events that occur within the node, such as the reception of messages,
// storage of records, etc. Thereby allowing the parent application, making use of the DHT,
// to undertake application-level operation in response to these events.
func (n *Node) RegisterNodeEventListener(id string, listener events.NodeEventListener) {
	n.nodeEventListeners.Store(id, listener)
}

func (n *Node) UnregisterNodeEventListener(id string) {
	n.nodeEventListeners.Delete(id)
}

func (n *Node) ListNodeEventListeners() []events.NodeEventListener {
	var listeners []events.NodeEventListener
	n.nodeEventListeners.Range(func(key, value interface{}) bool {
		listeners = append(listeners, value.(events.NodeEventListener))
		return true
	})
	return listeners
}

// -----------------------------------------------------------------------------
// DHT Incoming Message Handler (uses makeMessage + encode/decode)
// -----------------------------------------------------------------------------

func (n *Node) onMessage(from string, data []byte) {
	op, reqID, isResp, fromID, fromAddr, nodeType, payload, err := n.cd.Unwrap(data)

	n.logger.Debug("Received message from node: %s", fromID)

	//if an error occurred during the unwrap, log and exit
	if err != nil {
		n.logger.Error("Failed to Unwrap message received from node @:%s of type:%s failed on node:%s ERROR:%v", fromAddr, nodeType, n.Addr, err)
		return
	}

	//the node that this message was received from MUST have published its address
	if fromAddr == "" {
		n.logger.Error("Received message from node: %s with no published address, dropping message.", fromID)
		return
	}

	//parse the (hex encoded) ID of the sender from the message envolope, to its equivilant (binary) types.NodeID valye
	var senderNodeID types.NodeID
	var senderNodeIDParseErr error
	senderNodeID, senderNodeIDParseErr = ParseNodeID(fromID)
	if senderNodeIDParseErr != nil {
		n.logger.Error("Failed to parse sender Node ID from inbound request, the request will therefore be dropped. ERROR: %v", senderNodeIDParseErr)
		return
	}

	//crucially, update our routing table with the sender's details, providing of course
	//the node is not on our blacklist.
	if n.IsBlacklisted(fromAddr) {
		n.logger.Debug("Note: Received message of type %d from blacklisted node: %s the message will be dropped", op, fromAddr)
		return
	}

	n.routingTable.Update(senderNodeID, fromAddr)

	//if this is a response message, look up the pending request channel and queue the payload for processing
	if isResp {
		n.logger.Fine("Received response from node: %s", fromID)
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
			n.logger.Error("Failed to decode STORE request from node %s: %v", fromID, err)
			return
		}

		// Apply request
		var key string
		var val []byte
		var ttlms int64
		var reps []string
		var pubId []byte

		r := reqAny.(*dhtpb.StoreRequest)
		key = r.Key
		val = r.Value
		ttlms = r.TtlMs
		reps = r.Replicas
		pubId = r.PublisherId

		var exp time.Time
		switch {
		case ttlms < 0:
			n.logger.Fine("Received TTL: %d indicating deletion request for key: %s", ttlms, key)
			exp = time.Now().Add(-time.Second) // negative ttl = force-expire (delete)
		case ttlms == 0:
			exp = time.Time{} // zero tll = permanent
		default:
			exp = time.Now().Add(time.Duration(ttlms) * time.Millisecond) // positive ttl = normal expiry
		}

		n.mu.Lock()
		var publisherId types.NodeID
		copy(publisherId[:], pubId)
		n.dataStore[key] = &Record{Key: key, Value: val, Expiry: exp, IsIndex: false, Replicas: reps, TTL: time.Duration(ttlms) * time.Millisecond, Publisher: publisherId}
		n.mu.Unlock()

		n.logger.Fine("Node: %s Succesfully handled request to store/update standard entry for key:%s", n.Addr, key)
		senderNodeID, parseNodeErr := ParseNodeID(fromID)
		if parseNodeErr != nil {
			n.logger.Error("An error occurred whilst attempting to parse sender Node ID: %s ERROR: %v", fromID, parseNodeErr)
			return
		}
		n.logger.Fine("Request was sent from node: %s", senderNodeID.String())
		n.logger.Fine("Request was received to node: %s", n.ID.String())

		// Build response
		resp := respAny.(*dhtpb.StoreResponse)
		resp.Ok = true

		b, _ := n.encode(respAny)
		msg, _ := n.cd.Wrap(OP_STORE, reqID, true, n.ID.String(), n.Addr, n.nodeType, b)
		n.logger.Fine("Sending response to: %s", fromAddr)
		_ = n.transport.Send(fromAddr, msg)

	case OP_FIND:
		reqAny, respAny := n.makeMessage(OP_FIND)
		if err := n.decode(payload, reqAny); err != nil {
			n.logger.Error("Failed to decode FIND request from node %s: %v", fromID, err)
			return
		}

		var key string
		r := reqAny.(*dhtpb.FindRequest)
		key = r.Key

		val, ok := n.FindLocal(key)
		resp := respAny.(*dhtpb.FindResponse)
		resp.Ok = ok
		resp.Value = val

		b, _ := n.encode(respAny)
		msg, _ := n.cd.Wrap(OP_FIND, reqID, true, n.ID.String(), n.Addr, n.nodeType, b)
		_ = n.transport.Send(fromAddr, msg)

	case OP_STORE_INDEX:
		reqAny, respAny := n.makeMessage(OP_STORE_INDEX)
		if err := n.decode(payload, reqAny); err != nil {
			n.logger.Error("Failed to decode STORE_INDEX request from node %s: %v", fromID, err)
			return
		}

		var key string
		var entries []RecordIndexEntry
		var reps []string

		r := reqAny.(*dhtpb.StoreIndexRequest)
		key = r.Key
		if len(r.Entries) > 0 {
			for _, pbEntry := range r.Entries {
				entry := RecordIndexEntry{
					Source:                  pbEntry.Source,
					Target:                  pbEntry.Target,
					Meta:                    pbEntry.Meta,
					UpdatedUnix:             pbEntry.UpdatedUnix,
					TTL:                     pbEntry.Ttl,
					PublisherAddr:           pbEntry.PublisherAddr,
					CreatedUnix:             pbEntry.CreatedUnix,
					EnableIndexUpdateEvents: pbEntry.EnableIndexUpdateEvents,
				}
				copy(entry.Publisher[:], pbEntry.PublisherId[:])

				entries = append(entries, entry)
			}
		}
		reps = r.Replicas

		n.logger.Fine("Node @ %s received request from publisher: %s to merge entry with key: %s at: %s", n.Addr, senderNodeID.String(), key, time.Now())

		//pass in the id of the SENDER as the publisher of this index entry, the mergeIndexLocal
		//will take care of ensuring only the original publisher can store/update their own entries.
		//mergeIndexErr := n.mergeIndexLocal(key, entry, reps, senderNodeID)
		mergeIndexErr := n.mergeIndexEntriesLocal(key, entries, reps, senderNodeID)
		if mergeIndexErr != nil {
			n.logger.Error("An error occurred whilst attempting to handle request to store index entry for key: %s on node: %s ERROR: %v", key, n.Addr, mergeIndexErr)
		} else {
			n.logger.Fine("Node: %s Succesfully handled request to store/update index entry for key: %s", n.Addr, key)
		}
		res := respAny.(*dhtpb.StoreIndexResponse)
		res.Ok = true

		b, _ := n.encode(respAny)
		msg, _ := n.cd.Wrap(OP_STORE_INDEX, reqID, true, n.ID.String(), n.Addr, n.nodeType, b)

		_ = n.transport.Send(fromAddr, msg)

	case OP_FIND_INDEX:
		reqAny, respAny := n.makeMessage(OP_FIND_INDEX)
		if err := n.decode(payload, reqAny); err != nil {
			n.logger.Error("Failed to decode FIND_INDEX request from node %s: %v", fromID, err)
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
					Source:                  e.Source,
					Target:                  e.Target,
					Meta:                    e.Meta,
					UpdatedUnix:             e.UpdatedUnix,
					PublisherId:             e.Publisher[:],
					PublisherAddr:           e.PublisherAddr,
					Ttl:                     e.TTL,
					CreatedUnix:             e.CreatedUnix,
					EnableIndexUpdateEvents: e.EnableIndexUpdateEvents,
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
			n.logger.Error("Failed to decode DELETE_INDEX request from node %s: %v", fromID, err)
			return
		}

		var indexKey string
		var source string
		var publisherId types.NodeID
		var isSyncRelated bool

		r := req.(*dhtpb.DeleteIndexRequest)
		indexKey = r.Key
		source = r.Source
		isSyncRelated = r.IsSyncRelated
		copy(publisherId[:], r.PublisherId)

		//the id of the sender must match the publisher id of the entry they are attempting to update
		//UNLESS this delete is being actioned as a result of a SYNC operation.
		if !isSyncRelated && senderNodeID != publisherId {
			n.logger.Error("An error occurred whilst processing received request to delete index entry for key: %s on node: %s. The sender of the request at: %s with id: %s is not the original publisher of the entry: %s AND this is NOT a sync related request, therefore the request will be rejected.", indexKey, n.Addr, fromAddr, senderNodeID.String(), publisherId.String())
			return
		}

		deleted, deletionErr := n.deleteIndexLocal(indexKey, publisherId, source)
		n.logger.Debug("Node %s Received request to delete IndexEntry with key: %s from node %s. The result was: %v", n.Addr, indexKey, fromAddr, deleted)
		if deletionErr != nil {
			n.logger.Error("An error occurred whilst processing received request to delete index entry for key: %s on node: %s ERROR: %v", indexKey, n.Addr, deletionErr)
		}

		respObj := resp.(*dhtpb.DeleteIndexResponse)
		respObj.Ok = true

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

		//The FindRaw method is a special case as it provides the HASHED key directly
		//this is mainly used for periodic refresh of routing table buckets where keys
		//are specially crafted to fall within the scope of the bucket(s) being refreshed.
		var target types.NodeID
		var key string
		if len(req.EncryptedKey) > 0 {
			copy(target[:], req.EncryptedKey)
		} else {
			//otherwise ALL standard find value requests are expected to provide the key in string format
			//which we must then hash to derive the target id.
			key = req.Key
			target = HashKey(key)
		}

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

	case OP_SYNC_INDEX:

		req := &dhtpb.SyncIndexRequest{}
		var malformedRequestErr error = nil
		if err := n.decode(payload, req); err != nil {
			n.logger.Error("An error occurred whilst attempting to decode SyncIndexRequest received from node: %s at: %s ERROR: %v", senderNodeID.String(), time.Now().String(), err)
			malformedRequestErr = fmt.Errorf("malformed request. an error occurred whilst attempting to decode SyncIndexRequest: %w", err)

		}

		if req.Key == "" {
			n.logger.Error("Received SyncIndexRequest with empty key from node: %s at: %s This request will be dropped.", senderNodeID.String(), time.Now().String())
			malformedRequestErr = fmt.Errorf("malformed request. received SyncIndexRequest with empty key from node: %s", senderNodeID.String())
		}

		if len(req.PublisherId) == 0 {
			n.logger.Error("Received SyncIndexRequest with empty publisher id from node: %s at: %s This request will be dropped.", senderNodeID.String(), time.Now().String())
			malformedRequestErr = fmt.Errorf("malformed request. received SyncIndexRequest with empty publisher id from node: %s", senderNodeID.String())

		}

		//if the request was malformed in some way we abort processing and return an error.
		if malformedRequestErr != nil {
			n.logger.Error("Failed to process SyncIndexRequest for index with key: %s from node: %s at: %s ERROR: %v", req.Key, senderNodeID.String(), time.Now().String(), malformedRequestErr)
			var resp *dhtpb.SyncIndexResponse
			resp = &dhtpb.SyncIndexResponse{Ok: false, Err: malformedRequestErr.Error()}
			b, _ := n.encode(resp)
			msg, _ := n.cd.Wrap(OP_SYNC_INDEX, reqID, true, n.ID.String(), n.Addr, n.nodeType, b)
			_ = n.transport.Send(fromAddr, msg)
			return

		}

		n.logger.Debug("RECEIVED ***SYNC INDEX REQUEST*** TO NODE: %s FROM NODE @ %s\n IS DELETED: %t\n", n.Addr, fromAddr, req.IndexDeleted)

		/*
				IMPORTANT: HERE WE MERELY **SCHEDULE** EXECUTION OF THE SYNCHRONIZATION ROUTINE
			    AND RETURN IMMEDIATELY, THIS IS VITAL AS SYNCHRONIZATION WILL NESSISTATE AS SERIES
				OF ADDITIONAL NETWORK LOOKUPS. THUS ANY RETURNED SUCCESS RESPONSE MERELY INDICATES THAT
			    THE SYNC REQUEST WAS RECEIVED! IT WILL NOT PROVIDE ANY GURANTEES THAT THE SYNC
				ROUTINE ITSLEF WAS SUCCESFULLY EXECUTED; IT IS ASSUMED THAT INDEXS RECORDS WILL BE
				EVENTUALLY CONSISTENT.
		*/

		//if this SyncIndexRequest was received in respect of a delete operation
		if req.IndexDeleted {

			time.AfterFunc(250*time.Millisecond, func() {

				deletionErr := n.DeleteIndex(req.Key, req.IndexSource, true, req.PublisherId...)

				var resp *dhtpb.SyncIndexResponse
				if deletionErr != nil {
					n.logger.Error("Failed to process SyncIndexRequest for index with key: %s from node: %s at: %s ERROR: %v", req.Key, senderNodeID.String(), time.Now().String(), deletionErr)
					resp = &dhtpb.SyncIndexResponse{Ok: false, Err: deletionErr.Error()}
				} else {
					n.logger.Debug("Successfully processed SyncIndexRequest for index with key: %s from node: %s at: %s", req.Key, senderNodeID.String(), time.Now().String())
					resp = &dhtpb.SyncIndexResponse{Ok: true, Err: ""}
				}

				if deletionErr != nil && n.isFatalError(deletionErr) {
					n.logger.Error("A fatal error occurred whilst processing SyncIndexRequest (events related to this request WILL NOT be published.) for index with key: %s from node: %s at: %s ERROR: %v", req.Key, senderNodeID.String(), time.Now().String(), deletionErr)
				} else {

					//Where the delete operation was successful (or partially successful) we will want to publish
					//an event,where possible, to notify any listeners that an index entry has been deleted.

					//parse node id from the request
					nodeID, parseErr := types.NodeIDFromBytes(req.PublisherId)
					if parseErr != nil {
						n.logger.Error("An error occurred whilst attempting to parse SYNC (DELETE) INDEX UPDATE origin node ID from SyncIndexRequest: %v", parseErr)
					}

					//if index update events are enabled globally and for this key specifically
					//publish an IndexUpdateEvent to notify listeners that the index has been mutated/updated
					if n.cfg.IndexUpdateEventsEnabled && n.indexUpdateEventsEnabledForKey(req.Key) {
						n.publishIndexUpdateEvent(req.Key, nil, nodeID, fromAddr, true)
					}

				}

				//unlike the update path below, which may nessitate several sub async requests
				//and thus take longer than we are able to resonably wait for a response,
				//DELETE is undertaken synchronously and thus we may wait on its execution
				//and directly return result in the response to the requester.
				b, _ := n.encode(resp)
				msg, _ := n.cd.Wrap(OP_SYNC_INDEX, reqID, true, n.ID.String(), n.Addr, n.nodeType, b)
				n.logger.Debug("SENDING SYNC INDEX (DELETE) RESPONSE TO NODE @ %s", fromAddr)
				_ = n.transport.Send(fromAddr, msg)

			})

		} else {

			//Receipt of this request indicates that an index to which we are subscribed,as a publisher,
			//has been mutated (i.e an entry has been added or updated).
			time.AfterFunc(time.Millisecond*250, func() {

				/*
					// 1) Pull the updated index record (value) from the network
					var processingErr error = nil
					updatedIndexRecEntries, found := n.FindIndex(req.Key)
					if !found {
						n.logger.Debug("Received SyncIndexRequest for index with key: %s but failed to find the index record on the network. This may indicate that the record has been deleted or that there was an error during the lookup process.", req.Key)
						processingErr = fmt.Errorf("failed to find index record on the network for key: %s", req.Key)
					}

					// 2) Write its updated value to our local store AND also propogate the update to the replica set
					// for this index (nearest K nodes to the index key), our public interface StoreIndexWithTTL will
					// implicitly handle this.
					nodeID, err := types.NodeIDFromBytes(req.PublisherId)
					if err != nil {
						n.logger.Error("Failed to parse publisher NodeID from SyncIndexRequest: %v", err)
						processingErr = fmt.Errorf("failed to parse publisher NodeID from SyncIndexRequest: %w", err)
					}
					ttl := n.cfg.DefaultIndexEntryTTL

					//to enforce the invarient that nodes should only be updated via the replication process
					//or sync process, but not both. We temporarily add all top-level publisher of our index entries
					//to the blacklist to ensure they are not selected to be in the replica set of THIS subsequent
					//sync-related store operation.
					indexPublisherAddresses := make([]string, 0, len(updatedIndexRecEntries))
					for _, e := range updatedIndexRecEntries {
						if e.PublisherAddr != "" {
							indexPublisherAddresses = append(indexPublisherAddresses, e.PublisherAddr)
						}
					}


					//NOTE: We set isIndexSyncRelated to true to inidicate that this store operation is being performed as part of
					//the processing of a SyncIndexRequest, this will ensure that we do not trigger another round of syncs
					//calls to other nodes/publisher associated with this index which would result in an infinite loop of sync calls across
					//the network.
					processingErr = n.StoreIndexWithTTL(req.Key, updatedIndexRecEntries, ttl, nodeID, false, true)
				*/

				var processingErr error
				var updatedIndexRecEntries []RecordIndexEntry

				nodeID, err := types.NodeIDFromBytes(req.PublisherId)
				if err != nil {
					n.logger.Error("Failed to parse publisher NodeID from SyncIndexRequest: %v", err)
					processingErr = fmt.Errorf("failed to parse publisher NodeID from SyncIndexRequest: %w", err)
				} else {
					ttl := n.cfg.DefaultIndexEntryTTL
					updatedIndexRecEntries, processingErr = n.storeSyncedIndexWithRetry(req.Key, nodeID, ttl)
				}

				//just log any processing errors.
				if processingErr != nil && (errors.Is(processingErr, errSyncIndexPublisherSnapshotMiss) || errors.Is(processingErr, errSyncIndexRecordNotFound) || isPublisherInvariantStoreErr(processingErr)) {
					n.logger.Warn("SyncIndexRequest transient miss for index with key: %s from node: %s at: %s ERROR: %v", req.Key, senderNodeID.String(), time.Now().String(), processingErr)
				} else if processingErr != nil && n.isFatalError(processingErr) {
					n.logger.Error("A fatal error occurred whilst processing SyncIndexRequest (events related to this request WILL NOT be published.) for index with key: %s from node: %s at: %s ERROR: %v", req.Key, senderNodeID.String(), time.Now().String(), processingErr)
				} else {
					if processingErr != nil {
						n.logger.Error("A non-fatal error occurred whilst processing SyncIndexRequest for index with key: %s from node: %s at: %s ERROR: %v", req.Key, senderNodeID.String(), time.Now().String(), processingErr)
					} else {
						n.logger.Debug("Successfully processed SyncIndexRequest for index with key: %s from node: %s at: %s", req.Key, senderNodeID.String(), time.Now().String())
					}

					if processingErr == nil && n.cfg.IndexUpdateEventsEnabled && n.indexUpdateEventsEnabledForKey(req.Key) {
						n.publishIndexUpdateEvent(req.Key, updatedIndexRecEntries, nodeID, fromAddr, false)
					}
				}

				/*
				if processingErr != nil && n.isFatalError(processingErr) {
					n.logger.Error("A fatal error occurred whilst processing SyncIndexRequest (events related to this request WILL NOT be published.) for index with key: %s from node: %s at: %s ERROR: %v", req.Key, senderNodeID.String(), time.Now().String(), processingErr)

				} else {
					if processingErr != nil {
						n.logger.Error("A non-fatal error occurred whilst processing SyncIndexRequest for index with key: %s from node: %s at: %s ERROR: %v", req.Key, senderNodeID.String(), time.Now().String(), processingErr)
					} else {
						n.logger.Debug("Successfully processed SyncIndexRequest for index with key: %s from node: %s at: %s", req.Key, senderNodeID.String(), time.Now().String())
					}

					//if index update events are enabled globally and for this key specifically
					//publish an IndexUpdateEvent to notify listeners that the index has been mutated/updated
					if n.cfg.IndexUpdateEventsEnabled && n.indexUpdateEventsEnabledForKey(req.Key) {
						n.publishIndexUpdateEvent(req.Key, updatedIndexRecEntries, nodeID, fromAddr, false)
					}
				}
                */
			})

			//we ALWAYS return a success response to indicate that the SyncIndexRequest was received
			// and that the synchronization process has been scheduled, we do not wait for the
			// synchronization process to complete, which may take some time.
			var resp *dhtpb.SyncIndexResponse
			resp = &dhtpb.SyncIndexResponse{Ok: true, Err: ""}

			b, _ := n.encode(resp)
			msg, _ := n.cd.Wrap(OP_SYNC_INDEX, reqID, true, n.ID.String(), n.Addr, n.nodeType, b)
			n.logger.Debug("SENDING SYNC INDEX RESPONSE TO NODE @ %s", fromAddr)
			_ = n.transport.Send(fromAddr, msg)
			return
		}
	case OP_SEND_MESSAGE:

		req, resp := n.makeMessage(OP_SEND_MESSAGE)
		if err := n.decode(payload, req); err != nil {
			n.logger.Error("An error occurred whilst attempting to decode SendMessageRequest received from node: %s at: %s ERROR: %v", senderNodeID.String(), time.Now().String(), err)
			return
		}

		//cast generic request object to concrete type.
		r := req.(*dhtpb.SendMessageRequest)

		//parse messageHeaders from request and convert them to high-level Header model array.
		messageHeaders := make([]MessageHeader, 0, len(r.Headers))
		for _, h := range r.Headers {
			messageHeaders = append(messageHeaders, MessageHeader{
				Key:   h.Key,
				Value: h.Value,
			})
		}

		//next extract the message body from the request.
		messageBody := r.Body

		//construct a high-level Message model, this will be provided to the subsequent event
		message := NewMessage(
			messageHeaders,
			messageBody,
		)

		//next parse all remaining request metadata, we'll need them to publish the subsequent event.
		senderPlaintextId := r.SenderPlaintextId
		recipientPlaintextId := r.RecipientPlaintextId

		//where the message has been successfully parsed of the wire
		//we return a success response to the sender.
		respObj := resp.(*dhtpb.SendMessageResponse)
		respObj.Ok = true

		b, _ := n.encode(resp)
		msg, _ := n.cd.Wrap(OP_SEND_MESSAGE, reqID, true, n.ID.String(), n.Addr, n.nodeType, b)
		_ = n.transport.Send(fromAddr, msg)

		//finally publish the MessageReceivedEvent to any registered listeners.
		n.publishMessageReceivedEvent(fromAddr, senderPlaintextId, recipientPlaintextId, message)

	default:
		return
	}

}

// -----------------------------------------------------------------------------
// DHT private helper/utility methods.
// -----------------------------------------------------------------------------
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
		sendFindNodeReq := func(addr string) {

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
		}
		for _, p := range batch {
			queried[p.ID] = true

			//wrapper function to (statically) capture the current peer address for unpanicked.RunSafe as it expects a pointer to a function with no parameters as input.
			sendFindNodeReqWrapper := func() {
				sendFindNodeReq(p.Addr)
			}
			go unpanicked.RunSafe(sendFindNodeReqWrapper, func(rec any, stacktrace []byte) {
				n.logger.Error("Panic occurred during execution of FindNode request to peer: %s with id: %s at address: %s. This error was safely recovered from and will not crash the node. ERROR: %v\nSTACKTRACE: %s", p.ID.String(), p.ID.String(), p.Addr, rec, string(stacktrace))
			})

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
					if np == nil || np.Addr == "" || np.Addr == n.Addr || np.ID == n.ID { //|| n.IsBlacklisted(np.Addr) {
						continue
					}
					if n.IsBlacklisted(np.Addr) {
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

// resolveNeighbouringNodes - executed once, on start-up of the node, the method
// implicitly executes a standard "find" operation, passing IT'S ID in as the target.
// The find operation will obviously fail as no actual resource will be associated
// with the node's ID however, the mere process of attempting the operation will allow
// the node to progressively discover the nodes that are closest to it, each of which
// is then implicitly be used to seed its routing table. A delay period
// (as defined in the prevailing config) is introduced to enable bootstrap operations
// to complete before the resolution process commences.
// NB: This activity, at start up, is explicitly called for in the Kademlia paper
//
//	to increase the liklihood of any subsequent (real) "find" operations converging on the
//	correct target, irrespective of how narrow a nodes initial view of the
//	network is, on connection.
func (n *Node) resolveNeighbouringNodes() {
	n.logger.Info("Attempting to resolve neighbouring nodes..")
	n.Find(n.ID.String()) //we don't really care about the result.
}

func (n *Node) dispatchSyncIndexRequest(publisherId types.NodeID, key string, replicaSetAddrs []string, updatedAt time.Time, indexDeleted bool, deletedIndexSource string, deletedIndexCoPublisherAddrs []string) {

	n.logger.Debug("Dispatching Sync Index request for index record with key: %s from node: %s at: %s is deleted: %t", key, n.Addr, updatedAt.String(), indexDeleted)

	//var to store merged entries where this sync update DOES NOT relate to a deletion
	var mergedEntries []RecordIndexEntry

	//first grab all entries related to this key from our local store
	localFindIndexEntries, localFindIndexOk := n.FindIndexLocal(key)

	//next re-pull the IndexRecord (or more specifically its value) from the
	//network to ensure we obtain the most up=to-date copy complete with all associated publishers.
	refetchedEntries, found := n.FindIndex(key)
	if !found && !indexDeleted {
		n.logger.Error("An error occurred during the post update notification process, the updated index record could not be refetched from the network with key: %s", key)
		return
	}

	//thc sync candidates we will dispatch to.
	var syncIndexCandidateAddresses []string

	//where this is a NON-DELETION request we can derive the addresses from the IndexEntry themselves..
	if !indexDeleted {

		//helper function, merges UNIQUE local and refetched entries into a single collection.
		mergeLocalAndRefetched := func(local []RecordIndexEntry, refetched []RecordIndexEntry) []RecordIndexEntry {

			//we merge the entries by taking the refetched entries as the source of truth and then adding in any local entries
			// which have a publisher that is not represented in the refetched entries, this ensures we do not loose any publishers
			// which may be associated with this index record but were not included in the refetched copy for some reason (e.g network issues during lookup, replication delays etc).
			merged := make([]RecordIndexEntry, 0, len(refetched))
			merged = append(merged, refetched...)

			for _, localEntry := range local {
				localEntryPublisherAddr := localEntry.PublisherAddr
				found := false
				for _, refetchedEntry := range refetched {
					if refetchedEntry.PublisherAddr == localEntryPublisherAddr {
						found = true
						break
					}
				}
				if !found {
					merged = append(merged, localEntry)
				}
			}

			return merged
		}

		mergedEntries = mergeLocalAndRefetched(localFindIndexEntries, refetchedEntries)

		n.logger.Debug("Refetched Index Entries for key: %s on node:%s %v\n", key, n.Addr, refetchedEntries)

		//iterate over the returned entries to parse publishers who are candidates for the sync notifications
		//the peers must not be ourselves. We intentionally still notify co-publishers even if they were in the
		//replica set for the preceding store operation so that they execute the sync path and publish events.

		for _, entry := range mergedEntries {
			curEntryPublisherAddr := entry.PublisherAddr

			//ensure that we do not include ourselves as candidates for the sync notification
			if curEntryPublisherAddr == n.Addr {
				continue
			}
			syncIndexCandidateAddresses = append(syncIndexCandidateAddresses, curEntryPublisherAddr)
		}
	} else {
		//Otherwise where this call DOES related to a deleted index we set our sync candidates
		//to the provided list of co-publishers of the deleted index, a snapshot of these peers
		//would have been taken by the DeleteIndex method, prior to the point of deletion.
		syncIndexCandidateAddresses = deletedIndexCoPublisherAddrs
	}

	//build request
	reqAny, _ := n.makeMessage(OP_SYNC_INDEX)
	r := reqAny.(*dhtpb.SyncIndexRequest)
	r.PublisherId = publisherId[:]
	r.Key = key
	r.IndexDeleted = indexDeleted
	r.IndexSource = deletedIndexSource

	n.logger.Debug("Candidate Addresses: %v", syncIndexCandidateAddresses)

	//dispatch SyncIndexRequest to each of the candidates to prompt them to pull in the latest version of the record.
	for _, candidateAddr := range syncIndexCandidateAddresses {

		n.logger.Debug("Sending SyncIndexRequest to candidate peer: %s", candidateAddr)
		n.logger.Debug("All candidates length: %d", len(syncIndexCandidateAddresses))
		if _, err := n.sendRequest(candidateAddr, OP_SYNC_INDEX, reqAny); err != nil {
			n.logger.Error("An error occurred during the post index record storage, sync process, the candidate peer: %s could not be notified of the update on node: %s the error was: %v", candidateAddr, n.Addr, err)
		} else {
			n.logger.Debug("Index deleted: %t", indexDeleted)
			if indexDeleted {
				n.logger.Debug("Successfully sent SyncIndexRequest to candidate peer: %s to notify of DELETION of index with key: %s on node: %s", candidateAddr, key, n.Addr)
			}
		}
	}

	//finally where this sync DOES NOT relate to a deletion, we also want to propergate
	//any newly discovered entries (returned in the call to FindIndex) to THIS nodes replica set.
	//The FindIndex call would have implicitly taken care of merging these entries into our
	//local store.
	if !indexDeleted {
		if localFindIndexOk &&
			len(localFindIndexEntries) < len(refetchedEntries) {
			n.logger.Debug("New entries discovered for index with key: %s on node: %s during sync process, propergating to replica set by executing StoreIndexWithTTL \n", key, n.Addr)

			//Important: temporarily add syncIndexCandidateAddresses to the blacklist to prevent them being chosen
			//as part of the replica set for THIS sync-based store operation, as always: nodes in the replica set and sync candidates
			//list should be mutually exclusive. Nodes will be updated by either of the two mechanisms but not both.
			//n.AddToBlacklist(syncIndexCandidateAddresses...)

			//use the merged entries we collated above to ensure we propogate all known entries to the replica set, not just the refetched entries which may in certain edge cases not contain reference to THIS nodes entries.
			n.StoreIndexWithTTL(key, mergedEntries, n.cfg.DefaultIndexEntryTTL, n.ID, false, true) //We pass in TRUE to indicste the storage is "index Related" to prevent triggering another round of syncs which would result in an infinite loop.

			//n.RemoveFromBlacklist(syncIndexCandidateAddresses...)
		}

	}
}

func (n *Node) nearestK(key string) []string {
	t := HashKey(key)
	closest := n.routingTable.Closest(t, n.cfg.K)

	addrs := make([]string, 0, len(closest)+1)

	// include self as candidate
	//addrs = append(addrs, n.Addr)

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

func (n *Node) encode(v any) ([]byte, error) {
	if n.cfg.UseProtobuf {
		msg, ok := v.(proto.Message)
		if !ok {
			return nil, fmt.Errorf("expected proto.Message when UseProtobuf enabled: %T", v)
		}
		return proto.Marshal(msg)
	}
	return nil, fmt.Errorf("An unsupported wire codec was specified, please set UseProtobuf to TRUE in the node config.")

}

func (n *Node) decode(b []byte, v any) error {
	if n.cfg.UseProtobuf {
		msg, ok := v.(proto.Message)
		if !ok {
			return fmt.Errorf("expected proto.Message when UseProtobuf enabled: %T", v)
		}
		return proto.Unmarshal(b, msg)
	}
	return fmt.Errorf("An unsupported wire codec was specified, please set UseProtobuf to TRUE in the node config.")

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
		case OP_SYNC_INDEX:
			return &dhtpb.SyncIndexRequest{}, &dhtpb.SyncIndexResponse{}
		case OP_SEND_MESSAGE:
			return &dhtpb.SendMessageRequest{}, &dhtpb.SendMessageResponse{}

		default:
			return nil, nil
		}
	} else {
		n.logger.Error("An unsupported wire codec was specified, Protobuf is currently the only supported codec. Please set UseProtobuf to TRUE in the node config.")
		return nil, nil
	}
}

func (n *Node) sendRequest(to string, op int, payload any) ([]byte, error) {
	n.logger.Fine("Sending request to: %s with opcode: %d", to, op)

	if n.IsBlacklisted(to) {
		return nil, fmt.Errorf("Unable to send request,node is blacklisted: %s", to)
	}
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
		n.markPeerConnectionResult(to, false)
		return nil, err
	}
	select {
	case resp := <-ch:

		//where we ALREADY have the peer in our routing table mark
		//the connection as success since we have received a response.
		n.markPeerConnectionResult(to, true)
		n.logger.Fine("Received response to sent request from: %s with opcode: %d", to, op)
		return resp, nil
	case <-time.After(n.cfg.RequestTimeout):
		//where we ALREADY have the peer in our routing table mark
		//the connection as success since we have received a response.
		n.markPeerConnectionResult(to, false)
		return nil, fmt.Errorf("node request timeout")
	}
}

func (n *Node) markPeerConnectionResult(peerAddr string, success bool) {
	if peerAddr == "" {
		return
	}
	select {
	case <-n.stop:
		return
	default:
	}
	existingPeer, found := n.routingTable.GetPeerByAddr(peerAddr)
	if !found {
		return
	}
	if success {
		n.routingTable.MarkPeerConnectionSuccess(existingPeer.ID)
		return
	}
	n.routingTable.MarkPeerConnectionFailure(existingPeer.ID)
}

func (n *Node) mergeIndexEntriesLocal(key string, newEntries []RecordIndexEntry, reps []string, publisherId types.NodeID) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	//at least one of the entries being appended must be associated with the publisher id provided,
	// this is to ensure that a malicious actor cannot append arbitrary entries to an index record
	// that they are not associated with.
	if publisherId == (types.NodeID{}) {
		return fmt.Errorf("publisherId must be set")
	}
	var publisherIdFoundInEntries bool = false
	for _, e := range newEntries {
		if e.Publisher == publisherId {
			publisherIdFoundInEntries = true
			break
		}
	}
	if !publisherIdFoundInEntries {
		return fmt.Errorf("at least one of the entries being appended must be associated with the publisher id provided")
	}

	rec, ok := n.dataStore[key]
	if !ok {
		//PublisherId is not used for index records, it will be stored per enry instead
		//TTL is also stored per entry.
		rec = &Record{Key: key, IsIndex: true, Replicas: reps, Publisher: types.NodeID{}}
	}
	var entries []RecordIndexEntry
	_ = json.Unmarshal(rec.Value, &entries)
	for _, newEntry := range newEntries {
		found := false
		for i := range entries {
			if entries[i].Publisher == newEntry.Publisher &&
				entries[i].Source == newEntry.Source {
				//entries[i].Target == e.Target{
				if newEntry.Publisher == n.ID || newEntry.UpdatedUnix > entries[i].UpdatedUnix {
					entries[i] = newEntry
				}
				found = true
				break
			}
		}
		if !found {
			entries = append(entries, newEntry)
		}
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
		if p == nil || p.Addr == "" || p.ID == n.ID {
			continue
		}
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

func (n *Node) excludeBlackListedPeers(peers []*routing.Peer) ([]string, error) {

	filtered := make([]string, 0, len(peers))
	for _, peer := range peers {
		if peer == nil || peer.Addr == "" || peer.Addr == n.Addr {
			continue
		}
		if n.IsBlacklisted(peer.Addr) {
			removed := n.DropPeer(peer.ID) //ensure we also remove any blacklisted peers from our routing table
			if !removed {
				return nil, fmt.Errorf("peer with addr: %s was blacklisted but could not be removed from routing table", peer.Addr)
			}
			continue
		}
		filtered = append(filtered, peer.Addr)
	}
	return filtered, nil
}

func (n *Node) excludeBlackListedPeerAddresses(peerAddresses []string) []string {

	knownPeers := n.routingTable.ListKnownPeers()

	//local function to indicate if this node is currently connected to the
	//peer with the specified address.
	connectedToPeer := func(peerAddr string) *routing.Peer {
		for _, p := range knownPeers {
			if p.Addr == peerAddr {
				return &p
			}
		}
		return nil
	}

	filtered := make([]string, 0, len(peerAddresses))
	for _, peerAddr := range peerAddresses {
		if peerAddr == "" || peerAddr == n.Addr {
			continue
		}
		if n.IsBlacklisted(peerAddr) {

			//if we ae currently connected to this peer we drop the connection,
			peer := connectedToPeer(peerAddr)
			if peer != nil {
				removed := n.DropPeer(peer.ID) //ensure we also remove any blacklisted peers from our routing table
				if !removed {
					n.logger.Error("peer with addr: %s was blacklisted but could not be removed from routing table", peer.Addr)
				}
			}
			continue
		}
		filtered = append(filtered, peerAddr)
	}
	return filtered
}

func (n *Node) excludeCoPublisherAddresses(peerAddresses []string, indexEntries []RecordIndexEntry, key string) []string {

	//build up our filtered list of peer addresses,ensuring co-publishers are excluded.
	filtered := make([]string, 0, len(peerAddresses))
	for _, peerAddr := range peerAddresses {
		if peerAddr == "" || peerAddr == n.Addr {
			continue
		}
		if n.isCoPublisher(peerAddr, indexEntries) {
			n.logger.Debug("Peer address: %s belongs to a co-publisher of index with key: %s and thus will be excluded from the replica set.", peerAddr, key)
			continue
		}
		filtered = append(filtered, peerAddr)
	}
	return filtered

}

func (n *Node) indexUpdateEventsEnabledForKey(key string) bool {
	_, ok := n.indexUpdateEventKeys.Load(key)
	return ok
}

func (n *Node) isCoPublisher(address string, entries []RecordIndexEntry) bool {
	for _, curEntry := range entries {
		if curEntry.PublisherAddr == address {
			return true
		}
	}
	return false
}

// setUpdateEventsEnabled Determines whether events should be enabled for the given NEW index
//
//	entry based on the below criteria, the goal is to ensure the flag is
//	set consistently between different nodes/publisher to the shared index
//	without necessiating the sending of the parent Record struct (which contains
//	local contextual sensitive data) across the network.
//	1)If the entry is part of a set of two or more existing entries
//	  then its event flag is ALWAYS set according to the oldest of these entries.
//
//	2)If this is the ONLY entry in the set then its event flag value is accepted as is
//	  and becomes the default value for any subsequent entries added to the set.
func (n *Node) setUpdateEventsEnabled(newIndexEntry *RecordIndexEntry, allIndexEntries []RecordIndexEntry) {

	//helper inner func finds the oldest entry, by creation date, in the
	// provided array of Index Entries
	findOldestEntry := func(entries []RecordIndexEntry) *RecordIndexEntry {

		if len(entries) < 1 {
			return nil
		}

		if len(entries) == 1 {
			return &entries[0]
		}

		oldest := &entries[0]
		for _, e := range entries {
			if e.CreatedUnix < oldest.CreatedUnix {
				oldest = &e
			}
		}
		return oldest
	}

	if len(allIndexEntries) >= 2 {
		oldestEntry := findOldestEntry(allIndexEntries)
		newIndexEntry.EnableIndexUpdateEvents = oldestEntry.EnableIndexUpdateEvents
		n.logger.Debug("Entry related to node @ %s has had its EnableIndexUpdateEvents flag set to %v based on the oldest entry in the set from node @ %s", newIndexEntry.PublisherAddr, oldestEntry.EnableIndexUpdateEvents, oldestEntry.PublisherAddr)
		return
	}
}

// publishIndexUpdateEvent - responsible for firing an IndexUpdateEvent which will
// notify NodeEventListeners that an update to a specific index record has occurred.
//
//	NOTE: The call to this method schedules the publishing of the event
//	      on alternate thread/goroutine and returns immediately, thereby
//	      ensuring event prodution and dispatch does not tie up the main thread.
func (n *Node) publishIndexUpdateEvent(indexKey string, entries []RecordIndexEntry, publisherId types.NodeID, publisherAddress string, isDeletion bool) {

	//convert record index entries to record index entries like
	var recordIndexEntryLikeArr []commons.RecordIndexEntryLike
	if entries != nil {
		recordIndexEntryLikeArr = n.toRecordIndexEntriesLike(entries)
	} else {
		recordIndexEntryLikeArr = make([]commons.RecordIndexEntryLike, 0)
	}

	time.AfterFunc(500*time.Millisecond, func() {

		n.logger.Debug("Publishing Index Update Event for index with key: %s on node: %s isDeletion: %t", indexKey, n.Addr, isDeletion)

		n.nodeEventListeners.Range(func(key, value any) bool {

			listener, ok := value.(events.NodeEventListener)
			if !ok {
				n.logger.Debug("Listener type assertion failed, skipping dispatch of Index Update Event for index with key: %s to listener with key: %s", indexKey, key)
				return true
			}

			n.logger.Debug("Node: %s is dispatching Index Update Event to listener for index with key: %s to target node: %s", n.ID.String(), indexKey, n.Addr)

			//create a new event instance
			event := events.NewIndexUpdateEvent(
				indexKey,
				recordIndexEntryLikeArr,
				publisherId.String(),
				publisherAddress,
				isDeletion,
				time.Now(),
			)

			//dispatch the event to the listener
			listener.OnIndexUpdated(event)

			return true
		})
	})
}

func (n *Node) publishMessageReceivedEvent(fromAddr string, senderPlaintextId string, recipientPlaintextId string, message commons.MessageLike) {

	//wrap the message in a time.AfterFunc to ensure the event is published on a separate thread and does not
	//block the main execution flow of the node.
	time.AfterFunc(500*time.Millisecond, func() {
		n.logger.Debug("Publishing Message Received Event for message received from peer @:  %s", fromAddr)

		//build MessageReceivedEvent using the message and meta-data parsed of the wire
		event := events.NewMessageReceivedEvent(
			fromAddr,
			senderPlaintextId,
			recipientPlaintextId,
			message,
		)

		//notify all NodeEventListeners of the new message received event by passing the event to their OnMessageReceived method
		n.nodeEventListeners.Range(func(key, value any) bool {

			listener, ok := value.(events.NodeEventListener)
			if !ok {
				n.logger.Debug("Listener type assertion failed, skipping dispatch of Message Received Event for message received from peer @:  %s", fromAddr)
				return true
			}

			listener.OnMessageReceived(event)

			return true

		})

	})
}

func (n *Node) publishOnBootstrapCompleteEvent() {

	n.nodeEventListeners.Range(func(key, value any) bool {

		listener, ok := value.(events.NodeEventListener)
		if !ok {
			n.logger.Debug("Listener type assertion failed, skipping dispatch of OnBootstrapComplete event to listener with key: %s", key)
			return true
		}

		listener.OnBootstrapComplete()

		return true

	})

}

func (n *Node) isFatalError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "Fatal:")
}

func (n *Node) toRecordIndexEntriesLike(entries []RecordIndexEntry) []commons.RecordIndexEntryLike {
	recordIndexEntryLikesArr := make([]commons.RecordIndexEntryLike, 0, len(entries))
	for _, v := range entries {
		recordIndexEntryLikesArr = append(recordIndexEntryLikesArr, &v)
	}
	return recordIndexEntryLikesArr
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
					var entries []RecordIndexEntry
					if err := json.Unmarshal(rec.Value, &entries); err != nil {
						continue
					}

					filtered := entries[:0]
					for _, e := range entries {

						//check if the entry has expired
						if e.TTL > 0 {
							entryExpiryTime := time.UnixMilli(e.UpdatedUnix).Add(time.Duration(e.TTL) * time.Millisecond)
							if now.After(entryExpiryTime) {
								n.logger.Warn("IndexEntry on node: %s with key: %s by publisher: %s expired at: %s the time is now: %s", n.Addr, k, e.Publisher.String(), entryExpiryTime.String(), now.String())
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
						n.logger.Warn("Record with key: %s has expired will be deleted from node: %s", k, n.Addr)
						n.logger.Warn("Expiry: %s", rec.Expiry.String())
						n.logger.Warn("Now: %s", now.String())
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

	var entries []RecordIndexEntry
	if err := json.Unmarshal(rec.Value, &entries); err != nil {
		return false, fmt.Errorf("failed to unmarshal index entries for key: %s", key)
	}

	//filtered := entries[:0]
	var filtered []RecordIndexEntry

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
		publisherAddr, _ := n.routingTable.GetPeerAddr(e.Publisher)

		n.logger.Info("Deleted Index entry with source: %s and with key: %s originally published by: %s deleted from node: %s", source, key, publisherAddr, n.Addr)

	}

	// If nothing remains, remove the whole index record for the specified key
	if len(filtered) == 0 {
		delete(n.dataStore, key)
		n.logger.Info("All index entries have been deleted for index with key: %s on node: %s", key, n.Addr)
		return deleted, nil
	}

	newRec := &Record{
		Key:               rec.Key,
		Expiry:            rec.Expiry,
		IsIndex:           rec.IsIndex,
		Replicas:          rec.Replicas,
		TTL:               rec.TTL,
		Publisher:         rec.Publisher,
		LocalRefreshCount: rec.LocalRefreshCount,
	}

	newRec.Value, _ = json.Marshal(filtered)
	n.dataStore[key] = newRec

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

			unpanicked.RunSafe(n.refresh, func(rec any, stacktrace []byte) {
				n.logger.Error("Panic occurred in refresher with panic: %v and stacktrace: %s", rec, string(stacktrace))
			})

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
	recs := make([]*Record, 0, len(n.dataStore))
	for k, r := range n.dataStore {
		keys = append(keys, k)
		recs = append(recs, r)
	}
	n.mu.RUnlock()

	//if there are no stored recrods log this edge case
	//once full logging is implemented this will be a low-level DEBUF log entry
	if len(keys) == 0 {
		n.logger.Debug("No records to refresh on node: %s", n.Addr)
		return
	}

	for i, k := range keys {
		rec := recs[i]

		if rec.IsIndex {

			// Decode existing entries
			var entries []RecordIndexEntry
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
						indexRefreshErr := n.StoreIndexWithTTL(k, []RecordIndexEntry{e}, ttlDuration, e.Publisher, recomputeReplicas, false)
						if indexRefreshErr != nil {
							n.logger.Error("An error occurred whilst attempting to refresh index entry with key: %s the error was: %v", k, indexRefreshErr)
						}
					}
				}

			}
			n.logger.Debug("SUCCESSFULLY REFRESHED INDEX KEY: %s Refresh Count: %d", k, n.refreshCount)
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
				n.logger.Debug("Skipping refresh for Key: %s as it is not near expiry.", rec.Key)
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
				n.logger.Error("An error occurred whilst attempting to refresh entry with key: %s the error was: %v", k, entryRefreshErr)
			}
			n.logger.Debug("SUCCESSFULLY REFRESHED ENTRY KEY: %s Refresh Count: %d", k, n.refreshCount)
		}
	}
}

var errSyncIndexPublisherSnapshotMiss = errors.New("sync index publisher missing from fetched index snapshot")
var errSyncIndexRecordNotFound = errors.New("sync index record not found")

func containsPublisher(entries []RecordIndexEntry, publisherID types.NodeID) bool {
	for _, e := range entries {
		if e.Publisher == publisherID {
			return true
		}
	}
	return false
}

func isPublisherInvariantStoreErr(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "at least one of the entries being appended must be associated with the publisher id provided")
}

func (n *Node) storeSyncedIndexWithRetry(indexKey string, publisherID types.NodeID, ttl time.Duration) ([]RecordIndexEntry, error) {
	backoffs := []time.Duration{
		0,
		100 * time.Millisecond,
		250 * time.Millisecond,
		500 * time.Millisecond,
	}

	var lastErr error
	for attempt, wait := range backoffs {
		if wait > 0 {
			time.Sleep(wait)
		}

		//if we have encounter a  transient FindIndex error we want to retry to allow sufficient time for the publisher's snapshot to be 
		// available on this node, however if the error persists then this could indicates the publisher id is not associated with 
		// any of the entries in the index record and in that case we want to fail the operation as only nodes that are associated 
		// with the index record as publishers should be attempting to store synced entries for that record.
		entries, found := n.FindIndex(indexKey)
		if !found {
			lastErr = fmt.Errorf("%w: key=%s attempt=%d/%d", errSyncIndexRecordNotFound, indexKey, attempt+1, len(backoffs))
			continue
		}
		if !containsPublisher(entries, publisherID) {
			lastErr = fmt.Errorf("%w: key=%s publisher=%s attempt=%d/%d", errSyncIndexPublisherSnapshotMiss, indexKey, publisherID.String(), attempt+1, len(backoffs))
			continue
		}

		//NOTE: We set isIndexSyncRelated to true to inidicate that this store operation is being performed as part of
		//the processing of a SyncIndexRequest, this will ensure that we do not trigger another round of syncs
		//calls to other nodes/publisher associated with this index which would result in an infinite loop of sync calls across
		//the network.
		storeErr := n.StoreIndexWithTTL(indexKey, entries, ttl, publisherID, false, true)
		if storeErr == nil {
			return entries, nil
		}

		if isPublisherInvariantStoreErr(storeErr) {
			lastErr = storeErr
			continue
		}

		return entries, storeErr
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("%w: key=%s publisher=%s", errSyncIndexPublisherSnapshotMiss, indexKey, publisherID.String())
	}
	return nil, lastErr
}

// -----------------------------------------------------------------------------
// Static (pure) helper/utility functions.
// -----------------------------------------------------------------------------
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

func XOR(a, b types.NodeID) (o types.NodeID) {
	for i := 0; i < types.IDBytes; i++ {
		o[i] = a[i] ^ b[i]
	}
	return
}
func CompareDistance(a, b, t types.NodeID) int {
	da := XOR(a, t)
	db := XOR(b, t)
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
