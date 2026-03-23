package config

import (
	"time"

	"github.com/SharefulNetworks/shareful-utils-slog/slog"
)

type Config struct {
	K                                         int //the max number of closest nodes to return in response to find node or find value requests, this is useful to control the size of the routing table and to ensure that responses to find operations are manageable in size and do not overwhelm the network or the requesting node with too much information.
	Alpha                                     int //max simultaneous outbound requests that can be made when performing network operations such as find node or find value, this is useful to prevent overwhelming the network or the node itself with too many concurrent requests which may lead to resource exhaustion or degraded performance.
	DefaultEntryTTL                           time.Duration
	DefaultIndexEntryTTL                      time.Duration
	AllowPermanentDefault                     bool
	RefreshInterval                           time.Duration
	JanitorInterval                           time.Duration
	UseProtobuf                               bool
	RequestTimeout                            time.Duration
	BatchRequestTimeout                       time.Duration
	OutboundQueueWorkerCount                  int
	ReplicationFactor                         int
	EnableAuthorativeStorage                  bool
	MaxLocalEntryRefreshCount                 int           //the max local stabndard entry refreshes (i.e refresh to known nodes) that the node can undertake before a network-wide refresh is required which may result in new k-nearest candidates.
	MaxLocalIndexEntryRefreshCount            int           //the max local index entry refreshes (i.e refresh to known nodes) that the node can undertake before a network-wide refresh is required which may result in new k-nearest candidates.
	BootstrapConnectDelayMillis               int           //the delay, in milliseconds, that should elapse before the node attempts to connect to each bootstrap node. The delay is useful where all or multiple nodes in the list are being started in tandem as it allows sufficient time for each respective node to spin up and drop into a ready state.
	PooledConnectionIdleTimeout               time.Duration //the duration after which idle connections is removed from the transport internal connection pool and duly closed. This is useful to prevent resource exhaustion in scenarios where the network is large and/or highly dynamic resulting in a large number of potential peer connections over time.
	PooledConnectionIdleCheckInterval         time.Duration //the interval at which the transport should check for idle connections in the internal connection pool. This is useful to prevent resource exhaustion in scenarios where the network is large and/or highly dynamic resulting in a large number of potential peer connections over time.
	PostBootstrapNeighboorhoodResolutionDelay time.Duration //the duration of time to wait, post the call to the nodes bootstrap method, before attempting to auto discover neighbouring peers.
	BucketRefreshInterval                     time.Duration //the duration of time to wait before refreshing buckets in the routing table. This is useful to ensure that the routing table is kept up to date with the current state of the network and to prevent stale entries from accumulating in the routing table over time.
	BucketRefreshBatchSize                    int           //the number of bucket refresh jobs to process in a single batch. This is useful to prevent overwhelming the network or the node itself with a large number of find operations in scenarios where many buckets require refreshing at the same time.
	BucketRefreshBatchDelayInterval           time.Duration //the duration of time to wait between processing batches of bucket refresh jobs. This is useful to prevent overwhelming the network or the node itself with a large number of find operations in scenarios where many buckets require refreshing at the same time.
	IndexSyncDelay                            time.Duration //the duration of time to wait after a successful store index operation before dispatching sync index requests to other peers. This is useful to allow sufficient time for the store operation to propagate and for the publisher's peer list to be updated with any new peers that may have been added as part of the store operation before dispatching sync requests to ensure that applicable peers are included in the sync process.
	IndexUpdateEventsEnabled                  bool          //determines whether IndexEntryUpdateEvents are enabled globally, where this is set to FALSE the value defined in the IndexEntry is ignored.
	MaxNodeConnectionFailureThreshold         int           //the maximum number of consecutive connection failures to a given peer before the peer is considered unreaachable and an event id published to signal its removal from the routing table.
	UnhealthyPeerGracePeriod                  time.Duration //the duration of time to wait after a peer is deemed unhealthy before actually removing the peer from the routing table. This is useful to allow for transient network issues or temporary peer unavailability without prematurely removing peers from the routing table.
	NodeAddress                               string        //The globally reachable address of this node, where this is not supplied by a command line argument or environment variable it will be set to the value provided to the Node's constuctor.
	MessageVersion                            string        //the version of the DHT protocol to use in message headers, this is useful to allow for future protocol updates and to maintain backward compatibility with older versions of the protocol.
	GlobalLogLevel                            slog.LogLevel //the global log level to apply to all loggers in the DHT, this is useful to control the verbosity of logging output across the entire DHT from a single configuration setting.
}

var singletonConfig *Config

func GetDefaultSingletonInstance() *Config {
	if singletonConfig == nil {
		singletonConfig = &Config{
			K:                                 20,
			Alpha:                             3,
			DefaultEntryTTL:                   10 * time.Minute,
			DefaultIndexEntryTTL:              10 * time.Minute,
			RefreshInterval:                   2 * time.Minute,
			JanitorInterval:                   time.Minute,
			UseProtobuf:                       true,
			RequestTimeout:                    1500 * time.Millisecond,
			BatchRequestTimeout:               4500 * time.Millisecond,
			OutboundQueueWorkerCount:          4,
			ReplicationFactor:                 3,
			EnableAuthorativeStorage:          false,
			MaxLocalEntryRefreshCount:         50,
			MaxLocalIndexEntryRefreshCount:    50,
			BootstrapConnectDelayMillis:       20000,
			PooledConnectionIdleTimeout:       3 * time.Minute,
			PooledConnectionIdleCheckInterval: 1 * time.Minute,
			PostBootstrapNeighboorhoodResolutionDelay: 30 * time.Second,
			BucketRefreshInterval:                     15 * time.Minute,
			BucketRefreshBatchSize:                    10,
			BucketRefreshBatchDelayInterval:           10 * time.Second,
			IndexSyncDelay:                            2 * time.Second,
			IndexUpdateEventsEnabled:                  true,
			MaxNodeConnectionFailureThreshold:         5,
			UnhealthyPeerGracePeriod:                  10 * time.Second,
			NodeAddress:                               "",
			MessageVersion:                            "1.0",
			GlobalLogLevel:                            slog.INFO,
		}

	}
	validateConfig()
	return singletonConfig
}

func Reset() {
	singletonConfig = nil
}

func validateConfig() {

	//santisie settings to ensure they aew within acceptable bounds and do not conflict with
	// one another which may lead to unintended consequences such as network instability or resource exhaustion.
	if singletonConfig.RequestTimeout >= singletonConfig.BatchRequestTimeout {
		panic("RequestTimeout must be less than BatchRequestTimeout")
	}

	if singletonConfig.K <= 0 {
		panic("K must be greater than 0")
	}

	if singletonConfig.Alpha < 1 {
		panic("Alpha must be greater than or equal to 1")
	}

	if singletonConfig.ReplicationFactor < 1 {
		panic("ReplicationFactor must be greater than or equal to 1")
	}

	if singletonConfig.MaxNodeConnectionFailureThreshold < 1 {
		panic("MaxNodeConnectionFailureThreshold must be greater than or equal to 1")
	}

	if !singletonConfig.UseProtobuf {
		panic("Protobuf must now be used.")
	}

}
