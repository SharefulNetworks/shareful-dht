package config

import (
	"time"
)

type Config struct {
	K                                         int
	Alpha                                     int
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
	IndexRecordSyncDelay                      time.Duration //the duration of time to wait after a successful store index operation before dispatching sync index requests to other peers. This is useful to allow sufficient time for the store operation to propagate and for the publisher's peer list to be updated with any new peers that may have been added as part of the store operation before dispatching sync requests to ensure that applicable peers are included in the sync process.
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
			IndexRecordSyncDelay:                      2 * time.Second,
		}
	}
	return singletonConfig
}
