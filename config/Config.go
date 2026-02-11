package config

import (
	"time"
)

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
	MaxLocalEntryRefreshCount      int           //the max local stabndard entry refreshes (i.e refresh to known nodes) that the node can undertake before a network-wide refresh is required which may result in new k-nearest candidates.
	MaxLocalIndexEntryRefreshCount int           //the max local index entry refreshes (i.e refresh to known nodes) that the node can undertake before a network-wide refresh is required which may result in new k-nearest candidates.
	BootstrapConnectDelayMillis    int           //the delay, in milliseconds, that should elapse before the node attempts to connect to each bootstrap node. The delay is useful where all or multiple nodes in the list are being started in tandem as it allows sufficient time for each respective node to spin up and drop into a ready state.
	PooledConnectionIdleTimeout    time.Duration //the duration after which idle connections is removed from the transport internal connection pool and duly closed. This is useful to prevent resource exhaustion in scenarios where the network is large and/or highly dynamic resulting in a large number of potential peer connections over time.
	PooledConnectionIdleCheckInterval time.Duration //the interval at which the transport should check for idle connections in the internal connection pool. This is useful to prevent resource exhaustion in scenarios where the network is large and/or highly dynamic resulting in a large number of potential peer connections over time.
}

var singletonConfig *Config

func GetDefaultSingletonInstance() *Config {
	if singletonConfig == nil {
		singletonConfig = &Config{
			K:                              20,
			Alpha:                          3,
			DefaultEntryTTL:                10 * time.Minute,
			DefaultIndexEntryTTL:           10 * time.Minute,
			RefreshInterval:                2 * time.Minute,
			JanitorInterval:                time.Minute,
			UseProtobuf:                    true,
			RequestTimeout:                 1500 * time.Millisecond,
			BatchRequestTimeout:            4500 * time.Millisecond,
			OutboundQueueWorkerCount:       4,
			ReplicationFactor:              3,
			EnableAuthorativeStorage:       false,
			MaxLocalEntryRefreshCount:      50,
			MaxLocalIndexEntryRefreshCount: 50,
			BootstrapConnectDelayMillis:    20000,
			PooledConnectionIdleTimeout:    3 * time.Minute,
			PooledConnectionIdleCheckInterval: 1 * time.Minute}
	}
	return singletonConfig
}
