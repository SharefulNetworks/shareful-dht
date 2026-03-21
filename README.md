# Shareful DHT

A lightweight, kademlia-spec compliant, distributed hashtable. It serves as a core component of the Shareful platform & ergo includes some shareful-specific features which fall outside the scope of the Kademlia paper however, these are purely additive; thus the library may be used as-is in any project requiring a drop-in Kademlia DHT implementation.

## Features

- TCP transport with connection pooling, backpressure on outbound queue, and idle-connection reaping.
- Periodic, panic-safe, bucket refresh, record refresh, and janitor tasks to ensure the internal routing table remains fresh AND adapts to network churn.
- Integrated support for the exchange of arbitrary comms messages between peers.
- Indexing support (record + index entries) allows for the sharing of a single key 
between multiple peers, each entry will be tied to the lifecycle of the peer that 
created it and automatically go out of scope (i.e be deleted) on exit/disconnection of
the associated peer.
- Simple event hooks for message/index dispatch, allows third-party application to be
notified on receipt of Index entry update/deletions as well as incomming message receipt.
- Protobuf envelope and payloads, for easier, on-going protocol/schema evolution and extensibility.


## Prerequisites
- Go 1.25+


## Quick Start
1) Installation

```bash
go get github.com/SharefulNetworks/shareful-dht@latest
```

2)Import, Instantiate and Initialise the Shareful DHT Node

```go
package main

import (
	"time"

	"github.com/SharefulNetworks/shareful-dht/config"
	"github.com/SharefulNetworks/shareful-dht/dht"
	"github.com/SharefulNetworks/shareful-dht/net"
)

func main() {
	cfg := config.GetDefaultSingletonInstance()
	cfg.UseProtobuf = true
	cfg.RequestTimeout = 1000 * time.Millisecond

	node, err := dht.NewNode("node1", ":9301", net.NewTCP(), cfg, dht.NT_CORE)
	if err != nil {
		panic(err)
	}
	defer node.Shutdown()

	// supply one or more known peers; pass an empty slice if this is the first node
	if err := node.Bootstrap([]string{":9302"}, 0); err != nil {
		panic(err)
	}
}
```

See [cmd/demo/main.go](cmd/demo/main.go) for a fuller example including storing and discovering records.


## Configuration Highlights
Key knobs are in `config/Config.go` (defaults via `GetDefaultSingletonInstance()`):
- `K`, `Alpha`, `ReplicationFactor`
- `RefreshInterval`, `JanitorInterval`, `BucketRefreshInterval`
- `OutboundQueueWorkerCount`, `PooledConnectionIdleTimeout`, `PooledConnectionIdleCheckInterval`
- `RequestTimeout`, `BatchRequestTimeout`
- `GlobalLogLevel`


## Development
Great!! You'd like to get involved with active development of the Shareful DHT library, the details below will assist you in this endeavour. To contribute back up stream to the mainline build, please open an issue first, additionally you may reach out directly via the contact details below.

### Project Layout
- `cmd/demo` — simple runnable demo node.
- `dht` — core node logic, ops, encode/decode, background tasks.
- `routing` — K-buckets, routing table, bucket refresher.
- `net` — TCP transport, outbound queue, connection pooling.
- `proto` — protobuf definitions and generated code.
- `dht_tests` — end-to-end tests.

### Protocol Extension
- DHT protocol messages may be added/updated by editing the plain text **./proto/dhtpb/dht.proto** file. You will then need to **re-generate** go protobuf source file by running the following:
```bash
protoc --go_out=. --go_opt=paths=source_relative proto/dhtpb/dht.proto
```
**Note:** failure to do this will result in your protocol changes taking no effect.

- To ensure your update hasn't introduced any breaking changes it is advisable to undertake a post update, regression test:
```bash
go test ./...
```
**Note:** This may some time as the project has an extensive e2e test suite:

## Author
Giles Thompson
giles@shareful.net

## License
GPL-2


