# Shareful DHT

A lightweight, kademlia-spec compliant, distributed hashtable. It serves as a core component of the Shareful platform & ergo includes some shareful-specific features which fall outside the scope of the Kademlia paper however, these are purely additive; thus the library may be used as-is in any project requiring a drop-in Kademlia DHT implementation.

## Features

- TCP transport with connection pooling, backpressure on outbound queue, and idle-connection reaping.
- Periodic, panic-safr, bucket refresh, record refresh, and janitor tasks to ensure the internal routing table remains fresh AND adapts to network churn.
- Integrated support for the exchange of arbitrary comms messages between peers.
- Indexing support (record + index entries) allows for the sharing of a single key 
between multiple peers, each entry will be tied to the lifecycle of the peer that 
created it and automatically go out of scope (i.e be deleted) on exit/disconnection of
the associated peer.
- Simple event hooks for message/index dispatch, allows third-party application to be
notified on receipt of Index entry update/deletions as well as incomming message receipt.
- Protobuf envelope and payloads, for easier, on-going protocol/schema evolution.


## Prerequisites
- Go 1.25+
- `protoc` for regenerating protobufs.

## Quick Start
1) Install dependencies (Go modules):

```bash
go mod download
```

2) Generate protobufs

```bash
protoc --go_out=. --go_opt=paths=source_relative proto/dht.proto
```

3) Run the demo node:

```bash
go run ./cmd/demo
```

4) Run tests:

```bash
go test ./...
```

## Configuration Highlights
Key knobs are in `config/Config.go` (defaults via `GetDefaultSingletonInstance()`):
- `K`, `Alpha`, `ReplicationFactor`
- `RefreshInterval`, `JanitorInterval`, `BucketRefreshInterval`
- `OutboundQueueWorkerCount`, `PooledConnectionIdleTimeout`, `PooledConnectionIdleCheckInterval`
- `RequestTimeout`, `BatchRequestTimeout`
- `GlobalLogLevel`

## Project Layout
- `cmd/demo` — simple runnable demo node.
- `dht` — core node logic, ops, encode/decode, background tasks.
- `routing` — K-buckets, routing table, bucket refresher.
- `net` — TCP transport, outbound queue, connection pooling.
- `proto` — protobuf definitions and generated code.
- `dht_tests` — end-to-end tests.

## Notes
- Panic recovery wrappers are provided by `shareful-utils-unpanicked` to keep background loops alive and log stack traces.
- Logging uses `shareful-utils-slog`; set `GlobalLogLevel` to control verbosity.
# minidht (real request/response, protobuf-first)

Generate protobuf:
  protoc --go_out=. --go_opt=paths=source_relative proto/dht.proto

Run demo:
  go run ./cmd/demo

Run tests:
  go test ./...
