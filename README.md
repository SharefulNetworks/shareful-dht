# Shareful DHT

Minimal Kademlia-inspired DHT with protobuf-first messaging, TCP transport, pooled outbound connections, and panic-safe background workers.

## Features
- Protobuf envelope and payloads (default), with JSON fallback.
- TCP transport with connection pooling, backpressure on outbound queue, and idle-connection reaping.
- Periodic bucket refresh, record refresh, and janitor tasks guarded by `unpanicked.RunSafe`.
- Indexing support (record + index entries) and simple event hooks for message/index updates.

## Prerequisites
- Go 1.25+
- `protoc` for regenerating protobufs.

## Quick Start
1) Install dependencies (Go modules):

```bash
go mod download
```

2) Generate protobufs (only if you change `proto/dht.proto`):

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
