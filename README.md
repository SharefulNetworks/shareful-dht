# minidht (real request/response, protobuf-first)

Generate protobuf:
  protoc --go_out=. --go_opt=paths=source_relative proto/dht.proto

Run demo:
  go run ./cmd/demo

Run tests:
  go test ./...
