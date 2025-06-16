module github.com/Cryptovate-India/websocket-service

go 1.23.0

toolchain go1.24.4

require (
	github.com/gorilla/websocket v1.5.1
	github.com/stretchr/testify v1.10.0
	google.golang.org/grpc v1.69.4
	google.golang.org/protobuf v1.36.3
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	go.opentelemetry.io/otel v1.34.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.34.0 // indirect
	golang.org/x/net v0.34.0 // indirect
	golang.org/x/sys v0.29.0 // indirect
	golang.org/x/text v0.21.0 // indirect
	golang.org/x/time v0.12.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250115164207-1a7da9e5054f // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/Cryptovate-India/server-utils => ../../pkg/server-utils
