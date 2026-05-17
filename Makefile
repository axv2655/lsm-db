.PHONY: proto build test

proto:
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		protos/lsm_service.proto

build:
	go build -o bin/lsm-db ./main.go

test:
	go test ./...
