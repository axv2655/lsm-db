.PHONY: proto build test

proto:
	mkdir -p pb
	cd protos && PATH="$$HOME/go/bin:$$PATH" protoc \
		--go_out=../pb --go_opt=paths=source_relative \
		--go-grpc_out=../pb --go-grpc_opt=paths=source_relative \
		lsm_service.proto

build:
	go build -o bin/lsm-db ./main.go

test:
	go test ./...
