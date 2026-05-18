package server_test

import (
	"context"
	"net"
	"path/filepath"
	"runtime"
	"testing"

	database "github.com/axv2655/lsm-db/internal"
	"github.com/axv2655/lsm-db/internal/server"
	pb "github.com/axv2655/lsm-db/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// protoDir returns the absolute path to the repo-root protos/ directory,
// regardless of which directory go test sets as cwd.
func protoDir() string {
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "protos")
}

const bufSize = 1 << 20 // 1 MiB

func setupTestServer(t *testing.T) (pb.LSMServiceClient, func()) {
	t.Helper()

	cfg := &database.Config{
		SkipListP:        0.5,
		SkipListMaxLevel: 4,
		MemtableMaxBytes: 1 << 20,
		WalDir:           t.TempDir(),
		SSTDir:           t.TempDir(),
		ProtoDir:         protoDir(),
	}
	db, err := database.Open(cfg)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	lis := bufconn.Listen(bufSize)
	grpcSrv := grpc.NewServer()
	pb.RegisterLSMServiceServer(grpcSrv, server.NewLSMServer(db))
	go grpcSrv.Serve(lis) //nolint:errcheck

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	client := pb.NewLSMServiceClient(conn)
	cleanup := func() {
		conn.Close()
		grpcSrv.Stop()
		db.Close()
	}
	return client, cleanup
}

func TestSet_Get_RoundTrip(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	_, err := client.Set(context.Background(), &pb.SetRequest{
		Key:        "u1",
		Value:      []byte(`{"name":"Alice","age":30,"email":"alice@example.com"}`),
		ProtoClass: "User",
	})
	if err != nil {
		t.Fatalf("Set: %v", err)
	}

	resp, err := client.Get(context.Background(), &pb.GetRequest{
		Key:        "u1",
		ProtoClass: "User",
	})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !resp.Found {
		t.Fatal("expected Found=true")
	}
	if len(resp.Value) == 0 {
		t.Fatal("expected non-empty value")
	}
}

func TestGet_MissingKey(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	resp, err := client.Get(context.Background(), &pb.GetRequest{
		Key:        "does-not-exist",
		ProtoClass: "User",
	})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if resp.Found {
		t.Fatal("expected Found=false for missing key")
	}
}

func TestDelete_RemovesKey(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	_, err := client.Set(context.Background(), &pb.SetRequest{
		Key:        "u2",
		Value:      []byte(`{"name":"Bob","age":25,"email":"bob@example.com"}`),
		ProtoClass: "User",
	})
	if err != nil {
		t.Fatalf("Set: %v", err)
	}

	_, err = client.Delete(context.Background(), &pb.DeleteRequest{
		Key:        "u2",
		ProtoClass: "User",
	})
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}

	resp, err := client.Get(context.Background(), &pb.GetRequest{
		Key:        "u2",
		ProtoClass: "User",
	})
	if err != nil {
		t.Fatalf("Get after Delete: %v", err)
	}
	if resp.Found {
		t.Fatal("expected Found=false after delete")
	}
}

func TestSet_MissingProtoClass_ReturnsError(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	_, err := client.Set(context.Background(), &pb.SetRequest{
		Key:   "u3",
		Value: []byte(`{"name":"Carol"}`),
	})
	if err == nil {
		t.Fatal("expected error when proto_class is empty")
	}
}

func TestClose(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	_, err := client.Close(context.Background(), &pb.CloseRequest{})
	if err != nil {
		t.Fatalf("Close: %v", err)
	}
}
