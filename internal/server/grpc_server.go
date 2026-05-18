package server

import (
	"context"
	"net"

	database "github.com/axv2655/lsm-db/internal"
	pb "github.com/axv2655/lsm-db/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// LSMServer implements pb.LSMServiceServer.
type LSMServer struct {
	pb.UnimplementedLSMServiceServer
	db *database.DB
}

// NewLSMServer creates a server that delegates to db.
func NewLSMServer(db *database.DB) *LSMServer {
	return &LSMServer{db: db}
}

func (s *LSMServer) Set(ctx context.Context, req *pb.SetRequest) (*pb.SetResponse, error) {
	if req.ProtoClass == "" {
		return nil, status.Error(codes.InvalidArgument, "proto_class is required")
	}
	if !s.db.Registry.HasMessage(req.ProtoClass) {
		return nil, status.Errorf(codes.InvalidArgument, "unknown proto_class: %s", req.ProtoClass)
	}
	encoded, err := s.db.Registry.Encode(req.ProtoClass, req.Value)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "encode: %v", err)
	}
	if err := s.db.Put([]byte(req.Key), encoded, []byte(req.ProtoClass)); err != nil {
		return nil, status.Errorf(codes.Internal, "put: %v", err)
	}
	return &pb.SetResponse{}, nil
}

func (s *LSMServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if req.ProtoClass == "" {
		return nil, status.Error(codes.InvalidArgument, "proto_class is required")
	}
	raw, err := s.db.Get([]byte(req.Key), []byte(req.ProtoClass))
	if err != nil {
		// Both "no store for proto class" and "key not found" mean the key is absent.
		return &pb.GetResponse{Found: false}, nil
	}
	jsonVal, err := s.db.Registry.Decode(req.ProtoClass, raw)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "decode: %v", err)
	}
	return &pb.GetResponse{Value: jsonVal, Found: true}, nil
}

func (s *LSMServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if req.ProtoClass == "" {
		return nil, status.Error(codes.InvalidArgument, "proto_class is required")
	}
	if err := s.db.Delete([]byte(req.Key), []byte(req.ProtoClass)); err != nil {
		return nil, status.Errorf(codes.Internal, "delete: %v", err)
	}
	return &pb.DeleteResponse{}, nil
}

func (s *LSMServer) Close(ctx context.Context, req *pb.CloseRequest) (*pb.CloseResponse, error) {
	if err := s.db.Close(); err != nil {
		return nil, status.Errorf(codes.Internal, "close: %v", err)
	}
	return &pb.CloseResponse{}, nil
}

// Serve starts a gRPC server on addr (e.g. ":50051") and blocks until it exits.
func Serve(db *database.DB, addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	srv := grpc.NewServer()
	pb.RegisterLSMServiceServer(srv, NewLSMServer(db))
	return srv.Serve(lis)
}
