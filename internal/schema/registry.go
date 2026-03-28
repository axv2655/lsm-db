package schema

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/bufbuild/protocompile"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

type Registry struct {
	messages map[string]protoreflect.MessageDescriptor
}

func NewRegistry(protoDir string) (*Registry, error) {
	r := &Registry{
		messages: make(map[string]protoreflect.MessageDescriptor),
	}
	if err := r.loadDir(protoDir); err != nil {
		return nil, fmt.Errorf("failed to load proto dir %s: %w", protoDir, err)
	}
	return r, nil
}

func (r *Registry) loadDir(protoDir string) error {
	entries, err := os.ReadDir(protoDir)
	if err != nil {
		return fmt.Errorf("failed to read proto directory: %w", err)
	}

	var protoFiles []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".proto") {
			protoFiles = append(protoFiles, entry.Name())
		}
	}

	if len(protoFiles) == 0 {
		return nil
	}

	compiler := protocompile.Compiler{
		Resolver: &protocompile.SourceResolver{
			ImportPaths: []string{protoDir},
			Accessor: func(path string) (io.ReadCloser, error) {
				fullPath := filepath.Join(protoDir, path)
				return os.Open(fullPath)
			},
		},
		SourceInfoMode: protocompile.SourceInfoStandard,
	}

	compiled, err := compiler.Compile(context.Background(), protoFiles...)
	if err != nil {
		return fmt.Errorf("failed to compile proto files: %w", err)
	}

	for _, file := range compiled {
		msgs := file.Messages()
		for i := 0; i < msgs.Len(); i++ {
			msg := msgs.Get(i)
			name := string(msg.Name())
			r.messages[name] = msg
		}
	}

	return nil
}

func (r *Registry) HasMessage(protoClass string) bool {
	_, ok := r.messages[protoClass]
	return ok
}

// Encode validates JSON input against the proto schema and returns protobuf bytes.
func (r *Registry) Encode(protoClass string, jsonValue []byte) ([]byte, error) {
	desc, ok := r.messages[protoClass]
	if !ok {
		return nil, fmt.Errorf("unknown proto class: %s", protoClass)
	}

	msg := dynamicpb.NewMessage(desc)
	if err := protojson.Unmarshal(jsonValue, msg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON to proto %s: %w", protoClass, err)
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal proto %s: %w", protoClass, err)
	}
	return data, nil
}

// Decode converts protobuf bytes back to JSON.
func (r *Registry) Decode(protoClass string, data []byte) ([]byte, error) {
	desc, ok := r.messages[protoClass]
	if !ok {
		return nil, fmt.Errorf("unknown proto class: %s", protoClass)
	}

	msg := dynamicpb.NewMessage(desc)
	if err := proto.Unmarshal(data, msg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal proto %s: %w", protoClass, err)
	}

	jsonBytes, err := protojson.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal proto %s to JSON: %w", protoClass, err)
	}
	return jsonBytes, nil
}
