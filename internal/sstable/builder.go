package sstable

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"

	"github.com/axv2655/lsm-db/internal/memtable"
)

type IndexEntry struct {
	key    []byte
	offSet int64
}

type sstable struct {
	file  *os.File
	index []IndexEntry
}

type Builder struct {
	file          *os.File
	index         []IndexEntry
	currentOffset int64

	mu sync.Mutex
}

func (s *sstable) NewBuilder(filepath string) (*Builder, error) {
	file, err := os.OpenFile(filepath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("file string %s ran into error when opening file: %w", filepath, err)
	}
	return &Builder{
		file,
		[]IndexEntry{},
		0,
		sync.Mutex{},
	}, nil
}

func (b *Builder) AddEntry(KV []memtable.KVEntry) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, v := range KV {
		payload := formatEntry(v.Key, v.Value)
		b.currentOffset += int64(4 + len(v.Key) + 4 + len(v.Value))
		if _, err := b.file.Write(payload); err != nil {
			return fmt.Errorf("writing to sstable has errored: %w", err)
		}
	}

	return nil
}

func formatEntry(key []byte, value []byte) []byte {
	// bytes for each input, 1 for the opCode (0/1), 4 for len of the key in decimal then key, 4 for len of the value in decimal then key
	totalBytes := 4 + len(key) + 4 + len(value)
	// make soemthing with the number of bytes needed for the entry
	payload := make([]byte, totalBytes)
	offset := 0
	binary.LittleEndian.PutUint32(payload[offset:], uint32(len(key)))
	offset += 4

	// copy the entry's key to make a new value and not just use existing address
	copy(payload[offset:], key)
	offset += len(key)
	binary.LittleEndian.PutUint32(payload[offset:], uint32(len(value)))
	offset += 4

	// copy the entry's value to make a new value and not just use existing address
	copy(payload[offset:], value)
	return payload
}
