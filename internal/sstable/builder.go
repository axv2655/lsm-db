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

type Builder struct {
	file          *os.File
	index         []IndexEntry
	currentOffset int64

	mu sync.Mutex
}

func NewBuilder(filepath string) (*Builder, error) {
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
		b.index = append(b.index, IndexEntry{key: v.Key, offSet: b.currentOffset})
		payload := formatEntry(v.Key, v.Value)
		b.currentOffset += int64(4 + len(v.Key) + 4 + len(v.Value))
		if _, err := b.file.Write(payload); err != nil {
			return fmt.Errorf("writing to sstable has errored: %w", err)
		}
	}

	return nil
}

func formatEntry(key []byte, value []byte) []byte {
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

func (b *Builder) Finish() error {
	indexOffset := b.currentOffset
	for _, v := range b.index {
		offset := 4 + len(v.key) + 8 // 4 for size of key, len of key, 8 for 64bit int for offset in the acutal ss table
		entryBytes := make([]byte, offset)

		binary.LittleEndian.PutUint32(entryBytes[0:], uint32(len(v.key)))
		copy(entryBytes[4:], v.key)

		binary.LittleEndian.PutUint64(entryBytes[(offset-8):], uint64((v.offSet)))
		if _, err := b.file.Write(entryBytes); err != nil {
			return fmt.Errorf("writing to sstable has errored: %w", err)
		}
		b.currentOffset += int64(offset)
	}
	footer := make([]byte, 8)
	binary.LittleEndian.PutUint64(footer, uint64(indexOffset))

	if _, err := b.file.Write(footer); err != nil {
		return fmt.Errorf("failed to write footer: %w", err)
	}
	lenOfEntry := make([]byte, 8)
	binary.LittleEndian.PutUint64(lenOfEntry, uint64(len(b.index)))

	if _, err := b.file.Write(lenOfEntry); err != nil {
		return fmt.Errorf("failed to write footer: %w", err)
	}
	if err := b.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}
	if err := b.file.Close(); err != nil {
		return fmt.Errorf("failed to close file: %w", err)
	}

	return nil
}
