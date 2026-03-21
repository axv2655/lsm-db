package wal

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

type OpType byte

const (
	OpPut    OpType = 1
	OpDelete OpType = 0
)

type Entry struct {
	Op    OpType
	Key   []byte
	Value []byte
}

type WAL struct {
	file *os.File
	mu   sync.Mutex
}

func Open(filepath string) (*WAL, error) {
	file, err := os.OpenFile(filepath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("File string %s ran into error when opening file: %w", filepath, err)
	}
	return &WAL{
		file: file,
	}, nil
}

func (w *WAL) Append(entry Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// bytes for each input, 1 for the opCode (0/1), 4 for len of the key in decimal then key, 4 for len of the value in decimal then key
	total_bytes := 1 + 4 + len(entry.Key) + 4 + len(entry.Value)
	// make soemthing with the number of bytes needed for the entry
	payload := make([]byte, total_bytes)
	offset := 0
	payload[offset] = byte(entry.Op) // adds the operation to the payload, convert int to byte format
	offset += 1
	binary.LittleEndian.PutUint32(payload[offset:], uint32(len(entry.Key)))
	offset += 4

	// copy the entry's key to make a new value and not just use existing address
	copy(payload[offset:], entry.Key)
	offset += len(entry.Key)
	binary.LittleEndian.PutUint32(payload[offset:], uint32(len(entry.Value)))
	offset += 4

	// copy the entry's value to make a new value and not just use existing address
	copy(payload[offset:], entry.Value)

	// now payload has all the entry and we can write it to the file
	if _, err := w.file.Write(payload); err != nil {
		return fmt.Errorf("Writing to file has errored: %w", err)
	}

	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("Syncing file has errored: %w", err)
	}
	return nil
}

func (w *WAL) Read() error {
	offset := 0
	while
	data := make([]byte, 5)
	count, err := w.file.Read(data)

	return nil
}
