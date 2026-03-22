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

func (w *WAL) Read() ([]Entry, error) {
	opBytes := make([]byte, 1)
	lenOfKeyBytes := make([]byte, 4)
	lenOfValueBytes := make([]byte, 4)
	if _, err := w.file.Read(opBytes); err != nil {
		return nil, fmt.Errorf("No data stored in file")
	}
	op := OpType(opBytes[0])
	var opErr error = nil
	var entries []Entry
	for opErr == nil {
		w.file.Read(lenOfKeyBytes)
		lenOfKey := binary.LittleEndian.Uint32(lenOfKeyBytes)

		keyBytes := make([]byte, lenOfKey)
		w.file.Read(keyBytes)

		w.file.Read(lenOfValueBytes)
		lenOfValue := binary.LittleEndian.Uint32(lenOfValueBytes)

		valueBytes := make([]byte, lenOfValue)
		w.file.Read(valueBytes)
		entry := Entry{
			op,
			keyBytes,
			valueBytes,
		}
		entries = append(entries, entry)

		_, opErr = w.file.Read(opBytes)
		op = OpType(opBytes[0])
	}

	return entries, nil
}
