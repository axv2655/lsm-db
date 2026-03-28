package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

type OpType byte

const (
	OpPut    OpType = 1
	OpDelete OpType = 0
)

type Entry struct {
	Op         OpType
	Key        []byte
	Value      []byte
	ProtoClass []byte
}

type WAL struct {
	file *os.File
	mu   sync.Mutex
}

func Open(filepath string) (*WAL, error) {
	file, err := os.OpenFile(filepath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("file string %s ran into error when opening file: %w", filepath, err)
	}
	return &WAL{
		file: file,
		mu:   sync.Mutex{},
	}, nil
}

func (w *WAL) Append(entry Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// format: [1B op][4B keyLen][key][4B valueLen][value][4B classLen][class]
	totalBytes := 1 + 4 + len(entry.Key) + 4 + len(entry.Value) + 4 + len(entry.ProtoClass)
	payload := make([]byte, totalBytes)
	offset := 0
	payload[offset] = byte(entry.Op)
	offset += 1
	binary.LittleEndian.PutUint32(payload[offset:], uint32(len(entry.Key)))
	offset += 4

	copy(payload[offset:], entry.Key)
	offset += len(entry.Key)
	binary.LittleEndian.PutUint32(payload[offset:], uint32(len(entry.Value)))
	offset += 4

	copy(payload[offset:], entry.Value)
	offset += len(entry.Value)
	binary.LittleEndian.PutUint32(payload[offset:], uint32(len(entry.ProtoClass)))
	offset += 4

	copy(payload[offset:], entry.ProtoClass)

	// now payload has all the entry and we can write it to the file
	if _, err := w.file.Write(payload); err != nil {
		return fmt.Errorf("writing to file has errored: %w", err)
	}

	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("syncing file has errored: %w", err)
	}
	return nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync wal: %w", err)
	}
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("failed to close wal: %w", err)
	}
	return nil
}

func (w *WAL) Clear(copyPath string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// seek to start of WAL to copy all contents
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek wal for copy: %w", err)
	}

	dst, err := os.Create(copyPath)
	if err != nil {
		return fmt.Errorf("failed to create wal copy at %s: %w", copyPath, err)
	}
	defer dst.Close()

	if _, err := io.Copy(dst, w.file); err != nil {
		return fmt.Errorf("failed to copy wal to %s: %w", copyPath, err)
	}
	if err := dst.Sync(); err != nil {
		return fmt.Errorf("failed to sync wal copy: %w", err)
	}

	// truncate and reset the WAL file
	if err := w.file.Truncate(0); err != nil {
		return fmt.Errorf("failed to truncate wal: %w", err)
	}
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek wal after truncate: %w", err)
	}

	return nil
}

func (w *WAL) ReadFile() ([]Entry, error) {
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek wal to start: %w", err)
	}

	opBytes := make([]byte, 1)
	lenBuf := make([]byte, 4)

	var entries []Entry
	for {
		if _, err := w.file.Read(opBytes); err != nil {
			break
		}
		op := OpType(opBytes[0])

		if _, err := io.ReadFull(w.file, lenBuf); err != nil {
			return nil, fmt.Errorf("error reading wal file for len of key: %w", err)
		}
		keyBytes := make([]byte, binary.LittleEndian.Uint32(lenBuf))
		if _, err := io.ReadFull(w.file, keyBytes); err != nil {
			return nil, fmt.Errorf("error reading wal file for key: %w", err)
		}

		if _, err := io.ReadFull(w.file, lenBuf); err != nil {
			return nil, fmt.Errorf("error reading wal file for len of value: %w", err)
		}
		valueBytes := make([]byte, binary.LittleEndian.Uint32(lenBuf))
		if _, err := io.ReadFull(w.file, valueBytes); err != nil {
			return nil, fmt.Errorf("error reading wal file for value: %w", err)
		}

		if _, err := io.ReadFull(w.file, lenBuf); err != nil {
			return nil, fmt.Errorf("error reading wal file for len of proto class: %w", err)
		}
		classBytes := make([]byte, binary.LittleEndian.Uint32(lenBuf))
		if _, err := io.ReadFull(w.file, classBytes); err != nil {
			return nil, fmt.Errorf("error reading wal file for proto class: %w", err)
		}

		entry := Entry{
			Op:         op,
			Key:        keyBytes,
			Value:      valueBytes,
			ProtoClass: classBytes,
		}
		entries = append(entries, entry)
	}

	return entries, nil
}
