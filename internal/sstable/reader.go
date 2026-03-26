package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

type SSTable struct {
	file  *os.File
	index []IndexEntry
}

func Open(filepath string) (*SSTable, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("error opening sstable file %s: %w", filepath, err)
	}

	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	footerPos := stat.Size() - 16

	buf := make([]byte, 16)
	_, err = file.ReadAt(buf, footerPos)
	if err != nil {
		return nil, err
	}

	indexOffset := binary.LittleEndian.Uint64(buf[0:8])
	lenEntry := binary.LittleEndian.Uint64(buf[8:16])
	_, err = file.Seek(int64(indexOffset), io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("error seeking to index of sstable file %s: %w", filepath, err)
	}
	offset := uint64(0)

	entries := make([]IndexEntry, lenEntry)
	for i := 0; i < int(lenEntry); i++ {
		lenBuf := make([]byte, 4)
		_, err = file.ReadAt(lenBuf, int64(indexOffset)+int64(offset))
		offset += 4
		keyLen := binary.LittleEndian.Uint32(lenBuf)

		keyBuf := make([]byte, keyLen)
		_, err = file.ReadAt(keyBuf, int64(indexOffset)+int64(offset))
		offset += uint64(keyLen)

		offsetBuf := make([]byte, 8)
		_, err = file.ReadAt(offsetBuf, int64(indexOffset)+int64(offset))
		offset += 8
		entryOffset := binary.LittleEndian.Uint64(offsetBuf)

		entries[i] = IndexEntry{key: keyBuf, offSet: int64(entryOffset)}
	}
	return &SSTable{
		file:  file,
		index: entries,
	}, nil
}

func (s *SSTable) Close() error {
	if err := s.file.Close(); err != nil {
		return fmt.Errorf("failed to close sstable: %w", err)
	}
	return nil
}

func (s *SSTable) getIndex(key []byte) (*IndexEntry, error) {
	if bytes.Compare(key, s.index[0].key) < 0 || bytes.Compare(key, s.index[len(s.index)-1].key) > 0 {
		return nil, fmt.Errorf("key %s is out of bounds of the sstable index", key)
	}
	left := 0
	right := len(s.index) - 1
	for left <= right {
		mid := (left + right) / 2
		if bytes.Compare(key, s.index[mid].key) < 0 {
			right = mid - 1
			continue
		}
		if bytes.Compare(key, s.index[mid].key) > 0 {
			left = mid + 1
			continue
		}
		if bytes.Equal(key, s.index[mid].key) {
			return &s.index[mid], nil
		}
	}

	return nil, fmt.Errorf("key %s was not found in the sstable index", key)
}

func (s *SSTable) Get(key []byte) ([]byte, error) {
	if len(s.index) == 0 {
		return nil, fmt.Errorf("sstable index is empty")
	}
	entry, err := s.getIndex(key)
	if err != nil {
		return nil, fmt.Errorf("error getting data from SSTable index: %w", err)
	}

	_, err = s.file.Seek(entry.offSet, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("error seeking to offset %d in SSTable: %w", entry.offSet, err)
	}

	lengthBuf := make([]byte, 4)
	if _, err := io.ReadFull(s.file, lengthBuf); err != nil {
		return nil, fmt.Errorf("error reading key length: %w", err)
	}
	keyLen := binary.LittleEndian.Uint32(lengthBuf)

	_, err = s.file.Seek(int64(keyLen), io.SeekCurrent)
	if err != nil {
		return nil, fmt.Errorf("error skipping key bytes: %w", err)
	}

	if _, err := io.ReadFull(s.file, lengthBuf); err != nil {
		return nil, fmt.Errorf("error reading value length: %w", err)
	}
	valLen := binary.LittleEndian.Uint32(lengthBuf)

	valueBytes := make([]byte, valLen)
	if _, err := io.ReadFull(s.file, valueBytes); err != nil {
		return nil, fmt.Errorf("error reading value bytes: %w", err)
	}

	if len(valueBytes) == 0 {
		return nil, fmt.Errorf("key %s was deleted (tombstone)", key)
	}

	return valueBytes, nil
}
