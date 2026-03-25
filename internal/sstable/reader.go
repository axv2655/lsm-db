package sstable

import (
	"bytes"
	"fmt"
	"io"
	"os"
)

type SSTable struct {
	file  *os.File
	index []IndexEntry
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
	entry, err := s.getIndex(key)
	if err != nil {
		return nil, fmt.Errorf("Error getting data from SSTable: %w", err)
	}

	_, err = s.file.Seek(entry.offSet, io.SeekStart)
	if err != nil {
		fmt.Errorf("Error seeking to offset %d in SSTable: %w", entry.offSet, err)
	}
	valueLenBytes := make([]byte, 4)
	valueLen, err := s.file.Read(valueLenBytes)
	if err != nil && err != io.EOF {
		fmt.Errorf("Error reading value length from SSTable: %w", err)
	}

	valueBytes := make([]byte, valueLen)
	_, err = s.file.Read(valueBytes)
	if err != nil && err != io.EOF {
		fmt.Errorf("Error reading key length from SSTable: %w", err)
	}
	if len(valueBytes) != 0 {
		return valueBytes, nil
	}
	return nil, fmt.Errorf("Key %s was not found in SSTable", key)
}
