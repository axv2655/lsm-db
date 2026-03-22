package wal

import (
	"bytes"
	"path/filepath"
	"testing"
)

func TestWAL_CrashRecovery(t *testing.T) {
	// Create a temporary directory so the test cleans up after itself
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")

	// 1. Initial Start: Open the WAL
	w, err := Open(walPath)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}

	// Prepare a slice of test entries
	entries := []Entry{
		{Op: OpPut, Key: []byte("user:1"), Value: []byte("alice")},
		{Op: OpPut, Key: []byte("user:2"), Value: []byte("bob")},
		{Op: OpDelete, Key: []byte("user:1"), Value: nil}, // Deletes usually have nil/empty values
		{Op: OpPut, Key: []byte("user:3"), Value: []byte("charlie")},
	}

	// 2. Normal Operation: Append entries
	for _, e := range entries {
		if err := w.Append(e); err != nil {
			t.Fatalf("Failed to append entry: %v", err)
		}
	}

	// 3. Simulate Crash: Close the file to flush and sever the connection
	if err := w.file.Close(); err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// 4. System Restart: Reopen the WAL for a fresh load
	w2, err := Open(walPath)
	if err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}
	// Ensure this second handle gets closed when the test ends
	defer w2.file.Close()

	// 5. Recovery: Read back the entries from the new file handle
	readEntries, err := w2.ReadFile()
	if err != nil {
		t.Fatalf("Failed to read WAL: %v", err)
	}

	// 6. Verification: Check Length
	if len(readEntries) != len(entries) {
		t.Fatalf("Expected %d entries, got %d", len(entries), len(readEntries))
	}

	// 7. Verification: Check Contents
	for i, expected := range entries {
		actual := readEntries[i]

		if expected.Op != actual.Op {
			t.Errorf("Entry %d: expected Op %d, got %d", i, expected.Op, actual.Op)
		}
		if !bytes.Equal(expected.Key, actual.Key) {
			t.Errorf("Entry %d: expected Key %s, got %s", i, expected.Key, actual.Key)
		}
		if !bytes.Equal(expected.Value, actual.Value) {
			t.Errorf("Entry %d: expected Value %s, got %s", i, expected.Value, actual.Value)
		}
	}
}
