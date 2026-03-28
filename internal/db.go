package database

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/axv2655/lsm-db/internal/memtable"
	"github.com/axv2655/lsm-db/internal/schema"
	"github.com/axv2655/lsm-db/internal/sstable"
)

type Config struct {
	SkipListP        float32 `json:"skip_list_p"`
	SkipListMaxLevel int     `json:"skip_list_max_level"`
	MemtableMaxBytes int     `json:"memtable_max_size_bytes"`
	WalDir           string  `json:"wal_dir"`
	SSTDir           string  `json:"sst_dir"`
	ProtoDir         string  `json:"proto_dir"`
}

func LoadConfig(filepath string) (*Config, error) {
	fileBytes, err := os.ReadFile(filepath)
	if err != nil {
		return nil, fmt.Errorf("could not read config file: %w", err)
	}
	var cfg Config
	if err := json.Unmarshal(fileBytes, &cfg); err != nil {
		return nil, fmt.Errorf("could not parse JSON: %w", err)
	}
	return &cfg, nil
}

type ClassStore struct {
	protoClass string
	memtable   *memtable.Memtable
	sstables   []*sstable.SSTable
}

func (cs *ClassStore) walPath(cfg *Config) string {
	return filepath.Join(cfg.WalDir, cs.protoClass+".wal")
}

func (cs *ClassStore) sstDir(cfg *Config) string {
	return filepath.Join(cfg.SSTDir, cs.protoClass)
}

func (cs *ClassStore) sstPathForSeq(cfg *Config, seq uint64) string {
	return fmt.Sprintf("%s/%06d.sst", cs.sstDir(cfg), seq)
}

func (cs *ClassStore) walCopyPathForSeq(cfg *Config, seq uint64) string {
	return fmt.Sprintf("%s/%06d.wal", cs.sstDir(cfg), seq)
}

func (cs *ClassStore) flush(cfg *Config, nextSeqFunc func() uint64) error {
	entries := cs.memtable.GetAll()
	seq := nextSeqFunc()
	path := cs.sstPathForSeq(cfg, seq)
	builder, err := sstable.NewBuilder(path)
	if err != nil {
		return fmt.Errorf("could not create sstable builder: %w", err)
	}
	if err := builder.AddEntry(entries); err != nil {
		return fmt.Errorf("could not add entries to sstable: %w", err)
	}
	if err := builder.Finish(); err != nil {
		return fmt.Errorf("could not finish sstable: %w", err)
	}
	sst, err := sstable.Open(path)
	if err != nil {
		return fmt.Errorf("could not open flushed sstable: %w", err)
	}
	cs.sstables = append(cs.sstables, sst)

	walCopyPath := cs.walCopyPathForSeq(cfg, seq)
	if err := cs.memtable.ClearWAL(walCopyPath); err != nil {
		return fmt.Errorf("could not clear wal: %w", err)
	}
	return nil
}

func (cs *ClassStore) put(key, value, protoClass []byte, cfg *Config, nextSeqFunc func() uint64) error {
	size := len(key) + len(value) + len(protoClass)
	if size > cfg.MemtableMaxBytes {
		return fmt.Errorf("entry size %d exceeds memtable max size %d", size, cfg.MemtableMaxBytes)
	}
	if cs.memtable.IsFull(size) {
		if err := cs.flush(cfg, nextSeqFunc); err != nil {
			return fmt.Errorf("could not flush memtable to SSTable: %w", err)
		}
		if err := cs.memtable.Close(); err != nil {
			return fmt.Errorf("could not close old memtable: %w", err)
		}
		mem, err := memtable.Open(cs.walPath(cfg), cfg.MemtableMaxBytes, cfg.SkipListP, cfg.SkipListMaxLevel)
		if err != nil {
			return fmt.Errorf("could not create new memtable: %w", err)
		}
		cs.memtable = mem
	}
	return cs.memtable.Put(key, value, protoClass)
}

func (cs *ClassStore) get(key []byte) ([]byte, error) {
	val, _, err := cs.memtable.Get(key)
	if err == nil {
		return val, nil
	}
	for i := len(cs.sstables) - 1; i >= 0; i-- {
		val, err = cs.sstables[i].Get(key)
		if err == nil {
			return val, nil
		}
	}
	return nil, fmt.Errorf("key not found")
}

func (cs *ClassStore) delete(key []byte, cfg *Config, nextSeqFunc func() uint64) error {
	size := len(key)
	if cs.memtable.IsFull(size) {
		if err := cs.flush(cfg, nextSeqFunc); err != nil {
			return fmt.Errorf("could not flush memtable to SSTable: %w", err)
		}
		if err := cs.memtable.Close(); err != nil {
			return fmt.Errorf("could not close old memtable: %w", err)
		}
		mem, err := memtable.Open(cs.walPath(cfg), cfg.MemtableMaxBytes, cfg.SkipListP, cfg.SkipListMaxLevel)
		if err != nil {
			return fmt.Errorf("could not create new memtable: %w", err)
		}
		cs.memtable = mem
	}
	return cs.memtable.Delete(key)
}

func (cs *ClassStore) close() error {
	if err := cs.memtable.Close(); err != nil {
		return fmt.Errorf("could not close memtable for %s: %w", cs.protoClass, err)
	}
	for _, sst := range cs.sstables {
		if err := sst.Close(); err != nil {
			return fmt.Errorf("could not close sstable for %s: %w", cs.protoClass, err)
		}
	}
	return nil
}

type DB struct {
	stores   map[string]*ClassStore
	storesMu sync.RWMutex
	config   *Config
	Registry *schema.Registry
	nextSeq  uint64
}

func openClassStore(protoClass string, cfg *Config) (*ClassStore, uint64, error) {
	cs := &ClassStore{protoClass: protoClass}

	sstDir := cs.sstDir(cfg)
	if err := os.MkdirAll(sstDir, 0o755); err != nil {
		return nil, 0, fmt.Errorf("could not create sst directory for %s: %w", protoClass, err)
	}

	mem, err := memtable.Open(cs.walPath(cfg), cfg.MemtableMaxBytes, cfg.SkipListP, cfg.SkipListMaxLevel)
	if err != nil {
		return nil, 0, fmt.Errorf("could not open memtable for %s: %w", protoClass, err)
	}
	cs.memtable = mem

	entries, err := os.ReadDir(sstDir)
	if err != nil {
		return nil, 0, fmt.Errorf("could not read sst directory for %s: %w", protoClass, err)
	}

	var maxSeq uint64
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".sst") {
			continue
		}
		seqStr := strings.TrimSuffix(name, ".sst")
		seq, err := strconv.ParseUint(seqStr, 10, 64)
		if err != nil {
			continue
		}
		path := filepath.Join(sstDir, name)
		sst, err := sstable.Open(path)
		if err != nil {
			return nil, 0, fmt.Errorf("could not open sstable %s: %w", path, err)
		}
		cs.sstables = append(cs.sstables, sst)
		if seq > maxSeq {
			maxSeq = seq
		}
	}

	return cs, maxSeq, nil
}

func Open(cfg *Config) (*DB, error) {
	if err := os.MkdirAll(cfg.WalDir, 0o755); err != nil {
		return nil, fmt.Errorf("could not create wal directory: %w", err)
	}
	if err := os.MkdirAll(cfg.SSTDir, 0o755); err != nil {
		return nil, fmt.Errorf("could not create sst directory: %w", err)
	}
	if err := os.MkdirAll(cfg.ProtoDir, 0o755); err != nil {
		return nil, fmt.Errorf("could not create proto directory: %w", err)
	}

	reg, err := schema.NewRegistry(cfg.ProtoDir)
	if err != nil {
		return nil, fmt.Errorf("could not load proto schemas: %w", err)
	}

	// Discover existing class stores from WAL files and SSTable subdirectories
	classNames := make(map[string]bool)

	walEntries, err := os.ReadDir(cfg.WalDir)
	if err != nil {
		return nil, fmt.Errorf("could not read wal directory: %w", err)
	}
	for _, entry := range walEntries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".wal") {
			name := strings.TrimSuffix(entry.Name(), ".wal")
			classNames[name] = true
		}
	}

	sstEntries, err := os.ReadDir(cfg.SSTDir)
	if err != nil {
		return nil, fmt.Errorf("could not read sst directory: %w", err)
	}
	for _, entry := range sstEntries {
		if entry.IsDir() {
			classNames[entry.Name()] = true
		}
	}

	stores := make(map[string]*ClassStore)
	var maxSeq uint64
	for name := range classNames {
		cs, seq, err := openClassStore(name, cfg)
		if err != nil {
			return nil, fmt.Errorf("could not open class store %s: %w", name, err)
		}
		stores[name] = cs
		if seq > maxSeq {
			maxSeq = seq
		}
	}

	return &DB{
		stores:   stores,
		config:   cfg,
		Registry: reg,
		nextSeq:  maxSeq,
	}, nil
}

func (db *DB) nextSeqNum() uint64 {
	return atomic.AddUint64(&db.nextSeq, 1)
}

func (db *DB) getOrCreateStore(protoClass string) (*ClassStore, error) {
	db.storesMu.RLock()
	cs, ok := db.stores[protoClass]
	db.storesMu.RUnlock()
	if ok {
		return cs, nil
	}

	db.storesMu.Lock()
	defer db.storesMu.Unlock()
	if cs, ok = db.stores[protoClass]; ok {
		return cs, nil
	}
	cs, _, err := openClassStore(protoClass, db.config)
	if err != nil {
		return nil, err
	}
	db.stores[protoClass] = cs
	return cs, nil
}

func (db *DB) Put(key, value, protoClass []byte) error {
	cs, err := db.getOrCreateStore(string(protoClass))
	if err != nil {
		return err
	}
	return cs.put(key, value, protoClass, db.config, db.nextSeqNum)
}

func (db *DB) Get(key []byte, protoClass []byte) ([]byte, error) {
	db.storesMu.RLock()
	cs, ok := db.stores[string(protoClass)]
	db.storesMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("no store for proto class %s", protoClass)
	}
	return cs.get(key)
}

func (db *DB) Delete(key []byte, protoClass []byte) error {
	cs, err := db.getOrCreateStore(string(protoClass))
	if err != nil {
		return err
	}
	return cs.delete(key, db.config, db.nextSeqNum)
}

func (db *DB) Close() error {
	db.storesMu.RLock()
	defer db.storesMu.RUnlock()
	for _, cs := range db.stores {
		if err := cs.close(); err != nil {
			return err
		}
	}
	return nil
}
