package database

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/axv2655/lsm-db/internal/memtable"
	"github.com/axv2655/lsm-db/internal/schema"
	"github.com/axv2655/lsm-db/internal/sstable"
)

type Config struct {
	SkipListP        float32 `json:"skip_list_p"`
	SkipListMaxLevel int     `json:"skip_list_max_level"`
	MemtableMaxBytes int     `json:"memtable_max_size_bytes"`
	WalPath          string  `json:"wal_path"`
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

type DB struct {
	memtable *memtable.Memtable
	sstables []*sstable.SSTable
	config   *Config
	Registry *schema.Registry
	nextSeq  uint64
}

func Open(cfg *Config) (*DB, error) {
	mem, err := memtable.Open(cfg.WalPath, cfg.MemtableMaxBytes, cfg.SkipListP, cfg.SkipListMaxLevel)
	if err != nil {
		return nil, fmt.Errorf("could not open memtable: %w", err)
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

	entries, err := os.ReadDir(cfg.SSTDir)
	if err != nil {
		return nil, fmt.Errorf("could not read sst directory: %w", err)
	}

	var tables []*sstable.SSTable
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
		path := fmt.Sprintf("%s/%s", cfg.SSTDir, name)
		sst, err := sstable.Open(path)
		if err != nil {
			return nil, fmt.Errorf("could not open sstable %s: %w", path, err)
		}
		tables = append(tables, sst)
		if seq > maxSeq {
			maxSeq = seq
		}
	}

	return &DB{
		memtable: mem,
		sstables: tables,
		config:   cfg,
		Registry: reg,
		nextSeq:  maxSeq,
	}, nil
}

func (db *DB) nextSeqNum() uint64 {
	return atomic.AddUint64(&db.nextSeq, 1)
}

func (db *DB) sstPathForSeq(seq uint64) string {
	return fmt.Sprintf("%s/%06d.sst", db.config.SSTDir, seq)
}

func (db *DB) walCopyPathForSeq(seq uint64) string {
	return fmt.Sprintf("%s/%06d.wal", db.config.SSTDir, seq)
}

func (db *DB) Flush() error {
	entries := db.memtable.GetAll()
	seq := db.nextSeqNum()
	path := db.sstPathForSeq(seq)
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
	db.sstables = append(db.sstables, sst)

	walCopyPath := db.walCopyPathForSeq(seq)
	if err := db.memtable.ClearWAL(walCopyPath); err != nil {
		return fmt.Errorf("could not clear wal: %w", err)
	}
	return nil
}

func (db *DB) Put(key, value, protoClass []byte) error {
	size := len(key) + len(value) + len(protoClass)
	if size > db.config.MemtableMaxBytes {
		return fmt.Errorf("entry size %d exceeds memtable max size %d", size, db.config.MemtableMaxBytes)
	}
	if db.memtable.IsFull(size) {
		if err := db.Flush(); err != nil {
			return fmt.Errorf("could not flush memtable to SSTable: %w", err)
		}
		if err := db.memtable.Close(); err != nil {
			return fmt.Errorf("could not close old memtable: %w", err)
		}
		mem, err := memtable.Open(db.config.WalPath, db.config.MemtableMaxBytes, db.config.SkipListP, db.config.SkipListMaxLevel)
		if err != nil {
			return fmt.Errorf("could not create new memtable: %w", err)
		}
		db.memtable = mem
	}
	return db.memtable.Put(key, value, protoClass)
}

func (db *DB) Get(key []byte) (value []byte, protoClass []byte, err error) {
	val, class, err := db.memtable.Get(key)
	if err == nil {
		return val, class, nil
	}
	for i := len(db.sstables) - 1; i >= 0; i-- {
		val, err = db.sstables[i].Get(key)
		if err == nil {
			return val, nil, nil
		}
	}
	return nil, nil, fmt.Errorf("key not found")
}

func (db *DB) Close() error {
	if err := db.memtable.Close(); err != nil {
		return fmt.Errorf("could not close memtable: %w", err)
	}
	for _, sst := range db.sstables {
		if err := sst.Close(); err != nil {
			return fmt.Errorf("could not close sstable: %w", err)
		}
	}
	return nil
}

func (db *DB) Delete(key []byte) error {
	size := len(key)
	if db.memtable.IsFull(size) {
		if err := db.Flush(); err != nil {
			return fmt.Errorf("could not flush memtable to SSTable: %w", err)
		}
		if err := db.memtable.Close(); err != nil {
			return fmt.Errorf("could not close old memtable: %w", err)
		}
		mem, err := memtable.Open(db.config.WalPath, db.config.MemtableMaxBytes, db.config.SkipListP, db.config.SkipListMaxLevel)
		if err != nil {
			return fmt.Errorf("could not create new memtable: %w", err)
		}
		db.memtable = mem
	}
	return db.memtable.Delete(key)
}
