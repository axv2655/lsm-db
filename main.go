package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/axv2655/lsm-db/internal/memtable"
)

// Config maps exactly to your config.json file
type Config struct {
	SkipListP        float32 `json:"skip_list_p"`
	SkipListMaxLevel int     `json:"skip_list_max_level"`
	MemtableMaxBytes int     `json:"memtable_max_size_bytes"`
	WalPath          string  `json:"wal_path"`
}

func LoadConfig(filepath string) (*Config, error) {
	// 1. Read the raw bytes from the file
	fileBytes, err := os.ReadFile(filepath)
	if err != nil {
		return nil, fmt.Errorf("could not read config file: %w", err)
	}

	// 2. Create an empty Config struct
	var cfg Config

	// 3. Unmarshal (Decode) the JSON bytes into the struct
	err = json.Unmarshal(fileBytes, &cfg)
	if err != nil {
		return nil, fmt.Errorf("could not parse JSON: %w", err)
	}

	return &cfg, nil
}

func main() {
	// 1. Load the settings
	cfg, err := LoadConfig("config.json")
	if err != nil {
		panic(err)
	}

	fmt.Printf("Booting DB with SkipList Max Level: %d\n", cfg.SkipListMaxLevel)

	mem, err := memtable.Open(
		cfg.WalPath,
		cfg.MemtableMaxBytes,
		cfg.SkipListP,
		cfg.SkipListMaxLevel,
	)
	if err != nil {
		panic(err)
	}

	// Your DB is now fully configurable via JSON!
}
