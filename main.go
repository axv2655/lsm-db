package main

import (
	"fmt"

	database "github.com/axv2655/lsm-db/internal"
)

func main() {
	cfg, err := database.LoadConfig("config.json")
	if err != nil {
		panic(err)
	}

	fmt.Printf("Booting DB with SkipList Max Level: %d\n", cfg.SkipListMaxLevel)

	db, err := database.Open(cfg)
	if err != nil {
		panic(err)
	}

	_ = db
}
