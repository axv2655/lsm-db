package main

import (
	"log"

	database "github.com/axv2655/lsm-db/internal"
	"github.com/axv2655/lsm-db/internal/server"
)

func main() {
	cfg, err := database.LoadConfig("config.json")
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	log.Printf("Booting DB with SkipList Max Level: %d", cfg.SkipListMaxLevel)

	db, err := database.Open(cfg)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}

	log.Println("gRPC server listening on :50051")
	if err := server.Serve(db, ":50051"); err != nil {
		log.Fatalf("server: %v", err)
	}
}
