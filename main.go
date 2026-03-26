package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	database "github.com/axv2655/lsm-db/internal"
)

type SetRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type KeyRequest struct {
	Key string `json:"key"`
}

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

	http.HandleFunc("/api/set", func(w http.ResponseWriter, r *http.Request) {
		var req SetRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := db.Put([]byte(req.Key), []byte(req.Value)); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	http.HandleFunc("/api/get", func(w http.ResponseWriter, r *http.Request) {
		var req KeyRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		val, err := db.Get([]byte(req.Key))
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"value": string(val)})
	})

	http.HandleFunc("/api/delete", func(w http.ResponseWriter, r *http.Request) {
		var req KeyRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := db.Delete([]byte(req.Key)); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	http.HandleFunc("/api/close", func(w http.ResponseWriter, r *http.Request) {
		if err := db.Close(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "closed"})
		go func() { os.Exit(0) }()
	})

	fmt.Println("Server listening on http://localhost:4069")
	if err := http.ListenAndServe(":4069", nil); err != nil {
		panic(err)
	}
}
