package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	database "github.com/axv2655/lsm-db/internal"
)

type SetRequest struct {
	Key        string          `json:"key"`
	Value      json.RawMessage `json:"value"`
	ProtoClass string          `json:"protobufClass"`
}

type KeyRequest struct {
	Key        string `json:"key"`
	ProtoClass string `json:"protobufClass"`
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
		if req.ProtoClass == "" {
			http.Error(w, "protobufClass is required", http.StatusBadRequest)
			return
		}
		if !db.Registry.HasMessage(req.ProtoClass) {
			http.Error(w, fmt.Sprintf("unknown protobufClass: %s", req.ProtoClass), http.StatusBadRequest)
			return
		}

		encoded, err := db.Registry.Encode(req.ProtoClass, req.Value)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if err := db.Put([]byte(req.Key), encoded, []byte(req.ProtoClass)); err != nil {
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
		if req.ProtoClass == "" {
			http.Error(w, "protobufClass is required", http.StatusBadRequest)
			return
		}

		val, err := db.Get([]byte(req.Key), []byte(req.ProtoClass))
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}

		if db.Registry.HasMessage(req.ProtoClass) {
			decoded, err := db.Registry.Decode(req.ProtoClass, val)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(fmt.Sprintf(`{"protobufClass":%q,"value":%s}`, req.ProtoClass, string(decoded))))
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
		if req.ProtoClass == "" {
			http.Error(w, "protobufClass is required", http.StatusBadRequest)
			return
		}
		if err := db.Delete([]byte(req.Key), []byte(req.ProtoClass)); err != nil {
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
