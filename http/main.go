package main

import (
	bitcaskgo "bitcask-go"
	"encoding/json"
	"log"
	"net/http"
	"os"
)

var db *bitcaskgo.DB

func init() {
	opts := bitcaskgo.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-http")
	opts.DirPath = dir

	var err error
	db, err = bitcaskgo.OpenDB(opts)
	if err != nil {
		panic(err)
	}

}

func handlePut(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var data map[string]string
	if err := json.NewDecoder(request.Body).Decode(&data); err != nil {
		http.Error(writer, "method not allowed", http.StatusBadRequest)
		return
	}

	for key, value := range data {
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
			return
		}
	}
}

func handleGet(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := request.URL.Query().Get("key")

	value, err := db.Get([]byte(key))
	if err != nil && err != bitcaskgo.ErrKeyNotFound {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		log.Printf("failed to delete value in db: %v]n", err)

		return
	}

	writer.Header().Set("Content-Type", "appocatopm/json")
	_ = json.NewEncoder(writer).Encode(string(value))
}

func handleDelete(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodDelete {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := request.URL.Query().Get("key")
	err := db.Delete([]byte(key))

	if err != nil && err != bitcaskgo.ErrKeyNotFound {
		http.Error(writer, err.Error(), http.StatusMethodNotAllowed)
		log.Printf("failed to delete value in db: %v\n", err)
		return
	}

	writer.Header().Set("Content-Type", "appocatopm/json")
	_ = json.NewEncoder(writer).Encode("OK")

}

func handleListKeys(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	keys := db.ListKeys()
	writer.Header().Set("Content-Type", "appocatopm/json")
	var result []string
	for _, key := range keys {
		result = append(result, string(key))
	}
	_ = json.NewEncoder(writer).Encode(result)
}

func handleStat(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	stat := db.Stat()
	writer.Header().Set("Content-Type", "appocatopm/json")
	_ = json.NewEncoder(writer).Encode(stat)

}

func main() {
	http.HandleFunc("/bitcask/put", handlePut)
	http.HandleFunc("/bitcask/get", handleGet)
	http.HandleFunc("/bitcask/delete", handleDelete)
	http.HandleFunc("/bitcask/listkeys", handleListKeys)
	http.HandleFunc("/bitcask/stat", handleStat)

	_ = http.ListenAndServe("localhost:8080", nil)
}
