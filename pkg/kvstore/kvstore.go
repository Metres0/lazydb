package kvstore

import (
	"bufio"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type KVStore struct {
	data           map[string]string
	dataMutex      sync.RWMutex
	walFile        *os.File
	cache          *LRUCache
	backupFilePath string
}

func NewKVstore(walFilePath string, backupFilePath string, cacheCapacity int) *KVStore {
	walFile, err := os.OpenFile("walFilePath", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	store := &KVStore{
		data:           make(map[string]string),
		walFile:        walFile,
		cache:          NewLRUCache(cacheCapacity),
		backupFilePath: backupFilePath,
	}
	store.LoadFromWAl(walFilePath)
	store.LoadFromBackup(backupFilePath) // 加载备份数据

	go store.PeriodicBackup(time.Hour * 1) // 每小时备份一次
	return store
}

func (store *KVStore) LoadFromWAl(walFilePath string) {
	walFile, err := os.Open(walFilePath)
	if err != nil {
		log.Fatal(err)
	}
	defer walFile.Close()
	scanner := bufio.NewScanner(walFile)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ":", 2)
		if len(parts) == 2 {
			key, value := parts[0], parts[1]
			if value == "" {
				delete(store.data, key)
			} else {
				store.data[key] = value
			}
		}
	}
	err = scanner.Err()
	if err != nil {
		log.Fatal(err)
	}
}

func (store *KVStore) Set(key, value string) {
	store.dataMutex.Lock()
	defer store.dataMutex.Unlock()
	_, err := store.walFile.WriteString(key + ":" + value + "\n")
	if err != nil {
		log.Fatal(err)
	}
	store.data[key] = value
	store.cache.Put(key, value)
}
func (store *KVStore) Get(key string) (string, bool) {
	if value, found := store.cache.Get(key); found {
		return value, true
	}
	store.dataMutex.Lock()
	defer store.dataMutex.Unlock()
	value, exists := store.data[key]
	if exists {
		store.cache.Put(key, value)
	}
	return value, exists
}
func (store *KVStore) Delete(key string) {
	store.dataMutex.Lock()
	defer store.dataMutex.Unlock()
	_, err := store.walFile.WriteString(key + "\n")
	if err != nil {
		log.Fatal(err)
	}
	delete(store.data, key)
	store.cache.Remove(key)
}
func (store *KVStore) Close() {
	store.walFile.Close()
}

func (store *KVStore) HandleGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Key parameter is missing", http.StatusBadRequest)
		return
	}

	value, exists := store.Get(key)
	if !exists {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}

	json.NewEncoder(w).Encode(map[string]string{"key": key, "value": value})
}
func (store *KVStore) HandleSet(w http.ResponseWriter, r *http.Request) {
	var payload map[string]string
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	key, keyExists := payload["key"]
	value, valueExists := payload["value"]
	if !keyExists || !valueExists {
		http.Error(w, "Missing key or value in request payload", http.StatusBadRequest)
		return
	}

	store.Set(key, value)
	json.NewEncoder(w).Encode(map[string]string{"key": key, "value": value})
}

// HandleDelete handles HTTP DELETE requests to remove a key-value pair.
func (store *KVStore) HandleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Key parameter is missing", http.StatusBadRequest)
		return
	}

	store.Delete(key)
	json.NewEncoder(w).Encode(map[string]string{"key": key})
}
