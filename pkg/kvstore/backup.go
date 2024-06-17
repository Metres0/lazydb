package kvstore

import (
	"bufio"
	"log"
	"os"
	"strings"
	"time"
)

func (store *KVStore) LoadFromBackup(backupFilePath string) {
	backupFile, err := os.Open(backupFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		log.Fatal(err)
	}
	defer backupFile.Close()

	scanner := bufio.NewScanner(backupFile)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ":", 2)
		if len(parts) == 2 {
			key, value := parts[0], parts[1]
			store.data[key] = value
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

func (store *KVStore) Backup() {
	store.dataMutex.RLock()
	defer store.dataMutex.RUnlock()

	backupFile, err := os.Create(store.backupFilePath)
	if err != nil {
		log.Fatal(err)
	}
	defer backupFile.Close()

	writer := bufio.NewWriter(backupFile)
	for key, value := range store.data {
		_, err := writer.WriteString(key + ":" + value + "\n")
		if err != nil {
			log.Fatal(err)
		}
	}
	writer.Flush()
}

func (store *KVStore) PeriodicBackup(interval time.Duration) {
	for {
		time.Sleep(interval)
		store.Backup()
	}
}
