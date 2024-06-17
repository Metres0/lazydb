package kvstore

import (
	"bufio"
	"log"
	"os"
	"strings"
)

func (store *KVStore) LoadFromWAL(walFilePath string) {
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
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}
