package tests

import (
	"io/ioutil"
	"lazydb/pkg/kvstore"
	"log"
	"os"
	"testing"
)

func TestKVStore(t *testing.T) {
	wal, err := ioutil.TempFile("", "test_kv")
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(wal.Name())

	store := kvstore.NewKVstore(wal.Name())
	defer store.Close()

	store.Set("key1", "value1")
	value, exists := store.Get("key1")
	if !exists || value != "value1" {
		t.Fatalf("excepted value1, but get: %s", value)
	}

	store.Delete("key1")
	_, exists = store.Get("key1")
	if exists {
		t.Fatalf("Expected key1 to be deleted")
	}
}
