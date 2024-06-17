package tests

import (
	"io/ioutil"
	"lazydb/pkg/kvstore"
	"log"
	"os"
	"testing"
)

func TestCache(t *testing.T) {
	wal, err := ioutil.TempFile("", "test_kv")
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(wal.Name())

	store := kvstore.NewKVstore(wal.Name(), 5)
	defer store.Close()

	store.Set("key2", "value2")
	value, exists := store.Get("key2")
	if !exists || value != "value2" {
		t.Fatalf("excepted value2, but get: %s", value)
	}

	store.Delete("key2")
	_, exists = store.Get("key2")
	if exists {
		t.Fatalf("Expected key2 to be deleted")
	}
}
