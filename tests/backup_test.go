package tests

import (
	"lazydb/pkg/kvstore"
	"os"
	"testing"
)

func TestBackupAndRestore(t *testing.T) {

	// 创建一个临时的 WAL 文件和备份文件
	walFilePath := "test_wal.log"
	backupFilePath := "test_backup.txt"

	// 清理上一次的测试文件
	defer os.Remove(walFilePath)
	defer os.Remove(backupFilePath)

	// 初始化 KVStore
	store := kvstore.NewKVstore(walFilePath, backupFilePath, 5)

	// 设置一些键值对
	store.Set("key1", "value1")
	store.Set("key2", "value2")
	store.Set("key3", "value3")

	// 创建备份
	store.Backup()

	// 关闭当前的 KVStore
	store.Close()

	// 在这里重新创建 WAL 文件，以便新实例可以正确读取
	os.Create(walFilePath)

	// 创建一个新的 KVStore 实例，加载备份文件
	newStore := kvstore.NewKVstore(walFilePath, backupFilePath, 2)

	// 检查是否正确恢复了数据
	value, exists := newStore.Get("key1")
	if !exists || value != "value1" {
		t.Fatalf("Expected to find key1 with value 'value1', got %v", value)
	}

	value, exists = newStore.Get("key2")
	if !exists || value != "value2" {
		t.Fatalf("Expected to find key2 with value 'value2', got %v", value)
	}

	value, exists = newStore.Get("key3")
	if !exists || value != "value3" {
		t.Fatalf("Expected to find key3 with value 'value3', got %v", value)
	}

	// 关闭新实例
	newStore.Close()

	// 清理测试文件
	os.Remove(walFilePath)
	os.Remove(backupFilePath)
}
