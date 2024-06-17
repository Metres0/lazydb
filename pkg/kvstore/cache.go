package kvstore

import (
	"container/list"
	"sync"
)

type CacheEntry struct {
	key   string
	value string
}

type LRUCache struct {
	capacity int
	list     *list.List
	cache    map[string]*list.Element
	mutex    sync.RWMutex
}

func NewLRUCache(n int) *LRUCache {
	return &LRUCache{
		capacity: n,
		list:     list.New(),
		cache:    make(map[string]*list.Element),
	}
}

func (c *LRUCache) Get(key string) (string, bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	element, exists := c.cache[key]
	if exists {
		c.list.MoveToFront(element)
		return element.Value.(*CacheEntry).value, true
	}
	return "", false
}

func (c *LRUCache) Put(key, value string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	element, exist := c.cache[key]
	if exist {
		c.list.MoveToFront(element)
		element.Value.(*CacheEntry).value = value
	} else {
		if c.list.Len() >= c.capacity {
			oldest := c.list.Back()
			if oldest != nil {
				c.list.Remove(oldest)
				delete(c.cache, oldest.Value.(*CacheEntry).key)
			}
		}
		entry := &CacheEntry{key: key, value: value}
		element := c.list.PushFront(entry)
		c.cache[key] = element
	}
}

func (c *LRUCache) Remove(key string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if element, found := c.cache[key]; found {
		c.list.Remove(element)
		delete(c.cache, key)
	}
}
