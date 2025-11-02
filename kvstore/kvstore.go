package kvstore

import (
	"encoding/json"
	"sync"
	"time"
)

// item represents a key-value pair with an expiration time.
// The value is the actual data, and expiresAt is the time when the item should be considered expired.
type item struct {
	value     string
	expiresAt time.Time
}

// KVStore is a simple in-memory key-value store with optional expiration support.
// It implements the Storage interface, allowing for setting, getting, and deleting key-value pairs.
type KVStore struct {
	mu    sync.RWMutex
	store map[string]item
}

// New creates a new instance of KVStore.
// It initializes the store map to hold key-value pairs.
func New() *KVStore {
	return &KVStore{
		store: make(map[string]item),
	}
}

// NewWithSize creates a new instance of KVStore with a specified initial size.
// It initializes the store map with the given size to optimize memory allocation.
func (kv *KVStore) Set(key, value string) {
	kv.SetWithTTL(key, value, 0)
}

// SetWithTTL sets a key-value pair in the store with an optional time-to-live (TTL) value.
// If the TTL is greater than zero, the item will expire after the specified duration.
func (kv *KVStore) SetWithTTL(key, value string, ttl time.Duration) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}

	kv.store[key] = item{
		value:     value,
		expiresAt: expiresAt,
	}
}

// Get retrieves the value associated with the given key from the store.
// If the key does not exist or has expired, it returns an empty string and false.
func (kv *KVStore) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	it, ok := kv.store[key]
	if !ok {
		return "", false
	}
	if !it.expiresAt.IsZero() && time.Now().After(it.expiresAt) {
		go kv.deleteKeyAsync(key)
		return "", false
	}
	return it.value, true
}

// Delete removes the key-value pair associated with the given key from the store.
// It returns true if the key was found and deleted, or false if the key did not exist.
func (kv *KVStore) Delete(key string) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, ok := kv.store[key]; ok {
		delete(kv.store, key)
		return true
	}
	return false
}

// deleteKeyAsync is a helper function that deletes a key-value pair asynchronously.
// It is called when a key has expired and needs to be removed from the store.
func (kv *KVStore) deleteKeyAsync(key string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.store, key)
}

// Snapshot serializes the in-memory store and returns a byte slice suitable
// for Raft snapshot persistence. Implements kvstore.SnapshotStore.
func (kv *KVStore) Snapshot() ([]byte, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	out := make(map[string]struct {
		Value   string `json:"value"`
		Expires int64  `json:"expires"`
	})
	for k, it := range kv.store {
		var exp int64
		if !it.expiresAt.IsZero() {
			exp = it.expiresAt.Unix()
		}
		out[k] = struct {
			Value   string `json:"value"`
			Expires int64  `json:"expires"`
		}{Value: it.value, Expires: exp}
	}
	return json.Marshal(out)
}

// Restore replaces the in-memory store by deserializing the provided snapshot
// data. Implements kvstore.SnapshotStore.
func (kv *KVStore) Restore(b []byte) error {
	var in map[string]struct {
		Value   string `json:"value"`
		Expires int64  `json:"expires"`
	}
	if err := json.Unmarshal(b, &in); err != nil {
		return err
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.store = make(map[string]item, len(in))
	for k, v := range in {
		var expiresAt time.Time
		if v.Expires > 0 {
			expiresAt = time.Unix(v.Expires, 0)
		}
		kv.store[k] = item{value: v.Value, expiresAt: expiresAt}
	}
	return nil
}
