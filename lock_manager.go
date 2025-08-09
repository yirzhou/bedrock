package main

import "sync"

// LockManager is a manager for per-key locks.
type LockManager struct {
	// This top-level mutex protects the 'locks' map itself.
	mu    sync.Mutex
	locks map[string]*sync.RWMutex
}

// NewLockManager creates a new LockManager.
func NewLockManager() *LockManager {
	return &LockManager{
		locks: make(map[string]*sync.RWMutex),
	}
}

// createLockIfNotExists creates a lock for the given key if it doesn't exist.
// It returns the lock for the key.
func (lm *LockManager) createLockIfNotExists(key []byte) *sync.RWMutex {
	lm.mu.Lock()
	keyLock, ok := lm.locks[string(key)]
	if !ok {
		keyLock = &sync.RWMutex{}
		lm.locks[string(key)] = keyLock
	}
	lm.mu.Unlock()
	return keyLock
}

func (lm *LockManager) AcquireReadLock(key []byte) {
	keyLock := lm.createLockIfNotExists(key)
	keyLock.RLock()
}

// AcquireWriteLock acquires a write lock for the given key.
// If the key doesn't exist, the lock for it will be created.
func (lm *LockManager) AcquireWriteLock(key []byte) {
	keyLock := lm.createLockIfNotExists(key)
	keyLock.Lock()
}

// ReleaseWriteLocks releases the write locks for the given keys.
func (lm *LockManager) ReleaseWriteLocks(keys []string) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	for _, key := range keys {
		keyLock, ok := lm.locks[key]
		if ok {
			keyLock.Unlock()
		}
	}
}

// ReleaseReadLocks releases the read locks for the given keys.
func (lm *LockManager) ReleaseReadLocks(keys []string) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	for _, key := range keys {
		keyLock, ok := lm.locks[key]
		if ok {
			keyLock.RUnlock()
		}
	}
}
