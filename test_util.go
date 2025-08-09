package main

import (
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func NewConfigurationNoMaintenance() *KVStoreConfig {
	return NewDefaultConfiguration().WithNoLog().WithMaintenanceIntervalMs(0).WithMemtableSizeThreshold(32).WithEnableMaintenance(false)
}

// GenerateRandomString generates a random string of a given length.
func GenerateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	seededRand := rand.New(rand.NewSource(time.Now().UnixNano()))
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

// getTestDir returns a temporary directory for the test.
func GetTestDir() string {
	timestamp := strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
	prefix := "kv-store-test-" + timestamp
	return filepath.Join("/tmp", prefix)
}

// GetTempDirForTest creates a temporary directory for the test and returns the path.
func GetTempDirForTest(t *testing.T) string {
	dir := t.TempDir()
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	return dir
}
