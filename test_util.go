package bedrock

import (
	"math/rand"
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




