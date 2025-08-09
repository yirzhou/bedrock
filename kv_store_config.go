package main

import (
	"io/ioutil"
	"log"
	"os"
)

// KVStoreConfig is the configuration for the KVStore.
type KVStoreConfig struct {
	CheckpointSize             int64
	BaseDir                    string
	SegmentFileSizeThresholdLX int64
	MaintenanceIntervalMs      uint64
	MemtableSizeThreshold      int64
	EnableSyncCheckpoint       bool
	EnableCheckpoint           bool
	EnableCompaction           bool
	EnableMaintenance          bool
}

// NewDefaultConfiguration returns the default DB configuration.
func NewDefaultConfiguration() *KVStoreConfig {
	return &KVStoreConfig{
		CheckpointSize:             1024, // 1KiB
		BaseDir:                    "./db",
		SegmentFileSizeThresholdLX: 1024,
		MaintenanceIntervalMs:      20,  // 20ms
		MemtableSizeThreshold:      128, // 128 keys
		EnableSyncCheckpoint:       true,
		EnableCheckpoint:           true,
		EnableCompaction:           true,
		EnableMaintenance:          true,
	}
}

func (c *KVStoreConfig) WithEnableMaintenance(enable bool) *KVStoreConfig {
	c.EnableMaintenance = enable
	return c
}

func (c *KVStoreConfig) WithEnableCompaction(enable bool) *KVStoreConfig {
	c.EnableCompaction = enable
	return c
}

func (c *KVStoreConfig) WithMaintenanceIntervalMs(intervalMs uint64) *KVStoreConfig {
	c.MaintenanceIntervalMs = intervalMs
	return c
}

func (c *KVStoreConfig) WithEnableCheckpoint(enable bool) *KVStoreConfig {
	c.EnableCheckpoint = enable
	return c
}

func (c *KVStoreConfig) WithEnableSyncCheckpoint(enable bool) *KVStoreConfig {
	c.EnableSyncCheckpoint = enable
	return c
}

func (c *KVStoreConfig) WithLog() *KVStoreConfig {
	log.SetOutput(os.Stdout)
	return c
}

func (c *KVStoreConfig) WithNoLog() *KVStoreConfig {
	log.SetOutput(ioutil.Discard) // Suppress log output
	return c
}

// WithBaseDir sets the base directory for the DB.
func (c *KVStoreConfig) WithBaseDir(dir string) *KVStoreConfig {
	c.BaseDir = dir
	return c
}

// GetCompactionIntervalMs returns the compaction interval for the DB.
func (c *KVStoreConfig) GetMaintenanceIntervalMs() uint64 {
	return c.MaintenanceIntervalMs
}

// WithMemtableSizeThreshold sets the memtable size threshold for the DB.
func (c *KVStoreConfig) WithMemtableSizeThreshold(size int64) *KVStoreConfig {
	c.MemtableSizeThreshold = size
	return c
}

// GetMemtableSizeThreshold returns the memtable size threshold for the DB.
func (c *KVStoreConfig) GetMemtableSizeThreshold() int64 {
	return c.MemtableSizeThreshold
}

// WithSegmentFileSizeThresholdLX sets the segment file size threshold for the DB.
func (c *KVStoreConfig) WithSegmentFileSizeThresholdLX(size int64) *KVStoreConfig {
	c.SegmentFileSizeThresholdLX = size
	return c
}

// WithCheckpointSize sets the checkpoint size for the DB.
func (c *KVStoreConfig) WithCheckpointSize(size int64) *KVStoreConfig {
	c.CheckpointSize = size
	return c
}

// GetCheckpointSize returns the checkpoint size for the DB.
func (c *KVStoreConfig) GetCheckpointSize() int64 {
	return c.CheckpointSize
}

// GetBaseDir returns the base directory for the DB.
func (c *KVStoreConfig) GetBaseDir() string {
	return c.BaseDir
}

// GetSegmentFileSizeThresholdLX returns the segment file size threshold for the DB.
func (c *KVStoreConfig) GetSegmentFileSizeThresholdLX() int64 {
	return c.SegmentFileSizeThresholdLX
}
