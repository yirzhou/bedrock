package bedrock

import (
	"io/ioutil"
	"log"
	"os"
)

type Configuration struct {
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
func NewDefaultConfiguration() *Configuration {
	return &Configuration{
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

func (c *Configuration) WithEnableMaintenance(enable bool) *Configuration {
	c.EnableMaintenance = enable
	return c
}

func (c *Configuration) WithEnableCompaction(enable bool) *Configuration {
	c.EnableCompaction = enable
	return c
}

func (c *Configuration) WithMaintenanceIntervalMs(intervalMs uint64) *Configuration {
	c.MaintenanceIntervalMs = intervalMs
	return c
}

func (c *Configuration) WithEnableCheckpoint(enable bool) *Configuration {
	c.EnableCheckpoint = enable
	return c
}

func (c *Configuration) WithEnableSyncCheckpoint(enable bool) *Configuration {
	c.EnableSyncCheckpoint = enable
	return c
}

func (c *Configuration) WithLog() *Configuration {
	log.SetOutput(os.Stdout)
	return c
}

func (c *Configuration) WithNoLog() *Configuration {
	log.SetOutput(ioutil.Discard) // Suppress log output
	return c
}

// WithBaseDir sets the base directory for the DB.
func (c *Configuration) WithBaseDir(dir string) *Configuration {
	c.BaseDir = dir
	return c
}

// GetCompactionIntervalMs returns the compaction interval for the DB.
func (c *Configuration) GetMaintenanceIntervalMs() uint64 {
	return c.MaintenanceIntervalMs
}

// WithMemtableSizeThreshold sets the memtable size threshold for the DB.
func (c *Configuration) WithMemtableSizeThreshold(size int64) *Configuration {
	c.MemtableSizeThreshold = size
	return c
}

// GetMemtableSizeThreshold returns the memtable size threshold for the DB.
func (c *Configuration) GetMemtableSizeThreshold() int64 {
	return c.MemtableSizeThreshold
}

// WithSegmentFileSizeThresholdLX sets the segment file size threshold for the DB.
func (c *Configuration) WithSegmentFileSizeThresholdLX(size int64) *Configuration {
	c.SegmentFileSizeThresholdLX = size
	return c
}

// WithCheckpointSize sets the checkpoint size for the DB.
func (c *Configuration) WithCheckpointSize(size int64) *Configuration {
	c.CheckpointSize = size
	return c
}

// GetCheckpointSize returns the checkpoint size for the DB.
func (c *Configuration) GetCheckpointSize() int64 {
	return c.CheckpointSize
}

// GetBaseDir returns the base directory for the DB.
func (c *Configuration) GetBaseDir() string {
	return c.BaseDir
}

// GetSegmentFileSizeThresholdLX returns the segment file size threshold for the DB.
func (c *Configuration) GetSegmentFileSizeThresholdLX() int64 {
	return c.SegmentFileSizeThresholdLX
}
