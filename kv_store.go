package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"bedrock/lib"
)

const (
	checkpointDir         = "checkpoints"
	logsDir               = "logs"
	checkpointFile        = "CHECKPOINT"
	currentFile           = "CURRENT"
	walFilePrefix         = "wal-"
	segmentFilePrefix     = "segment-"
	manifestFilePrefix    = "MANIFEST-"
	sparseIndexFilePrefix = "index-"
	segmentThresholdL0    = 4
)

// Write to WAL -> Update Memtable -> (When ready) -> Flush Memtable to SSTable -> Checkpoint -> Delete old WAL
// In this process, the flush is done by the memtable.

// Pair is a key-value pair.
type Pair struct {
	Key   string
	Value []byte
}

type KVStore struct {
	lock            sync.RWMutex
	wal             *WAL
	memState        *MemState
	activeSegmentID uint64
	// Outer slice index is the level number.
	// Inner slice holds all the segment files for that level.
	levels [][]SegmentMetadata

	// A channel to signal the compaction loop to stop.
	shutdownChan chan struct{}

	// The configuration for the KVStore.
	config *KVStoreConfig

	// Lock manager for transactions.
	lockManager *LockManager

	checkpointNeeded bool
}

// NewIterator creates an iterator for range queries.
func (kv *KVStore) NewIterator() Iterator {
	iterators := make([]Iterator, 0, len(kv.levels))
	for _, level := range kv.levels {
		for _, segment := range level {
			sparseIndex := kv.memState.GetSparseIndex(segment.id)
			iter, err := NewSegmentIterator(segment.filePath, sparseIndex)
			if err != nil {
				log.Println("NewIterator: Error creating segment iterator:", err)
				continue
			}
			iterators = append(iterators, iter)
		}
	}
	return NewMergingIterator(kv.memState.NewIterator(), iterators)
}

// GetSegmentIDFromManifestFileName returns the segment ID from a manifest file name.
func GetSegmentIDFromManifestFileName(fileName string) (uint64, error) {
	segmentID, err := strconv.ParseUint(strings.TrimPrefix(fileName, manifestFilePrefix), 10, 64)
	if err != nil {
		return 0, err
	}
	return segmentID, nil
}

// Open opens/creates the log file and initializes the KVStore object.
//
// 1. Try to get the last checkpoint file path from the checkpoint file and the WAL files.
//
// 2. If the checkpoint is found, open it and read the last segment file path.
//
// - Load the segment file into memtable.
//
// - For all WAL files with segment ID greater than the last segment ID in the checkpoint file, replay them into memtable in ascending order.
//
// - Truncate the WAL file (likely the last one) to the last good offset.
//
// 3. If the checkpoint is not found, simply start from the beginning of the WAL files and replay them into memtable in ascending order.
//
// - Truncate the WAL file (likely the last one) to the last good offset.
//
// 4. Create and return the KVStore object.
// TODOs:
// 1. Add a compaction job that removes old checkpoints and sparse index files.
func Open(config *KVStoreConfig) (*KVStore, error) {
	// Validate the configuration.
	if config.EnableMaintenance && config.MaintenanceIntervalMs <= 0 {
		return nil, errors.New("maintenance interval must be greater than 0")
	}

	lastTimestamp := time.Now()
	err := PreCreateDirectories(config)
	if err != nil {
		log.Fatalf("Error creating directories: %v", err)
	}
	// Read the "CURRENT" file
	levels, lastSegmentID, err := tryRecoverFromCurrentFile(filepath.Join(config.GetBaseDir(), checkpointDir, currentFile))
	if err != nil && !os.IsNotExist(err) {
		log.Println("Open: Error recovering from CURRENT file:", err)
	}
	log.Println("Open: Time taken to recover from CURRENT file:", time.Since(lastTimestamp))
	lastTimestamp = time.Now()

	// Recover the sparse indexes from levels metadata.
	memState := NewMemState()
	err = recoverMemStateFromLevels(filepath.Join(config.GetBaseDir(), checkpointDir), levels, memState)
	if err != nil {
		log.Println("Open: Error recovering from levels:", err)
		return nil, err
	}
	log.Println("Open: Time taken to recover from levels:", time.Since(lastTimestamp))
	lastTimestamp = time.Now()

	// Recover from WAL files.
	wal, err := recoverFromWALs(lastSegmentID, filepath.Join(config.GetBaseDir(), logsDir), memState, config.GetCheckpointSize())
	if err != nil {
		log.Println("Open: Error recovering from WAL files:", err)
		return nil, err
	}
	log.Println("Open: Time taken to recover from WAL files:", time.Since(lastTimestamp))

	// Return the fully initialized, ready-to-use KVStore object.
	kv := &KVStore{
		wal:              wal,
		memState:         memState,
		activeSegmentID:  lastSegmentID + 1,
		levels:           levels,
		shutdownChan:     make(chan struct{}),
		config:           config,
		lockManager:      NewLockManager(),
		checkpointNeeded: false,
	}
	// Start the maintenance loop.
	if config.EnableMaintenance {
		go kv.maintenanceLoop()
	}

	return kv, nil
}

// clearAllData clears all the data in the KVStore.
// It is used when restoring from a snapshot.
//
// The method assumes that the lock is currently held by the caller.
func (kv *KVStore) clearAllData() error {
	// Clear the memstate.
	kv.memState.ClearState()
	// Clear all levels.
	kv.levels = make([][]SegmentMetadata, 0)
	kv.activeSegmentID = 0

	// Clear all locks.
	kv.lockManager = NewLockManager()
	return nil
}

// BeginTransaction creates a new transaction.
func (kv *KVStore) BeginTransaction() *Transaction {
	return NewTransaction(kv)
}

// GetCurrentSegmentIDSafe returns the ID of the current segment.
func (s *KVStore) GetCurrentSegmentIDSafe() uint64 {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.activeSegmentID
}

// This method assumes that the lock is currently withheld.
func (s *KVStore) GetCurrentSegmentIDUnsafe() uint64 {
	return s.activeSegmentID
}

// This method assumes that the lock is currently withheld.
func (s *KVStore) AllocateSegmentIDUnsafe() uint64 {
	s.activeSegmentID++
	return s.activeSegmentID
}

// This method assumes that the lock is currently not withheld.
func (s *KVStore) AllocateNewSegmentID() uint64 {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.activeSegmentID++
	return s.activeSegmentID
}

func (kv *KVStore) findKeyInL0(key []byte) ([]byte, error) {
	levelZero := kv.levels[0]
	for i := len(levelZero) - 1; i >= 0; i-- {
		segmentMetadata := levelZero[i]
		segmentFilePath := segmentMetadata.filePath
		offset := kv.memState.FindKeyInSparseIndex(segmentMetadata.id, key)
		if offset == -1 {
			continue
		}
		value, err := kv.searchInSegmentFile(segmentFilePath, offset, key)
		if err != nil {
			log.Println("findKeyInL0: Error searching in segment file:", err)
			return nil, err
		}
		if value != nil {
			return value, nil
		}
	}
	return nil, nil
}

func binSearchSegmentMetadata(level []SegmentMetadata, key []byte) int {
	l := 0
	r := len(level) - 1
	searchKeyStr := string(key)
	for l <= r {
		mid := (l + r) / 2
		minKeyStr := string(level[mid].minKey)
		maxKeyStr := string(level[mid].maxKey)
		if searchKeyStr >= minKeyStr && searchKeyStr <= maxKeyStr {
			return mid
		}
		if searchKeyStr < minKeyStr {
			r = mid - 1
		} else {
			l = mid + 1
		}
	}
	return -1
}

// getInternalV2 is the internal implementation of Get using leveled compaction.
func (kv *KVStore) getInternalV2(key []byte) ([]byte, bool) {
	// Find the key in L0 first.
	value, err := kv.findKeyInL0(key)
	if err != nil {
		log.Println("getInternal: Error finding key in L0:", err)
		return nil, false
	}
	if value != nil {
		// Handle tombstone.
		if bytes.Equal(value, lib.TOMBSTONE) {
			return nil, false
		}
		return value, true
	}

	// Find from L1 onwards.
	for i := 1; i < len(kv.levels); i++ {
		level := kv.levels[i]
		// Do a binary search on the segment list based on minKey and maxKey.
		idx := binSearchSegmentMetadata(level, key)
		if idx == -1 {
			continue
		}
		// Found level.
		log.Println("getInternalV2: Found key in level:", i)
		segmentMetadata := level[idx]
		segmentFilePath := segmentMetadata.filePath
		offset := kv.memState.FindKeyInSparseIndex(segmentMetadata.id, key)
		if offset == -1 {
			continue
		}
		log.Println("getInternalV2: Found key in segment file:", segmentFilePath, "with offset:", offset)
		// Try to search from the segment file.
		value, err := kv.searchInSegmentFile(segmentFilePath, offset, key)
		if err != nil {
			log.Println("getInternal: Error searching in segment file:", err)
			return nil, false
		}
		if value != nil {
			log.Println("getInternal: Found key in segment file:", string(key))
			if bytes.Equal(value, lib.TOMBSTONE) {
				return nil, false
			}
			return value, true
		}
	}
	return nil, false
}

// Deprecated: Use getInternalV2 instead.
// getInternal is the internal implementation of Get using the old way of going through all sparse indexes from newest to oldest (without leveled compaction).
func (kv *KVStore) getInternal(key []byte) ([]byte, bool) {
	// Search in the segment files.
	segmentIDs, err := kv.memState.GetAllSegmentIDsDescendingOrder()
	if err != nil {
		log.Println("Get: Error getting all segment files:", err)
		return nil, false
	}
	log.Println("Get: Searching in segment files:", segmentIDs)
	var value []byte = nil
	for _, segmentID := range segmentIDs {
		offset := kv.memState.FindKeyInSparseIndex(segmentID, key)
		if offset == -1 {
			// The segment file does contain the key.
			continue
		}
		segmentFilePath := filepath.Join(kv.config.GetBaseDir(), checkpointDir, getSegmentFileNameFromSegmentId(segmentID))
		value, err = kv.searchInSegmentFile(segmentFilePath, offset, key)
		if err != nil {
			// Technically we shouldn't get an error here.
			log.Printf("Get: Error searching in segment file: %v\n", err)
			return nil, false
		}
		// This is the value we are looking for since we are searching in the segment files in descending order.
		if value != nil {
			log.Println("Get: Found key in segment file:", string(key))
			if !bytes.Equal(value, lib.TOMBSTONE) {
				return value, true
			}
		}
	}
	return nil, false
}

// Get returns the value for a given key. The value is nil if the key is not found.
// It creates a new transaction and commits it.
// It is a wrapper around Transaction.Get.
func (kv *KVStore) Get(key []byte) ([]byte, bool) {
	txn := kv.BeginTransaction()
	value, found := txn.Get(key)
	txn.Commit()
	return value, found
}

// get returns the value for a given key. The value is nil if the key is not found.
func (kv *KVStore) get(key []byte) ([]byte, bool) {
	kv.lock.RLock()
	defer kv.lock.RUnlock()
	value, found := kv.memState.Get(key)
	if found {
		// Directly return the value from the memtable.
		log.Printf("Get: Found key [%s] in memtable with value [%s]\n", string(key), string(value))
	} else {
		// Search in L0 first.
		value, found = kv.getInternalV2(key)
		if !found {
			return nil, false
		}
	}
	// If the value is a tombstone, it means the key is deleted.
	if bytes.Equal(value, lib.TOMBSTONE) {
		log.Println("Get: Key is deleted:", string(key))
		value = nil
		found = false
	}
	return value, found
}

// Put writes a key-value pair to the KVStore.
// It creates a new transaction and commits it.
// It is a wrapper around Transaction.Put.
func (kv *KVStore) Put(key, value []byte) error {
	txn := kv.BeginTransaction()
	txn.Put(key, value)
	txn.Commit()
	return nil
}

// Deprecated: Use Put instead.
//
// Warning: Directly calling this method is a blind write.
// Put writes a key-value pair to the KVStore.
func (kv *KVStore) PutV1(key, value []byte) error {
	// Makes sure that the key and value are not nil.
	if key == nil || value == nil || bytes.Equal(key, lib.CHECKPOINT) || bytes.Equal(value, lib.TOMBSTONE) {
		log.Printf("Put: Invalid key or value: %s, %s\n", string(key), string(value))
		return errors.New("invalid key or value")
	}
	return kv.putInternal(key, value)
}

// putInternal is the internal implementation of the Put method.
func (kv *KVStore) putInternal(key, value []byte) error {
	kv.lock.Lock()
	defer kv.lock.Unlock()
	// 1. Write to WAL
	walErr := kv.wal.Append(key, value)

	// 2. Check if the memtable is ready to be flushed (checkpoint)
	if walErr != nil && walErr != lib.ErrCheckpointNeeded {
		log.Println("Error writing to WAL:", walErr)
		return walErr
	}

	// 3. Update Memtable
	kv.memState.Put(key, value)

	// 4. Check if the WAL is ready to be checkpointed
	if walErr == lib.ErrCheckpointNeeded {
		// checkpoint the memtable
		err := kv.doCheckpoint(true)
		if err != nil {
			log.Println("Error checkpointing:", err)
			return err
		}
	}
	return nil
}

// Delete deletes a key-value pair from the KVStore.
// It creates a new transaction and commits it.
// It is a wrapper around Transaction.Delete.
func (kv *KVStore) Delete(key []byte) error {
	txn := kv.BeginTransaction()
	txn.Delete(key)
	txn.Commit()
	return nil
}

// DeleteV1 deletes a key-value pair from the KVStore.
// It writes a tombstone to the WAL and the memtable.
// TODO: It also updates the offset in the sparse index.
func (kv *KVStore) DeleteV1(key []byte) error {
	// Ensures that the key is not CHECKPOINT.
	if bytes.Equal(key, lib.CHECKPOINT) {
		log.Printf("Delete: Invalid key: %s\n", string(key))
		return errors.New("invalid key")
	}
	err := kv.putInternal(key, lib.TOMBSTONE)
	if err != nil {
		log.Printf("Delete: Error [%v] deleting key [%s]\n", err, string(key))
		return err
	}
	return nil
}

// compareWalFilesAscending compares two WAL file names in ascending order.
func compareWalFilesAscending(a, b string) int {
	segmentIDA, err := getSegmentIDFromWalFileName(a)
	if err != nil {
		return 1
	}
	segmentIDB, err := getSegmentIDFromWalFileName(b)
	if err != nil {
		return -1
	}
	return int(segmentIDA - segmentIDB)
}

// getWalDir returns the directory where the WAL files are stored.
func (kv *KVStore) getWalDir() string {
	return filepath.Join(kv.config.GetBaseDir(), logsDir)
}

// getCheckpointDir returns the directory where the checkpoint files are stored.
func (kv *KVStore) getCheckpointDir() string {
	return filepath.Join(kv.config.GetBaseDir(), checkpointDir)
}

// getManifestFile returns the manifest file.
func (kv *KVStore) getManifestFilePath(segmentID uint64) string {
	return filepath.Join(kv.config.GetBaseDir(), checkpointDir, fmt.Sprintf("%s%06d", manifestFilePrefix, segmentID))
}

// flushManifestFileWithLevels flushes the manifest file for the given levels.
func flushManifestFileWithLevels(filePath string, levels [][]SegmentMetadata) error {
	file, err := os.OpenFile(filePath, AppendFlags, 0644)
	if err != nil {
		log.Println("flushManifestFileWithLevels: Error opening manifest file:", err)
		return err
	}
	defer file.Close()
	for _, level := range levels {
		for _, segment := range level {
			bytes := segment.GetBytes()
			_, err = file.Write(bytes)
			if err != nil {
				log.Println("flushManifestFileWithLevels: Error writing to manifest file:", err)
				return err
			}
			err = file.Sync() // Sync after each segment.
			if err != nil {
				log.Println("flushManifestFileWithLevels: Error syncing manifest file:", err)
				return err
			}
		}
	}
	return nil
}

// getSegmentFile returns the segment file for a given segment ID.
func (kv *KVStore) searchInSegmentFile(segmentFilePath string, offset int64, key []byte) ([]byte, error) {
	log.Println("searchInSegmentFile: Searching in segment file:", segmentFilePath, "with offset:", offset, "and key:", string(key))
	segmentFile, err := os.Open(segmentFilePath)
	if err != nil {
		log.Println("searchInSegmentFile: Error opening segment file:", err)
		return nil, err
	}
	defer segmentFile.Close()

	// Seek to the offset.
	_, _ = segmentFile.Seek(offset, io.SeekStart)

	var value []byte = nil
	for {
		_, err = segmentFile.Seek(0, io.SeekCurrent)
		if err != nil {
			log.Println("searchInSegmentFile: Error seeking to current position:", err)
			return nil, err
		}
		// Check if we've found the record.
		record, err := getNextKVRecord(segmentFile)
		if err != nil {
			if err == io.EOF {
				// No more records.
				break
			}
			log.Println("searchInSegmentFile: Error recovering next record:", err)
			return nil, err
		}
		// Found an exact match.
		if bytes.Equal(record.Key, key) {
			// Update the value to the latest one.
			value = record.Value
		} else if bytes.Compare(record.Key, key) > 0 {
			// The key is greater than the current key. We don't need to search further.
			break
		}
	}
	return value, nil
}

// getSegmentIDFromWalFileName returns the segment ID from a WAL file name.
func getSegmentIDFromWalFileName(fileName string) (uint64, error) {
	segmentId := strings.TrimPrefix(fileName, walFilePrefix)
	segmentIdInt, err := strconv.ParseUint(segmentId, 10, 64)
	if err != nil {
		return 0, err
	}
	return segmentIdInt, nil
}

// getSegmentIDFromSegmentFilePath returns the segment ID from a segment file path.
func GetSegmentIDFromIndexFilePath(filePath string) (uint64, error) {
	fileName := filepath.Base(filePath)
	segmentId := strings.TrimPrefix(fileName, sparseIndexFilePrefix)
	segmentIdInt, err := strconv.ParseUint(segmentId, 10, 64)
	if err != nil {
		return 0, err
	}
	return segmentIdInt, nil
}

// getSegmentIDFromSegmentFilePath returns the segment ID from a segment file path.
func GetSegmentIDFromSegmentFilePath(filePath string) (uint64, error) {
	fileName := filepath.Base(filePath)
	segmentId := strings.TrimPrefix(fileName, segmentFilePrefix)
	segmentIdInt, err := strconv.ParseUint(segmentId, 10, 64)
	if err != nil {
		return 0, err
	}
	return segmentIdInt, nil
}

// getSegmentIDFromSegmentFileName returns the segment ID from a segment file name.
func getSegmentIDFromSegmentFileName(fileName string) (uint64, error) {
	segmentId := strings.TrimPrefix(fileName, segmentFilePrefix)
	segmentIdInt, err := strconv.ParseUint(segmentId, 10, 64)
	if err != nil {
		return 0, err
	}
	return segmentIdInt, nil
}

// getSegmentFileNameFromSegmentId constructs the filename for a segment file from a segment ID.
func getSegmentFileNameFromSegmentId(segmentId uint64) string {
	return fmt.Sprintf("%s%06d", segmentFilePrefix, segmentId)
}

// getSparseIndexFileNameFromSegmentId constructs the filename for a sparse index file from a segment ID.
func getSparseIndexFileNameFromSegmentId(segmentId uint64) string {
	return fmt.Sprintf("%s%06d", sparseIndexFilePrefix, segmentId)
}

// tryGetLastCheckpoint tries to get the last checkpoint file path from the checkpoint file and the WAL files.
func tryGetLastCheckpoint(checkpointDir, walDir string) string {
	// Create the checkpoint directory if it doesn't exist.
	err := os.MkdirAll(checkpointDir, 0755)
	if err != nil {
		log.Println("Error creating checkpoint directory:", err)
		return ""
	}

	// Create the WAL directory if it doesn't exist.
	err = os.MkdirAll(walDir, 0755)
	if err != nil {
		log.Println("Error creating WAL directory:", err)
		return ""
	}

	checkpointFilePath, _ := tryGetLastCheckpointFromFile(checkpointDir)
	if checkpointFilePath != "" {
		return checkpointFilePath
	}
	checkpointFilePath, _ = tryGetLastCheckpointFromWalFiles(walDir)
	if checkpointFilePath != "" {
		return checkpointFilePath
	}
	return ""
}

// tryGetLastCheckpoint tries to get the last checkpoint file path.
// If the file does not exist, it returns an empty string and no error.
func tryGetLastCheckpointFromFile(checkpointDir string) (string, error) {
	checkpointFilePath := filepath.Join(checkpointDir, checkpointFile)
	// Creates the file if it doesn't exist.
	checkpointFile, err := os.OpenFile(checkpointFilePath, os.O_RDONLY, 0644)
	if err != nil {
		log.Println("Error opening checkpoint file:", err)
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}
	defer checkpointFile.Close()

	// Read the file.
	bytes, err := io.ReadAll(checkpointFile)
	if err != nil {
		log.Println("Error reading checkpoint file:", err)
		return "", err
	}
	// Check the bytes length.
	if len(bytes) < 4 {
		log.Println("Checkpoint file is too short")
		return "", lib.ErrCheckpointCorrupted
	}

	// Verify the checksum of the file.
	checksum := binary.LittleEndian.Uint32(bytes[:4])
	if checksum != ComputeChecksum(bytes[4:]) {
		log.Println("Checksum mismatch in checkpoint file")
		return "", lib.ErrBadChecksum
	}
	// Return the segment file path.
	return string(bytes[4:]), nil
}

// getLastCheckpointFilePathFromWalFile tries to get the last checkpoint file path from a WAL file.
// If the WAL file does not contain a CHECKPOINT record, it returns an empty string and no error.
func getLastCheckpointFilePathFromWalFile(walFilePath string) (string, error) {
	walFile, err := os.Open(walFilePath)
	if err != nil {
		return "", err
	}
	defer walFile.Close()
	// Read the file from the beginning.
	_, err = walFile.Seek(0, io.SeekStart)
	if err != nil {
		log.Println("Error seeking to start:", err)
		return "", err
	}
	lastCheckpointFilePath := ""
	for {
		_, err := walFile.Seek(0, io.SeekCurrent)
		if err != nil {
			log.Println("Error seeking to current position:", err)
			break
		}
		// Read the first record.
		record, err := recoverNextRecord(walFile)
		if err != nil && err != io.EOF {
			log.Println("Error recovering next record:", err)
			break
		}

		if record == nil {
			// EOF
			break
		}
		// Return the segment file path. If the record is not a CHECKPOINT record, return an empty string.
		if string(record.Key) == string(lib.CHECKPOINT) {
			log.Println("Found CHECKPOINT record in WAL file:", walFilePath)
			lastCheckpointFilePath = string(record.Value)
			// Keep updating the last checkpoint file.
		}
	}
	return lastCheckpointFilePath, nil
}

// tryGetLastCheckpointFromWalFiles tries to get the last checkpoint file path from the WAL files.
func tryGetLastCheckpointFromWalFiles(walDir string) (string, error) {
	walFiles, err := listWALFiles(walDir)
	if err != nil {
		log.Println("tryGetLastCheckpointFromWalFiles: Error listing WAL files:", err)
		return "", err
	}
	// Sort all files by segment ID in reverse order.
	slices.SortFunc(walFiles, compareWalFilesAscending)
	slices.Reverse(walFiles)
	// Get the last checkpoint file path from the WAL files (reverse order).
	for _, walFile := range walFiles {
		walPath := filepath.Join(walDir, walFile)
		walPath, err := getLastCheckpointFilePathFromWalFile(walPath)
		if err != nil {
			log.Println("tryGetLastCheckpointFromWalFiles: Error getting last checkpoint file path from WAL file:", err)
			// Skip this file.
		} else if walPath != "" {
			return walPath, nil
		}
	}
	return "", nil
}

// getNewSegmentFilePath returns the path to the next segment file.
func (kv *KVStore) getSegmentFilePath(segmentID uint64) string {
	segmentFileName := getSegmentFileNameFromSegmentId(segmentID)
	return filepath.Join(kv.config.GetBaseDir(), checkpointDir, segmentFileName)
}

// getNewSparseIndexFilePath returns the path to the next sparse index file.
func (kv *KVStore) getSparseIndexFilePath(segmentID uint64) string {
	sparseIndexFileName := getSparseIndexFileNameFromSegmentId(segmentID)
	return filepath.Join(kv.config.GetBaseDir(), checkpointDir, sparseIndexFileName)
}

// listWALFiles lists all the WAL files in the directory.
func listWALFiles(dir string) ([]string, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	walFiles := make([]string, 0)
	for _, file := range files {
		if strings.HasPrefix(file.Name(), walFilePrefix) {
			walFiles = append(walFiles, file.Name())
		}
	}
	return walFiles, nil
}

// Deprecated
// listSegmentFiles lists all the segment files in the directory.
func listSegmentFiles(dir string) ([]string, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	segmentFiles := make([]string, 0)
	for _, file := range files {
		if strings.HasPrefix(file.Name(), segmentFilePrefix) {
			segmentFiles = append(segmentFiles, file.Name())
		}
	}
	return segmentFiles, nil
}

// getHighestSegmentID returns the highest segment ID from the list of WAL files. If any file is not named correctly, it will be skipped.
// Note that zero will be returned if no files are found.
func getHighestSegmentID(files []string) uint64 {
	highestId := uint64(0)
	for _, file := range files {
		segmentId := strings.TrimPrefix(file, "wal-")
		segmentIdInt, err := strconv.ParseUint(segmentId, 10, 64)
		if err != nil {
			log.Println("Error parsing segment ID:", err)
			// Skip this file.
		} else {
			highestId = max(highestId, segmentIdInt)
		}
	}
	return highestId
}

// getWalFileNameFromSegmentID constructs the filename for a WAL file from a segment ID.
func getWalFileNameFromSegmentID(segmentId uint64) string {
	return fmt.Sprintf("%s%06d", walFilePrefix, segmentId)
}

// Close closes the WAL file.
func (kv *KVStore) Close() error {
	// Signal the compaction loop to stop.
	close(kv.shutdownChan)
	// Close the WAL file.
	return kv.wal.Close()
}

// Mostly used in tests.
func (kv *KVStore) CloseAndCleanUp() error {
	err := kv.Close()
	if err != nil {
		log.Println("CloseAndCleanUp: Error closing WAL:", err)
		return err
	}
	err = kv.CleanUpDirectories()
	if err != nil {
		log.Println("CloseAndCleanUp: Error cleaning up directories:", err)
		return err
	}
	return nil
}

// CleanUpDirectories cleans up the checkpoint and WAL directories.
func (kv *KVStore) CleanUpDirectories() error {
	checkpointDir := filepath.Join(kv.config.GetBaseDir(), checkpointDir)
	walDir := filepath.Join(kv.config.GetBaseDir(), logsDir)
	err := os.RemoveAll(checkpointDir)
	if err != nil {
		log.Println("CleanUpDirectories: Error cleaning up checkpoint directory:", err)
		return err
	}
	err = os.RemoveAll(walDir)
	if err != nil {
		log.Println("CleanUpDirectories: Error cleaning up WAL directory:", err)
		return err
	}
	// Delete the master directory.
	err = os.RemoveAll(kv.config.GetBaseDir())
	if err != nil {
		log.Println("CleanUpDirectories: Error cleaning up master directory:", err)
		return err
	}
	return nil
}

// GetLastSequenceNum returns the last sequence number.
func (kv *KVStore) GetLastSequenceNum() uint64 {
	return kv.wal.lastSequenceNum
}

// Print prints the memtable.
func (kv *KVStore) Print() {
	log.Println("Last sequence number:", kv.GetLastSequenceNum())
	kv.memState.Print()
}
