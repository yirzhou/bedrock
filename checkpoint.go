package bedrock

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// RollToNewSegment rolls to a new segment.
// It creates a new WAL file and updates the WAL object.
// It also updates the current segment ID and the last sequence number.
func (kv *KVStore) RollToNewSegment() (uint64, error) {
	currentSegmentID := kv.GetCurrentSegmentIDUnsafe()
	segmentID := currentSegmentID + 1
	walFileName := getWalFileNameFromSegmentID(segmentID)
	walFilePath := filepath.Join(kv.config.GetBaseDir(), logsDir, walFileName)
	walFile, err := os.OpenFile(walFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println("Error opening WAL file:", err)
		return 0, err
	}
	// Close the current file
	err = kv.wal.activeFile.Close()
	if err != nil {
		log.Println("Error closing current WAL file:", err)
		return 0, err
	}
	kv.wal.activeFile = walFile
	// kv.wal.activeSegmentID = segmentID
	kv.wal.lastSequenceNum = 0

	// Update the active segment ID.
	kv.activeSegmentID = segmentID

	return currentSegmentID, nil
}

// doCheckpoint is the main function that performs a checkpoint.
// It creates a new segment file, converts the memtable to an SSTable,
// and writes the SSTable to the segment file.
// It also appends a special CHECKPOINT record to the WAL and updates the checkpoint file.
// Finally, it removes all old WAL files.
// If locked is true, it means that the caller has already acquired the lock.
func (kv *KVStore) doCheckpoint(locked bool) error {
	if !locked {
		kv.lock.Lock()
	}

	// 1. "Freeze and Swap".
	memTableToFlush := kv.memState
	sparseIndexToFlush := kv.memState.sparseIndexMap
	if memTableToFlush.Size() == 0 {
		// nothing to flush.
		if !locked {
			kv.lock.Unlock()
		}
		return nil
	}
	// Save the sparse index map to the new memtable.
	kv.memState = NewMemState()
	kv.memState.sparseIndexMap = sparseIndexToFlush
	kv.checkpointNeeded = false

	// Create the checkpoint directory if it doesn't exist.
	checkpointDir := filepath.Join(kv.config.GetBaseDir(), checkpointDir)
	err := os.MkdirAll(checkpointDir, 0755)
	if err != nil {
		log.Println("Error creating checkpoint directory:", err)
		return err
	}

	// 2. Roll to a new WAL segment. This gives us the ID for our new data segment.
	// The old WAL is now closed and ready to be archived/deleted.
	segmentID, err := kv.RollToNewSegment()
	if err != nil {
		log.Println("Error rolling to a new segment:", err)
		if !locked {
			kv.lock.Unlock()
		}
		return err
	}

	// We are done with the critical section. The database can now accept new writes
	// into the new, empty memtable and the new WAL segment.
	if !locked {
		kv.lock.Unlock()
	}

	log.Printf("Checkpoint triggered for WAL segment %d. Flushing memtable in background.", segmentID)

	// --- Phase 2: I/O Work (No Lock Held) ---
	// This is the slow part. We are working with the 'memtableToFlush' which is
	// now immutable and no longer part of the live database state.

	// 3. Persist the memtable to a new segment file.
	segmentFilePath := kv.getSegmentFilePath(segmentID)
	minKey, maxKey, err := memTableToFlush.Flush(segmentFilePath)
	if err != nil {
		log.Println("Error flushing memtable to segment file:", err)
		return err
	}

	// 5. Persist the sparse index to a new sparse index file.
	sparseIndexFilePath := kv.getSparseIndexFilePath(segmentID)
	err = kv.memState.FlushSparseIndex(sparseIndexFilePath)
	if err != nil {
		log.Println("Error flushing sparse index to sparse index file:", err)
		return err
	}

	// --- Phase 3: Commit (Lock Held) ---
	// Now that the new files are durably on disk, we re-acquire the lock to
	// atomically update the database's metadata. This is a fast operation.
	if !locked {
		kv.lock.Lock()
	}
	defer func() {
		if !locked {
			kv.lock.Unlock()
		}
	}()

	// Initialize the first level if it's not already initialized.
	if len(kv.levels) == 0 {
		kv.levels = make([][]SegmentMetadata, 1)
	}
	// Add the segment file to Level 0.
	kv.levels[0] = append(kv.levels[0], SegmentMetadata{
		id:       segmentID,
		level:    0,
		minKey:   minKey,
		maxKey:   maxKey,
		filePath: segmentFilePath,
	})

	// 6. Atomically update a CHECKPOINT file which records the last segment file path.
	manifestFilePath, err := kv.flushManifestFile(segmentID)
	if err != nil {
		log.Println("Error flushing manifest file:", err)
		return err
	}
	err = kv.updateCheckpointFile(manifestFilePath)
	if err != nil {
		log.Println("Error updating checkpoint file:", err)
		return err
	}

	// 7. Remove all old WAL files. This process can be done in the background without holding back the main thread.
	err = kv.cleanUpWALFiles(segmentID)
	if err != nil {
		log.Println("Error cleaning up WAL files:", err)
		return err
	}

	return nil
}

// cleanUpWALFiles removes all WAL files with segment ID less than or equal to the last segment ID.
func (kv *KVStore) cleanUpWALFiles(lastSegmentID uint64) error {
	walFiles, err := listWALFiles(kv.getWalDir())
	if err != nil {
		log.Println("Error listing WAL files during cleanup:", err)
		return err
	} else {
		// Remove all WAL files.
		for _, walFile := range walFiles {
			segmentID, err := getSegmentIDFromWalFileName(walFile)
			if err != nil {
				log.Printf("Error getting segment ID from WAL file %s: %v\n", walFile, err)
				// Skip this file. The file name is potentially corrupted.
				continue
			}
			if segmentID <= lastSegmentID {
				err = os.Remove(filepath.Join(kv.getWalDir(), walFile))
				if err != nil {
					log.Printf("Error removing WAL file %s: %v\n", walFile, err)
					// Skip this file for now which will be picked up by future checkpoints or cleanup jobs.
				} else {
					log.Printf("Removed WAL file %s\n", walFile)
				}
			}
		}
	}
	return nil
}

// updateCheckpointFile atomically updates the checkpoint file with the new segment file path.
func (kv *KVStore) updateCheckpointFile(manifestFilePath string) error {
	// Get the current unix timestamp as a string.
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	tempCheckpointFilePath := filepath.Join(kv.config.GetBaseDir(), checkpointDir, fmt.Sprintf("%s.%s", currentFile, timestamp))
	tempCheckpointFile, err := os.OpenFile(tempCheckpointFilePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Println("Error opening temp checkpoint file:", err)
		return err
	}
	defer tempCheckpointFile.Close()

	// Compute the checksum of the segment file path.
	checksum := ComputeChecksum([]byte(manifestFilePath))
	bytes := make([]byte, 4+len(manifestFilePath))
	binary.LittleEndian.PutUint32(bytes[:4], checksum)
	copy(bytes[4:], manifestFilePath)

	// Write the segment file path to the temp checkpoint file.
	_, err = tempCheckpointFile.Write(bytes)
	if err != nil {
		log.Println("Error writing to temp checkpoint file:", err)
		return err
	}
	tempCheckpointFile.Sync()

	// Rename the temp checkpoint file to the actual checkpoint file.
	err = os.Rename(tempCheckpointFilePath, filepath.Join(kv.config.GetBaseDir(), checkpointDir, currentFile))
	if err != nil {
		log.Println("Error renaming temp checkpoint file:", err)
		return err
	}
	return nil
}

// flushManifestFile flushes the manifest file for a given segment ID.
func (kv *KVStore) flushManifestFile(segmentID uint64) (string, error) {
	manifestFilePath := kv.getManifestFilePath(segmentID)
	manifestFile, err := os.OpenFile(manifestFilePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Println("Error opening manifest file:", err)
		return "", err
	}
	defer manifestFile.Close()

	// Write the segment metadata to the manifest file.
	for _, level := range kv.levels {
		for _, segment := range level {
			bytes := segment.GetBytes()
			_, err := manifestFile.Write(bytes)
			if err != nil {
				log.Println("Error writing to manifest file:", err)
				return "", err
			}
			err = manifestFile.Sync() // Sync after each segment.
			if err != nil {
				log.Println("Error syncing manifest file:", err)
				return "", err
			}
		}
	}
	return manifestFilePath, nil
}

// FlushSparseIndex flushes the sparse index to the sparse index file.
func (m *MemState) FlushSparseIndex(filePath string) error {
	segmentID, err := GetSegmentIDFromIndexFilePath(filePath)
	if err != nil {
		log.Println("Error getting segment ID from segment file path:", err)
		return err
	}
	sparseIndexFile, err := os.OpenFile(filePath, AppendFlags, 0644)
	if err != nil {
		log.Println("Error creating sparse index file:", err)
		return err
	}
	defer sparseIndexFile.Close()
	sparseIndexFile.Seek(0, io.SeekStart)
	for _, entry := range m.sparseIndexMap[segmentID] {
		sparseIndexFile.Seek(0, io.SeekCurrent)
		_, err := sparseIndexFile.Write(GetSparseIndexBytes(segmentID, entry.key, entry.offset))
		if err != nil {
			log.Println("FlushSparseIndex: Error writing to sparse index file:", err)
			return err
		}
	}
	err = sparseIndexFile.Sync()
	if err != nil {
		log.Println("FlushSparseIndex: Error syncing sparse index file:", err)
		return err
	}
	log.Println("FlushSparseIndex: Successfully flushed sparse index to file:", filePath)
	return nil
}
