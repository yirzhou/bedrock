package main

import (
	"encoding/binary"
	"io"
	"log"
	"os"
	"path/filepath"
	"slices"
	"bedrock/lib"
)

// PreCreateDirectories creates the directories for the KVStore if they don't exist.
func PreCreateDirectories(config *KVStoreConfig) error {
	// Create the master directory if it doesn't exist.
	err := os.MkdirAll(config.GetBaseDir(), 0755)
	if err != nil {
		log.Fatalf("Error creating master directory: %v", err)
	}

	// Create logs directory if it doesn't exist.
	err = os.MkdirAll(filepath.Join(config.GetBaseDir(), logsDir), 0755)
	if err != nil {
		log.Fatalf("Error creating logs directory: %v", err)
	}

	// Create checkpoint directory if it doesn't exist.
	err = os.MkdirAll(filepath.Join(config.GetBaseDir(), checkpointDir), 0755)
	if err != nil {
		log.Fatalf("Error creating checkpoint directory: %v", err)
	}
	return nil
}

// emptyLevels returns an empty levels slice.
func emptyLevels() [][]SegmentMetadata {
	levels := make([][]SegmentMetadata, 1)
	levels[0] = make([]SegmentMetadata, 0)
	return levels
}

// GetCurrentManifest returns the path to the current manifest file.
func (s *KVStore) GetCurrentManifestFilePath() (string, error) {
	return tryGetCurrentManifest(filepath.Join(s.config.GetBaseDir(), checkpointDir, currentFile))
}

// tryGetCurrentManifest tries to get the manifest file path from the CURRENT file.
func tryGetCurrentManifest(currentFilePath string) (string, error) {
	// Check if the CURRENT file exists
	if _, err := os.Stat(currentFilePath); os.IsNotExist(err) {
		log.Println("tryRecoverFromCurrentFile: CURRENT file does not exist")
		// Return an empty levels slice.
		return "", err
	}
	// Read the CURRENT file
	content, err := os.ReadFile(currentFilePath)
	if err != nil {
		log.Println("tryRecoverFromCurrentFile: Error reading CURRENT file:", err)
		return "", err
	}
	// Get the content of the CURRENT file
	checksumBytes := make([]byte, 4)
	copy(checksumBytes, content[:4])
	checksum := binary.LittleEndian.Uint32(checksumBytes)
	// Compute the checksum of the content
	computedChecksum := ComputeChecksum(content[4:])
	// Check if the checksum is correct
	if checksum != computedChecksum {
		log.Println("tryRecoverFromCurrentFile: Bad checksum")
		return "", lib.ErrBadChecksum
	}

	// Get the manifest file path
	manifestFilePath := string(content[4:])
	return manifestFilePath, nil
}

// recoverFromManifest recovers the levels layout from the manifest file.
// It returns the levels layout and the segment ID.
func recoverFromManifest(manifestFilePath string) ([][]SegmentMetadata, uint64, error) {
	// TODO: Implement this.
	manifestFileName := filepath.Base(manifestFilePath)
	// Get the segment ID from the manifest file path
	segmentID, err := GetSegmentIDFromManifestFileName(manifestFileName)
	if err != nil {
		log.Println("tryRecoverFromCurrentFile: Error getting segment ID from manifest file:", err)
		return emptyLevels(), 0, err
	}
	// Read the manifest file and recover the levels layout
	manifestFile, err := os.Open(manifestFilePath)
	if err != nil {
		log.Println("tryRecoverFromCurrentFile: Error opening manifest file:", err)
		return emptyLevels(), 0, err
	}
	defer manifestFile.Close()
	// Read the manifest file and recover the levels layout
	manifestFile.Seek(0, io.SeekStart)
	segmentMetadataList := make([]SegmentMetadata, 0)
	for {
		manifestFile.Seek(0, io.SeekCurrent)
		segmentMetadata, err := ReadSegmentMetadata(manifestFile)
		if err != nil {
			// Handle EOF
			if err == io.EOF {
				log.Println("tryRecoverFromCurrentFile: Reached EOF")
				break
			}
			log.Println("tryRecoverFromCurrentFile: Error reading segment metadata:", err)
			return emptyLevels(), 0, err
		}
		segmentMetadataList = append(segmentMetadataList, *segmentMetadata)
	}
	// Sort the segment metadata list by level and id'
	slices.SortFunc(segmentMetadataList, func(a, b SegmentMetadata) int {
		if a.level != b.level {
			return int(a.level - b.level)
		}
		return int(a.id - b.id)
	})
	// Handle the case where the segment metadata list is empty
	highestLevel := uint32(0)
	if len(segmentMetadataList) != 0 {
		highestLevel = segmentMetadataList[len(segmentMetadataList)-1].level
	}
	levels := make([][]SegmentMetadata, highestLevel+1)
	// Initialize every level.
	for i := range highestLevel + 1 {
		levels[i] = make([]SegmentMetadata, 0)
	}
	for _, segmentMetadata := range segmentMetadataList {
		levels[segmentMetadata.level] = append(levels[segmentMetadata.level], segmentMetadata)
	}
	return levels, segmentID, nil
}

// tryRecoverFromCurrentFile tries to recover the last segment ID from the CURRENT file.
func tryRecoverFromCurrentFile(currentFilePath string) ([][]SegmentMetadata, uint64, error) {
	// Check if the CURRENT file exists
	if _, err := os.Stat(currentFilePath); os.IsNotExist(err) {
		log.Println("tryRecoverFromCurrentFile: CURRENT file does not exist")
		// Return an empty levels slice.
		return emptyLevels(), 0, err
	}
	// Read the CURRENT file
	content, err := os.ReadFile(currentFilePath)
	if err != nil {
		log.Println("tryRecoverFromCurrentFile: Error reading CURRENT file:", err)
		return emptyLevels(), 0, err
	}
	// Get the content of the CURRENT file
	checksumBytes := make([]byte, 4)
	copy(checksumBytes, content[:4])
	checksum := binary.LittleEndian.Uint32(checksumBytes)
	// Compute the checksum of the content
	computedChecksum := ComputeChecksum(content[4:])
	// Check if the checksum is correct
	if checksum != computedChecksum {
		log.Println("tryRecoverFromCurrentFile: Bad checksum")
		return emptyLevels(), 0, lib.ErrBadChecksum
	}

	// Get the manifest file path
	manifestFilePath := string(content[4:])
	return recoverFromManifest(manifestFilePath)
}

// recoverSparseIndexFromSegmentFile recovers the sparse index from the segment file.
// It returns an error if the segment file is corrupted.
func recoverSparseIndexFromSegmentFile(segmentFilePath string, memState *MemState) error {
	segmentID, err := GetSegmentIDFromSegmentFilePath(segmentFilePath)
	if err != nil {
		log.Println("recoverSparseIndexFromSegmentFile: Error getting segment ID from segment file:", err)
		return err
	}
	segmentFile, err := os.Open(segmentFilePath)
	if err != nil {
		log.Println("recoverSparseIndexFromSegmentFile: Error opening segment file:", err)
		return err
	}
	defer segmentFile.Close()
	segmentFile.Seek(0, io.SeekStart)

	var offset int64 = 0
	var cnt int = 0
	for {
		_, err = segmentFile.Seek(0, io.SeekCurrent)
		if err != nil {
			log.Println("recoverSparseIndexFromSegmentFile: Error seeking:", err)
			return err
		}
		record, err := getNextKVRecord(segmentFile)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Println("recoverSparseIndexFromSegmentFile: Error reading KV record:", err)
			return err
		}
		if cnt%16 == 0 {
			memState.AddSparseIndexEntry(segmentID, record.Key, offset)
		}
		offset += int64(record.Size())
		cnt++
	}

	return nil
}

// recoverMemStateFromLevels recovers the memstate from the levels by adding sparse index entries to the memstate.
func recoverMemStateFromLevels(indexDir string, levels [][]SegmentMetadata, memState *MemState) error {
	for _, level := range levels {
		for _, segmentMetadata := range level {
			sparseIndexFilePath := filepath.Join(indexDir, segmentMetadata.GetSparseIndexFileName())
			sparseIndexFile, err := os.Open(sparseIndexFilePath)
			if err != nil {
				log.Println("recoverMemStateFromLevels: Error opening sparse index file:", err)
				return err
			}
			defer sparseIndexFile.Close()
			sparseIndexFile.Seek(0, io.SeekStart)
			for {
				offset, _ := sparseIndexFile.Seek(0, io.SeekCurrent)
				sparseIndexRecord, err := getNextSparseIndexRecord(sparseIndexFile)
				if err != nil {
					if err == io.EOF {
						break
					}
					log.Println("recoverMemStateFromLevels: Error reading sparse index record:", err)
					os.Truncate(sparseIndexFilePath, offset)
					// Build the sparse index from the segment file
					// Delet the current sparse index
					memState.RemoveSparseIndexEntry(segmentMetadata.id)
					// Recover the sparse index from the segment file
					err = recoverSparseIndexFromSegmentFile(segmentMetadata.filePath, memState)
					if err != nil {
						log.Println("recoverMemStateFromLevels: Error recovering sparse index from segment file:", err)
						return err
					}
					// Also, flush the sparse index to the sparse index file.
					sparseIndexFilePath := filepath.Join(indexDir, getSparseIndexFileNameFromSegmentId(segmentMetadata.id))
					err = memState.FlushSparseIndex(sparseIndexFilePath)
					if err != nil {
						log.Println("recoverMemStateFromLevels: Error flushing sparse index to file:", err)
						return err
					}
					break
				}
				if sparseIndexRecord == nil {
					// No more records.
					break
				}
				memState.AddSparseIndexEntry(segmentMetadata.id, sparseIndexRecord.Key, sparseIndexRecord.Offset)
			}
		}
	}
	return nil
}

// recoverFromWALs recovers the WAL files and returns the WAL object.
func recoverFromWALs(lastSegmentID uint64, walDir string, memState *MemState, checkpointSize int64) (*WAL, error) {
	// Create the WAL directory if it doesn't exist.
	err := os.MkdirAll(walDir, 0755)
	if err != nil {
		log.Println("Error creating WAL directory:", err)
		return nil, err
	}
	wal := &WAL{
		dir: walDir,
	}
	walFiles, err := listWALFiles(walDir)
	if err != nil {
		log.Println("recoverFromWALs: Error listing WAL files:", err)
		return nil, err
	}
	// If no WAL files are found, create a new WAL file.
	if len(walFiles) == 0 {
		log.Println("recoverFromWALs: No WAL files found, creating a new WAL file")
		walFile, err := os.OpenFile(filepath.Join(walDir, getWalFileNameFromSegmentID(lastSegmentID+1)), AppendFlags, 0644)
		if err != nil {
			log.Println("recoverFromWALs: Error creating new WAL file:", err)
			return nil, err
		}
		wal.activeFile = walFile
		wal.dir = walDir
		wal.segmentSize = checkpointSize
		wal.lastSequenceNum = 0
		return wal, nil
	}
	// Sort all files by segment ID in ascending order.
	slices.SortFunc(walFiles, compareWalFilesAscending)
	// Get the last checkpoint file path from the WAL files (ascending order).
	for idx, walFile := range walFiles {
		segmentID, err := getSegmentIDFromWalFileName(walFile)
		if err != nil {
			log.Println("Error getting segment ID from WAL file:", err)
			// Skip this file if the name does not match the expected format.
			continue
		}
		// Process the WAL file only if the segment ID is greater than the last segment ID.
		if segmentID > lastSegmentID {
			log.Println("Recovering from WAL file:", walFile)
			isLastWal := idx == len(walFiles)-1
			// Open the WAL file.
			walFile, err := os.OpenFile(filepath.Join(walDir, walFile), AppendFlags, 0644)
			if err != nil {
				log.Printf("Error opening WAL file: %s: %v\n", walFile.Name(), err)
				log.Fatalf("Error opening WAL file: %s: %v\n", walFile.Name(), err)
			}

			if !isLastWal {
				// The last WAL file will be kept open for future appends.
				defer walFile.Close()
			}

			lastSequenceNum, err := recoverFromWALFile(walFile, memState, isLastWal)
			if err != nil {
				log.Fatalf("Error processing WAL file: %s: %v", walFile.Name(), err)
			}
			if isLastWal {
				// Construct the WAL
				wal = &WAL{
					activeFile:      walFile,
					dir:             walDir,
					segmentSize:     checkpointSize,
					lastSequenceNum: lastSequenceNum,
				}
			}
		}
	}
	return wal, nil
}

// recoverFromWALFile processes a WAL file and returns the last sequence number.
// It also updates the in-memory state.
// If the WAL file is the last one, it truncates the file to the last good offset.
func recoverFromWALFile(reader *os.File, memState *MemState, isLastWal bool) (uint64, error) {
	var goodOffset int64 = 0
	var lastSequenceNum uint64 = 0
	_, err := reader.Seek(0, io.SeekStart)
	if err != nil {
		log.Printf("Error seeking to start of WAL file: %s: %v\n", reader.Name(), err)
		return 0, err
	}
	// Scan the file to populate lastSequenceNum.
	for {
		offset, err := reader.Seek(0, io.SeekCurrent)
		if err != nil {
			log.Println("Error seeking:", err)
			return 0, err
		}

		record, err := recoverNextRecordV2(reader)
		// Check the current offset of the reader.
		if err != nil {
			// If we get an End-Of-File error, it's a clean stop.
			// This is the expected way to finish recovery.
			if err == io.EOF {
				log.Println("Completed recovery of WAL file:", reader.Name())
				goodOffset = offset
				break
			}
			// If we get a bad checksum, it means the last write was torn.
			// We stop here and trust the log up to this point.
			if err == lib.ErrBadChecksum || err == io.ErrUnexpectedEOF {
				if !isLastWal {
					// Having corruption in an intermediate WAL file is a fatal error!
					log.Fatalf("Bad checksum or unexpected EOF in WAL file: %s", reader.Name())
				}
				log.Println("Bad checksum or unexpected EOF", err.Error())
				goodOffset = offset
				break
			}
			// Any other error is unexpected.
			log.Printf("Error recovering next record in WAL file: %s: %v", reader.Name(), err)
			return 0, err
		}

		lastSequenceNum = record.SequenceNum
		// Update the in-memory state.
		ops, err := record.GetOps()
		if err != nil {
			log.Println("Error getting operations from log record:", err)
			return 0, err
		}
		for _, op := range ops {
			memState.Put(op.Key, op.Value)
		}
	}

	if isLastWal {
		// Truncate the file if it's the last WAL file.
		// This is done to remove the old records that have been recovered.
		log.Println("Truncating to", goodOffset, "for WAL file:", reader.Name())
		err = reader.Truncate(goodOffset)
		if err != nil {
			log.Println("Error truncating file:", err)
			return 0, err
		}
		// Move to the good offset for future appends if it's the last WAL file.
		_, err = reader.Seek(goodOffset, io.SeekStart)
		if err != nil {
			log.Println("Error seeking to good offset::", err)
			return 0, err
		}
	}
	return lastSequenceNum, nil
}

// tryRecoverSparseIndex tries to recover the sparse index from the checkpoint file. Returns the last segment ID.
func tryRecoverSparseIndex(dir string, memState *MemState) (uint64, error) {
	// Read the checkpoint if exists.
	lastCheckpointFilePath := tryGetLastCheckpoint(filepath.Join(dir, checkpointDir), filepath.Join(dir, logsDir))
	lastSegmentID := uint64(0)

	if lastCheckpointFilePath != "" {
		log.Println("tryRecoverSparseIndex: Last checkpoint found:", lastCheckpointFilePath)
		checkpointFileName := filepath.Base(lastCheckpointFilePath)
		segmentID, err := getSegmentIDFromSegmentFileName(checkpointFileName)
		if err != nil {
			log.Println("tryRecoverSparseIndex: Error getting segment ID from last checkpoint file:", err)
			return 0, lib.ErrCheckpointCorrupted
		}
		lastSegmentID = segmentID
		log.Println("tryRecoverSparseIndex: Last segment ID:", lastSegmentID)

		// Recover sparse index from sparse index file.
		sparseIndexFilePath := filepath.Join(dir, checkpointDir, getSparseIndexFileNameFromSegmentId(segmentID))
		offset, err := recoverFromSparseIndexFile(sparseIndexFilePath, memState)
		if err != nil {
			log.Println("tryRecoverSparseIndex: Error recovering from sparse index file:", err)
			if err == lib.ErrSparseIndexCorrupted {
				// Truncate the sparse index file to the last good offset.
				os.Truncate(sparseIndexFilePath, offset)
			}
			return lastSegmentID, err
		}
	}
	return lastSegmentID, nil
}

// Deprecated: Use recoverFromSparseIndexFileFromLevels instead.
// recoverFromSparseIndexFile recovers the sparse index from the sparse index file.
// It returns the last good offset and an error.
func recoverFromSparseIndexFile(filePath string, memState *MemState) (int64, error) {
	segmentID, err := GetSegmentIDFromIndexFilePath(filePath)
	if err != nil {
		log.Println("recoverFromSparseIndexFile: Error getting segment ID from index file:", err)
		return 0, err
	}
	sparseIndexFile, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		log.Println("recoverFromSparseIndexFile: Error opening sparse index file:", err)
		return 0, err
	}
	defer sparseIndexFile.Close()
	var offset int64 = 0
	sparseIndexFile.Seek(0, io.SeekStart)
	for {
		offset, err = sparseIndexFile.Seek(0, io.SeekCurrent)
		if err != nil {
			log.Println("recoverFromSparseIndexFile: Error seeking:", err)
			return 0, err
		}
		record, err := getNextSparseIndexRecord(sparseIndexFile)
		if err != nil {
			if err == io.EOF {
				log.Println("recoverFromSparseIndexFile: End of file reached")
				break
			}
			log.Println("recoverFromSparseIndexFile: Error getting next sparse index record:", err)
			// Return a special error to indicate that the checkpoint file is corrupted.
			return offset, lib.ErrSparseIndexCorrupted
		}
		if record == nil {
			log.Println("recoverFromSparseIndexFile: End of file reached")
			break
		}
		log.Printf("recoverFromSparseIndexFile: Adding sparse index entry: %s, %d\n", string(record.Key), record.Offset)
		// Add the entry to the sparse index in memory.
		memState.AddSparseIndexEntry(segmentID, record.Key, record.Offset)
	}
	return offset, nil
}
