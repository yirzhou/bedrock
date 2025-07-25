package bedrock

import (
	"bytes"
	"container/heap"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"wal/lib"
)

// CompactionPlan is the plan for the compaction.
// It contains the base segments and the overlapping segments.
type CompactionPlan struct {
	baseSegments        []SegmentMetadata
	overlappingSegments []SegmentMetadata
}

// GetAllSegments returns all the segments in the compaction plan.
func (c *CompactionPlan) GetAllSegments() []SegmentMetadata {
	segments := make([]SegmentMetadata, 0)
	segments = append(segments, c.baseSegments...)
	segments = append(segments, c.overlappingSegments...)
	return segments
}

// needsCompaction checks if the level needs compaction.
// For L0, it checks if the number of segments is greater than or equal to the threshold.
// For L1 and above, it checks if the total size of the segment files is greater than or equal to the threshold.
func (kv *KVStore) needsCompaction(level int) bool {
	if level == 0 && len(kv.levels[level]) >= segmentThresholdL0 {
		return true
	}

	// For L1 and above, we need to check the size of the segment file.
	totalFileSize := int64(0)
	for _, segmentMetadata := range kv.levels[level] {
		fileInfo, err := os.Stat(segmentMetadata.filePath)
		if err != nil {
			log.Println("needsCompaction: Error getting file info:", err)
			return false
		}
		totalFileSize += fileInfo.Size()
	}
	return totalFileSize >= kv.config.GetSegmentFileSizeThresholdLX()
}

func (kv *KVStore) getNextCompactionPlan() *CompactionPlan {
	for level := range len(kv.levels) {
		if !kv.needsCompaction(level) {
			continue
		}
		compactionPlan := kv.getCompactionPlan(level)
		if len(compactionPlan.baseSegments) > 0 {
			return compactionPlan
		}
	}
	return nil
}

// This method assumes that for L1 and above, we only find overlapping segments for the first segment file.
func (kv *KVStore) getCompactionPlan(level int) *CompactionPlan {
	// Get the segment metadata for the level.
	segmentMetadata := kv.levels[level]
	if level > 0 && len(segmentMetadata) > 0 {
		// Only get the first segment file instead.
		segmentMetadata = []SegmentMetadata{segmentMetadata[0]}
	}
	// Get the segment metadata for the next level.
	if level+1 >= len(kv.levels) {
		// Add a new level.
		kv.levels = append(kv.levels, make([]SegmentMetadata, 0))
	}
	// Get the segment metadata for the next level.
	nextLevelSegmentMetadata := kv.levels[level+1]

	segments := make([]SegmentMetadata, 0)
	for _, segment := range segmentMetadata {
		// Check if the segment has overlapping keys with the next level.
		for _, nextLevelSegment := range nextLevelSegmentMetadata {
			if segment.HasOverlappingKeys(&nextLevelSegment) {
				segments = append(segments, segment)
			}
		}
	}
	return &CompactionPlan{
		baseSegments:        segmentMetadata,
		overlappingSegments: segments,
	}
}

func GetTempSegmentFileName(segmentId uint64) string {
	return fmt.Sprintf("%stmp-%06d", segmentFilePrefix, segmentId)
}

func GetTempSparseIndexFileName(segmentId uint64) string {
	return fmt.Sprintf("%stmp-%06d", sparseIndexFilePrefix, segmentId)
}

func (s *KVStore) getTempSegmentFilePath(segmentId uint64) string {
	return filepath.Join(s.getCheckpointDir(), GetTempSegmentFileName(segmentId))
}

func (s *KVStore) getTempSparseIndexFilePath(segmentId uint64) string {
	return filepath.Join(s.getCheckpointDir(), GetTempSparseIndexFileName(segmentId))
}

// performMerge performs the compaction for the given compaction plan.
// It returns the path to the new segment file.
func (kv *KVStore) performMerge(compactionPlan *CompactionPlan) ([]SegmentMetadata, error) {
	// Create a temporary segment file.
	tempSegmentId := kv.AllocateNewSegmentID()
	tempFile, err := os.OpenFile(kv.getTempSegmentFilePath(tempSegmentId), AppendFlags, 0644)
	if err != nil {
		log.Println("performCompaction: Error opening temp file:", err)
		return nil, err
	}
	defer tempFile.Close()
	tempSparseIndexFile, err := os.OpenFile(kv.getTempSparseIndexFilePath(tempSegmentId), AppendFlags, 0644)
	if err != nil {
		log.Println("performCompaction: Error opening temp sparse index file:", err)
		return nil, err
	}
	defer tempSparseIndexFile.Close()

	// Create a min heap.
	pq := MinHeap{}
	heap.Init(&pq)

	// Create a list of segment metadata because it's possible that the merged file is too big and needs to be split further.
	segmentMetadataList := make([]SegmentMetadata, 0)
	currentNewSegmentOffset := int64(0)
	sparseIndexRecords := make(sparseIndex, 0)
	// Open all overlapping segments.
	pathToFile := make(map[string]*os.File)
	for _, segment := range compactionPlan.GetAllSegments() {
		segmentFile, err := os.OpenFile(segment.filePath, os.O_RDONLY, 0644)
		if err != nil {
			log.Println("performCompaction: Error opening segment file:", err)
			return nil, err
		}
		segmentFile.Seek(0, io.SeekStart)
		pathToFile[segment.filePath] = segmentFile
		record, err := getNextKVRecord(segmentFile)
		if err != nil && err != io.EOF {
			log.Println("performCompaction: Error getting next record:", err)
			return nil, err
		}
		if err == io.EOF {
			continue
		}
		heap.Push(&pq, &MinHeapRecord{
			Record:          record,
			SegmentFilePath: segment.filePath,
		})
	}
	var prevKey []byte = nil
	var minKey []byte = nil
	for pq.Len() > 0 {
		minRecord := heap.Pop(&pq).(*MinHeapRecord)
		// Initialize the min key.
		if minKey == nil {
			minKey = minRecord.Record.Key
		}
		var fileToAdvance string
		if prevKey != nil && bytes.Equal(minRecord.Record.Key, prevKey) {
			fileToAdvance = minRecord.SegmentFilePath
		} else {
			// Check if offset is over the threshold and split the file further.
			isTombstone := bytes.Equal(minRecord.Record.Value, lib.TOMBSTONE)
			if currentNewSegmentOffset+int64(minRecord.Record.Size()) > kv.config.GetSegmentFileSizeThresholdLX() && !isTombstone {
				// Split the file further.
				finalFilePath := kv.getSegmentFilePath(tempSegmentId)
				segmentMetadataList = append(segmentMetadataList, SegmentMetadata{
					filePath:    finalFilePath,
					minKey:      minKey,
					maxKey:      prevKey,
					level:       compactionPlan.baseSegments[0].level + 1,
					id:          tempSegmentId,
					sparseIndex: sparseIndexRecords,
				})
				// Create a new temp file.
				tempSegmentId = kv.AllocateNewSegmentID()
				tempFile, err = os.OpenFile(kv.getTempSegmentFilePath(tempSegmentId), AppendFlags, 0644)
				if err != nil {
					log.Println("performCompaction: Error opening temp file:", err)
					return nil, err
				}
				tempSparseIndexFile, err = os.OpenFile(kv.getTempSparseIndexFilePath(tempSegmentId), AppendFlags, 0644)
				if err != nil {
					log.Println("performCompaction: Error opening temp sparse index file:", err)
					return nil, err
				}
				defer tempSparseIndexFile.Close()
				defer tempFile.Close()
				currentNewSegmentOffset = 0
				minKey = nil
				sparseIndexRecords = make(sparseIndex, 0)
			}

			// Write the record to the temp file.
			if !isTombstone {
				tempFile.Write(minRecord.Record.Serialize())
				tempFile.Sync()
				// Update sparse index
				sparseIndexRecords = append(sparseIndexRecords, sparseIndexEntry{
					key:    minRecord.Record.Key,
					offset: currentNewSegmentOffset,
				})
				// Persist sparse index.
				sparseIndexBytes := GetSparseIndexBytes(tempSegmentId, minRecord.Record.Key, currentNewSegmentOffset)
				tempSparseIndexFile.Write(sparseIndexBytes)
				tempSparseIndexFile.Sync()
				currentNewSegmentOffset += int64(minRecord.Record.Size())
			}
			fileToAdvance = minRecord.SegmentFilePath
		}

		prevKey = minRecord.Record.Key
		// Advance the file
		record, err := getNextKVRecord(pathToFile[fileToAdvance])
		if err != nil && err != io.EOF {
			log.Println("performCompaction: Error getting next record:", err)
			return nil, err
		}
		if err == io.EOF {
			pathToFile[fileToAdvance].Close()
			delete(pathToFile, fileToAdvance)
		} else {
			heap.Push(&pq, &MinHeapRecord{
				Record:          record,
				SegmentFilePath: fileToAdvance,
			})
		}
	}
	// Add to the segment metadata list.
	segmentMetadataList = append(segmentMetadataList, SegmentMetadata{
		filePath:    kv.getSegmentFilePath(tempSegmentId),
		minKey:      minKey,
		maxKey:      prevKey,
		id:          tempSegmentId,
		level:       compactionPlan.baseSegments[0].level + 1,
		sparseIndex: sparseIndexRecords,
	})

	// Close all the segment files.
	for _, file := range pathToFile {
		file.Close()
	}
	log.Println("performCompaction: Compaction completed")
	return segmentMetadataList, nil
}
func (kv *KVStore) doCompaction() error {
	kv.lock.Lock()

	compactionPlan := kv.getNextCompactionPlan()
	if compactionPlan == nil {
		kv.lock.Unlock()
		return nil
	}
	kv.lock.Unlock()

	// Do the compaction.
	segmentMetadataList, err := kv.performMerge(compactionPlan)
	if err != nil {
		log.Println("doCompaction: Error performing compaction:", err)
		return err
	}

	kv.lock.Lock()
	defer kv.lock.Unlock()

	// Save the new levels.
	newLevels := getNewLevels(compactionPlan, kv.levels, segmentMetadataList)

	// Update MANIFEST.
	tmpManifestFilePath := filepath.Join(kv.getCheckpointDir(), "MANIFEST.tmp")
	err = flushManifestFileWithLevels(tmpManifestFilePath, newLevels)
	if err != nil {
		log.Println("doCompaction: Error flushing manifest file:", err)
		return err
	}

	// Atomically rename the manifest file.
	manifestSegmentID := kv.AllocateSegmentIDUnsafe()
	manifestFilePath := kv.getManifestFilePath(manifestSegmentID)
	err = os.Rename(tmpManifestFilePath, manifestFilePath)
	if err != nil {
		log.Println("doCompaction: Error renaming manifest file:", err)
		return err
	}

	// Update the CURRENT file.
	kv.updateCheckpointFile(manifestFilePath)

	// Rename all the segment files and sparse index files.
	for _, segmentMetadata := range segmentMetadataList {
		segmentID := segmentMetadata.id
		segmentFilePath := segmentMetadata.filePath
		sparseIndexFilePath := kv.getSparseIndexFilePath(segmentID)
		tempSegmentFilePath := kv.getTempSegmentFilePath(segmentID)
		tempSparseIndexFilePath := kv.getTempSparseIndexFilePath(segmentID)
		err := os.Rename(tempSegmentFilePath, segmentFilePath)
		if err != nil {
			log.Println("doCompaction: Error renaming segment file:", err)
			return err
		}
		err = os.Rename(tempSparseIndexFilePath, sparseIndexFilePath)
		if err != nil {
			log.Println("doCompaction: Error renaming sparse index file:", err)
			return err
		}
	}

	// Update the levels in memory
	kv.levels = newLevels

	// Update sparse index in memory
	kv.removeSparseIndexEntries(compactionPlan.GetAllSegments())
	kv.addSparseIndexEntriesBulk(segmentMetadataList)

	// Delete the old segments and indexes.
	err = kv.deleteOldSegmentsAndIndexes(compactionPlan)
	if err != nil {
		log.Println("doCompaction: Error deleting old segments and indexes:", err)
		return err
	}
	// Get the new starting segment ID.
	return nil
}

// addSparseIndexEntriesBulk adds the sparse index entries to the mem state.
func (kv *KVStore) addSparseIndexEntriesBulk(segmentMetadataList []SegmentMetadata) {
	for _, segmentMetadata := range segmentMetadataList {
		kv.memState.AddSparseIndexEntriesBulk(segmentMetadata.id, segmentMetadata.sparseIndex)
	}
}

// removeSparseIndexEntries removes the sparse index entries from the mem state.
func (kv *KVStore) removeSparseIndexEntries(segmentMetadataList []SegmentMetadata) {
	for _, segment := range segmentMetadataList {
		kv.memState.RemoveSparseIndexEntry(segment.id)
	}
}

// deleteOldSegmentsAndIndexes deletes the old segments and indexes.
func (kv *KVStore) deleteOldSegmentsAndIndexes(compactionPlan *CompactionPlan) error {
	segments := make([]SegmentMetadata, 0)
	segments = append(segments, compactionPlan.baseSegments...)
	segments = append(segments, compactionPlan.overlappingSegments...)

	for _, segmentMetadata := range segments {
		segmentID := segmentMetadata.id
		err := os.Remove(segmentMetadata.filePath)
		if err != nil {
			log.Println("deleteOldSegmentsAndIndexes: Error removing segment file:", err)
			return err
		}
		// Remove the sparse index file as well
		sparseIndexFilePath := kv.getSparseIndexFilePath(segmentID)
		err = os.Remove(sparseIndexFilePath)
		if err != nil {
			log.Println("deleteOldSegmentsAndIndexes: Error removing sparse index file:", err)
			return err
		}
	}
	return nil
}

// getNewLevels gets the new levels from the old levels and the segment metadata list.
func getNewLevels(compactionPlan *CompactionPlan, oldLevels [][]SegmentMetadata, segmentMetadataList []SegmentMetadata) [][]SegmentMetadata {
	newLevels := make([][]SegmentMetadata, len(oldLevels))
	for i := range len(oldLevels) {
		newLevels[i] = make([]SegmentMetadata, 0)
	}
	oldSegmentIDs := make(map[uint64]bool)
	for _, segmentMetadata := range compactionPlan.baseSegments {
		oldSegmentIDs[segmentMetadata.id] = true
	}
	for _, segmentMetadata := range compactionPlan.overlappingSegments {
		oldSegmentIDs[segmentMetadata.id] = true
	}
	// Add old segments to the new levels but skip the ones that got compacted.
	for _, level := range oldLevels {
		for _, segmentMetadata := range level {
			if !oldSegmentIDs[segmentMetadata.id] {
				newLevels[segmentMetadata.level] = append(newLevels[segmentMetadata.level], segmentMetadata)
			}
		}
	}
	// Add new segments to the new levels.
	for _, segmentMetadata := range segmentMetadataList {
		if segmentMetadata.level == uint32(len(newLevels)) {
			newLevels[segmentMetadata.level] = make([]SegmentMetadata, 0)
		}
		newLevels[segmentMetadata.level] = append(newLevels[segmentMetadata.level], segmentMetadata)
	}
	return newLevels
}
