package main

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/hashicorp/raft"
)

const (
	OP_PUT                           byte = 0x01
	OP_DELETE                        byte = 0x02
	SNAPSHOT_MANIFEST_FILE_TYPE      byte = 0x01
	SNAPSHOT_MANIFEST_FILE_PATH_TYPE byte = 0x02
	SNAPSHOT_MEMSTATE_FILE_TYPE      byte = 0x03
	SNAPSHOT_SEGMENT_FILE_TYPE       byte = 0x04
	SNAPSHOT_INDEX_FILE_TYPE         byte = 0x05
)

// fsmSnapshot is our implementation of the raft.FSMSnapshot interface.
type fsmSnapshot struct {
	kv *KVStore // A reference to the KVStore to access its state
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	// 1. Get a consistent view of the database state.
	// We need to acquire a read lock to ensure that a compaction doesn't
	// change the list of files while we are creating the snapshot.
	s.kv.lock.RLock()
	defer s.kv.lock.RUnlock()

	// 2. Write the MANIFEST file to the sink first.
	// The manifest is the "table of contents" for our snapshot.
	manifestFilePath, err := s.kv.GetCurrentManifestFilePath()
	if err != nil {
		// Manifest file doesn't exist yet.
		// Write memstate to the sink.
		// Write the memstate to the sink.
		err := s.writeMemtableSnapshot(sink)
		if err != nil {
			return err
		}
	} else {
		// Write the manifest file to the sink.
		err = s.writeManifestSnapshot(manifestFilePath, sink)
		if err != nil {
			return err
		}
	}

	// 3. Finalize the sink.
	return sink.Close()
}

// writeBlock writes a block of data to the sink.
// Format: [BlockType (1B)] [DataLen (4B)] [Data]
func writeBlock(blockType byte, data []byte, sink raft.SnapshotSink) error {
	// Using bytes.Buffer is more efficient and less error-prone.
	buf := new(bytes.Buffer)
	if err := buf.WriteByte(blockType); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(data))); err != nil {
		return err
	}
	if _, err := buf.Write(data); err != nil {
		return err
	}
	if _, err := sink.Write(buf.Bytes()); err != nil {
		return err
	}
	return nil
}

// writeMemtableSnapshot writes the memtable snapshot to the sink.
// Format: [{KVRecordBytes}]
func (s *fsmSnapshot) writeMemtableSnapshot(sink raft.SnapshotSink) error {
	// Create KVRecords.
	dataBytes, err := s.kv.memState.Serialize()
	if err != nil {
		log.Println("writeMemtableSnapshot: Error serializing memtable:", err)
		return err
	}
	return writeBlock(byte(SNAPSHOT_MEMSTATE_FILE_TYPE), dataBytes, sink)
}

// writeManifestSnapshot writes the manifest file to the sink.
// It also adds the type to the beginning of the bytes.
//
// Format: type (1 byte)
//
// Content:
// [{segmentMetadataBytes, sparseIndexBytesSize, sparseIndexBytes, segmentDataBytesSize, segmentDataBytes}]
func (s *fsmSnapshot) writeManifestSnapshot(manifestFilePath string, sink raft.SnapshotSink) error {
	// 1. Stream the manifest file.
	if err := streamFileToSink(sink, byte(SNAPSHOT_MANIFEST_FILE_TYPE), manifestFilePath); err != nil {
		log.Println("writeManifest: Error streaming manifest file:", err)
		return err
	}

	// 2. Stream all the required segment and index files.
	// We iterate through the in-memory 'levels' structure, which is a mirror of the manifest.
	for _, level := range s.kv.levels {
		for _, segmentMeta := range level {
			// Stream the data segment file.
			if err := streamFileToSink(sink, byte(SNAPSHOT_SEGMENT_FILE_TYPE), segmentMeta.filePath); err != nil {
				log.Println("writeManifest: Error streaming segment file:", err)
				return err
			}

			// Stream the index file.
			indexFilePath := s.kv.getSparseIndexFilePath(segmentMeta.id)
			if err := streamFileToSink(sink, byte(SNAPSHOT_INDEX_FILE_TYPE), indexFilePath); err != nil {
				log.Println("writeManifest: Error streaming index file:", err)
				return err
			}
		}
	}
	return nil
}

func (s *fsmSnapshot) Release() {
	// Our snapshot implementation doesn't hold any open file handles,
	// so this method can be empty.
}

func (s *KVStore) Apply(logEntry *raft.Log) interface{} {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Apply the log to the state machine.
	command := logEntry.Data
	opType := command[0]

	switch opType {
	case OP_PUT:
		key, value := decodePutCommand(command)
		s.memState.Put(key, value)
		return nil
	case OP_DELETE:
		key := decodeDeleteCommand(command)
		s.memState.Delete(key)
		return nil
	}
	log.Printf("Error: unknown command type received in Apply: %v", opType)
	return nil

}

func (s *KVStore) Snapshot() (raft.FSMSnapshot, error) {
	// Just return a new instance of our snapshot struct.
	return &fsmSnapshot{kv: s}, nil
}

func (s *KVStore) Restore(snapshot io.ReadCloser) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	log.Println("Restore: Starting to restore from snapshot.")

	// 1. Wipe the current state.
	if err := s.clearAllData(); err != nil {
		return err
	}

	manifestFilePath := ""

	// 2. Read the snapshot stream block by block.
	for {
		// Read the block type.
		typeByte := make([]byte, 1)
		if _, err := io.ReadFull(snapshot, typeByte); err != nil {
			if err == io.EOF {
				log.Println("Restore: Reached end of snapshot.")
				break // Clean end of snapshot
			}
			log.Println("Restore: Error reading block type:", err)
			return err
		}

		blockType := typeByte[0]

		if blockType == byte(SNAPSHOT_MEMSTATE_FILE_TYPE) {
			// This snapshot is just a memtable.
			lenBuf := make([]byte, 4)
			if _, err := io.ReadFull(snapshot, lenBuf); err != nil {
				return err
			}
			dataLen := binary.LittleEndian.Uint32(lenBuf)
			data := make([]byte, dataLen)
			if _, err := io.ReadFull(snapshot, data); err != nil {
				return err
			}
			return s.memState.Deserialize(data)
		} else {
			// This is a file-based snapshot. Read the file path and data.
			pathLenBuf := make([]byte, 4)
			if _, err := io.ReadFull(snapshot, pathLenBuf); err != nil {
				return err
			}
			pathLen := binary.LittleEndian.Uint32(pathLenBuf)
			pathBuf := make([]byte, pathLen)
			if _, err := io.ReadFull(snapshot, pathBuf); err != nil {
				return err
			}
			filePath := string(pathBuf)
			if blockType == byte(SNAPSHOT_MANIFEST_FILE_TYPE) {
				manifestFilePath = filePath
			}
			dataLenBuf := make([]byte, 4)
			if _, err := io.ReadFull(snapshot, dataLenBuf); err != nil {
				return err
			}
			dataLen := binary.LittleEndian.Uint32(dataLenBuf)

			// Create the file and copy the data from the snapshot.
			if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
				log.Println("Restore: Error creating directory:", err)
				return err
			}
			file, err := os.Create(filePath)
			if err != nil {
				return err
			}
			if _, err := io.CopyN(file, snapshot, int64(dataLen)); err != nil {
				file.Close()
				return err
			}
			file.Close()
		}
	}

	// 3. After all files are written, load the new state from the manifest.
	log.Println("Restore: All snapshot files written. Reloading state from new manifest.")
	if manifestFilePath == "" {
		log.Panicln("Restore: No manifest file path found in snapshot.")
	}
	log.Println("Restore: Manifest file path:", manifestFilePath)
	return s.recoverFromManifest(manifestFilePath)
}

// encodePutCommand serializes a Put operation into a byte slice.
func encodePutCommand(key, value []byte) []byte {
	// Allocate a buffer with enough space.
	// 1 byte for op-code + 4 for keyLen + key + 4 for valueLen + value
	buf := make([]byte, 1+4+len(key)+4+len(value))

	// Write the op-code.
	buf[0] = OP_PUT

	// Write the key length and the key.
	binary.LittleEndian.PutUint32(buf[1:5], uint32(len(key)))
	copy(buf[5:], key)

	// Write the value length and the value.
	offset := 5 + len(key)
	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(value)))
	copy(buf[offset+4:], value)

	return buf
}

// encodeDeleteCommand serializes a Delete operation into a byte slice.
func encodeDeleteCommand(key []byte) []byte {
	buf := make([]byte, 1+4+len(key))
	buf[0] = OP_DELETE
	binary.LittleEndian.PutUint32(buf[1:5], uint32(len(key)))
	copy(buf[5:], key)
	return buf
}

// [OP_PUT (1 byte)] [KeyLen (4 byte)] [Key (variable)] [ValueLen (4 byte)] [Value (variable)]
// All encoding and using little endian.
func decodePutCommand(command []byte) (key, value []byte) {
	if command[0] != OP_PUT {
		log.Panicf("Error: decodePutCommand received a non-PUT command")
	}
	keyLen := binary.LittleEndian.Uint32(command[1:5])
	key = command[5 : 5+keyLen]
	valueLen := binary.LittleEndian.Uint32(command[5+keyLen : 5+keyLen+4])
	value = command[5+keyLen+4 : 5+keyLen+4+valueLen]
	return key, value
}

// [OP_DELETE (1 byte)] [KeyLen (4 byte)] [Key (variable)]
// All encoding and using little endian.
func decodeDeleteCommand(command []byte) (key []byte) {
	if command[0] != OP_DELETE {
		log.Panicf("Error: decodeDeleteCommand received a non-DELETE command")
	}
	keyLen := binary.LittleEndian.Uint32(command[1:5])
	key = command[5 : 5+keyLen]
	return key
}

// streamFileToSink streams a file to the sink.
// Format: [BlockType (1B)] [DataLen (4B)] [Data]
func streamFileToSink(sink raft.SnapshotSink, blockType byte, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return err
	}

	// Write the header for this file block.
	headerBuf := new(bytes.Buffer)
	headerBuf.WriteByte(blockType)
	binary.Write(headerBuf, binary.LittleEndian, uint32(len(filePath)))
	headerBuf.WriteString(filePath)
	binary.Write(headerBuf, binary.LittleEndian, uint32(fileInfo.Size())) // Announce the total size
	if _, err := sink.Write(headerBuf.Bytes()); err != nil {
		return err
	}

	// Now, copy the file's contents directly to the sink in chunks.
	// This avoids loading the whole file into memory.
	_, err = io.Copy(sink, file)
	return err
}

// recoverFromManifest recovers the state from the manifest file.
func (s *KVStore) recoverFromManifest(manifestFilePath string) error {
	levels, segmentID, err := recoverFromManifest(manifestFilePath)
	if err != nil {
		log.Println("recoverFromManifest: Error recovering from manifest:", err)
		return err
	}
	s.levels = levels
	s.activeSegmentID = segmentID + 1

	// Recover memstate from levels.
	err = recoverMemStateFromLevels(filepath.Dir(manifestFilePath), levels, s.memState)
	if err != nil {
		log.Println("recoverFromManifest: Error recovering memstate from levels:", err)
		return err
	}
	// Open a new WAL file.
	walFile, err := os.OpenFile(filepath.Join(s.wal.dir, getWalFileNameFromSegmentID(s.activeSegmentID)), AppendFlags, 0644)
	if err != nil {
		log.Println("recoverFromManifest: Error opening WAL file:", err)
		return err
	}
	s.wal = &WAL{
		dir:             s.wal.dir,
		activeFile:      walFile,
		segmentSize:     s.config.GetCheckpointSize(),
		lastSequenceNum: 0,
	}

	return nil
}
