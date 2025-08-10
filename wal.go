package main

import (
	"encoding/binary"
	"io"
	"log"
	"os"
	"sync"

	"github.com/yirzhou/bedrock/lib"
)

const (
	headerSize    = 20
	newHeaderSize = 25
	checksumSize  = 4
	// CheckpointSize = 64 * 1024 // 64KiB
	CheckpointSize = 1024 // 1KiB for testing
	AppendFlags    = os.O_RDWR | os.O_CREATE | os.O_APPEND
)

type WAL struct {
	// Lock is needed because the WAL can be a standalone component used by other components so it must take care of its own concurrency.
	mu sync.RWMutex

	// The directory where the WAL files are stored.
	dir string

	// The file handle for the current, active segment we are writing to.
	activeFile *os.File

	// The max size for each segment file before we roll to a new one.
	segmentSize int64

	// The last sequence number we have written to the active segment.
	lastSequenceNum uint64
}

// AppendTransaction appends a transaction record to the log.
func (l *WAL) AppendTransaction(term uint64, payload []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// 1. Increment the sequence number.
	l.lastSequenceNum++

	// 2. Create the record using this new sequence number in binary format.
	data := SerializeV2(lib.TXN_COMMIT, l.lastSequenceNum, term, payload)

	// 3. Write and Sync.
	_, err := l.activeFile.Write(data)
	if err != nil {
		log.Println("Error writing commit record:", err)
		return err
	}
	err = l.activeFile.Sync()
	if err != nil {
		log.Println("Error syncing file:", err)
		return err
	}

	// 4. Check if the log has reached its size threshold.
	fileInfo, err := l.activeFile.Stat()
	if err != nil {
		log.Println("Error getting file info:", err)
		return err
	}

	// 5. Check if the segment size has been reached. If so, roll to a new segment
	if fileInfo.Size() >= l.segmentSize {
		// Special error to signal that the log has reached its size threshold.
		// This is used to trigger a checkpoint.
		log.Println("WAL is ready to be checkpointed")
		return lib.ErrCheckpointNeeded
	}
	return nil
}

// Append appends a new record to the log.
func (l *WAL) Append(key, value []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// 1. Increment the sequence number.
	l.lastSequenceNum++

	// 2. Encode the record to its binary format.
	encodedRecord := SerializeV2(lib.TXN_PUT, l.lastSequenceNum, 0, GetPayloadForPut(key, value))

	// 3. Write and Sync.
	_, err := l.activeFile.Write(encodedRecord)
	if err != nil {
		return err
	}

	err = l.activeFile.Sync() // fsync()
	if err != nil {
		log.Println("Error syncing file:", err)
		return err
	}

	// 4. Check if the log has reached its size threshold.
	fileInfo, err := l.activeFile.Stat()
	if err != nil {
		log.Println("Error getting file info:", err)
		return err
	}

	// 5. Check if the segment size has been reached. If so, roll to a new segment
	if fileInfo.Size() >= l.segmentSize {
		// Special error to signal that the log has reached its size threshold.
		// This is used to trigger a checkpoint.
		log.Println("WAL is ready to be checkpointed")
		return lib.ErrCheckpointNeeded
	}
	return nil
}

// recoverNextRecordV2 recovers the next record from the WAL file.
func recoverNextRecordV2(reader io.Reader) (*LogRecordV2, error) {
	buf := make([]byte, newHeaderSize)
	_, err := io.ReadFull(reader, buf)
	if err != nil {
		if err != io.EOF {
			log.Println("Error reading the header of the next WAL record v2:", err)
		}
		return nil, err
	}

	// Read the header fields.
	checksum := binary.LittleEndian.Uint32(buf[:checksumSize])
	sequenceNum := binary.LittleEndian.Uint64(buf[checksumSize : checksumSize+8])
	term := binary.LittleEndian.Uint64(buf[checksumSize+8 : checksumSize+16])
	recordType := buf[checksumSize+16]
	payloadSize := binary.LittleEndian.Uint32(buf[checksumSize+17 : newHeaderSize])

	// Read the payload.
	payloadBuf := make([]byte, payloadSize)
	_, err = io.ReadFull(reader, payloadBuf)
	if err != nil {
		if err != io.EOF {
			log.Println("Error reading the payload of the next WAL record v2:", err)
		}
		return nil, err
	}
	// Compute checksum of entire record.
	dataToVerify := append(buf[checksumSize:], payloadBuf...)
	computedChecksum := ComputeChecksum(dataToVerify)
	if computedChecksum != checksum {
		// Bad checksum.
		log.Println("Error reading next WAL record: checksum mismatch")
		return nil, lib.ErrBadChecksum
	}
	return &LogRecordV2{
		CheckSum:    checksum,
		SequenceNum: sequenceNum,
		Term:        term,
		RecordType:  recordType,
		PayloadSize: payloadSize,
		Payload:     payloadBuf,
	}, nil
}

// Close shuts down the log file.
func (l *WAL) Close() error {
	// Implementation will:
	// 1. Lock the mutex.
	// 2. Defer unlocking.
	// 3. Close the file handle: l.file.Close()
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.activeFile.Close()
}
