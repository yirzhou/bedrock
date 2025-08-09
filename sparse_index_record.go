package main

import (
	"encoding/binary"
	"errors"
)

type SparseIndexRecord struct {
	Checksum  uint32 // 4 bytes
	SegmentID uint64 // 8 bytes
	KeySize   uint32 // 4 bytes
	Offset    int64  // 8 bytes
	Key       []byte // KeySize bytes
}



func DecodeSparseIndexHeader(headerBytes []byte) (*SparseIndexRecord, error) {
	if len(headerBytes) < 24 {
		return nil, errors.New("DecodeSparseIndexHeader: data too short")
	}
	checksum := binary.LittleEndian.Uint32(headerBytes[:4])
	segmentID := binary.LittleEndian.Uint64(headerBytes[4:12])
	keySize := binary.LittleEndian.Uint32(headerBytes[12:16])
	offset := binary.LittleEndian.Uint64(headerBytes[16:24])
	return &SparseIndexRecord{Checksum: checksum, SegmentID: segmentID, KeySize: keySize, Offset: int64(offset)}, nil
}

// GetSparseIndexBytes returns the bytes of the sparse index record.
func GetSparseIndexBytes(segmentID uint64, key []byte, offset int64) []byte {
	// Create the header except for the checksum.
	headers := make([]byte, 20)
	binary.LittleEndian.PutUint64(headers[0:8], segmentID)
	binary.LittleEndian.PutUint32(headers[8:12], uint32(len(key)))
	binary.LittleEndian.PutUint64(headers[12:20], uint64(offset))
	// Create the data by combining the header and the key.
	data := append(headers, key...)
	// Compute the checksum.
	checksum := ComputeChecksum(data)
	// Create the checksum bytes.
	checksumBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(checksumBytes, checksum)
	// Combine the checksum and the data to form the full sparse index record.
	data = append(checksumBytes, data...)
	return data
}
