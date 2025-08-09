package main

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"bedrock/lib"
)

// The new log record format in which the header is 25 bytes: 4 + 8 + 8 + 1 + 4 = 25
// +----------+---------------+-----------+-----------+-------------+---------+
// | Checksum | Sequence Num  | Term      | RecordType| PayloadSize | Payload |
// | (4 bytes)|   (8 bytes)   | (8 bytes) | (1 byte)  |  (4 bytes)  | ...     |
// +----------+---------------+-----------+-----------+-------------+---------+
// | <----------- Header (25 bytes) ------------------------------> |         |
// The record type can be:
// 0x01 -> Put -- a single, non-transactional write: [KeySize (4B)] [ValueSize (4B)] [Key (Var)] [Value (Var)]
// 0x02 -> Commit -- [NumOps (4B)] [Op1_KeySize] [Op1_ValueSize] [Op1_Key] [Op1_Value] [Op2_KeySize] ...
type LogRecordV2 struct {
	CheckSum    uint32
	SequenceNum uint64
	Term        uint64
	RecordType  byte
	PayloadSize uint32
	Payload     []byte
}











// GetPayloadForPut returns the payload for a single, non-transactional write.
func GetPayloadForPut(key []byte, value []byte) []byte {
	payload := make([]byte, 8)
	binary.LittleEndian.PutUint32(payload[0:4], uint32(len(key)))
	binary.LittleEndian.PutUint32(payload[4:8], uint32(len(value)))
	payload = append(payload, key...)
	payload = append(payload, value...)
	return payload
}

// GetPayloadForCommit returns the payload for a commit operation.
// The commit is a list of PUT operations.
func GetPayloadForCommit(ops [][]byte) []byte {
	numOps := len(ops)
	payload := make([]byte, 4)
	binary.LittleEndian.PutUint32(payload[0:4], uint32(numOps))
	for _, op := range ops {
		payload = append(payload, op...)
	}
	return payload
}

// SerializeV2 returns the bytes of the log record.
func SerializeV2(recordType byte, sequenceNum uint64, term uint64, payload []byte) []byte {
	// This is a single, non-transactional write.
	payloadSize := uint32(len(payload))
	header := make([]byte, 25)
	// Put the sequence number in the header.
	binary.LittleEndian.PutUint32(header[4:12], uint32(sequenceNum))
	// Put the term in the header.
	binary.LittleEndian.PutUint64(header[12:20], term)
	// Put the record type in the header.
	header[20] = recordType
	// Put the payload size in the header.
	binary.LittleEndian.PutUint32(header[21:25], payloadSize)
	// Append the payload to the header.
	header = append(header, payload...)
	// Calculate the checksum.
	checksum := crc32.ChecksumIEEE(header[4:])
	// Put the checksum in the header.
	binary.LittleEndian.PutUint32(header[0:4], checksum)
	return header
}

// DeserializeV2 deserializes a log record from a byte array.
func DeserializeV2(data []byte) (LogRecordV2, error) {
	if len(data) < 25 {
		return LogRecordV2{}, fmt.Errorf("log record is too short")
	}

	record := LogRecordV2{}
	record.CheckSum = binary.LittleEndian.Uint32(data[0:4])
	record.SequenceNum = binary.LittleEndian.Uint64(data[4:12])
	record.RecordType = data[12]
	record.Term = binary.LittleEndian.Uint64(data[12:20])
	record.PayloadSize = binary.LittleEndian.Uint32(data[21:25])
	record.Payload = data[25:]
	return record, nil
}

// GetOps returns the operations in the log record.
// If the record type is a Put, it returns a single operation.
// If the record type is a Commit, it returns a list of operations.
// Note that each KVRecord is assumed to be valid at this point so it contains no checksum.
func (r *LogRecordV2) GetOps() ([]KVRecord, error) {
	// If the type is a Put, it's a single record.
	if r.RecordType == lib.TXN_PUT {
		record := KVRecord{}
		record.CheckSum = r.CheckSum
		// Single record.
		record.KeySize = binary.LittleEndian.Uint32(r.Payload[0:4])
		record.ValueSize = binary.LittleEndian.Uint32(r.Payload[4:8])
		record.Key = r.Payload[8 : 8+record.KeySize]
		record.Value = r.Payload[8+record.KeySize : 8+record.KeySize+record.ValueSize]
		return []KVRecord{record}, nil
	} else if r.RecordType == lib.TXN_COMMIT {
		// Multiple payloads.
		numOps := binary.LittleEndian.Uint32(r.Payload[0:4])
		ops := make([]KVRecord, numOps)
		currentOffset := 4
		for i := 0; i < int(numOps); i++ {
			// Each key and value is 4 bytes.
			ops[i].KeySize = binary.LittleEndian.Uint32(r.Payload[currentOffset : currentOffset+4])
			ops[i].ValueSize = binary.LittleEndian.Uint32(r.Payload[currentOffset+4 : currentOffset+8])
			keyStart := currentOffset + 8
			keyEnd := keyStart + int(ops[i].KeySize)
			valueStart := keyEnd
			valueEnd := valueStart + int(ops[i].ValueSize)
			ops[i].Key = r.Payload[keyStart:keyEnd]
			ops[i].Value = r.Payload[valueStart:valueEnd]
			currentOffset = valueEnd
		}
		return ops, nil
	} else {
		return nil, fmt.Errorf("invalid record type: %d", r.RecordType)
	}
}
