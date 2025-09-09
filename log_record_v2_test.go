package bedrock

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeserializeV2(t *testing.T) {
	// Test case 1: Valid LogRecordV2
	recordType := byte(0x01) // Put
	sequenceNum := uint64(1)
	term := uint64(1)
	key := []byte("testKey")
	value := []byte("testValue")
	payload := GetPayloadForPut(key, value)
	serializedData := SerializeV2(recordType, sequenceNum, term, payload)

	deserializedRecord, err := DeserializeV2(serializedData)
	assert.NoError(t, err)
	assert.Equal(t, recordType, deserializedRecord.RecordType)
	assert.Equal(t, sequenceNum, deserializedRecord.SequenceNum)
	assert.Equal(t, term, deserializedRecord.Term)
	assert.Equal(t, uint32(len(payload)), deserializedRecord.PayloadSize)
	assert.Equal(t, payload, deserializedRecord.Payload)

	// Test case 2: Data too short (less than 25 bytes)
	shortData := []byte{1, 2, 3, 4, 5}
	_, err = DeserializeV2(shortData)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "log record is too short")
}
