package tests

import (
	"encoding/json"
	"os"
	"testing"
	"walstore/internal/wal"

	"github.com/stretchr/testify/assert"
)

func Test_WriteAndRecover(t *testing.T) {
	logDirectory := "/tmp/wal_test"
	defer os.RemoveAll(logDirectory) // Clean up after test

	defaultConfig := wal.CreateDefaultConfig(logDirectory)

	walog, err := wal.StartLogger(defaultConfig)
	if err != nil {
		t.Fatalf("Failed to start logger: %v", err)
	}

	testData := []TestRecord{
		{Op: 1, Key: "key1", Value: "value1"},
		{Op: 2, Key: "key2", Value: "value2"},
		{Op: 3, Key: "key3", Value: "value3"},
	}

	for _, record := range testData {
		marshaledData, err := json.Marshal(record)
		assert.NoError(t, err, "Failed to marshal record")
		assert.NoError(t, walog.WriteRecord(marshaledData), "Failed to write record")
	}

	// The WAL sync before closing
	assert.NoError(t, walog.Close(), "Failed to close logger")

	writtenLogs, err := walog.ReadAllRecords()
	assert.NoError(t, err, "Failed to read records")
	assert.Equal(t, len(testData), len(writtenLogs), "Number of written logs does not match")

	for i, log := range writtenLogs {
		var logEntry TestRecord
		err := json.Unmarshal(log.Data, &logEntry)
		assert.NoError(t, err, "Failed to unmarshal log entry")
		assert.Equal(t, testData[i].Op, logEntry.Op, "Operation does not match")
		assert.Equal(t, testData[i].Key, logEntry.Key, "Key does not match")
		assert.Equal(t, testData[i].Value, logEntry.Value, "Value does not match")
	}
}

func Test_LogSequenceNumber(t *testing.T) {
	logDirectory := "/tmp/wal_test_seq"
	defer os.RemoveAll(logDirectory) // Clean up after test

	defaultConfig := wal.CreateDefaultConfig(logDirectory)

	walog, err := wal.StartLogger(defaultConfig)
	if err != nil {
		t.Fatalf("Failed to start logger: %v", err)
	}

	testData := []TestRecord{
		{Op: 1, Key: "key1", Value: "value1"},
		{Op: 2, Key: "key2", Value: "value2"},
		{Op: 3, Key: "key3", Value: "value3"},
		{Op: 4, Key: "key4", Value: "value4"},
		{Op: 5, Key: "key5", Value: "value5"},
	}

	for _, record := range testData {
		marshaledData, err := json.Marshal(record)
		assert.NoError(t, err, "Failed to marshal record")
		assert.NoError(t, walog.WriteRecord(marshaledData), "Failed to write record")
	}

	// Sync the WAL before closing
	assert.NoError(t, walog.Close(), "Failed to close logger")

	writtenLogs, err := walog.ReadAllRecords()
	assert.NoError(t, err, "Failed to read records")
	assert.Equal(t, len(testData), len(writtenLogs), "Number of written logs does not match")

	lastLogEntry := writtenLogs[len(writtenLogs)-1]

	assert.Equal(t, uint64(len(testData)), lastLogEntry.GetLogSequenceNumber(), "Last log sequence number does not match")
}
