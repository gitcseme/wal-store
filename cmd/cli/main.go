package main

import (
	"encoding/json"
	"fmt"
	"time"
	"walstore/internal/wal"
)

var (
	insertOp = 1
	updateOp = 2
	deleteOp = 3
)

type Record struct {
	Op    int    `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

func main() {
	defaultConfig := wal.CreateDefaultConfig("/home/shuvo/logs")
	walog, err := wal.StartLogger(defaultConfig)
	if err != nil {
		panic(err)
	}

	entries := []Record{
		{Op: insertOp, Key: "key1", Value: "value1"},
		{Op: updateOp, Key: "key2", Value: "value2"},
		{Op: deleteOp, Key: "key3", Value: "value3"},
	}

	for _, entry := range entries {
		marshaledData, err := json.Marshal(entry)
		if err != nil {
			panic(err)
		}

		if err := walog.WriteRecord(marshaledData); err != nil {
			panic(err)
		}
	}

	time.Sleep(2 * time.Second)

	writtenLogs, err := walog.ReadAllRecords()
	if err != nil {
		panic(err)
	}

	fmt.Println("Written log count: %d", len(writtenLogs))

	for _, log := range writtenLogs {
		logEntry := Record{}
		if err := json.Unmarshal(log.Data, &logEntry); err != nil {
			panic(err)
		}

		fmt.Println(log.LogSequenceNumber)
		fmt.Println(logEntry)
		fmt.Println("--------------------")
	}

	if err := walog.Close(); err != nil {
		panic(err)
	}

}
