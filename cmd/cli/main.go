package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	pb "walproto/proto"
	"walstore/internal/wal"
)

func main() {
	fmt.Println("Starting WAL CLI...")
	dir, _ := os.Getwd()
	fmt.Printf("CWD: %s\n", dir)

	logDir := "wal_directory"
	logFile := "wal_segment_0001"
	logPath := filepath.Join(logDir, logFile)

	os.MkdirAll(logDir, 0755)
	// Create the log file if it doesn't exist
	file, err := os.Create(logPath)
	if err != nil {
		fmt.Printf("Error creating log file: %v\n", err)
		return
	}

	walInstance := wal.WriteAheadLog{
		Directory:          logDir,
		CurrentSegmentFile: file,
		BufferWriter:       bufio.NewWriter(file),
	}

	for i := 1; i <= 5; i++ {
		walRecord := pb.WalRecord{
			LogSequenceNumber: uint64(i), // Log Sequence Number
			Timestamp:         uint64(i), // Timestamp of the record
			Data:              []byte(fmt.Sprintf("Sample data for WAL record %d", i)),
			Checksum:          uint32(i), // Checksum for data integrity
		}

		if err := walInstance.Write(walRecord); err != nil {
			fmt.Printf("Error writing WAL record: %v\n", err)
			return
		}
	}

	walInstance.BufferWriter.Flush()
	walInstance.CurrentSegmentFile.Close()
	fmt.Println("WAL record written successfully.")

	// Read from the WAL
	walRecords, err := walInstance.ReadAllRecords()
	if err != nil {
		fmt.Printf("Error reading WAL records: %v\n", err)
		return
	}

	// print the read records
	for _, record := range walRecords {
		fmt.Println(record)
		fmt.Println("----------------------")
	}
}
