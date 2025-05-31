package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	pb "walproto/proto"

	gpb "google.golang.org/protobuf/proto"
)

type WriteAheadLog struct {
	Directory          string
	CurrentSegmentFile *os.File
	BufferWriter       *bufio.Writer
}

func (wal *WriteAheadLog) ReadAllRecords() ([]*pb.WalRecord, error) {
	file, err := os.OpenFile(wal.CurrentSegmentFile.Name(), os.O_RDONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL segment file: %w", err)
	}
	defer file.Close()
	var walRecords []*pb.WalRecord

	for {
		var recordSize int32
		// Read the size of the next record
		if err := binary.Read(file, binary.LittleEndian, &recordSize); err != nil {
			if err == io.EOF {
				// End of file reached, break the loop
				break
			}
			return walRecords, err
		}

		data := make([]byte, recordSize)
		// Read the record data
		if _, err := io.ReadFull(file, data); err != nil {
			return walRecords, err
		}

		var record pb.WalRecord
		if err := gpb.Unmarshal(data, &record); err != nil {
			return walRecords, err
		}

		walRecords = append(walRecords, &record)
	}

	return walRecords, nil
}

func (wal *WriteAheadLog) Write(record pb.WalRecord) error {
	marshaledRecord, err := gpb.Marshal(&record)
	if err != nil {
		return fmt.Errorf("failed to marshal record: %w", err)
	}
	recordSize := int32(len(marshaledRecord))
	// write the record size to the buffer
	if err := binary.Write(wal.BufferWriter, binary.LittleEndian, recordSize); err != nil {
		return fmt.Errorf("failed to write record size: %w", err)
	}
	// write the marshaled record to the buffer
	_, err = wal.BufferWriter.Write(marshaledRecord)

	return err
}
