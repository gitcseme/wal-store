package wal

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
	pb "walproto/proto"

	gpb "google.golang.org/protobuf/proto"
)

var (
	SyncInterval  = 500 * time.Millisecond // Default sync interval
	SegmentPrefix = "wal-segment-"         // Default segment file prefix
)

func StartLogger(config *Config) error {
	if err := validateConfig(config); err != nil {
		return err
	}

	segmentFile, segmentNumber, err := loadLastSegmentFile(config)
	if err != nil {
		return err
	}

	// seek to the end of the file to start writing new records
	if _, err := segmentFile.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("Failed to seek: %w", err)
	}

	context, cancel := context.WithCancel(context.Background())

	wal := &WriteAheadLog{
		directory:             config.Directory,
		currSegmentFile:       segmentFile,
		bufferWriter:          bufio.NewWriter(segmentFile),
		currSegmentNumber:     segmentNumber,
		lastLogSequenceNumber: 0,
		syncTimer:             time.NewTimer(SyncInterval),

		maxFileSize:     config.MaxFileSize,
		maxSegments:     config.MaxSegments,
		enableForceSync: config.EnableForceSync,
		context:         context,
		cancel:          cancel,
	}

	if wal.lastLogSequenceNumber, err = wal.getLastLogSequenceNumber(); err != nil {
		return fmt.Errorf("Failed to get last log sequence number: %w", err)
	}

	go wal.syncPeriodically()

	return nil
}

func (wal *WriteAheadLog) getLastLogSequenceNumber() (uint64, error) {
	panic("unimplemented")
}

func (wal *WriteAheadLog) syncPeriodically() {
	for {
		select {
		case <-wal.syncTimer.C:
			err := wal.Sync()

			if err != nil {
				fmt.Printf("Error syncing WAL: %v\n", err)
			}

		case <-wal.context.Done():
			return
		}
	}
}

func (wal *WriteAheadLog) Sync() error {
	if err := wal.bufferWriter.Flush(); err != nil {
		return fmt.Errorf("Failed to flush buffer: %w", err)
	}

	if wal.enableForceSync {
		if err := wal.currSegmentFile.Sync(); err != nil {
			return fmt.Errorf("Failed to sync segment file: %w", err)
		}
	}

	wal.syncTimer.Reset(SyncInterval)
	return nil
}

func loadLastSegmentFile(config *Config) (*os.File, int, error) {
	files, err := filepath.Glob(filepath.Join(config.Directory, SegmentPrefix+"*"))
	if err != nil {
		return nil, 0, fmt.Errorf("Failed reading WAL files: %w", err)
	}

	// No existing WAL files, create a new one
	if len(files) == 0 {
		file, err := createNewSegmentFile(config.Directory, 0)
		if err != nil {
			return nil, 0, fmt.Errorf("Failed creating new WAL segment file: %w", err)
		}

		return file, 0, nil
	}

	lastSegmentFileNumber, err := getLastSegmentFileNumber(files, SegmentPrefix)
	if err != nil {
		return nil, 0, fmt.Errorf("Failed getting last segment file number: %w", err)
	}

	segmentFilePath := filepath.Join(config.Directory, fmt.Sprintf("%s%d.log", SegmentPrefix, lastSegmentFileNumber))
	file, err := os.OpenFile(segmentFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, lastSegmentFileNumber, fmt.Errorf("Failed opening last segment file: %w", err)
	}

	return file, lastSegmentFileNumber, nil
}

func getLastSegmentFileNumber(files []string, segmentPrefix string) (int, error) {
	var lastSegmentNumber int
	for _, file := range files {
		var segmentNumber int
		_, err := fmt.Sscanf(filepath.Base(file), segmentPrefix+"%d.log", &segmentNumber)
		if err != nil {
			return 0, err
		}
		if segmentNumber > lastSegmentNumber {
			lastSegmentNumber = segmentNumber
		}
	}

	return lastSegmentNumber, nil
}

func createNewSegmentFile(dir string, segmentId int) (*os.File, error) {
	fileName := fmt.Sprintf("%s%d.log", SegmentPrefix, segmentId)
	filePath := filepath.Join(dir, fileName)

	file, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}

	return file, nil
}

func (wal *WriteAheadLog) ReadAllRecords() ([]*pb.WalRecord, error) {
	file, err := os.OpenFile(wal.currSegmentFile.Name(), os.O_RDONLY, 0644)
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
	if err := binary.Write(wal.bufferWriter, binary.LittleEndian, recordSize); err != nil {
		return fmt.Errorf("failed to write record size: %w", err)
	}
	// write the marshaled record to the buffer
	_, err = wal.bufferWriter.Write(marshaledRecord)

	return err
}
