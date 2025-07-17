package wal

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"time"
	pb "walstore/proto"

	gpb "google.golang.org/protobuf/proto"
)

var (
	SyncInterval  = 200 * time.Millisecond // Default sync interval
	SegmentPrefix = "wal-segment-"         // Default segment file prefix
)

func StartLogger(config *Config) (*WriteAheadLog, error) {
	if config.SyncInterval > 0 {
		SyncInterval = time.Duration(config.SyncInterval) * time.Millisecond
	}

	if err := validateConfig(config); err != nil {
		return nil, err
	}

	if err := os.MkdirAll(config.Directory, 0755); err != nil {
		return nil, err
	}

	segmentFile, segmentNumber, err := loadLastSegmentFile(config)
	if err != nil {
		return nil, err
	}

	// seek to the end of the file to start writing new records
	if _, err := segmentFile.Seek(0, io.SeekEnd); err != nil {
		return nil, fmt.Errorf("failed to seek: %w", err)
	}

	context, cancel := context.WithCancel(context.Background())

	wal := &WriteAheadLog{
		directory:             config.Directory,
		currSegmentFile:       segmentFile,
		currSegmentNumber:     segmentNumber,
		maxFileSize:           config.MaxFileSize,
		maxSegments:           config.MaxSegments,
		enableForceSync:       config.EnableForceSync,
		lastLogSequenceNumber: 0,
		bufferWriter:          bufio.NewWriter(segmentFile),
		syncTimer:             time.NewTimer(SyncInterval),
		context:               context,
		cancel:                cancel,
	}

	if wal.lastLogSequenceNumber, err = wal.getLastLogSequenceNumber(); err != nil {
		return nil, fmt.Errorf("failed getting lsn: %w", err)
	}

	go wal.syncPeriodically()

	return wal, nil
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

func (wal *WriteAheadLog) WriteRecord(data []byte) error {
	wal.lock.Lock()
	defer wal.lock.Unlock()

	logSeqNumber := wal.lastLogSequenceNumber + 1

	newRecord := &pb.WalRecord{
		Data:              data,
		LogSequenceNumber: logSeqNumber,
		Timestamp:         time.Now().UnixNano(),
		Checksum:          crc32.ChecksumIEEE(append(data, byte(logSeqNumber))),
	}

	marshaledRecord, err := gpb.Marshal(newRecord)
	if err != nil {
		return err
	}

	if err := wal.rotateLogIfNeeded(len(marshaledRecord)); err != nil {
		return err
	}

	if err := wal.writeToBuffer(marshaledRecord); err != nil {
		return err
	}
	wal.lastLogSequenceNumber = logSeqNumber
	return nil
}

func (wal *WriteAheadLog) rotateLogIfNeeded(currDataLength int) error {
	fileInfo, err := wal.currSegmentFile.Stat()
	if err != nil {
		return err
	}

	bufferSizeWouldBe := fileInfo.Size() + int64(wal.bufferWriter.Buffered()) + int64(currDataLength)

	if bufferSizeWouldBe >= wal.maxFileSize {
		if err := wal.rotateLog(); err != nil {
			return err
		}
	}

	return nil
}

func (wal *WriteAheadLog) rotateLog() error {
	if err := wal.Sync(); err != nil {
		return err
	}

	if err := wal.currSegmentFile.Close(); err != nil {
		return err
	}

	if wal.currSegmentNumber+1 > wal.maxSegments {
		if err := wal.deleteOldestSegmentFile(); err != nil {
			return err
		}
	}

	newSegmentFile, err := createNewSegmentFile(wal.directory, wal.currSegmentNumber+1)
	if err != nil {
		return fmt.Errorf("failed to create new segment file: %w", err)
	}

	wal.currSegmentFile = newSegmentFile
	wal.bufferWriter = bufio.NewWriter(newSegmentFile)
	wal.currSegmentNumber++

	return nil
}

func (wal *WriteAheadLog) deleteOldestSegmentFile() error {
	files, err := filepath.Glob(filepath.Join(wal.directory, SegmentPrefix+"*"))
	if err != nil {
		return err
	}

	if len(files) == 0 {
		return nil
	}

	oldestSegmentFile, err := getOldestSegmentFile(files)
	if err != nil {
		return err
	}

	if err := os.Remove(oldestSegmentFile); err != nil {
		return err
	}

	return nil
}

func getOldestSegmentFile(files []string) (string, error) {
	if len(files) == 0 {
		return "", nil
	}

	var oldestFile string
	oldestSegmentNumber := int(^uint(0) >> 1) // Max int value
	for _, file := range files {
		var segmentNumber int
		if _, err := fmt.Sscanf(filepath.Base(file), SegmentPrefix+"%d.log", &segmentNumber); err != nil {
			return "", fmt.Errorf("failed to parse segment number from file %s: %w", file, err)
		}
		if segmentNumber < oldestSegmentNumber {
			oldestSegmentNumber = segmentNumber
			oldestFile = file
		}
	}
	return oldestFile, nil
}

func (wal *WriteAheadLog) Close() error {
	// Stop the periodic sync timer
	wal.cancel()
	// Sync before closing
	if err := wal.Sync(); err != nil {
		return err
	}
	// Close the current segment file
	return wal.currSegmentFile.Close()
}

func (wal *WriteAheadLog) writeToBuffer(marshaledRecord []byte) error {
	recordSize := int32(len(marshaledRecord))
	// write the record size to the buffer
	if err := binary.Write(wal.bufferWriter, binary.LittleEndian, recordSize); err != nil {
		return fmt.Errorf("failed to write record size: %w", err)
	}
	// write the marshaled record to the buffer
	_, err := wal.bufferWriter.Write(marshaledRecord)

	return err
}

// TODO: Return one record only
func (wal *WriteAheadLog) getLastLogSequenceNumber() (uint64, error) {
	records, err := wal.ReadAllRecords()
	if err != nil {
		return 0, err
	}
	if len(records) == 0 {
		return 0, nil // No records found, return 0
	}
	lastRecord := records[len(records)-1]
	return lastRecord.GetLogSequenceNumber(), nil
}

func (wal *WriteAheadLog) syncPeriodically() {
	for {
		select {
		case <-wal.syncTimer.C:
			wal.lock.Lock()
			err := wal.Sync()
			wal.lock.Unlock()

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
		return fmt.Errorf("failed to flush buffer: %w", err)
	}

	if wal.enableForceSync {
		if err := wal.currSegmentFile.Sync(); err != nil {
			return fmt.Errorf("failed to sync segment file: %w", err)
		}
	}

	wal.syncTimer.Reset(SyncInterval)
	return nil
}

func loadLastSegmentFile(config *Config) (*os.File, int, error) {
	files, err := filepath.Glob(filepath.Join(config.Directory, SegmentPrefix+"*"))
	if err != nil {
		return nil, 1, fmt.Errorf("failed reading WAL files: %w", err)
	}

	// No existing WAL files, create a new one
	if len(files) == 0 {
		file, err := createNewSegmentFile(config.Directory, 1)
		if err != nil {
			return nil, 1, fmt.Errorf("failed creating new WAL segment file: %w", err)
		}

		return file, 1, nil
	}

	lastSegmentFileNumber, err := getLastSegmentFileNumber(files, SegmentPrefix)
	if err != nil {
		return nil, 1, fmt.Errorf("failed getting last segment file number: %w", err)
	}

	segmentFilePath := filepath.Join(config.Directory, fmt.Sprintf("%s%d.log", SegmentPrefix, lastSegmentFileNumber))
	file, err := os.OpenFile(segmentFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, lastSegmentFileNumber, fmt.Errorf("failed opening last segment file: %w", err)
	}

	return file, lastSegmentFileNumber, nil
}

func getLastSegmentFileNumber(files []string, segmentPrefix string) (int, error) {
	var lastSegmentNumber int = 0
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
