package wal

import (
	"bufio"
	"context"
	"os"
	"sync"
	"time"
)

type WriteAheadLog struct {
	directory             string        // Directory where WAL segments are stored
	currSegmentFile       *os.File      // Current segment file being written to
	bufferWriter          *bufio.Writer // Buffered writer for efficient writing
	currSegmentNumber     int           // Current segment number for naming segments
	lastLogSequenceNumber uint64        // Last log sequence number written
	maxFileSize           int64         // Maximum size of a segment file
	maxSegments           int           // Maximum number of segment files to keep
	lock                  sync.Mutex    // Mutex to protect concurrent access to the WAL
	syncTimer             *time.Timer   // Timer for periodic flushing of the buffer
	enableForceSync       bool          // Flag to force sync on next write
	context               context.Context
	cancel                context.CancelFunc // To cancel the background sync task
}
