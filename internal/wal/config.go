package wal

import (
	"fmt"
)

type Config struct {
	Directory       string
	MaxFileSize     int64
	MaxSegments     int
	EnableForceSync bool
	SyncInterval    uint32 // in milliseconds
}

func CreateDefaultConfig(logDirectory string) *Config {
	return &Config{
		Directory:       logDirectory,
		MaxFileSize:     1024 * 1024 * 16, // 16 MB
		MaxSegments:     100,
		EnableForceSync: true,
		SyncInterval:    200, // 200 milliseconds
	}
}

func validateConfig(config *Config) error {
	if config.Directory == "" {
		return fmt.Errorf("directory cannot be empty")
	}
	return nil
}
