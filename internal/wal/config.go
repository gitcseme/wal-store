package wal

import (
	"fmt"
)

type Config struct {
	Directory       string
	MaxFileSize     uint64
	MaxSegments     int
	EnableForceSync bool
}

func CreateDefaultConfig(logDirectory string) *Config {
	return &Config{
		Directory:       logDirectory,
		MaxFileSize:     1024 * 1024 * 10, // 10 MB
		MaxSegments:     5,
		EnableForceSync: true,
	}
}

func validateConfig(config *Config) error {
	if config.Directory == "" {
		return fmt.Errorf("directory cannot be empty")
	}
	return nil
}
