package wal

import "fmt"

func Read() error {
	fmt.Println("Reading from WAL...")
	return nil
}

func Write() error {
	fmt.Println("Writing to WAL...")
	return nil
}
