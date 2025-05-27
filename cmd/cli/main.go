package main

import (
	"fmt"
	"walstore/internal/wal"
)

func main() {
	fmt.Println("Hello from CLI Client!")

	wal.Read()
	wal.Write()

}
