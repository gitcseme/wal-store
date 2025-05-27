package wal

import "testing"

func TestRead(t *testing.T) {
	err := Read()
	if err != nil {
		t.Errorf("Read() failed: %v", err)
	}
}

func TestWrite(t *testing.T) {
	err := Write()
	if err != nil {
		t.Errorf("Write() failed: %v", err)
	}
}
