package footer

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestFooterEncodeDecode(t *testing.T) {
	// Create a footer
	f := NewFooter(
		1000, // indexOffset
		500,  // indexSize
		1234, // numEntries
		100,  // minKeyOffset
		200,  // maxKeyOffset
		5000, // bloomFilterOffset
		300,  // bloomFilterSize
	)

	// Encode the footer
	encoded := f.Encode()

	// The encoded data should be exactly FooterSize bytes
	if len(encoded) != FooterSize {
		t.Errorf("Encoded footer size is %d, expected %d", len(encoded), FooterSize)
	}

	// Decode the encoded data
	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Failed to decode footer: %v", err)
	}

	// Verify fields match
	if decoded.Magic != f.Magic {
		t.Errorf("Magic mismatch: got %d, expected %d", decoded.Magic, f.Magic)
	}

	if decoded.Version != f.Version {
		t.Errorf("Version mismatch: got %d, expected %d", decoded.Version, f.Version)
	}

	if decoded.Timestamp != f.Timestamp {
		t.Errorf("Timestamp mismatch: got %d, expected %d", decoded.Timestamp, f.Timestamp)
	}

	if decoded.IndexOffset != f.IndexOffset {
		t.Errorf("IndexOffset mismatch: got %d, expected %d", decoded.IndexOffset, f.IndexOffset)
	}

	if decoded.IndexSize != f.IndexSize {
		t.Errorf("IndexSize mismatch: got %d, expected %d", decoded.IndexSize, f.IndexSize)
	}

	if decoded.NumEntries != f.NumEntries {
		t.Errorf("NumEntries mismatch: got %d, expected %d", decoded.NumEntries, f.NumEntries)
	}

	if decoded.MinKeyOffset != f.MinKeyOffset {
		t.Errorf("MinKeyOffset mismatch: got %d, expected %d", decoded.MinKeyOffset, f.MinKeyOffset)
	}

	if decoded.MaxKeyOffset != f.MaxKeyOffset {
		t.Errorf("MaxKeyOffset mismatch: got %d, expected %d", decoded.MaxKeyOffset, f.MaxKeyOffset)
	}

	if decoded.Checksum != f.Checksum {
		t.Errorf("Checksum mismatch: got %d, expected %d", decoded.Checksum, f.Checksum)
	}
}

func TestFooterWriteTo(t *testing.T) {
	// Create a footer
	f := NewFooter(
		1000, // indexOffset
		500,  // indexSize
		1234, // numEntries
		100,  // minKeyOffset
		200,  // maxKeyOffset
		5000, // bloomFilterOffset
		300,  // bloomFilterSize
	)

	// Write to a buffer
	var buf bytes.Buffer
	n, err := f.WriteTo(&buf)

	if err != nil {
		t.Fatalf("Failed to write footer: %v", err)
	}

	if n != int64(FooterSize) {
		t.Errorf("WriteTo wrote %d bytes, expected %d", n, FooterSize)
	}

	// Read back and verify
	data := buf.Bytes()
	decoded, err := Decode(data)

	if err != nil {
		t.Fatalf("Failed to decode footer: %v", err)
	}

	if decoded.Magic != f.Magic {
		t.Errorf("Magic mismatch after write/read")
	}

	if decoded.NumEntries != f.NumEntries {
		t.Errorf("NumEntries mismatch after write/read")
	}
}

func TestFooterCorruption(t *testing.T) {
	// Create a footer
	f := NewFooter(
		1000, // indexOffset
		500,  // indexSize
		1234, // numEntries
		100,  // minKeyOffset
		200,  // maxKeyOffset
		5000, // bloomFilterOffset
		300,  // bloomFilterSize
	)

	// Encode the footer
	encoded := f.Encode()

	// Corrupt the magic number
	corruptedMagic := make([]byte, len(encoded))
	copy(corruptedMagic, encoded)
	binary.LittleEndian.PutUint64(corruptedMagic[0:], 0x1234567812345678)

	_, err := Decode(corruptedMagic)
	if err == nil {
		t.Errorf("Expected error when decoding footer with corrupt magic, but got none")
	}

	// Corrupt the checksum
	corruptedChecksum := make([]byte, len(encoded))
	copy(corruptedChecksum, encoded)
	binary.LittleEndian.PutUint64(corruptedChecksum[44:], 0xBADBADBADBADBAD)

	_, err = Decode(corruptedChecksum)
	if err == nil {
		t.Errorf("Expected error when decoding footer with corrupt checksum, but got none")
	}

	// Truncated data
	truncated := encoded[:FooterSize-1]
	_, err = Decode(truncated)
	if err == nil {
		t.Errorf("Expected error when decoding truncated footer, but got none")
	}
}

func TestFooterVersionCheck(t *testing.T) {
	// Create a footer with the current version
	f := NewFooter(1000, 500, 1234, 100, 200, 5000, 300)

	// Create a modified version
	f.Version = 9999
	encoded := f.Encode()

	// Decode should still work since we don't verify version compatibility
	// in the Decode function directly
	decoded, err := Decode(encoded)
	if err != nil {
		t.Errorf("Unexpected error decoding footer with unknown version: %v", err)
	}

	if decoded.Version != 9999 {
		t.Errorf("Expected version 9999, got %d", decoded.Version)
	}
}
