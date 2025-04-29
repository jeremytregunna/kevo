package replication

import (
	"bytes"
	"io"
	"strings"
	"testing"

	proto "github.com/KevoDB/kevo/proto/kevo/replication"
	"github.com/klauspost/compress/zstd"
)

func TestCompressor(t *testing.T) {
	// Test data with a mix of random and repetitive content
	testData := []byte(strings.Repeat("hello world, this is a test message with some repetition. ", 100))

	// Create a new compressor
	comp, err := NewCompressionManager()
	if err != nil {
		t.Fatalf("Failed to create compressor: %v", err)
	}
	defer comp.Close()

	// Test different compression codecs
	testCodecs := []proto.CompressionCodec{
		proto.CompressionCodec_NONE,
		proto.CompressionCodec_ZSTD,
		proto.CompressionCodec_SNAPPY,
	}

	for _, codec := range testCodecs {
		t.Run(codec.String(), func(t *testing.T) {
			// Compress the data
			compressed, err := comp.Compress(testData, codec)
			if err != nil {
				t.Fatalf("Compression failed with codec %s: %v", codec, err)
			}

			// Check that compression actually worked (except for NONE)
			if codec != proto.CompressionCodec_NONE {
				if len(compressed) >= len(testData) {
					t.Logf("Warning: compressed size (%d) not smaller than original (%d) for codec %s",
						len(compressed), len(testData), codec)
				}
			} else if codec == proto.CompressionCodec_NONE {
				if len(compressed) != len(testData) {
					t.Errorf("Expected no compression with NONE codec, but sizes differ: %d vs %d",
						len(compressed), len(testData))
				}
			}

			// Decompress the data
			decompressed, err := comp.Decompress(compressed, codec)
			if err != nil {
				t.Fatalf("Decompression failed with codec %s: %v", codec, err)
			}

			// Verify the decompressed data matches the original
			if !bytes.Equal(testData, decompressed) {
				t.Errorf("Decompressed data does not match original for codec %s", codec)
			}
		})
	}
}

func TestCompressorWithInvalidData(t *testing.T) {
	// Create a new compressor
	comp, err := NewCompressionManager()
	if err != nil {
		t.Fatalf("Failed to create compressor: %v", err)
	}
	defer comp.Close()

	// Test decompression with invalid data
	invalidData := []byte("this is not valid compressed data")

	// Test with ZSTD
	_, err = comp.Decompress(invalidData, proto.CompressionCodec_ZSTD)
	if err == nil {
		t.Errorf("Expected error when decompressing invalid ZSTD data, got nil")
	}

	// Test with Snappy
	_, err = comp.Decompress(invalidData, proto.CompressionCodec_SNAPPY)
	if err == nil {
		t.Errorf("Expected error when decompressing invalid Snappy data, got nil")
	}

	// Test with unknown codec
	_, err = comp.Compress([]byte("test"), proto.CompressionCodec(999))
	if err == nil {
		t.Errorf("Expected error when using unknown compression codec, got nil")
	}

	_, err = comp.Decompress([]byte("test"), proto.CompressionCodec(999))
	if err == nil {
		t.Errorf("Expected error when using unknown decompression codec, got nil")
	}
}

func TestCompressorWithLevel(t *testing.T) {
	// Test data with repetitive content
	testData := []byte(strings.Repeat("compress me with different levels ", 1000))

	// Create compressors with different levels
	levels := []zstd.EncoderLevel{
		zstd.SpeedFastest,
		zstd.SpeedDefault,
		zstd.SpeedBestCompression,
	}

	var results []int

	for _, level := range levels {
		comp, err := NewCompressionManagerWithLevel(level)
		if err != nil {
			t.Fatalf("Failed to create compressor with level %v: %v", level, err)
		}

		// Compress the data
		compressed, err := comp.Compress(testData, proto.CompressionCodec_ZSTD)
		if err != nil {
			t.Fatalf("Compression failed with level %v: %v", level, err)
		}

		// Record the compressed size
		results = append(results, len(compressed))

		// Verify decompression works
		decompressed, err := comp.Decompress(compressed, proto.CompressionCodec_ZSTD)
		if err != nil {
			t.Fatalf("Decompression failed with level %v: %v", level, err)
		}

		if !bytes.Equal(testData, decompressed) {
			t.Errorf("Decompressed data does not match original for level %v", level)
		}

		comp.Close()
	}

	// Log the compression results - size should generally decrease as we move to better compression
	t.Logf("Compression sizes for different levels: %v", results)
}

func TestCompressStreams(t *testing.T) {
	// Test data
	testData := []byte(strings.Repeat("stream compression test data with some repetition ", 100))

	// Test each codec
	codecs := []proto.CompressionCodec{
		proto.CompressionCodec_NONE,
		proto.CompressionCodec_ZSTD,
		proto.CompressionCodec_SNAPPY,
	}

	for _, codec := range codecs {
		t.Run(codec.String(), func(t *testing.T) {
			// Create a buffer for the compressed data
			var compressedBuf bytes.Buffer

			// Create a compress writer
			compressWriter, err := NewCompressWriter(&compressedBuf, codec)
			if err != nil {
				t.Fatalf("Failed to create compress writer for codec %s: %v", codec, err)
			}

			// Write the data
			_, err = compressWriter.Write(testData)
			if err != nil {
				t.Fatalf("Failed to write data with codec %s: %v", codec, err)
			}

			// Close the writer to flush any buffers
			err = compressWriter.Close()
			if err != nil {
				t.Fatalf("Failed to close compress writer for codec %s: %v", codec, err)
			}

			// Create a buffer for the decompressed data
			var decompressedBuf bytes.Buffer

			// Create a compress reader
			compressReader, err := NewCompressReader(bytes.NewReader(compressedBuf.Bytes()), codec)
			if err != nil {
				t.Fatalf("Failed to create compress reader for codec %s: %v", codec, err)
			}

			// Read the data
			_, err = io.Copy(&decompressedBuf, compressReader)
			if err != nil {
				t.Fatalf("Failed to read data with codec %s: %v", codec, err)
			}

			// Close the reader
			err = compressReader.Close()
			if err != nil {
				t.Fatalf("Failed to close compress reader for codec %s: %v", codec, err)
			}

			// Verify the decompressed data matches the original
			if !bytes.Equal(testData, decompressedBuf.Bytes()) {
				t.Errorf("Decompressed data does not match original for codec %s", codec)
			}
		})
	}
}

func BenchmarkCompression(b *testing.B) {
	// Benchmark data with some repetition
	benchData := []byte(strings.Repeat("benchmark compression data with repetitive content for measuring performance ", 100))

	// Create a compressor
	comp, err := NewCompressionManager()
	if err != nil {
		b.Fatalf("Failed to create compressor: %v", err)
	}
	defer comp.Close()

	// Benchmark compression with different codecs
	codecs := []proto.CompressionCodec{
		proto.CompressionCodec_NONE,
		proto.CompressionCodec_ZSTD,
		proto.CompressionCodec_SNAPPY,
	}

	for _, codec := range codecs {
		b.Run("Compress_"+codec.String(), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, err := comp.Compress(benchData, codec)
				if err != nil {
					b.Fatalf("Compression failed: %v", err)
				}
			}
		})
	}

	// Prepare compressed data for decompression benchmarks
	compressedData := make(map[proto.CompressionCodec][]byte)
	for _, codec := range codecs {
		compressed, err := comp.Compress(benchData, codec)
		if err != nil {
			b.Fatalf("Failed to prepare compressed data for codec %s: %v", codec, err)
		}
		compressedData[codec] = compressed
	}

	// Benchmark decompression
	for _, codec := range codecs {
		b.Run("Decompress_"+codec.String(), func(b *testing.B) {
			data := compressedData[codec]
			for i := 0; i < b.N; i++ {
				_, err := comp.Decompress(data, codec)
				if err != nil {
					b.Fatalf("Decompression failed: %v", err)
				}
			}
		})
	}
}
