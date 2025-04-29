package replication

import (
	"errors"
	"fmt"
	"io"
	"sync"

	replication_proto "github.com/KevoDB/kevo/proto/kevo/replication"
	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
)

var (
	// ErrUnknownCodec is returned when an unsupported compression codec is specified
	ErrUnknownCodec = errors.New("unknown compression codec")

	// ErrInvalidCompressedData is returned when compressed data cannot be decompressed
	ErrInvalidCompressedData = errors.New("invalid compressed data")
)

// CompressionManager provides methods to compress and decompress data for replication
type CompressionManager struct {
	// ZSTD encoder and decoder
	zstdEncoder *zstd.Encoder
	zstdDecoder *zstd.Decoder

	// Mutex to protect encoder/decoder access
	mu sync.Mutex
}

// NewCompressionManager creates a new compressor with initialized codecs
func NewCompressionManager() (*CompressionManager, error) {
	// Create ZSTD encoder with default compression level
	zstdEncoder, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create ZSTD encoder: %w", err)
	}

	// Create ZSTD decoder
	zstdDecoder, err := zstd.NewReader(nil)
	if err != nil {
		zstdEncoder.Close()
		return nil, fmt.Errorf("failed to create ZSTD decoder: %w", err)
	}

	return &CompressionManager{
		zstdEncoder: zstdEncoder,
		zstdDecoder: zstdDecoder,
	}, nil
}

// NewCompressionManagerWithLevel creates a new compressor with a specific compression level for ZSTD
func NewCompressionManagerWithLevel(level zstd.EncoderLevel) (*CompressionManager, error) {
	// Create ZSTD encoder with specified compression level
	zstdEncoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(level))
	if err != nil {
		return nil, fmt.Errorf("failed to create ZSTD encoder with level %v: %w", level, err)
	}

	// Create ZSTD decoder
	zstdDecoder, err := zstd.NewReader(nil)
	if err != nil {
		zstdEncoder.Close()
		return nil, fmt.Errorf("failed to create ZSTD decoder: %w", err)
	}

	return &CompressionManager{
		zstdEncoder: zstdEncoder,
		zstdDecoder: zstdDecoder,
	}, nil
}

// Compress compresses data using the specified codec
func (c *CompressionManager) Compress(data []byte, codec replication_proto.CompressionCodec) ([]byte, error) {
	if len(data) == 0 {
		return data, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	switch codec {
	case replication_proto.CompressionCodec_NONE:
		return data, nil

	case replication_proto.CompressionCodec_ZSTD:
		return c.zstdEncoder.EncodeAll(data, nil), nil

	case replication_proto.CompressionCodec_SNAPPY:
		return snappy.Encode(nil, data), nil

	default:
		return nil, fmt.Errorf("%w: %v", ErrUnknownCodec, codec)
	}
}

// Decompress decompresses data using the specified codec
func (c *CompressionManager) Decompress(data []byte, codec replication_proto.CompressionCodec) ([]byte, error) {
	if len(data) == 0 {
		return data, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	switch codec {
	case replication_proto.CompressionCodec_NONE:
		return data, nil

	case replication_proto.CompressionCodec_ZSTD:
		result, err := c.zstdDecoder.DecodeAll(data, nil)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidCompressedData, err)
		}
		return result, nil

	case replication_proto.CompressionCodec_SNAPPY:
		result, err := snappy.Decode(nil, data)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidCompressedData, err)
		}
		return result, nil

	default:
		return nil, fmt.Errorf("%w: %v", ErrUnknownCodec, codec)
	}
}

// Close releases resources used by the compressor
func (c *CompressionManager) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.zstdEncoder != nil {
		c.zstdEncoder.Close()
		c.zstdEncoder = nil
	}

	if c.zstdDecoder != nil {
		c.zstdDecoder.Close()
		c.zstdDecoder = nil
	}

	return nil
}

// NewCompressWriter returns a writer that compresses data using the specified codec
func NewCompressWriter(w io.Writer, codec replication_proto.CompressionCodec) (io.WriteCloser, error) {
	switch codec {
	case replication_proto.CompressionCodec_NONE:
		return nopCloser{w}, nil

	case replication_proto.CompressionCodec_ZSTD:
		return zstd.NewWriter(w)

	case replication_proto.CompressionCodec_SNAPPY:
		return snappy.NewBufferedWriter(w), nil

	default:
		return nil, fmt.Errorf("%w: %v", ErrUnknownCodec, codec)
	}
}

// NewCompressReader returns a reader that decompresses data using the specified codec
func NewCompressReader(r io.Reader, codec replication_proto.CompressionCodec) (io.ReadCloser, error) {
	switch codec {
	case replication_proto.CompressionCodec_NONE:
		return io.NopCloser(r), nil

	case replication_proto.CompressionCodec_ZSTD:
		decoder, err := zstd.NewReader(r)
		if err != nil {
			return nil, err
		}
		return &zstdReadCloser{decoder}, nil

	case replication_proto.CompressionCodec_SNAPPY:
		return &snappyReadCloser{snappy.NewReader(r)}, nil

	default:
		return nil, fmt.Errorf("%w: %v", ErrUnknownCodec, codec)
	}
}

// nopCloser is an io.WriteCloser with a no-op Close method
type nopCloser struct {
	io.Writer
}

func (nopCloser) Close() error { return nil }

// zstdReadCloser wraps a zstd.Decoder to implement io.ReadCloser
type zstdReadCloser struct {
	*zstd.Decoder
}

func (z *zstdReadCloser) Close() error {
	z.Decoder.Close()
	return nil
}

// snappyReadCloser wraps a snappy.Reader to implement io.ReadCloser
type snappyReadCloser struct {
	*snappy.Reader
}

func (s *snappyReadCloser) Close() error {
	// The snappy Reader doesn't have a Close method, so this is a no-op
	return nil
}
