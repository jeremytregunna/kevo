package block

// Entry represents a key-value pair within the block
type Entry struct {
	Key         []byte
	Value       []byte
	SequenceNum uint64 // Sequence number for versioning
}

const (
	// BlockSize is the target size for each block
	BlockSize = 16 * 1024 // 16KB
	// RestartInterval defines how often we store a full key
	RestartInterval = 16
	// MaxBlockEntries is the maximum number of entries per block
	MaxBlockEntries = 1024
	// BlockFooterSize is the size of the footer (checksum + restart point count)
	BlockFooterSize = 8 + 4 // 8 bytes for checksum, 4 for restart count
)
