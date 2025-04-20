package compaction

import (
	"bytes"
	"fmt"

	"github.com/jer/kevo/pkg/sstable"
)

// SSTableInfo represents metadata about an SSTable file
type SSTableInfo struct {
	// Path of the SSTable file
	Path string

	// Level number (0 to N)
	Level int

	// Sequence number for the file within its level
	Sequence uint64

	// Timestamp when the file was created
	Timestamp int64

	// Approximate size of the file in bytes
	Size int64

	// Estimated key count (may be approximate)
	KeyCount int

	// First key in the SSTable
	FirstKey []byte

	// Last key in the SSTable
	LastKey []byte

	// Reader for the SSTable
	Reader *sstable.Reader
}

// Overlaps checks if this SSTable's key range overlaps with another SSTable
func (s *SSTableInfo) Overlaps(other *SSTableInfo) bool {
	// If either SSTable has no keys, they don't overlap
	if len(s.FirstKey) == 0 || len(s.LastKey) == 0 ||
		len(other.FirstKey) == 0 || len(other.LastKey) == 0 {
		return false
	}

	// Check for overlap: not (s ends before other starts OR s starts after other ends)
	// s.LastKey < other.FirstKey || s.FirstKey > other.LastKey
	return !(bytes.Compare(s.LastKey, other.FirstKey) < 0 ||
		bytes.Compare(s.FirstKey, other.LastKey) > 0)
}

// KeyRange returns a string representation of the key range in this SSTable
func (s *SSTableInfo) KeyRange() string {
	return fmt.Sprintf("[%s, %s]",
		string(s.FirstKey), string(s.LastKey))
}

// String returns a string representation of the SSTable info
func (s *SSTableInfo) String() string {
	return fmt.Sprintf("L%d-%06d-%020d.sst Size:%d Keys:%d Range:%s",
		s.Level, s.Sequence, s.Timestamp, s.Size, s.KeyCount, s.KeyRange())
}

// CompactionTask represents a set of SSTables to be compacted
type CompactionTask struct {
	// Input SSTables to compact, grouped by level
	InputFiles map[int][]*SSTableInfo

	// Target level for compaction output
	TargetLevel int

	// Output file path template
	OutputPathTemplate string
}
