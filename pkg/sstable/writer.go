package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	"github.com/jer/kevo/pkg/sstable/block"
	"github.com/jer/kevo/pkg/sstable/footer"
)

// FileManager handles file operations for SSTable writing
type FileManager struct {
	path    string
	tmpPath string
	file    *os.File
}

// NewFileManager creates a new FileManager for the given file path
func NewFileManager(path string) (*FileManager, error) {
	// Create temporary file for writing
	dir := filepath.Dir(path)
	tmpPath := filepath.Join(dir, fmt.Sprintf(".%s.tmp", filepath.Base(path)))

	file, err := os.Create(tmpPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create temporary file: %w", err)
	}

	return &FileManager{
		path:    path,
		tmpPath: tmpPath,
		file:    file,
	}, nil
}

// Write writes data to the file at the current position
func (fm *FileManager) Write(data []byte) (int, error) {
	return fm.file.Write(data)
}

// Sync flushes the file to disk
func (fm *FileManager) Sync() error {
	return fm.file.Sync()
}

// Close closes the file
func (fm *FileManager) Close() error {
	if fm.file == nil {
		return nil
	}
	err := fm.file.Close()
	fm.file = nil
	return err
}

// FinalizeFile closes the file and renames it to the final path
func (fm *FileManager) FinalizeFile() error {
	// Close the file before renaming
	if err := fm.Close(); err != nil {
		return fmt.Errorf("failed to close file: %w", err)
	}

	// Rename the temp file to the final path
	if err := os.Rename(fm.tmpPath, fm.path); err != nil {
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	return nil
}

// Cleanup removes the temporary file if writing is aborted
func (fm *FileManager) Cleanup() error {
	if fm.file != nil {
		fm.Close()
	}
	return os.Remove(fm.tmpPath)
}

// BlockManager handles block building and serialization
type BlockManager struct {
	builder *block.Builder
	offset  uint64
}

// NewBlockManager creates a new BlockManager
func NewBlockManager() *BlockManager {
	return &BlockManager{
		builder: block.NewBuilder(),
		offset:  0,
	}
}

// Add adds a key-value pair to the current block
func (bm *BlockManager) Add(key, value []byte) error {
	return bm.builder.Add(key, value)
}

// EstimatedSize returns the estimated size of the current block
func (bm *BlockManager) EstimatedSize() uint32 {
	return bm.builder.EstimatedSize()
}

// Entries returns the number of entries in the current block
func (bm *BlockManager) Entries() int {
	return bm.builder.Entries()
}

// GetEntries returns all entries in the current block
func (bm *BlockManager) GetEntries() []block.Entry {
	return bm.builder.GetEntries()
}

// Reset resets the block builder
func (bm *BlockManager) Reset() {
	bm.builder.Reset()
}

// Serialize serializes the current block
func (bm *BlockManager) Serialize() ([]byte, error) {
	var buf bytes.Buffer
	_, err := bm.builder.Finish(&buf)
	if err != nil {
		return nil, fmt.Errorf("failed to finish block: %w", err)
	}
	return buf.Bytes(), nil
}

// IndexBuilder constructs the index block
type IndexBuilder struct {
	builder *block.Builder
	entries []*IndexEntry
}

// NewIndexBuilder creates a new IndexBuilder
func NewIndexBuilder() *IndexBuilder {
	return &IndexBuilder{
		builder: block.NewBuilder(),
		entries: make([]*IndexEntry, 0),
	}
}

// AddIndexEntry adds an entry to the pending index entries
func (ib *IndexBuilder) AddIndexEntry(entry *IndexEntry) {
	ib.entries = append(ib.entries, entry)
}

// BuildIndex builds the index block from the collected entries
func (ib *IndexBuilder) BuildIndex() error {
	// Add all index entries to the index block
	for _, entry := range ib.entries {
		// Index entry format: key=firstKey, value=blockOffset+blockSize
		var valueBuf bytes.Buffer
		binary.Write(&valueBuf, binary.LittleEndian, entry.BlockOffset)
		binary.Write(&valueBuf, binary.LittleEndian, entry.BlockSize)

		if err := ib.builder.Add(entry.FirstKey, valueBuf.Bytes()); err != nil {
			return fmt.Errorf("failed to add index entry: %w", err)
		}
	}
	return nil
}

// Serialize serializes the index block
func (ib *IndexBuilder) Serialize() ([]byte, error) {
	var buf bytes.Buffer
	_, err := ib.builder.Finish(&buf)
	if err != nil {
		return nil, fmt.Errorf("failed to finish index block: %w", err)
	}
	return buf.Bytes(), nil
}

// Writer writes an SSTable file
type Writer struct {
	fileManager  *FileManager
	blockManager *BlockManager
	indexBuilder *IndexBuilder
	dataOffset   uint64
	firstKey     []byte
	lastKey      []byte
	entriesAdded uint32
}

// NewWriter creates a new SSTable writer
func NewWriter(path string) (*Writer, error) {
	fileManager, err := NewFileManager(path)
	if err != nil {
		return nil, err
	}

	return &Writer{
		fileManager:  fileManager,
		blockManager: NewBlockManager(),
		indexBuilder: NewIndexBuilder(),
		dataOffset:   0,
		entriesAdded: 0,
	}, nil
}

// Add adds a key-value pair to the SSTable
// Keys must be added in sorted order
func (w *Writer) Add(key, value []byte) error {
	// Keep track of first and last keys
	if w.entriesAdded == 0 {
		w.firstKey = append([]byte(nil), key...)
	}
	w.lastKey = append([]byte(nil), key...)

	// Add to block
	if err := w.blockManager.Add(key, value); err != nil {
		return fmt.Errorf("failed to add to block: %w", err)
	}

	w.entriesAdded++

	// Flush the block if it's getting too large
	// Use IndexKeyInterval to determine when to flush based on accumulated data size
	if w.blockManager.EstimatedSize() >= IndexKeyInterval {
		if err := w.flushBlock(); err != nil {
			return err
		}
	}

	return nil
}

// AddTombstone adds a deletion marker (tombstone) for a key to the SSTable
// This is functionally equivalent to Add(key, nil) but makes the intention explicit
func (w *Writer) AddTombstone(key []byte) error {
	return w.Add(key, nil)
}

// flushBlock writes the current block to the file and adds an index entry
func (w *Writer) flushBlock() error {
	// Skip if the block is empty
	if w.blockManager.Entries() == 0 {
		return nil
	}

	// Record the offset of this block
	blockOffset := w.dataOffset

	// Get first key
	entries := w.blockManager.GetEntries()
	if len(entries) == 0 {
		return fmt.Errorf("block has no entries")
	}
	firstKey := entries[0].Key

	// Serialize the block
	blockData, err := w.blockManager.Serialize()
	if err != nil {
		return err
	}

	blockSize := uint32(len(blockData))

	// Write the block to file
	n, err := w.fileManager.Write(blockData)
	if err != nil {
		return fmt.Errorf("failed to write block to file: %w", err)
	}
	if n != len(blockData) {
		return fmt.Errorf("wrote incomplete block: %d of %d bytes", n, len(blockData))
	}

	// Add the index entry
	w.indexBuilder.AddIndexEntry(&IndexEntry{
		BlockOffset: blockOffset,
		BlockSize:   blockSize,
		FirstKey:    firstKey,
	})

	// Update offset for next block
	w.dataOffset += uint64(n)

	// Reset the block builder for next block
	w.blockManager.Reset()

	return nil
}

// Finish completes the SSTable writing process
func (w *Writer) Finish() error {
	defer func() {
		w.fileManager.Close()
	}()

	// Flush any pending data block (only if we have entries that haven't been flushed)
	if w.blockManager.Entries() > 0 {
		if err := w.flushBlock(); err != nil {
			return err
		}
	}

	// Create index block
	indexOffset := w.dataOffset

	// Build the index from collected entries
	if err := w.indexBuilder.BuildIndex(); err != nil {
		return err
	}

	// Serialize and write the index block
	indexData, err := w.indexBuilder.Serialize()
	if err != nil {
		return err
	}

	indexSize := uint32(len(indexData))

	n, err := w.fileManager.Write(indexData)
	if err != nil {
		return fmt.Errorf("failed to write index block: %w", err)
	}
	if n != len(indexData) {
		return fmt.Errorf("wrote incomplete index block: %d of %d bytes",
			n, len(indexData))
	}

	// Create footer
	ft := footer.NewFooter(
		indexOffset,
		indexSize,
		w.entriesAdded,
		0, // MinKeyOffset - not implemented yet
		0, // MaxKeyOffset - not implemented yet
	)

	// Serialize footer
	footerData := ft.Encode()

	// Write footer
	n, err = w.fileManager.Write(footerData)
	if err != nil {
		return fmt.Errorf("failed to write footer: %w", err)
	}
	if n != len(footerData) {
		return fmt.Errorf("wrote incomplete footer: %d of %d bytes", n, len(footerData))
	}

	// Sync the file
	if err := w.fileManager.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	// Finalize file (close and rename)
	return w.fileManager.FinalizeFile()
}

// Abort cancels the SSTable writing process
func (w *Writer) Abort() error {
	return w.fileManager.Cleanup()
}
