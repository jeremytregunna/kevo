package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"sync"

	bloomfilter "github.com/KevoDB/kevo/pkg/bloom_filter"
	"github.com/KevoDB/kevo/pkg/sstable/block"
	"github.com/KevoDB/kevo/pkg/sstable/footer"
)

// IOManager handles file I/O operations for SSTable
type IOManager struct {
	path     string
	file     *os.File
	fileSize int64
	mu       sync.RWMutex
}

// NewIOManager creates a new IOManager for the given file path
func NewIOManager(path string) (*IOManager, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	// Get file size
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	return &IOManager{
		path:     path,
		file:     file,
		fileSize: stat.Size(),
	}, nil
}

// ReadAt reads data from the file at the given offset
func (io *IOManager) ReadAt(data []byte, offset int64) (int, error) {
	io.mu.RLock()
	defer io.mu.RUnlock()

	if io.file == nil {
		return 0, fmt.Errorf("file is closed")
	}

	return io.file.ReadAt(data, offset)
}

// GetFileSize returns the size of the file
func (io *IOManager) GetFileSize() int64 {
	io.mu.RLock()
	defer io.mu.RUnlock()
	return io.fileSize
}

// Close closes the file
func (io *IOManager) Close() error {
	io.mu.Lock()
	defer io.mu.Unlock()

	if io.file == nil {
		return nil
	}

	err := io.file.Close()
	io.file = nil
	return err
}

// BlockFetcher abstracts the fetching of data blocks
type BlockFetcher struct {
	io *IOManager
}

// NewBlockFetcher creates a new BlockFetcher
func NewBlockFetcher(io *IOManager) *BlockFetcher {
	return &BlockFetcher{io: io}
}

// FetchBlock reads and parses a data block at the given offset and size
func (bf *BlockFetcher) FetchBlock(offset uint64, size uint32) (*block.Reader, error) {
	// Read the data block
	blockData := make([]byte, size)
	n, err := bf.io.ReadAt(blockData, int64(offset))
	if err != nil {
		return nil, fmt.Errorf("failed to read data block at offset %d: %w", offset, err)
	}

	if n != int(size) {
		return nil, fmt.Errorf("incomplete block read: got %d bytes, expected %d: %w",
			n, size, ErrCorruption)
	}

	// Parse the block
	blockReader, err := block.NewReader(blockData)
	if err != nil {
		return nil, fmt.Errorf("failed to create block reader for block at offset %d: %w",
			offset, err)
	}

	return blockReader, nil
}

// BlockLocator represents an index entry pointing to a data block
type BlockLocator struct {
	Offset uint64
	Size   uint32
	Key    []byte
}

// ParseBlockLocator extracts block location information from an index entry
func ParseBlockLocator(key, value []byte) (BlockLocator, error) {
	if len(value) < 12 { // offset (8) + size (4)
		return BlockLocator{}, fmt.Errorf("invalid index entry (too short, length=%d): %w",
			len(value), ErrCorruption)
	}

	offset := binary.LittleEndian.Uint64(value[:8])
	size := binary.LittleEndian.Uint32(value[8:12])

	return BlockLocator{
		Offset: offset,
		Size:   size,
		Key:    key,
	}, nil
}

// BlockCache is a simple LRU cache for data blocks
type BlockCache struct {
	blocks    map[uint64]*block.Reader
	maxBlocks int
	// Using a simple approach for now - more sophisticated LRU could be implemented
	// with a linked list or other data structure for better eviction
	mu sync.RWMutex
}

// NewBlockCache creates a new block cache with the specified capacity
func NewBlockCache(capacity int) *BlockCache {
	return &BlockCache{
		blocks:    make(map[uint64]*block.Reader),
		maxBlocks: capacity,
	}
}

// Get retrieves a block from the cache
func (c *BlockCache) Get(offset uint64) (*block.Reader, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	block, found := c.blocks[offset]
	return block, found
}

// Put adds a block to the cache
func (c *BlockCache) Put(offset uint64, block *block.Reader) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	// If cache is full, evict a random block (simple strategy for now)
	if len(c.blocks) >= c.maxBlocks {
		// Pick a random offset to evict
		for k := range c.blocks {
			delete(c.blocks, k)
			break
		}
	}
	
	c.blocks[offset] = block
}

// BlockBloomFilter associates a bloom filter with a block offset
type BlockBloomFilter struct {
	blockOffset uint64
	filter      *bloomfilter.BloomFilter
}

// Reader reads an SSTable file
type Reader struct {
	ioManager    *IOManager
	blockFetcher *BlockFetcher
	indexOffset  uint64
	indexSize    uint32
	numEntries   uint32
	indexBlock   *block.Reader
	ft           *footer.Footer
	mu           sync.RWMutex
	// Add block cache
	blockCache   *BlockCache
	// Add bloom filters
	bloomFilters  []BlockBloomFilter
	hasBloomFilter bool
}

// OpenReader opens an SSTable file for reading
func OpenReader(path string) (*Reader, error) {
	ioManager, err := NewIOManager(path)
	if err != nil {
		return nil, err
	}

	fileSize := ioManager.GetFileSize()

	// Ensure file is large enough for a footer
	if fileSize < int64(footer.FooterSize) {
		ioManager.Close()
		return nil, fmt.Errorf("file too small to be valid SSTable: %d bytes", fileSize)
	}

	// Read footer
	footerData := make([]byte, footer.FooterSize)
	_, err = ioManager.ReadAt(footerData, fileSize-int64(footer.FooterSize))
	if err != nil {
		ioManager.Close()
		return nil, fmt.Errorf("failed to read footer: %w", err)
	}

	ft, err := footer.Decode(footerData)
	if err != nil {
		ioManager.Close()
		return nil, fmt.Errorf("failed to decode footer: %w", err)
	}

	blockFetcher := NewBlockFetcher(ioManager)

	// Read index block
	indexData := make([]byte, ft.IndexSize)
	_, err = ioManager.ReadAt(indexData, int64(ft.IndexOffset))
	if err != nil {
		ioManager.Close()
		return nil, fmt.Errorf("failed to read index block: %w", err)
	}

	indexBlock, err := block.NewReader(indexData)
	if err != nil {
		ioManager.Close()
		return nil, fmt.Errorf("failed to create index block reader: %w", err)
	}

	// Initialize reader with basic fields
	reader := &Reader{
		ioManager:     ioManager,
		blockFetcher:  blockFetcher,
		indexOffset:   ft.IndexOffset,
		indexSize:     ft.IndexSize,
		numEntries:    ft.NumEntries,
		indexBlock:    indexBlock,
		ft:            ft,
		blockCache:    NewBlockCache(100), // Cache up to 100 blocks by default
		bloomFilters:  make([]BlockBloomFilter, 0),
		hasBloomFilter: ft.BloomFilterOffset > 0 && ft.BloomFilterSize > 0,
	}
	
	// Load bloom filters if they exist
	if reader.hasBloomFilter {
		// Read the bloom filter data
		bloomFilterData := make([]byte, ft.BloomFilterSize)
		_, err = ioManager.ReadAt(bloomFilterData, int64(ft.BloomFilterOffset))
		if err != nil {
			ioManager.Close()
			return nil, fmt.Errorf("failed to read bloom filter data: %w", err)
		}
		
		// Process the bloom filter data
		var pos uint32 = 0
		for pos < ft.BloomFilterSize {
			// Read the block offset and filter size
			if pos+12 > ft.BloomFilterSize {
				break // Not enough data for header
			}
			
			blockOffset := binary.LittleEndian.Uint64(bloomFilterData[pos:pos+8])
			filterSize := binary.LittleEndian.Uint32(bloomFilterData[pos+8:pos+12])
			pos += 12
			
			// Ensure we have enough data for the filter
			if pos+filterSize > ft.BloomFilterSize {
				break
			}
			
			// Create a temporary file to load the bloom filter
			tempFile, err := os.CreateTemp("", "bloom-filter-*.tmp")
			if err != nil {
				continue // Skip this filter if we can't create temp file
			}
			tempPath := tempFile.Name()
			
			// Write the bloom filter data to the temp file
			_, err = tempFile.Write(bloomFilterData[pos:pos+filterSize])
			tempFile.Close()
			if err != nil {
				os.Remove(tempPath)
				continue
			}
			
			// Load the bloom filter
			filter, err := bloomfilter.LoadBloomFilter(tempPath)
			os.Remove(tempPath) // Clean up temp file
			
			if err != nil {
				continue // Skip this filter
			}
			
			// Add the bloom filter to our list
			reader.bloomFilters = append(reader.bloomFilters, BlockBloomFilter{
				blockOffset: blockOffset,
				filter:      filter,
			})
			
			// Move to the next filter
			pos += filterSize
		}
	}

	return reader, nil
}

// FindBlockForKey finds the block that might contain the given key
func (r *Reader) FindBlockForKey(key []byte) ([]BlockLocator, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var blocks []BlockLocator
	seenBlocks := make(map[uint64]bool)

	// First try binary search for efficiency - find the first block
	// where the first key is >= our target key
	indexIter := r.indexBlock.Iterator()
	indexIter.Seek(key)

	// If the seek fails, start from beginning to check all blocks
	if !indexIter.Valid() {
		indexIter.SeekToFirst()
	}

	// Process all potential blocks (starting from the one found by Seek)
	for ; indexIter.Valid(); indexIter.Next() {
		locator, err := ParseBlockLocator(indexIter.Key(), indexIter.Value())
		if err != nil {
			continue
		}

		// Skip blocks we've already seen
		if seenBlocks[locator.Offset] {
			continue
		}
		seenBlocks[locator.Offset] = true

		blocks = append(blocks, locator)
	}

	return blocks, nil
}

// SearchBlockForKey searches for a key within a specific block
func (r *Reader) SearchBlockForKey(blockReader *block.Reader, key []byte) ([]byte, bool) {
	blockIter := blockReader.Iterator()

	// Binary search within the block if possible
	if blockIter.Seek(key) && bytes.Equal(blockIter.Key(), key) {
		return blockIter.Value(), true
	}

	// If binary search fails, do a linear scan (for backup)
	for blockIter.SeekToFirst(); blockIter.Valid(); blockIter.Next() {
		if bytes.Equal(blockIter.Key(), key) {
			return blockIter.Value(), true
		}
	}

	return nil, false
}

// Get returns the value for a given key
func (r *Reader) Get(key []byte) ([]byte, error) {
	// Find potential blocks that might contain the key
	blocks, err := r.FindBlockForKey(key)
	if err != nil {
		return nil, err
	}

	// Search through each block
	for _, locator := range blocks {
		// Check bloom filter first if available
		if r.hasBloomFilter {
			// Find the bloom filter for this block
			var shouldSkip = true
			for _, bf := range r.bloomFilters {
				if bf.blockOffset == locator.Offset {
					// Found a bloom filter for this block
					// If the key might be in this block, we'll check it
					if bf.filter.Contains(key) {
						shouldSkip = false
					}
					break
				}
			}
			
			// If the bloom filter says the key definitely isn't in this block, skip it
			if shouldSkip {
				continue
			}
		}
		
		var blockReader *block.Reader
		
		// Try to get the block from cache first
		cachedBlock, found := r.blockCache.Get(locator.Offset)
		if found {
			// Use cached block
			blockReader = cachedBlock
		} else {
			// Block not in cache, fetch from disk
			blockReader, err = r.blockFetcher.FetchBlock(locator.Offset, locator.Size)
			if err != nil {
				return nil, err
			}
			
			// Add to cache for future use
			r.blockCache.Put(locator.Offset, blockReader)
		}

		// Search for the key in this block
		if value, found := r.SearchBlockForKey(blockReader, key); found {
			return value, nil
		}
	}

	return nil, ErrNotFound
}

// NewIterator returns an iterator over the entire SSTable
func (r *Reader) NewIterator() *Iterator {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Create a fresh block.Iterator for the index
	indexIter := r.indexBlock.Iterator()

	// Pre-check that we have at least one valid index entry
	indexIter.SeekToFirst()

	return &Iterator{
		reader:        r,
		indexIterator: indexIter,
		dataBlockIter: nil,
		currentBlock:  nil,
		initialized:   false,
	}
}

// Close closes the SSTable reader
func (r *Reader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.ioManager.Close()
}

// GetKeyCount returns the estimated number of keys in the SSTable
func (r *Reader) GetKeyCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return int(r.numEntries)
}

// FilePath returns the file path of this SSTable
func (r *Reader) FilePath() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	return r.ioManager.path
}
