package bloomfilter

import (
	"encoding/binary"
	"hash/fnv"
	"math"
	"os"
	"sync"
)

// BloomFilter is a probabilistic data structure that is used to test whether an element
// is a member of a set. False positives are possible, but false negatives are not.
type BloomFilter struct {
	mu         sync.RWMutex
	bits       []byte // The bit array
	size       uint64 // The size of the bit array in bits
	hashFuncs  uint64 // The number of hash functions
	expectedN  uint64 // The expected number of elements
	insertions uint64 // The number of elements inserted
}

// NewBloomFilter creates a new Bloom filter with the given parameters.
// falsePositiveRate: The desired false positive rate (e.g., 0.01 for 1%)
// expectedElements: The expected number of elements to be inserted
func NewBloomFilter(falsePositiveRate float64, expectedElements uint64) *BloomFilter {
	// Calculate optimal size and number of hash functions
	size := calculateOptimalSize(expectedElements, falsePositiveRate)
	hashFuncs := calculateOptimalHashFuncs(size, expectedElements)

	// Allocate the bit array (divide by 8 to convert bits to bytes)
	bits := make([]byte, (size+7)/8)

	return &BloomFilter{
		bits:       bits,
		size:       size,
		hashFuncs:  hashFuncs,
		expectedN:  expectedElements,
		insertions: 0,
	}
}

// Add inserts an element into the Bloom filter
func (bf *BloomFilter) Add(key []byte) {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	// Generate hash values and set corresponding bits
	for i := uint64(0); i < bf.hashFuncs; i++ {
		position := bf.hash(key, i)
		bf.setBit(position)
	}

	bf.insertions++
}

// Contains checks if an element might be in the Bloom filter
// Returns true if the element might be in the set (could be a false positive)
// Returns false if the element is definitely not in the set
func (bf *BloomFilter) Contains(key []byte) bool {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	// Generate hash values and check corresponding bits
	for i := uint64(0); i < bf.hashFuncs; i++ {
		position := bf.hash(key, i)
		if !bf.testBit(position) {
			return false
		}
	}

	// All bits were set, so the element might be in the set
	return true
}

// Clear removes all elements from the Bloom filter
func (bf *BloomFilter) Clear() {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	// Reset the bit array
	for i := range bf.bits {
		bf.bits[i] = 0
	}

	bf.insertions = 0
}

// EstimatedFalsePositiveRate returns the estimated false positive rate
// based on the current number of insertions
func (bf *BloomFilter) EstimatedFalsePositiveRate() float64 {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	// k: number of hash functions
	// m: size of the bit array in bits
	// n: number of elements inserted
	k := float64(bf.hashFuncs)
	m := float64(bf.size)
	n := float64(bf.insertions)

	// The formula for the false positive rate is (1 - e^(-kn/m))^k
	return math.Pow(1-math.Exp(-k*n/m), k)
}

// SaveToFile saves the Bloom filter to a file
func (bf *BloomFilter) SaveToFile(filePath string) error {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write header: size, hash functions, expected elements, insertions
	header := make([]byte, 32)
	binary.LittleEndian.PutUint64(header[0:8], bf.size)
	binary.LittleEndian.PutUint64(header[8:16], bf.hashFuncs)
	binary.LittleEndian.PutUint64(header[16:24], bf.expectedN)
	binary.LittleEndian.PutUint64(header[24:32], bf.insertions)

	// Write header
	if _, err := file.Write(header); err != nil {
		return err
	}

	// Write bit array
	if _, err := file.Write(bf.bits); err != nil {
		return err
	}

	return nil
}

// LoadFromFile loads a Bloom filter from a file
func LoadBloomFilter(filePath string) (*BloomFilter, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Read header: size, hash functions, expected elements, insertions
	header := make([]byte, 32)
	if _, err := file.Read(header); err != nil {
		return nil, err
	}

	size := binary.LittleEndian.Uint64(header[0:8])
	hashFuncs := binary.LittleEndian.Uint64(header[8:16])
	expectedN := binary.LittleEndian.Uint64(header[16:24])
	insertions := binary.LittleEndian.Uint64(header[24:32])

	// Read bit array
	bits := make([]byte, (size+7)/8)
	if _, err := file.Read(bits); err != nil {
		return nil, err
	}

	return &BloomFilter{
		bits:       bits,
		size:       size,
		hashFuncs:  hashFuncs,
		expectedN:  expectedN,
		insertions: insertions,
	}, nil
}

// hash generates a hash value for a key using a variant of the FNV hash
// i is used to generate multiple hash functions from a single hash function
func (bf *BloomFilter) hash(key []byte, i uint64) uint64 {
	// Use FNV-1a hash
	h := fnv.New64a()
	h.Write(key)
	h.Write([]byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24),
		byte(i >> 32), byte(i >> 40), byte(i >> 48), byte(i >> 56)})

	// Map the hash to a bit position
	return h.Sum64() % bf.size
}

// setBit sets the bit at the specified position
func (bf *BloomFilter) setBit(position uint64) {
	byteIndex := position / 8
	bitOffset := position % 8
	bf.bits[byteIndex] |= (1 << bitOffset)
}

// testBit checks if the bit at the specified position is set
func (bf *BloomFilter) testBit(position uint64) bool {
	byteIndex := position / 8
	bitOffset := position % 8
	return (bf.bits[byteIndex] & (1 << bitOffset)) != 0
}

// Size returns the size of the Bloom filter in bits
func (bf *BloomFilter) Size() uint64 {
	return bf.size
}

// HashFunctions returns the number of hash functions used
func (bf *BloomFilter) HashFunctions() uint64 {
	return bf.hashFuncs
}

// Insertions returns the number of elements inserted
func (bf *BloomFilter) Insertions() uint64 {
	bf.mu.RLock()
	defer bf.mu.RUnlock()
	return bf.insertions
}

// calculateOptimalSize calculates the optimal size of the bit array
// based on the expected number of elements and the desired false positive rate
func calculateOptimalSize(n uint64, p float64) uint64 {
	// m = -n*ln(p) / (ln(2)^2)
	m := float64(n) * math.Log(p) / (math.Log(2) * math.Log(2) * -1)
	return uint64(math.Ceil(m))
}

// calculateOptimalHashFuncs calculates the optimal number of hash functions
// based on the size of the bit array and the expected number of elements
func calculateOptimalHashFuncs(m, n uint64) uint64 {
	// k = (m/n) * ln(2)
	k := float64(m) / float64(n) * math.Log(2)
	return uint64(math.Max(1, math.Round(k)))
}
