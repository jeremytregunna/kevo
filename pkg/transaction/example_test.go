package transaction_test

import (
	"fmt"
	"os"

	"github.com/KevoDB/kevo/pkg/engine"
	"github.com/KevoDB/kevo/pkg/transaction"
	"github.com/KevoDB/kevo/pkg/wal"
)

// Disable all logs in tests
func init() {
	wal.DisableRecoveryLogs = true
}

func Example() {
	// Create a temporary directory for the example
	tempDir, err := os.MkdirTemp("", "transaction_example_*")
	if err != nil {
		fmt.Printf("Failed to create temp directory: %v\n", err)
		return
	}
	defer os.RemoveAll(tempDir)

	// Create a new storage engine
	eng, err := engine.NewEngine(tempDir)
	if err != nil {
		fmt.Printf("Failed to create engine: %v\n", err)
		return
	}
	defer eng.Close()

	// Add some initial data directly to the engine
	if err := eng.Put([]byte("user:1001"), []byte("Alice")); err != nil {
		fmt.Printf("Failed to add user: %v\n", err)
		return
	}
	if err := eng.Put([]byte("user:1002"), []byte("Bob")); err != nil {
		fmt.Printf("Failed to add user: %v\n", err)
		return
	}

	// Create a read-only transaction
	readTx, err := transaction.NewTransaction(eng, transaction.ReadOnly)
	if err != nil {
		fmt.Printf("Failed to create read transaction: %v\n", err)
		return
	}

	// Query data using the read transaction
	value, err := readTx.Get([]byte("user:1001"))
	if err != nil {
		fmt.Printf("Failed to get user: %v\n", err)
	} else {
		fmt.Printf("Read transaction found user: %s\n", value)
	}

	// Create an iterator to scan all users
	fmt.Println("All users (read transaction):")
	iter := readTx.NewIterator()
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		fmt.Printf("  %s: %s\n", iter.Key(), iter.Value())
	}

	// Commit the read transaction
	if err := readTx.Commit(); err != nil {
		fmt.Printf("Failed to commit read transaction: %v\n", err)
		return
	}

	// Create a read-write transaction
	writeTx, err := transaction.NewTransaction(eng, transaction.ReadWrite)
	if err != nil {
		fmt.Printf("Failed to create write transaction: %v\n", err)
		return
	}

	// Modify data within the transaction
	if err := writeTx.Put([]byte("user:1003"), []byte("Charlie")); err != nil {
		fmt.Printf("Failed to add user: %v\n", err)
		return
	}
	if err := writeTx.Delete([]byte("user:1001")); err != nil {
		fmt.Printf("Failed to delete user: %v\n", err)
		return
	}

	// Changes are visible within the transaction
	fmt.Println("All users (write transaction before commit):")
	iter = writeTx.NewIterator()
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		fmt.Printf("  %s: %s\n", iter.Key(), iter.Value())
	}

	// But not in the main engine yet
	val, err := eng.Get([]byte("user:1003"))
	if err != nil {
		fmt.Println("New user not yet visible in engine (correct)")
	} else {
		fmt.Printf("Unexpected: user visible before commit: %s\n", val)
	}

	// Commit the write transaction
	if err := writeTx.Commit(); err != nil {
		fmt.Printf("Failed to commit write transaction: %v\n", err)
		return
	}

	// Now changes are visible in the engine
	fmt.Println("All users (after commit):")
	users := []string{"user:1001", "user:1002", "user:1003"}
	for _, key := range users {
		val, err := eng.Get([]byte(key))
		if err != nil {
			fmt.Printf("  %s: <deleted>\n", key)
		} else {
			fmt.Printf("  %s: %s\n", key, val)
		}
	}

	// Output:
	// Read transaction found user: Alice
	// All users (read transaction):
	//   user:1001: Alice
	//   user:1002: Bob
	// All users (write transaction before commit):
	//   user:1002: Bob
	//   user:1003: Charlie
	// New user not yet visible in engine (correct)
	// All users (after commit):
	//   user:1001: <deleted>
	//   user:1002: Bob
	//   user:1003: Charlie
}
