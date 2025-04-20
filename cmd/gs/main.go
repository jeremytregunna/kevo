package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/chzyer/readline"

	"github.com/jeremytregunna/kevo/pkg/common/iterator"
	"github.com/jeremytregunna/kevo/pkg/engine"

	// Import transaction package to register the transaction creator
	_ "github.com/jeremytregunna/kevo/pkg/transaction"
)

// Command completer for readline
var completer = readline.NewPrefixCompleter(
	readline.PcItem(".help"),
	readline.PcItem(".open"),
	readline.PcItem(".close"),
	readline.PcItem(".exit"),
	readline.PcItem(".stats"),
	readline.PcItem(".flush"),
	readline.PcItem("BEGIN",
		readline.PcItem("TRANSACTION"),
		readline.PcItem("READONLY"),
	),
	readline.PcItem("COMMIT"),
	readline.PcItem("ROLLBACK"),
	readline.PcItem("PUT"),
	readline.PcItem("GET"),
	readline.PcItem("DELETE"),
	readline.PcItem("SCAN",
		readline.PcItem("RANGE"),
	),
)

const helpText = `
Kevo (gs) - SQLite-like interface for the storage engine

Usage:
  gs [database_path]      - Start with an optional database path

Commands:
  .help                   - Show this help message
  .open PATH              - Open a database at PATH
  .close                  - Close the current database
  .exit                   - Exit the program
  .stats                  - Show database statistics
  .flush                  - Force flush memtables to disk
  
  BEGIN [TRANSACTION]     - Begin a transaction (default: read-write)
  BEGIN READONLY          - Begin a read-only transaction
  COMMIT                  - Commit the current transaction
  ROLLBACK                - Rollback the current transaction
  
  PUT key value           - Store a key-value pair
  GET key                 - Retrieve a value by key
  DELETE key              - Delete a key-value pair
  
  SCAN                    - Scan all key-value pairs
  SCAN prefix             - Scan key-value pairs with given prefix
  SCAN RANGE start end    - Scan key-value pairs in range [start, end)
                          - Note: start and end are treated as string keys, not numeric indices
`

func main() {
	fmt.Println("Kevo (gs) version 1.0.0")
	fmt.Println("Enter .help for usage hints.")

	// Initialize variables
	var eng *engine.Engine
	var tx engine.Transaction
	var err error
	var dbPath string

	// Check if a database path was provided as an argument
	if len(os.Args) > 1 {
		dbPath = os.Args[1]
		fmt.Printf("Opening database at %s\n", dbPath)
		eng, err = engine.NewEngine(dbPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error opening database: %s\n", err)
			os.Exit(1)
		}
	}

	// Setup readline with history support
	historyFile := filepath.Join(os.TempDir(), ".gs_history")
	rl, err := readline.NewEx(&readline.Config{
		Prompt:          "gs> ",
		HistoryFile:     historyFile,
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error initializing readline: %s\n", err)
		os.Exit(1)
	}
	defer rl.Close()

	for {
		// Update prompt based on current state
		var prompt string
		if tx != nil {
			if tx.IsReadOnly() {
				if dbPath != "" {
					prompt = fmt.Sprintf("gs:%s[RO]> ", dbPath)
				} else {
					prompt = "gs[RO]> "
				}
			} else {
				if dbPath != "" {
					prompt = fmt.Sprintf("gs:%s[RW]> ", dbPath)
				} else {
					prompt = "gs[RW]> "
				}
			}
		} else {
			if dbPath != "" {
				prompt = fmt.Sprintf("gs:%s> ", dbPath)
			} else {
				prompt = "gs> "
			}
		}
		rl.SetPrompt(prompt)

		// Read command
		line, readErr := rl.Readline()
		if readErr != nil {
			if readErr == readline.ErrInterrupt {
				if len(line) == 0 {
					break
				} else {
					continue
				}
			} else if readErr == io.EOF {
				fmt.Println("Goodbye!")
				break
			}
			fmt.Fprintf(os.Stderr, "Error reading input: %s\n", readErr)
			continue
		}

		// Line is already trimmed by readline
		if line == "" {
			continue
		}

		// Add to history (readline handles this automatically for non-empty lines)
		// rl.SaveHistory(line)

		// Process command
		parts := strings.Fields(line)
		cmd := strings.ToUpper(parts[0])

		// Special dot commands
		if strings.HasPrefix(cmd, ".") {
			cmd = strings.ToLower(cmd)
			switch cmd {
			case ".help":
				fmt.Print(helpText)

			case ".open":
				if len(parts) < 2 {
					fmt.Println("Error: Missing path argument")
					continue
				}

				// Close any existing engine
				if eng != nil {
					eng.Close()
				}

				// Open the database
				dbPath = parts[1]
				eng, err = engine.NewEngine(dbPath)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error opening database: %s\n", err)
					dbPath = ""
					continue
				}
				fmt.Printf("Database opened at %s\n", dbPath)

			case ".close":
				if eng == nil {
					fmt.Println("No database open")
					continue
				}

				// Close any active transaction
				if tx != nil {
					tx.Rollback()
					tx = nil
				}

				// Close the engine
				err = eng.Close()
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error closing database: %s\n", err)
				} else {
					fmt.Printf("Database %s closed\n", dbPath)
					eng = nil
					dbPath = ""
				}

			case ".exit":
				// Close any active transaction
				if tx != nil {
					tx.Rollback()
				}

				// Close the engine
				if eng != nil {
					eng.Close()
				}

				fmt.Println("Goodbye!")
				return

			case ".stats":
				if eng == nil {
					fmt.Println("No database open")
					continue
				}

				// Print statistics
				stats := eng.GetStats()
				fmt.Println("Database Statistics:")
				fmt.Printf("  Operations: %d puts, %d gets (%d hits, %d misses), %d deletes\n",
					stats["put_ops"], stats["get_ops"], stats["get_hits"], stats["get_misses"], stats["delete_ops"])
				fmt.Printf("  Transactions: %d started, %d committed, %d aborted\n",
					stats["tx_started"], stats["tx_completed"], stats["tx_aborted"])
				fmt.Printf("  Storage: %d bytes read, %d bytes written, %d flushes\n",
					stats["total_bytes_read"], stats["total_bytes_written"], stats["flush_count"])
				fmt.Printf("  Tables: %d sstables, %d immutable memtables\n",
					stats["sstable_count"], stats["immutable_memtable_count"])

			case ".flush":
				if eng == nil {
					fmt.Println("No database open")
					continue
				}

				// Flush all memtables
				err = eng.FlushImMemTables()
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error flushing memtables: %s\n", err)
				} else {
					fmt.Println("Memtables flushed to disk")
				}

			default:
				fmt.Printf("Unknown command: %s\n", cmd)
			}
			continue
		}

		// Regular commands
		switch cmd {
		case "BEGIN":
			if eng == nil {
				fmt.Println("Error: No database open")
				continue
			}

			// Check if we already have a transaction
			if tx != nil {
				fmt.Println("Error: Transaction already in progress")
				continue
			}

			// Check if readonly
			readOnly := false
			if len(parts) >= 2 && strings.ToUpper(parts[1]) == "READONLY" {
				readOnly = true
			}

			// Begin transaction
			tx, err = eng.BeginTransaction(readOnly)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error beginning transaction: %s\n", err)
				continue
			}

			if readOnly {
				fmt.Println("Started read-only transaction")
			} else {
				fmt.Println("Started read-write transaction")
			}

		case "COMMIT":
			if tx == nil {
				fmt.Println("Error: No transaction in progress")
				continue
			}

			// Commit transaction
			startTime := time.Now()
			err = tx.Commit()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error committing transaction: %s\n", err)
			} else {
				fmt.Printf("Transaction committed (%.2f ms)\n", float64(time.Since(startTime).Microseconds())/1000.0)
				tx = nil
			}

		case "ROLLBACK":
			if tx == nil {
				fmt.Println("Error: No transaction in progress")
				continue
			}

			// Rollback transaction
			err = tx.Rollback()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error rolling back transaction: %s\n", err)
			} else {
				fmt.Println("Transaction rolled back")
				tx = nil
			}

		case "PUT":
			if len(parts) < 3 {
				fmt.Println("Error: PUT requires key and value arguments")
				continue
			}

			// Check if we're in a transaction
			if tx != nil {
				// Check if read-only
				if tx.IsReadOnly() {
					fmt.Println("Error: Cannot PUT in a read-only transaction")
					continue
				}

				// Use transaction PUT
				err = tx.Put([]byte(parts[1]), []byte(strings.Join(parts[2:], " ")))
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error putting value: %s\n", err)
				} else {
					fmt.Println("Value stored in transaction (will be visible after commit)")
				}
			} else {
				// Check if database is open
				if eng == nil {
					fmt.Println("Error: No database open")
					continue
				}

				// Use direct PUT
				err = eng.Put([]byte(parts[1]), []byte(strings.Join(parts[2:], " ")))
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error putting value: %s\n", err)
				} else {
					fmt.Println("Value stored")
				}
			}

		case "GET":
			if len(parts) < 2 {
				fmt.Println("Error: GET requires a key argument")
				continue
			}

			// Check if we're in a transaction
			if tx != nil {
				// Use transaction GET
				val, err := tx.Get([]byte(parts[1]))
				if err != nil {
					if err == engine.ErrKeyNotFound {
						fmt.Println("Key not found")
					} else {
						fmt.Fprintf(os.Stderr, "Error getting value: %s\n", err)
					}
				} else {
					fmt.Printf("%s\n", val)
				}
			} else {
				// Check if database is open
				if eng == nil {
					fmt.Println("Error: No database open")
					continue
				}

				// Use direct GET
				val, err := eng.Get([]byte(parts[1]))
				if err != nil {
					if err == engine.ErrKeyNotFound {
						fmt.Println("Key not found")
					} else {
						fmt.Fprintf(os.Stderr, "Error getting value: %s\n", err)
					}
				} else {
					fmt.Printf("%s\n", val)
				}
			}

		case "DELETE":
			if len(parts) < 2 {
				fmt.Println("Error: DELETE requires a key argument")
				continue
			}

			// Check if we're in a transaction
			if tx != nil {
				// Check if read-only
				if tx.IsReadOnly() {
					fmt.Println("Error: Cannot DELETE in a read-only transaction")
					continue
				}

				// Use transaction DELETE
				err = tx.Delete([]byte(parts[1]))
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error deleting key: %s\n", err)
				} else {
					fmt.Println("Key deleted in transaction (will be applied after commit)")
				}
			} else {
				// Check if database is open
				if eng == nil {
					fmt.Println("Error: No database open")
					continue
				}

				// Use direct DELETE
				err = eng.Delete([]byte(parts[1]))
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error deleting key: %s\n", err)
				} else {
					fmt.Println("Key deleted")
				}
			}

		case "SCAN":
			var iter iterator.Iterator

			// Check if we're in a transaction
			if tx != nil {
				if len(parts) == 1 {
					// Full scan
					iter = tx.NewIterator()
				} else if len(parts) == 2 {
					// Prefix scan
					prefix := []byte(parts[1])
					prefixEnd := makeKeySuccessor(prefix)
					iter = tx.NewRangeIterator(prefix, prefixEnd)
				} else if len(parts) == 3 && strings.ToUpper(parts[1]) == "RANGE" {
					// Syntax error
					fmt.Println("Error: SCAN RANGE requires start and end keys")
					continue
				} else if len(parts) == 4 && strings.ToUpper(parts[1]) == "RANGE" {
					// Range scan with explicit RANGE keyword
					iter = tx.NewRangeIterator([]byte(parts[2]), []byte(parts[3]))
				} else if len(parts) == 3 {
					// Old style range scan
					fmt.Println("Warning: Using deprecated range syntax. Use 'SCAN RANGE start end' instead.")
					iter = tx.NewRangeIterator([]byte(parts[1]), []byte(parts[2]))
				} else {
					fmt.Println("Error: Invalid SCAN syntax. See .help for usage")
					continue
				}
			} else {
				// Check if database is open
				if eng == nil {
					fmt.Println("Error: No database open")
					continue
				}

				// Use engine iterators
				var iterErr error
				if len(parts) == 1 {
					// Full scan
					iter, iterErr = eng.GetIterator()
				} else if len(parts) == 2 {
					// Prefix scan
					prefix := []byte(parts[1])
					prefixEnd := makeKeySuccessor(prefix)
					iter, iterErr = eng.GetRangeIterator(prefix, prefixEnd)
				} else if len(parts) == 3 && strings.ToUpper(parts[1]) == "RANGE" {
					// Syntax error
					fmt.Println("Error: SCAN RANGE requires start and end keys")
					continue
				} else if len(parts) == 4 && strings.ToUpper(parts[1]) == "RANGE" {
					// Range scan with explicit RANGE keyword
					iter, iterErr = eng.GetRangeIterator([]byte(parts[2]), []byte(parts[3]))
				} else if len(parts) == 3 {
					// Old style range scan
					fmt.Println("Warning: Using deprecated range syntax. Use 'SCAN RANGE start end' instead.")
					iter, iterErr = eng.GetRangeIterator([]byte(parts[1]), []byte(parts[2]))
				} else {
					fmt.Println("Error: Invalid SCAN syntax. See .help for usage")
					continue
				}

				if iterErr != nil {
					fmt.Fprintf(os.Stderr, "Error creating iterator: %s\n", iterErr)
					continue
				}
			}

			// Perform the scan
			count := 0
			seenKeys := make(map[string]bool)
			for iter.SeekToFirst(); iter.Valid(); iter.Next() {
				// Check if we've already seen this key
				keyStr := string(iter.Key())
				if seenKeys[keyStr] {
					continue
				}

				// Mark this key as seen
				seenKeys[keyStr] = true

				// Check if this key exists in the engine via Get to ensure consistency
				// (this handles tombstones which may still be visible in the iterator)
				var keyExists bool
				var keyValue []byte

				if tx != nil {
					// Use transaction Get
					keyValue, err = tx.Get(iter.Key())
					keyExists = (err == nil)
				} else {
					// Use engine Get
					keyValue, err = eng.Get(iter.Key())
					keyExists = (err == nil)
				}

				// Only display key if it actually exists
				if keyExists {
					fmt.Printf("%s: %s\n", iter.Key(), keyValue)
					count++
				}
			}
			fmt.Printf("%d entries found\n", count)

		default:
			fmt.Printf("Unknown command: %s\n", cmd)
		}
	}
}

// makeKeySuccessor creates the successor key for a prefix scan
// by adding a 0xFF byte to the end of the prefix
func makeKeySuccessor(prefix []byte) []byte {
	successor := make([]byte, len(prefix)+1)
	copy(successor, prefix)
	successor[len(prefix)] = 0xFF
	return successor
}
