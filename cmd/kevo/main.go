package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/chzyer/readline"

	"github.com/KevoDB/kevo/pkg/common/iterator"
	"github.com/KevoDB/kevo/pkg/engine"

	// Import transaction package to register the transaction creator
	_ "github.com/KevoDB/kevo/pkg/transaction"
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
Kevo (kevo) - A lightweight, minimalist, storage engine.

Usage:
  kevo [options] [database_path]  - Start with an optional database path

Options:
  -server                 - Run in server mode, exposing a gRPC API
  -daemon                 - Run in daemon mode (detached from terminal)
  -address string         - Address to listen on in server mode (default "localhost:50051")

Commands (interactive mode only):
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

// Config holds the application configuration
type Config struct {
	ServerMode   bool
	DaemonMode   bool
	ListenAddr   string
	DBPath       string
	TLSEnabled   bool
	TLSCertFile  string
	TLSKeyFile   string
	TLSCAFile    string
}

func main() {
	// Parse command line arguments and get configuration
	config := parseFlags()

	// Open database if path provided
	var eng *engine.Engine
	var err error
	
	if config.DBPath != "" {
		fmt.Printf("Opening database at %s\n", config.DBPath)
		eng, err = engine.NewEngine(config.DBPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error opening database: %s\n", err)
			os.Exit(1)
		}
		defer eng.Close()
	}
	
	// Check if we should run in server mode
	if config.ServerMode {
		if eng == nil {
			fmt.Fprintf(os.Stderr, "Error: Server mode requires a database path\n")
			os.Exit(1)
		}
		
		runServer(eng, config)
		return
	}
	
	// Run in interactive mode
	runInteractive(eng, config.DBPath)
}

// parseFlags parses command line flags and returns a Config
func parseFlags() Config {
	// Define custom usage message
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Kevo - A lightweight key-value storage engine\n\n")
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: kevo [options] [database_path]\n\n")
		fmt.Fprintf(flag.CommandLine.Output(), "By default, kevo runs in interactive mode with a command-line interface.\n")
		fmt.Fprintf(flag.CommandLine.Output(), "If -server flag is provided, kevo runs as a server exposing a gRPC API.\n\n")
		fmt.Fprintf(flag.CommandLine.Output(), "Options:\n")
		flag.PrintDefaults()
		fmt.Fprintf(flag.CommandLine.Output(), "\nInteractive mode commands (when not using -server):\n")
		fmt.Fprintf(flag.CommandLine.Output(), "  PUT key value           - Store a key-value pair\n")
		fmt.Fprintf(flag.CommandLine.Output(), "  GET key                 - Retrieve a value by key\n")
		fmt.Fprintf(flag.CommandLine.Output(), "  DELETE key              - Delete a key-value pair\n")
		fmt.Fprintf(flag.CommandLine.Output(), "  SCAN                    - Scan all key-value pairs\n")
		fmt.Fprintf(flag.CommandLine.Output(), "  BEGIN TRANSACTION       - Begin a read-write transaction\n")
		fmt.Fprintf(flag.CommandLine.Output(), "  BEGIN READONLY          - Begin a read-only transaction\n")
		fmt.Fprintf(flag.CommandLine.Output(), "  COMMIT                  - Commit the current transaction\n")
		fmt.Fprintf(flag.CommandLine.Output(), "  ROLLBACK                - Rollback the current transaction\n")
		fmt.Fprintf(flag.CommandLine.Output(), "  .help                   - Show detailed help\n")
		fmt.Fprintf(flag.CommandLine.Output(), "  .exit                   - Exit the program\n\n")
		fmt.Fprintf(flag.CommandLine.Output(), "For more details, start kevo and type .help\n")
	}

	serverMode := flag.Bool("server", false, "Run in server mode, exposing a gRPC API")
	daemonMode := flag.Bool("daemon", false, "Run in daemon mode (detached from terminal)")
	listenAddr := flag.String("address", "localhost:50051", "Address to listen on in server mode")
	
	// TLS options
	tlsEnabled := flag.Bool("tls", false, "Enable TLS for secure connections")
	tlsCertFile := flag.String("cert", "", "TLS certificate file path")
	tlsKeyFile := flag.String("key", "", "TLS private key file path")
	tlsCAFile := flag.String("ca", "", "TLS CA certificate file for client verification")
	
	// Parse flags
	flag.Parse()
	
	// Get database path from remaining arguments
	var dbPath string
	if flag.NArg() > 0 {
		dbPath = flag.Arg(0)
	}
	
	return Config{
		ServerMode:   *serverMode,
		DaemonMode:   *daemonMode,
		ListenAddr:   *listenAddr,
		DBPath:       dbPath,
		TLSEnabled:   *tlsEnabled,
		TLSCertFile:  *tlsCertFile,
		TLSKeyFile:   *tlsKeyFile,
		TLSCAFile:    *tlsCAFile,
	}
}

// runServer initializes and runs the Kevo server
func runServer(eng *engine.Engine, config Config) {
	// Set up daemon mode if requested
	if config.DaemonMode {
		setupDaemonMode()
	}
	
	// Create and start the server
	server := NewServer(eng, config)
	
	// Start the server (non-blocking)
	if err := server.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "Error starting server: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Kevo server started on %s\n", config.ListenAddr)
	
	// Set up signal handling for graceful shutdown
	setupGracefulShutdown(server, eng)
	
	// Start serving (blocking)
	if err := server.Serve(); err != nil {
		fmt.Fprintf(os.Stderr, "Error serving: %v\n", err)
		os.Exit(1)
	}
}

// setupDaemonMode configures process to run as a daemon
func setupDaemonMode() {
	// Redirect standard file descriptors to /dev/null
	null, err := os.OpenFile("/dev/null", os.O_RDWR, 0)
	if err != nil {
		log.Fatalf("Failed to open /dev/null: %v", err)
	}
	
	// Redirect standard file descriptors to /dev/null
	err = syscall.Dup2(int(null.Fd()), int(os.Stdin.Fd()))
	if err != nil {
		log.Fatalf("Failed to redirect stdin: %v", err)
	}
	
	err = syscall.Dup2(int(null.Fd()), int(os.Stdout.Fd()))
	if err != nil {
		log.Fatalf("Failed to redirect stdout: %v", err)
	}
	
	err = syscall.Dup2(int(null.Fd()), int(os.Stderr.Fd()))
	if err != nil {
		log.Fatalf("Failed to redirect stderr: %v", err)
	}
	
	// Create a new process group
	_, err = syscall.Setsid()
	if err != nil {
		log.Fatalf("Failed to create new session: %v", err)
	}
	
	fmt.Println("Daemon mode enabled, detaching from terminal...")
}

// setupGracefulShutdown configures graceful shutdown on signals
func setupGracefulShutdown(server *Server, eng *engine.Engine) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	
	go func() {
		sig := <-sigChan
		fmt.Printf("\nReceived signal %v, shutting down...\n", sig)
		
		// Graceful shutdown logic
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		
		// Shut down the server
		if err := server.Shutdown(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "Error shutting down server: %v\n", err)
		}
		
		// The engine will be closed by the defer in main()
		
		fmt.Println("Shutdown complete")
		os.Exit(0)
	}()
}

// runInteractive starts the interactive CLI mode
func runInteractive(eng *engine.Engine, dbPath string) {
	fmt.Println("Kevo (kevo) version 1.0.2")
	fmt.Println("Enter .help for usage hints.")
	
	var tx engine.Transaction
	var err error

	// Setup readline with history support
	historyFile := filepath.Join(os.TempDir(), ".kevo_history")
	rl, err := readline.NewEx(&readline.Config{
		Prompt:          "kevo> ",
		HistoryFile:     historyFile,
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
		AutoComplete:    completer,
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
					prompt = fmt.Sprintf("kevo:%s[RO]> ", dbPath)
				} else {
					prompt = "kevo[RO]> "
				}
			} else {
				if dbPath != "" {
					prompt = fmt.Sprintf("kevo:%s[RW]> ", dbPath)
				} else {
					prompt = "kevo[RW]> "
				}
			}
		} else {
			if dbPath != "" {
				prompt = fmt.Sprintf("kevo:%s> ", dbPath)
			} else {
				prompt = "kevo> "
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