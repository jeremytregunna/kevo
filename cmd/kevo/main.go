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
	"unicode"

	"github.com/chzyer/readline"

	"github.com/KevoDB/kevo/pkg/common/iterator"
	"github.com/KevoDB/kevo/pkg/engine"
	"github.com/KevoDB/kevo/pkg/engine/interfaces"

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
		readline.PcItem("SUFFIX"),
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
  SCAN SUFFIX suffix      - Scan key-value pairs with given suffix
  SCAN RANGE start end    - Scan key-value pairs in range [start, end)
                          - Note: start and end are treated as string keys, not numeric indices
`

// Config holds the application configuration
type Config struct {
	ServerMode  bool
	DaemonMode  bool
	ListenAddr  string
	DBPath      string
	TLSEnabled  bool
	TLSCertFile string
	TLSKeyFile  string
	TLSCAFile   string

	// Replication settings
	ReplicationEnabled bool
	ReplicationMode    string // "primary", "replica", or "standalone"
	ReplicationAddr    string // Address for replication service
	PrimaryAddr        string // Address of primary (for replicas)
}

func main() {
	// Parse command line arguments and get configuration
	config := parseFlags()

	// Open database if path provided
	var eng *engine.Engine
	var err error

	if config.DBPath != "" {
		fmt.Printf("Opening database at %s\n", config.DBPath)
		// Use the new facade-based engine implementation
		eng, err = engine.NewEngineFacade(config.DBPath)
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

	// Replication options
	replicationEnabled := flag.Bool("replication", false, "Enable replication")
	replicationMode := flag.String("replication-mode", "standalone", "Replication mode: primary, replica, or standalone")
	replicationAddr := flag.String("replication-address", "localhost:50052", "Address for replication service")
	primaryAddr := flag.String("primary", "localhost:50052", "Address of primary node (for replicas)")

	// Parse flags
	flag.Parse()

	// Get database path from remaining arguments
	var dbPath string
	if flag.NArg() > 0 {
		dbPath = flag.Arg(0)
	}

	// Debug output for flag values
	fmt.Printf("DEBUG: Parsed flags: replication=%v, mode=%s, addr=%s, primary=%s\n",
		*replicationEnabled, *replicationMode, *replicationAddr, *primaryAddr)

	config := Config{
		ServerMode:  *serverMode,
		DaemonMode:  *daemonMode,
		ListenAddr:  *listenAddr,
		DBPath:      dbPath,
		TLSEnabled:  *tlsEnabled,
		TLSCertFile: *tlsCertFile,
		TLSKeyFile:  *tlsKeyFile,
		TLSCAFile:   *tlsCAFile,

		// Replication settings
		ReplicationEnabled: *replicationEnabled,
		ReplicationMode:    *replicationMode,
		ReplicationAddr:    *replicationAddr,
		PrimaryAddr:        *primaryAddr,
	}
	fmt.Printf("DEBUG: Config created: ReplicationEnabled=%v, ReplicationMode=%s\n",
		config.ReplicationEnabled, config.ReplicationMode)

	return config
}

// runServer initializes and runs the Kevo server
func runServer(eng *engine.Engine, config Config) {
	// Set up daemon mode if requested
	if config.DaemonMode {
		setupDaemonMode()
	}

	// Create and start the server
	fmt.Printf("DEBUG: Before server creation: ReplicationEnabled=%v, ReplicationMode=%s\n",
		config.ReplicationEnabled, config.ReplicationMode)

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

	var tx interfaces.Transaction
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
				// Use the new facade-based engine implementation
				eng, err = engine.NewEngineFacade(dbPath)
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

				// Helper function to safely get a uint64 value with default
				getUint64 := func(m map[string]interface{}, key string, defaultVal uint64) uint64 {
					if val, ok := m[key]; ok {
						switch v := val.(type) {
						case uint64:
							return v
						case int64:
							return uint64(v)
						case int:
							return uint64(v)
						case float64:
							return uint64(v)
						default:
							return defaultVal
						}
					}
					return defaultVal
				}

				// Format human-readable time for the last operation timestamps
				var lastPutTime, lastGetTime, lastDeleteTime time.Time
				if putTime, ok := stats["last_put_time"].(int64); ok && putTime > 0 {
					lastPutTime = time.Unix(0, putTime)
				}
				if getTime, ok := stats["last_get_time"].(int64); ok && getTime > 0 {
					lastGetTime = time.Unix(0, getTime)
				}
				if deleteTime, ok := stats["last_delete_time"].(int64); ok && deleteTime > 0 {
					lastDeleteTime = time.Unix(0, deleteTime)
				}

				// Operations section
				fmt.Println("📊 Operations:")
				fmt.Printf("  • Puts: %d\n", getUint64(stats, "put_ops", 0))

				// Handle hits and misses
				getOps := getUint64(stats, "get_ops", 0)
				getHits := getUint64(stats, "get_hits", 0)
				getMisses := getUint64(stats, "get_misses", 0)

				// If get_hits and get_misses aren't available, just show operations
				if getHits == 0 && getMisses == 0 {
					fmt.Printf("  • Gets: %d\n", getOps)
				} else {
					fmt.Printf("  • Gets: %d (Hits: %d, Misses: %d)\n", getOps, getHits, getMisses)
				}
				fmt.Printf("  • Deletes: %d\n", getUint64(stats, "delete_ops", 0))

				// Last Operation Times
				fmt.Println("\n⏱️ Last Operation Times:")
				if !lastPutTime.IsZero() {
					fmt.Printf("  • Last Put: %s\n", lastPutTime.Format(time.RFC3339))
				} else {
					fmt.Printf("  • Last Put: Never\n")
				}
				if !lastGetTime.IsZero() {
					fmt.Printf("  • Last Get: %s\n", lastGetTime.Format(time.RFC3339))
				} else {
					fmt.Printf("  • Last Get: Never\n")
				}
				if !lastDeleteTime.IsZero() {
					fmt.Printf("  • Last Delete: %s\n", lastDeleteTime.Format(time.RFC3339))
				} else {
					fmt.Printf("  • Last Delete: Never\n")
				}

				// Transactions (using proper prefixes from txManager stats)
				fmt.Println("\n💼 Transactions:")
				fmt.Printf("  • Started: %d\n", getUint64(stats, "tx_tx_begin_ops", 0))
				fmt.Printf("  • Completed: %d\n", getUint64(stats, "tx_tx_commit_ops", 0))
				fmt.Printf("  • Aborted: %d\n", getUint64(stats, "tx_tx_rollback_ops", 0))

				// Latency statistics if available
				if latency, ok := stats["put_latency"].(map[string]interface{}); ok {
					fmt.Println("\n⚡ Latency (last):")
					if avgNs, ok := latency["avg_ns"].(uint64); ok {
						fmt.Printf("  • Put avg: %.2f ms\n", float64(avgNs)/1000000.0)
					}
					if getLatency, ok := stats["get_latency"].(map[string]interface{}); ok {
						if avgNs, ok := getLatency["avg_ns"].(uint64); ok {
							fmt.Printf("  • Get avg: %.2f ms\n", float64(avgNs)/1000000.0)
						}
					}
				}

				// Storage metrics
				fmt.Println("\n💾 Storage:")
				fmt.Printf("  • Total Bytes Read: %d\n", getUint64(stats, "total_bytes_read", 0))
				fmt.Printf("  • Total Bytes Written: %d\n", getUint64(stats, "total_bytes_written", 0))
				fmt.Printf("  • Flush Count: %d\n", getUint64(stats, "flush_count", 0))

				// Table stats - now get these from storage manager stats
				fmt.Println("\n📋 Tables:")
				fmt.Printf("  • SSTable Count: %d\n", getUint64(stats, "storage_sstable_count", 0))
				fmt.Printf("  • Immutable MemTable Count: %d\n", getUint64(stats, "storage_immutable_memtable_count", 0))
				fmt.Printf("  • Current MemTable Size: %d bytes\n", getUint64(stats, "memtable_size", 0))

				// Get recovery stats from the nested map if available
				if recoveryMap, ok := stats["recovery"].(map[string]interface{}); ok {
					fmt.Println("\n🔄 WAL Recovery:")
					fmt.Printf("  • Files Recovered: %d\n", getUint64(recoveryMap, "wal_files_recovered", 0))
					fmt.Printf("  • Entries Recovered: %d\n", getUint64(recoveryMap, "wal_entries_recovered", 0))
					fmt.Printf("  • Corrupted Entries: %d\n", getUint64(recoveryMap, "wal_corrupted_entries", 0))

					if durationMs, ok := recoveryMap["wal_recovery_duration_ms"]; ok {
						switch v := durationMs.(type) {
						case int64:
							fmt.Printf("  • Recovery Duration: %d ms\n", v)
						case uint64:
							fmt.Printf("  • Recovery Duration: %d ms\n", v)
						case int:
							fmt.Printf("  • Recovery Duration: %d ms\n", v)
						case float64:
							fmt.Printf("  • Recovery Duration: %.0f ms\n", v)
						}
					}
				}

				// Error counts from the nested errors map
				if errorsMap, ok := stats["errors"].(map[string]interface{}); ok && len(errorsMap) > 0 {
					fmt.Println("\n⚠️ Errors:")
					for errType, count := range errorsMap {
						// Format the error type for display
						displayKey := toTitle(strings.Replace(errType, "_", " ", -1))
						fmt.Printf("  • %s: %v\n", displayKey, count)
					}
				} else {
					// No error map or empty, show default counters
					fmt.Println("\n⚠️ Errors:")
					fmt.Printf("  • Read Errors: %d\n", getUint64(stats, "read_errors", 0))
					fmt.Printf("  • Write Errors: %d\n", getUint64(stats, "write_errors", 0))
				}

				// Compaction stats
				compactionCount := getUint64(stats, "compaction_count", 0)
				if compactionCount > 0 {
					fmt.Println("\n🧹 Compaction:")
					fmt.Printf("  • Compaction Count: %d\n", compactionCount)

					// Display any compaction-specific stats
					for key, value := range stats {
						if strings.HasPrefix(key, "compaction_") && key != "compaction_count" {
							// Format the key for display (remove prefix, replace underscores with spaces)
							displayKey := toTitle(strings.Replace(strings.TrimPrefix(key, "compaction_"), "_", " ", -1))
							fmt.Printf("  • %s: %v\n", displayKey, value)
						}
					}
				}

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
				} else if len(parts) == 3 && strings.ToUpper(parts[1]) == "SUFFIX" {
					// Suffix scan - we'll create a regular iterator and filter for the suffix later
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
				} else if len(parts) == 3 && strings.ToUpper(parts[1]) != "SUFFIX" {
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
				} else if len(parts) == 3 && strings.ToUpper(parts[1]) == "SUFFIX" {
					// Suffix scan - create a regular iterator and filter in the scan loop
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
				} else if len(parts) == 3 && strings.ToUpper(parts[1]) != "SUFFIX" {
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

			// Check if we're doing a suffix scan
			isSuffixScan := len(parts) == 3 && strings.ToUpper(parts[1]) == "SUFFIX"
			suffix := []byte{}
			if isSuffixScan {
				suffix = []byte(parts[2])
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

				// For suffix scans, check if the key ends with the suffix
				if isSuffixScan {
					key := iter.Key()
					if len(key) < len(suffix) || !hasSuffix(key, suffix) {
						continue
					}
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

// hasSuffix checks if a byte slice ends with a specific suffix
func hasSuffix(data, suffix []byte) bool {
	if len(data) < len(suffix) {
		return false
	}
	for i := 0; i < len(suffix); i++ {
		if data[len(data)-len(suffix)+i] != suffix[i] {
			return false
		}
	}
	return true
}

// toTitle replaces strings.Title which is deprecated
// It converts the first character of each word to title case
func toTitle(s string) string {
	prev := ' '
	return strings.Map(
		func(r rune) rune {
			if unicode.IsSpace(prev) || unicode.IsPunct(prev) {
				prev = r
				return unicode.ToTitle(r)
			}
			prev = r
			return r
		},
		s)
}
