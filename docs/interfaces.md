# Interfaces Package Documentation

The `interfaces` package defines the core contract between components in the Kevo engine's facade-based architecture. It provides clear, well-defined interfaces that enable modularity, testability, and separation of concerns.

## Overview

Interfaces are a crucial part of the engine's architecture, forming the boundaries between different subsystems. By defining clear interface contracts, the engine can achieve high cohesion within components and loose coupling between them.

Key responsibilities of the interfaces package include:
- Defining the Engine interface used by clients
- Specifying the contract for specialized managers (Storage, Transaction, Compaction)
- Establishing common patterns for component interaction
- Enabling dependency injection and testability
- Providing backward compatibility through interface contracts

## Core Interfaces

### Engine Interface

The `Engine` interface is the primary entry point for all client interactions:

```go
type Engine interface {
    // Data operations
    Put(key, value []byte) error
    Get(key []byte) ([]byte, error)
    Delete(key []byte) error
    IsDeleted(key []byte) (bool, error)
    
    // Iterator operations
    GetIterator() (iterator.Iterator, error)
    GetRangeIterator(startKey, endKey []byte) (iterator.Iterator, error)
    
    // Transaction support
    BeginTransaction(readOnly bool) (Transaction, error)
    
    // Management operations
    ApplyBatch(entries []*wal.Entry) error
    FlushImMemTables() error
    TriggerCompaction() error
    CompactRange(startKey, endKey []byte) error
    GetCompactionStats() (map[string]interface{}, error)
    GetStats() map[string]interface{}
    Close() error
}
```

This interface provides all core functionality expected of a storage engine.

### Manager Interfaces

The engine defines specialized manager interfaces for specific responsibilities:

#### StorageManager Interface

```go
type StorageManager interface {
    // Data operations
    Get(key []byte) ([]byte, error)
    Put(key, value []byte) error
    Delete(key []byte) error
    IsDeleted(key []byte) (bool, error)
    
    // Iterator operations
    GetIterator() (iterator.Iterator, error)
    GetRangeIterator(startKey, endKey []byte) (iterator.Iterator, error)
    
    // Management operations
    FlushMemTables() error
    ApplyBatch(entries []*wal.Entry) error
    Close() error
    
    // Statistics
    GetStorageStats() map[string]interface{}
}
```

Responsible for all data storage and retrieval operations.

#### TransactionManager Interface

```go
type TransactionManager interface {
    // Transaction operations
    BeginTransaction(readOnly bool) (Transaction, error)
    
    // Statistics
    GetTransactionStats() map[string]interface{}
}
```

Handles transaction creation and management.

#### CompactionManager Interface

```go
type CompactionManager interface {
    // Compaction operations
    TriggerCompaction() error
    CompactRange(startKey, endKey []byte) error
    
    // Lifecycle management
    Start() error
    Stop() error
    
    // Tombstone tracking
    TrackTombstone(key []byte)
    
    // Statistics
    GetCompactionStats() map[string]interface{}
}
```

Manages background compaction processes.

### Transaction Interfaces

The transaction system defines its own set of interfaces:

#### Transaction Interface

```go
type Transaction interface {
    // Data operations
    Get(key []byte) ([]byte, error)
    Put(key, value []byte) error
    Delete(key []byte) error
    
    // Iterator operations
    NewIterator() iterator.Iterator
    NewRangeIterator(startKey, endKey []byte) iterator.Iterator
    
    // Transaction control
    Commit() error
    Rollback() error
    
    // Status check
    IsReadOnly() bool
}
```

Represents an active transaction with data operations and lifecycle methods.

## Interface Implementation

### Implementation Strategies

The package defines interfaces that are implemented by concrete types in their respective packages:

1. **Facade Pattern**:
   - The `EngineFacade` implements the `Engine` interface
   - Provides a simplified interface to complex subsystems

2. **Manager Pattern**:
   - Specialized managers handle their respective areas of concern
   - Each implements the appropriate manager interface
   - Clear separation of responsibilities

3. **Backward Compatibility**:
   - Type aliasing connects the new interfaces to legacy code
   - Adapters bridge between legacy systems and new components

### Dependency Injection

The interfaces enable clean dependency injection:

```go
// The EngineFacade depends on interface contracts, not concrete implementations
type EngineFacade struct {
    storage    interfaces.StorageManager
    txManager  interfaces.TransactionManager
    compaction interfaces.CompactionManager
    // Other fields...
}
```

This makes components replaceable and testable in isolation.

## Interface Evolution

### Versioning Strategy

The interfaces package follows a careful versioning strategy:

1. **Interface Stability**:
   - Interface contracts should remain stable
   - Additions are allowed, but existing methods shouldn't change

2. **Backward Compatibility**:
   - New methods can be added to interfaces
   - Legacy systems can adapt to new interfaces via composition or wrapper types

3. **Type Aliasing**:
   - Uses Go's type aliasing for smooth transitions
   - For example: `type Engine = EngineFacade`

### Interface Design Principles

The interfaces follow several design principles:

1. **Single Responsibility**:
   - Each interface has a specific area of concern
   - Avoids bloated interfaces with mixed responsibilities

2. **Interface Segregation**:
   - Clients only depend on methods they actually use
   - Smaller, specialized interfaces

3. **Composition**:
   - Interfaces can be composed of other interfaces
   - Creates a hierarchy of capabilities

## Testing Support

The interface-based design enables easier testing:

1. **Mock Implementations**:
   - Interfaces can be mocked for unit testing
   - Tests can verify interactions with dependencies

2. **Stub Components**:
   - Simplified implementations for testing specific behaviors
   - Reduces test complexity

3. **Testable Design**:
   - Clear boundaries make integration testing more targeted
   - Each component can be tested in isolation

## Common Usage Patterns

### Client Usage

Clients interact with the engine through the Engine interface:

```go
// Create the engine
eng, err := engine.NewEngineFacade(dbPath)
if err != nil {
    log.Fatal(err)
}
defer eng.Close()

// Use the interface methods
err = eng.Put([]byte("key"), []byte("value"))
value, err := eng.Get([]byte("key"))
```

The interface hides the implementation details.

### Component Integration

Components integrate with each other through interfaces:

```go
// Transaction manager depends on storage manager
func NewManager(storage interfaces.StorageManager, stats stats.Collector) interfaces.TransactionManager {
    return &Manager{
        storage: storage,
        stats:   stats,
    }
}
```

This enables loose coupling between components.

### Extending Functionality

New functionality can be added by expanding interfaces or adding adapters:

```go
// Add a new capability through composition
type ExtendedEngine interface {
    interfaces.Engine
    
    // New methods
    GetStatistics() Statistics
    ApplySnapshot(snapshot []byte) error
}
```

## Best Practices

### Interface Design

When working with the interfaces package:

1. **Keep Interfaces Minimal**:
   - Only include methods that are essential for the interface contract
   - Avoid bloating interfaces with methods used only by a subset of clients

2. **Interface Cohesion**:
   - Methods in an interface should relate to a single responsibility
   - Prefer multiple small interfaces over single large ones

3. **Naming Conventions**:
   - Interface names should describe behavior, not implementation
   - Use method names that clearly communicate the action

### Implementing Interfaces

When implementing interfaces:

1. **Verify Implementation**:
   - Use Go's compile-time verification of interface implementation:
   ```go
   var _ interfaces.Engine = (*EngineFacade)(nil)
   ```

2. **Document interface contracts**:
   - Document performance expectations
   - Document threading and concurrency guarantees
   - Document error conditions and behaviors

3. **Consistent Error Handling**:
   - Use consistent error types across implementations
   - Document which errors can be returned by each method