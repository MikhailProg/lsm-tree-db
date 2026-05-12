# LSM-Tree Key-Value Storage Engine

It is a pet project. A lightweight (under 2.5k LOC) key-value storage engine implemented, based on the **LSM-tree (Log-Structured Merge-Tree)** architecture. This project implements concepts used in modern NoSQL databases (LevelDB).

## Core Features

### 1. In-Memory Storage (MemTable)
*   **SkipList**: In-memory storage with $O(\log N)$ complexity for search and insertion.
*   **WAL (Write-Ahead Log)**: Ensures durability by logging operations before applying them. Uses **CRC32 checksums** to detect data corruption during crashes.

### 2. Disk Persistence (SSTable)
*   **Simple Binary Format**: includes Data Blocks, Index Blocks, Bloom Filters, and a Footer with Magic verification.
*   **Full Indexing**: In-memory index for fast seeking within large SST files using binary search (can be improved with sparced index).
*   **Bloom Filter**: Integrated **CRC64-based Bloom filters** to minimize disk I/O by filtering out non-existent keys before hitting the disk.

### 3. LSM Engine Architecture
*   **Write Pipeline**: Optimized using a `Write Queue` and `sync.Pool` to achieve high throughput, linearize operations, and reduce GC pressure.
*   **Snapshot Isolation**: Supports consistent range scans. When a `Scan` is initiated, the engine captures a consistent "view" of the data without blocking incoming `Put` requests.
*   **Thread-Safe Resource Management**: Implements a custom **Reference Counting (RC)** system to manage the lifecycle of SSTables and MemTables. Files are deleted only when they are no longer held by any active iterators.
*   **Deterministic Crash Recovery**: Uses a **Write-Ahead Log (WAL)** and **Sequence Numbers** to guarantee data durability and restore the correct chronological order of files after a crash. Scans the data directory on startup, reconstructs the file index, loads metadata for existing SSTables, and replays unfinished WAL logs.
*   **Backpressure & Resource Control**: Managed via semaphores (`flushSemFrozen`) to prevent Out-Of-Memory (OOM) during high-load.
*   **Auto-Flush & Rotation**: Background goroutines automatically transition filled MemTables to Frozen state and flush them to disk as SSTables.
*   **Auto-Compaction**: Periodically merges multiple SSTables into a single file to reclaim space, handle duplicates, and maintain read performance.

### 4. Iteration
*   **Iterators**: Implementation of `Seek/Valid/Next/Key/Value` interface for both memory and disk layers.
*   **Merge Iterator**: A multi-layer iterator using a **Priority Queue (Heap)** to unify data from Active MemTable, Frozen MemTables, and multiple SSTables with proper version priority.

## Usage

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/MikhailProg/lsm-tree-db/lsm"
)

func main() {
	// Initialize the DB
	config := lsm.DefaultConfig("./demodb")
	db, err := lsm.Open(config, context.Background())
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Writing data
	db.Put("user100", []byte("Roy"))
	db.Put("user200", []byte("Tom"))

	// Point lookup
	val, ok, _ := db.Get("user100")
	if ok {
		fmt.Printf("Found: %s\n", string(val))
	}

	// Range Scan (Snapshot Isolation)
	it, _ := db.Scan("user0", "user999")
	for it.Valid() {
		fmt.Printf("%s: %s\n", it.Key(), string(it.Value()))
		it.Next()
	}
	it.Close()
}
```
