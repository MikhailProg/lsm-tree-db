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
