package main

// The orchestrator: kicks off some goroutines, listens for
// Ctrl+C.

import (
	"log"
	"os"
	"os/signal"

	_ "modernc.org/sqlite"
)

const dbPath = "./doj47.sqlite"

func main() {
	jetstreamDb, err := initDB(dbPath)
	if err != nil {
		log.Fatalf("Failed to init Jetstream DB: %v", err)
	}

	crawlPostMentionsDb, err := initDB(dbPath)
	if err != nil {
		log.Fatalf("Failed to init post mentions DB: %v", err)
	}

	// Channel to handle interrupt signals
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	// Read messages from Jetstream
	done := make(chan struct{})

	// Each goroutine gets its own DB. AFAICT, goroutines are green threads,
	// so if we re-use a DB handle, we'll have re-entrant calls to the DB
	// from the same thread. We could put locks in the db functions, or just give
	// each goroutine its own handle.
	go jetstreamListener(jetstreamDb, done)
	go crawlPostMentions(crawlPostMentionsDb, done)

	// Wait for interrupt signal to gracefully shutdown
	select {
	case <-done:
	case <-interrupt:
		log.Println("Interrupt received, closing connection")
		// c.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	}
}
