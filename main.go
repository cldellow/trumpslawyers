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
	jetstreamLikesDb, err := initDB(dbPath)
	if err != nil {
		log.Fatalf("Failed to init Jetstream likes DB: %v", err)
	}

	jetstreamPostsDb, err := initDB(dbPath)
	if err != nil {
		log.Fatalf("Failed to init Jetstream posts DB: %v", err)
	}

	mirrorBlueskyPostsDb, err := initDB(dbPath)
	if err != nil {
		log.Fatalf("Failed to init mirror Bluesky posts DB: %v", err)
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
	go jetstreamListener(jetstreamLikesDb, "likes", done)
	go jetstreamListener(jetstreamPostsDb, "posts", done)
	go mirrorBlueskyPosts(mirrorBlueskyPostsDb, done)

	// Wait for interrupt signal to gracefully shutdown
	select {
	case <-done:
	case <-interrupt:
		log.Println("Interrupt received, closing connection")
		// c.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	}
}
