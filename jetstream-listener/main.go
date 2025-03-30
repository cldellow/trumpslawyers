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
	db, err := initDB(dbPath)
	if err != nil {
		log.Fatalf("Failed to init DB: %v", err)
	}

	// Channel to handle interrupt signals
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	// Read messages from Jetstream
	done := make(chan struct{})

	go jetstreamListener(db, done)
	go crawlPostMentions(db, done)

	// Wait for interrupt signal to gracefully shutdown
	select {
	case <-done:
	case <-interrupt:
		log.Println("Interrupt received, closing connection")
		// c.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	}
}
