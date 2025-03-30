package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"

	"github.com/gorilla/websocket"
	_ "modernc.org/sqlite"
)

const jetstreamURL = "wss://jetstream1.us-east.bsky.network/subscribe"
const dbPath = "./doj47.sqlite"

func main() {
	db, err := initDB(dbPath)
	if err != nil {
		log.Fatalf("Failed to init DB: %v", err)
	}

	// Parse the Jetstream URL
	u, err := url.Parse(jetstreamURL)
	if err != nil {
		log.Fatalf("Invalid Jetstream URL: %v", err)
	}

	time_us, err := getCursor(db)
	fmt.Printf("Received time_us: %+v\n", time_us)
	if err != nil {
		log.Fatalf("Failed to find cursor: %v", err)
	}
	// Subscription parameters
	var params = map[string]interface{}{
		"wantedCollections": []string{"app.bsky.feed.post"},
		"cursor": time_us,
	}

	// Add query parameters to the URL
	query := u.Query()
	for key, value := range params {
		switch v := value.(type) {
		case []string:
			for _, item := range v {
				query.Add(key, item)
			}
		default:
			query.Add(key, fmt.Sprintf("%v", v))
		}
	}
	u.RawQuery = query.Encode()

	// Establish WebSocket connection
	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatalf("Failed to connect to Jetstream: %v", err)
	}
	defer c.Close()

	// Channel to handle interrupt signals
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	// Read messages from Jetstream
	done := make(chan struct{})

	go func() {
		defer close(done)
		counter := 0
		for {
			_, message, err := c.ReadMessage()
			if err != nil {
				log.Printf("Read error: %v", err)
				return
			}

			// Periodically sync time_us into the progress table
			// so that we can resume if interrupted.
			counter = counter + 1
			if counter % 1000 == 0 {
				updateCursor(db, message)
				counter = 0
			}

			// Only parse it if it contains a reference to doj47.com
			doj47Did := []byte("did:plc:dcclyrbpqvapa3f44zm4w4zq")
			if !bytes.Contains(message, doj47Did) {
				continue
			}

			// Parse the JSON message
			var event map[string]interface{}
			if err := json.Unmarshal(message, &event); err != nil {
				log.Printf("JSON unmarshal error: %v", err)
				continue
			}

			// Process the event
			processEvent(db, event, message)
		}
	}()

	// Wait for interrupt signal to gracefully shutdown
	select {
	case <-done:
	case <-interrupt:
		log.Println("Interrupt received, closing connection")
		c.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	}
}

// processEvent handles the incoming event
func processEvent(db *sql.DB, event map[string]interface{}, message []byte) {
	fmt.Printf("Received %v\n", string(message))

	kind := event["kind"]
	if kind == "commit" {
		commit := event["commit"].(map[string]interface{})
		operation := commit["operation"].(string)
		did := event["did"].(string)
		rkey := commit["rkey"].(string)

		record := commit["record"].(map[string]interface{})
		createdAt := record["createdAt"].(string)

		fmt.Printf("operation=%v, did=%v, rkey=%v\n", operation, did, rkey)

		// Eventually: extract if it was a reply, e.g. .commit.record.reply.parent.url
		if operation == "create" {
			err := upsertPostMention(db, did, rkey, createdAt, string(message))
			if err != nil {
				log.Fatalf("error upserting %v/%v: %v", did, rkey, err)
			}
		}
	}
//	upsertPostMessage(db, 
}
