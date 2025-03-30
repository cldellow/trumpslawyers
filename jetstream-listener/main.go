package main

import (
	"bytes"

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

	counter := 0

	// Read messages from Jetstream
	done := make(chan struct{})

	go func() {
		defer close(done)
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
				// Parse the JSON message
				var event map[string]interface{}
				//if err := json.Unmarshal(message, &event); err != nil {
				decoder := json.NewDecoder(bytes.NewReader(message))
				decoder.UseNumber() // 👈 prevent float64 conversion
				if err := decoder.Decode(&event); err != nil {
					log.Printf("JSON unmarshal error: %v", err)
					continue
				}
				time_us, ok := event["time_us"].(json.Number)
				if !ok {
					log.Fatalf("time_us is not a json.Number: %v")
				}

				time_usInt, err := time_us.Int64()
				if err != nil {
					log.Fatalf("time_us was not an int64: %v", err)
				}

				log.Printf("resetting time_us to %v", time_usInt)
				err = setCursor(db, time_usInt)
				if err != nil {
					log.Fatalf("Failed to set cursor: %v", err)
				}
				counter = 0
			}

			// Only parse it if it contains a reference to trumpslawyers,
			// did:plc:dcclyrbpqvapa3f44zm4w4zq
			trumpslawyersDid := []byte("did:plc:dcclyrbpqvapa3f44zm4w4zq")
			if !bytes.Contains(message, trumpslawyersDid) {
				continue
			}

			// Parse the JSON message
			var event map[string]interface{}
			if err := json.Unmarshal(message, &event); err != nil {
				log.Printf("JSON unmarshal error: %v", err)
				continue
			}

			// Process the event
			processEvent(event)
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
func processEvent(event map[string]interface{}) {
	// Extract relevant information from the event
	// For demonstration, we'll print the entire event
	// In a real application, you might want to extract specific fields
	fmt.Printf("Received event: %+v\n", event)
}
