package main

// Subscribes to the Jetstream, resuming where we last left off.
//
// Listens for posts mentioning @doj47.com, inserts them into
// post_mentions table with minimal processing for later evaluation.

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/url"

	"github.com/gorilla/websocket"
	_ "modernc.org/sqlite"
)

const jetstreamURL = "wss://jetstream1.us-east.bsky.network/subscribe"

func jetstreamListener(db *sql.DB, done chan struct{}) {
	defer close(done)
	time_us, err := getCursor(db)
	fmt.Printf("Received time_us: %+v\n", time_us)
	if err != nil {
		log.Fatalf("Failed to find cursor: %v", err)
	}

	// Parse the Jetstream URL
	u, err := url.Parse(jetstreamURL)
	if err != nil {
		log.Fatalf("Invalid Jetstream URL: %v", err)
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

		// Only parse it if it contains a reference to doj47.com, or if it's
		// a delete.
		doj47Did := []byte("did:plc:dcclyrbpqvapa3f44zm4w4zq")
		deleteBytes := []byte("\"delete\"")
		if !(bytes.Contains(message, doj47Did) || bytes.Contains(message, deleteBytes)) {
			continue
		}

		// Parse the JSON message
		var event map[string]interface{}
		if err := json.Unmarshal(message, &event); err != nil {
			log.Printf("JSON unmarshal error: %v", err)
			continue
		}

		// Process the event
		processJetstreamEvent(db, event, message)
	}
}

func processJetstreamEvent(db *sql.DB, event map[string]interface{}, message []byte) {
	// This is quite spammy with all the deletes - if we want to log for debugging,
	// we probably 
	// fmt.Printf("Received %v\n", string(message))

	kind := event["kind"]
	if kind == "commit" {
		commit := event["commit"].(map[string]interface{})
		operation := commit["operation"].(string)
		did := event["did"].(string)
		rkey := commit["rkey"].(string)


		if operation == "create" {
			fmt.Printf("operation=%v, did=%v, rkey=%v\n", operation, did, rkey)
			// Eventually: extract if it was a reply, e.g. .commit.record.reply.parent.url
			record := commit["record"].(map[string]interface{})
			reply := record["reply"]
			var reply_to *string = nil
			if reply != nil {
				replyMap := reply.(map[string]interface{})
				parent := replyMap["parent"].(map[string]interface{})
				url := parent["uri"].(string)
				reply_to = &url
			}
			createdAt := record["createdAt"].(string)
			err := upsertPostMention(db, did, rkey, createdAt, reply_to, string(message))
			if err != nil {
				log.Fatalf("error upserting %v/%v: %v", did, rkey, err)
			}
		} else if operation == "delete" {
			err := deletePostMention(db, did, rkey)
			if err != nil {
				log.Fatalf("error deleting %v/%v: %v", did, rkey, err)
			}
		}
	}
}
