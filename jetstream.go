package main

// Subscribes to the Jetstream, resuming where we last left off.
//
// Listens for posts mentioning @doj47.com, inserts them into
// post_mentions table with minimal processing for later evaluation.

import (
	//"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/url"

	"github.com/gorilla/websocket"
	_ "modernc.org/sqlite"
)

const jetstreamURL = "wss://jetstream1.us-east.bsky.network/subscribe"

func jetstreamListener(db *sql.DB, feed string, done chan struct{}) {
	defer close(done)
	time_us, err := getCursor(db, feed)
	log.Printf("feed %v has cursor time_us: %+v\n", feed, time_us)
	if err != nil {
		log.Fatalf("Failed to find cursor: %v", err)
	}

	// Parse the Jetstream URL
	u, err := url.Parse(jetstreamURL)
	if err != nil {
		log.Fatalf("Invalid Jetstream URL: %v", err)
	}

	var collections []string
	if feed == "likes" {
		// https://github.com/bluesky-social/atproto/blob/main/lexicons/app/bsky/feed/like.json
		collections = []string{"app.bsky.feed.like"}
	} else if feed == "posts" {
		// https://github.com/bluesky-social/atproto/blob/main/lexicons/app/bsky/feed/post.json
		// https://github.com/bluesky-social/atproto/blob/main/lexicons/app/bsky/feed/repost.json
		collections = []string{"app.bsky.feed.post", "app.bsky.feed.repost"}
	} else {
		log.Fatalf("unknown feed %v\n", feed)
	}

	wantedDids, err := getWatchedDids(db, feed)
	if err != nil {
		log.Fatalf("Cannot get wantedDids for feed %v: %v\n", feed, err)
	}
	// See https://github.com/bluesky-social/jetstream?tab=readme-ov-file#consuming-jetstream
	var params = map[string]interface{}{
		"wantedCollections": collections,
		"wantedDids": wantedDids,
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
		// Actually: now that we filter to specific accounts, sync immediately
		if counter % 1 == 0 {
			updateCursor(db, feed, message)
			counter = 0
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
	// fmt.Printf("Received %v\n", string(message))

	kind := event["kind"]
	if kind == "commit" {
		commit := event["commit"].(map[string]interface{})
		operation := commit["operation"].(string)
		did := event["did"].(string)
		rkey := commit["rkey"].(string)

		if operation == "create" {
			//fmt.Printf("operation=%v, did=%v, rkey=%v\n", operation, did, rkey)
			// Eventually: extract if it was a reply, e.g. .commit.record.reply.parent.url
			record := commit["record"].(map[string]interface{})

			type_ := record["$type"]

			if type_ == "app.bsky.feed.like" {
				subject := record["subject"].(map[string]interface{})
				uri := subject["uri"].(string)
				err := upsertLikes(db, did, rkey, uri)
				if err != nil {
					log.Fatalf("upsertLikes failed: %v\n", err)
				}
				err = upsertPostQueue(db, uri)
				if err != nil {
					log.Fatalf("upsertPostQueue failed: %v\n", err)
				}
			} else if type_ == "app.bsky.feed.post" {
				uri := "at://" + did + "/app.bsky.feed.post/" + rkey
				err := upsertPostQueue(db, uri)
				if err != nil {
					log.Fatalf("upsertPostQueue failed: %v\n", err)
				}
			} else if type_ == "app.bsky.feed.repost" {
				subject := record["subject"].(map[string]interface{})
				uri := subject["uri"].(string)
				err := upsertPostQueue(db, uri)
				if err != nil {
					log.Fatalf("upsertPostQueue failed: %v\n", err)
				}
			}
		} else if operation == "delete" {
			collection := commit["collection"]

			if collection == "app.bsky.feed.like" {
				err := deleteLike(db, did, rkey)
				if err != nil {
					log.Fatalf("error deleting like for did %v rkey %v\n", did, rkey)
				}
			}

			// We don't bother deleting posts, they'll get discovered and deleted
			// in our periodic post refreshing.
		}
	}
}
