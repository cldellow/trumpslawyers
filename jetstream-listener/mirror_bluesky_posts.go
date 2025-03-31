package main

// Polls post_queue for Bluesky posts that need to be fetched
// or re-fetched.
//
// Fetches them, writes them into `posts` table, and updates their
// `next_fetch_at` time.

import (
	"database/sql"
	"log"
	"time"
)

func mirrorBlueskyPosts(db *sql.DB, done chan struct{}) {
	defer close(done)
	for {
		// Poll post_queue for the next post to fetch.
		queued_post, err := getNextQueuedPost(db)

		if err != nil {
			log.Fatalf("mirrorBlueskyPosts error: %v\n", err)
		}

		if queued_post != nil && queued_post.fetchable {
			log.Printf("queued_post %v\n", queued_post)

			post, err := GetBlueskyPost(queued_post.uri)

			if err != nil {
				log.Fatalf("GetBlueskyPost err: %v\n", err)
			}

			if post != nil {
				err := upsertPost(db, *post)
				if err != nil {
					log.Fatalf("upsertPost failed %v\n", err)
				}

				err = updateQueuedPostNextFetchAt(db, queued_post.uri)
				if err != nil {
					log.Fatalf("updateQueuedPostNextFetchAt failed %v\n", err)
				}

			}

			// Try to fetch it from the Bluesky API
			time.Sleep(1 * time.Second)
		} else {
			time.Sleep(1 * time.Second)
		}
	}
}
