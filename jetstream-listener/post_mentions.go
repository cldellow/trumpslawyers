package main

import (
	"database/sql"
	"time"
)

// Polls post_mentions for entries that have processed = 0.
//
// Does processing and sets processed = 1:
// - fetch the post they flagged (the reply_to field)
// - fetches the docket / docket entry they referenced, if
//   not already fetched

func crawlPostMentions(db *sql.DB, done chan struct{}) {
	defer close(done)
	for {
		time.Sleep(1 * time.Second)
	}
}
