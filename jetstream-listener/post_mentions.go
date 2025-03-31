package main

import (
	"database/sql"
	"time"
	"log"
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
		upm, err := getUnprocessedPostMention(db)

		if err != nil {
			log.Fatalf("getUnprocessedPostMention error: %v\n", err)
		}

		if upm != nil {
			processPostMention(db, *upm)
		} else {
			time.Sleep(1 * time.Second)
		}
	}
}

func processPostMention(db *sql.DB, upm UnprocessedPostMention) (error) {
	// If they didn't reply to a post, then there's nothing to do.
	if upm.reply_to == nil {
		err := markPostMentionAsProcessed(db, upm.did, upm.rkey)
		return err
	}

	// If they replied to a post, we should try to fetch it.
	// Idea: we should enqueue it to be fetched.

	// We should detect if they included a CourtListener URL:
	// Docket: https://www.courtlistener.com/docket/69741724/jgg-v-trump/
	// RECAP file: https://storage.courtlistener.com/recap/gov.uscourts.dcd.278436/gov.uscourts.dcd.278436.25.0.pdf
	// ...and populate the top-level docket or recap fields accordingly.

	log.Printf("upm %v\n", upm)
	return nil
}


