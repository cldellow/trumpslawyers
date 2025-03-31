package main

import (
	"database/sql"
	"log"
	"regexp"
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
		upm, err := getUnprocessedPostMention(db)

		if err != nil {
			log.Fatalf("getUnprocessedPostMention error: %v\n", err)
		}

		if upm != nil {
			err = processPostMention(db, *upm)
			if err != nil {
				log.Fatalf("processPostMention error: %v\n", err)
			}

		} else {
			time.Sleep(1 * time.Second)
		}
	}
}

// ExtractDocketID extracts the numeric ID from a courtlistener.com docket URL.
// Example: https://www.courtlistener.com/docket/69741724/jgg-v-trump/ → "69741724"
func extractCourtListenerDocketID(url string) (string, string) {
	re := regexp.MustCompile(`https://www[.]courtlistener[.]com/docket/(\d+)/`)
	matches := re.FindStringSubmatch(url)
	if len(matches) < 2 {
		return "", ""
	}
	return matches[0], matches[1]
}

func extractCourtListenerRecapSlug(url string) (string, string) {
	// RECAP file: https://storage.courtlistener.com/recap/gov.uscourts.dcd.278436/gov.uscourts.dcd.278436.25.0.pdf
	re := regexp.MustCompile(`https://storage[.]courtlistener[.]com/recap/([^/]+)/[^ "/]+[.]pdf`)
	matches := re.FindStringSubmatch(url)
	if len(matches) < 2 {
		return "", ""
	}
	return matches[0], matches[1]

}

func processPostMention(db *sql.DB, upm UnprocessedPostMention) (error) {
	// If they didn't reply to a post, then there's nothing to do.
	if upm.reply_to == nil {
		err := markPostMentionAsProcessed(db, upm.did, upm.rkey)
		return err
	}
	log.Printf("upm %v\n", upm)

	// If they replied to a post, we should try to fetch the post they replied to.
	err := queuePostFetch(db, *upm.reply_to)
	if err != nil {
		return err
	}

	url, docket_id := extractCourtListenerDocketID(upm.json)
	if docket_id != "" {
		updatePostMentionCourtListenerUrl(db, upm.did, upm.rkey, url)
		updatePostMentionDocketId(db, upm.did, upm.rkey, docket_id)
	}

	url, recap_slug := extractCourtListenerRecapSlug(upm.json)
	if recap_slug != "" {
		updatePostMentionCourtListenerUrl(db, upm.did, upm.rkey, url)
		updatePostMentionRecapSlug(db, upm.did, upm.rkey, recap_slug)
	}
	
	err = markPostMentionAsProcessed(db, upm.did, upm.rkey)
	return err
}
