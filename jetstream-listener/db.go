package main

// Functions to interact with the doj47.sqlite database.

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"log"
	"time"
)

func initDB(dbPath string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, err
	}

	// Enable WAL mode
	_, err = db.Exec(`PRAGMA journal_mode=WAL`)
	if err != nil {
		return db, err
	}

	_, err = db.Exec(`PRAGMA busy_timeout = 30000`)
	if err != nil {
		return db, err
	}

	// A table to track the highwatermark of our Jetstream listener,
	// so we can resume if interrupted.
	stmt := `
	CREATE TABLE IF NOT EXISTS cursor (
		time_us INTEGER
	);`
	_, err = db.Exec(stmt)
	if err != nil {
		return db, err
	}

	_, err = db.Exec(`INSERT INTO cursor(time_us) SELECT 0 WHERE NOT EXISTS(SELECT * FROM cursor)`)
	if err != nil {
		return db, err
	}

	stmt = `
	CREATE TABLE IF NOT EXISTS post_mentions (
		did TEXT NOT NULL,
		rkey TEXT NOT NULL,
		created_at TEXT NOT NULL,
		json TEXT NOT NULL,
		reply_to TEXT,
		processed BOOLEAN NOT NULL DEFAULT FALSE,
		courtlistener_url TEXT, -- e.g. https://storage.courtlistener.com/recap/gov.uscourts.dcd.278436/gov.uscourts.dcd.278436.25.0.pdf or https://www.courtlistener.com/docket/69741724/jgg-v-trump/
		docket_id INTEGER, -- e.g. 69741724
		recap_slug TEXT, -- e.g. gov.uscourts.dcd.278436
		PRIMARY KEY(did, rkey)
	);`
	_, err = db.Exec(stmt)
	if err != nil {
		return db, err
	}

	_, err = db.Exec(`CREATE INDEX IF NOT EXISTS idx_post_mentions_unprocessed ON post_mentions(did, rkey) WHERE processed = FALSE`)
	if err != nil {
		return db, err
	}

	_, err = db.Exec(`CREATE INDEX IF NOT EXISTS idx_post_mentions_docket_id ON post_mentions(docket_id) WHERE docket_id IS NOT NULL`)
	if err != nil {
		return db, err
	}

	_, err = db.Exec(`CREATE INDEX IF NOT EXISTS idx_post_mentions_recap_slug ON post_mentions(recap_slug) WHERE recap_slug IS NOT NULL`)
	if err != nil {
		return db, err
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS post_queue(
	uri TEXT NOT NULL,
	next_fetch_at TEXT,
	PRIMARY KEY (uri)
)`)
	if err != nil {
		return db, err
	}

	_, err = db.Exec(`CREATE INDEX IF NOT EXISTS idx_post_queue_next_fetch_at ON post_queue(next_fetch_at DESC)`)
	if err != nil {
		return db, err
	}

	stmt = `
CREATE TABLE IF NOT EXISTS posts (
	url TEXT NOT NULL,
  did TEXT NOT NULL,
	handle TEXT NOT NULL,
	display_name TEXT NOT NULL,
	avatar TEXT NOT NULL,
	created_at TEXT NOT NULL,
	replies INTEGER NOT NULL,
	reposts INTEGER NOT NULL,
	likes INTEGER NOT NULL,
	quotes INTEGER NOT NULL,
	json TEXT NOT NULL,
	PRIMARY KEY (url)
)
`
	_, err = db.Exec(stmt)
	if err != nil {
		return db, err
	}

	return db, err
}

func getCursor(db *sql.DB) (int64, error) {
	var count int64
	err := db.QueryRow(`SELECT time_us FROM cursor`).Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, nil
}

func setCursor(db *sql.DB, time_us int64) (error) {
	_, err := db.Exec(`UPDATE cursor SET time_us = ?`, time_us)
	return err
}

func updateCursor(db *sql.DB, message []byte) {
	// Parse the JSON message
	var event map[string]interface{}
	//if err := json.Unmarshal(message, &event); err != nil {
	decoder := json.NewDecoder(bytes.NewReader(message))
	decoder.UseNumber() // 👈 prevent float64 conversion
	if err := decoder.Decode(&event); err != nil {
		log.Fatalf("JSON unmarshal error: %v", err)
	}
	time_us, ok := event["time_us"].(json.Number)
	if !ok {
		log.Fatalf("time_us is not a json.Number: %v")
	}

	time_usInt, err := time_us.Int64()
	if err != nil {
		log.Fatalf("time_us was not an int64: %v", err)
	}

	log.Printf("resetting time_us to %v (%vs ago)", time_usInt, (time.Now().UnixMicro() - time_usInt) / 1e6)
	err = setCursor(db, time_usInt)
	if err != nil {
		log.Fatalf("Failed to set cursor: %v", err)
	}
}

func upsertPostMention(db *sql.DB, did string, rkey string, createdAt string, replyTo *string, json string) (error) {
	_, err := db.Exec(`INSERT OR IGNORE INTO post_mentions(did, rkey, created_at, reply_to, json) VALUES (?, ?, ?, ?, ?)`, did, rkey, createdAt, replyTo, json)
	return err
}

func deletePostMention(db *sql.DB, did string, rkey string) (error) {
	_, err := db.Exec(`DELETE FROM post_mentions WHERE did = ? AND rkey = ?`, did, rkey)
	return err
}

type UnprocessedPostMention struct {
	did string
	rkey string
	reply_to *string
	json string
};

func getUnprocessedPostMention(db *sql.DB) (*UnprocessedPostMention, error) {
	row := db.QueryRow(`SELECT did, rkey, reply_to, json FROM post_mentions WHERE NOT processed LIMIT 1`)
	var upm UnprocessedPostMention
	err := row.Scan(&upm.did, &upm.rkey, &upm.reply_to, &upm.json)
	if err == sql.ErrNoRows {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	return &upm, nil
}

func markPostMentionAsProcessed(db *sql.DB, did string, rkey string) (error) {
	_, err := db.Exec(`UPDATE post_mentions SET processed = TRUE WHERE did = ? AND rkey = ?`, did, rkey)
	return err
}

func updatePostMentionDocketId(db *sql.DB, did string, rkey string, docket_id string) (error) {
	_, err := db.Exec(`UPDATE post_mentions SET docket_id = ? WHERE did = ? AND rkey = ?`, docket_id, did, rkey)
	return err
}

func updatePostMentionRecapSlug(db *sql.DB, did string, rkey string, recap_slug string) (error) {
	_, err := db.Exec(`UPDATE post_mentions SET recap_slug = ? WHERE did = ? AND rkey = ?`, recap_slug, did, rkey)
	return err
}

func updatePostMentionCourtListenerUrl(db *sql.DB, did string, rkey string, url string) (error) {
	_, err := db.Exec(`UPDATE post_mentions SET courtlistener_url = ? WHERE did = ? AND rkey = ?`, url, did, rkey)
	return err
}

func queuePostFetch(db *sql.DB, uri string) (error) {
	_, err := db.Exec(`INSERT OR IGNORE INTO post_queue(uri) VALUES(?)`, uri)
	return err
}

type NextQueuedPost struct {
	uri string
	fetchable bool
};
func getNextPost(db *sql.DB) (*NextQueuedPost, error) {
	row := db.QueryRow(`SELECT uri, next_fetch_at IS NULL OR next_fetch_at < datetime() AS fetchable FROM post_queue ORDER BY next_fetch_at DESC NULLS FiRST LIMIT 1`)
	var nqp NextQueuedPost
	err := row.Scan(&nqp.uri, &nqp.fetchable)
	if err == sql.ErrNoRows {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	return &nqp, nil
}
