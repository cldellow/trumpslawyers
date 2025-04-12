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
		feed TEXT PRIMARY KEY,
		time_us INTEGER
	);`
	_, err = db.Exec(stmt)
	if err != nil {
		return db, err
	}

	_, err = db.Exec(`INSERT INTO cursor(feed, time_us) SELECT 'likes', 0 WHERE NOT EXISTS(SELECT * FROM cursor WHERE feed = 'likes')`)
	if err != nil {
		return db, err
	}

	_, err = db.Exec(`INSERT INTO cursor(feed, time_us) SELECT 'posts', 0 WHERE NOT EXISTS(SELECT * FROM cursor WHERE feed = 'posts')`)
	if err != nil {
		return db, err
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS watched_dids(
	did TEXT NOT NULL,
	feed TEXT NOT NULL CHECK (feed IN ('likes', 'posts')),
	PRIMARY KEY (did, feed)
)`)
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
	uri TEXT NOT NULL,
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
	PRIMARY KEY (uri)
)
`
	_, err = db.Exec(stmt)
	if err != nil {
		return db, err
	}

	// For DIDs whose likes we track, track which posts they liked.
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS likes(
	uri TEXT NOT NULL,
	did TEXT NOT NULL,
	rkey TEXT NOT NULL,
	PRIMARY KEY (did, rkey, uri)
)`)
	if err != nil {
		return db, err
	}

	return db, err
}

func getCursor(db *sql.DB, feed string) (int64, error) {
	var count int64
	err := db.QueryRow(`SELECT time_us FROM cursor WHERE feed = ?`, feed).Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, nil
}

func setCursor(db *sql.DB, feed string, time_us int64) (error) {
	_, err := db.Exec(`UPDATE cursor SET time_us = ? WHERE feed = ?`, time_us, feed)
	return err
}

func updateCursor(db *sql.DB, feed string, message []byte) {
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
	err = setCursor(db, feed, time_usInt)
	if err != nil {
		log.Fatalf("Failed to set cursor: %v", err)
	}
}

func getWatchedDids(db *sql.DB, feed string) ([]string, error) {
	rows, err := db.Query(`SELECT did FROM watched_dids WHERE feed = $1`, feed)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var dids []string
	for rows.Next() {
		var did string
		if err := rows.Scan(&did); err != nil {
			return nil, err
		}
		dids = append(dids, did)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return dids, nil
}

func upsertLikes(db *sql.DB, did string, rkey string, uri string) (error) {
	_, err := db.Exec(`INSERT OR IGNORE INTO likes(did, rkey, uri) VALUES (?, ?, ?)`, did, rkey, uri)
	return err
}

func deleteLike(db *sql.DB, did string, rkey string) (error) {
	_, err := db.Exec(`DELETE FROM likes WHERE did = ? AND rkey = ?`, did, rkey)
	return err
}

func deletePost(db *sql.DB, uri string) (error) {
	_, err := db.Exec(`DELETE FROM posts WHERE uri = ?`, uri)
	return err
}

func deletePostQueue(db *sql.DB, uri string) (error) {
	_, err := db.Exec(`DELETE FROM post_queue WHERE uri = ?`, uri)
	return err
}

func upsertPostQueue(db *sql.DB, uri string) (error) {
	_, err := db.Exec(`INSERT OR IGNORE INTO post_queue(uri) VALUES(?)`, uri)
	return err
}

type NextQueuedPost struct {
	uri string
	fetchable bool
};
func getNextQueuedPost(db *sql.DB) (*NextQueuedPost, error) {
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

func updateQueuedPostNextFetchAt(db *sql.DB, uri string) (error) {
	_, err := db.Exec(`UPDATE post_queue SET next_fetch_at = DATETIME('now', '1 day') WHERE uri = ?`, uri)
	return err
}

func upsertPost(db *sql.DB, post PostInfo) (error) {
	/*
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
	*/
	_, err := db.Exec(`INSERT INTO posts(uri, did, handle, display_name, avatar, created_at, replies, reposts, likes, quotes, json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (uri) DO UPDATE SET handle = excluded.handle, display_name = excluded.display_name, avatar = excluded.avatar, created_at = excluded.created_at, replies = excluded.replies, reposts = excluded.reposts, likes = excluded.likes, quotes = excluded.quotes, json = excluded.json`,
	post.URL,
	post.DID,
	post.Handle,
	post.DisplayName,
	post.Avatar,
	post.CreatedAt,
	post.Replies,
	post.Reposts,
	post.Likes,
	post.Quotes,
	post.JSON,
)

	return err
}
