package main

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

	// A table to track the highwatermark of our Jetstream listener,
	// so we can resume if interrupted.
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS cursor (
		time_us INTEGER
	);`
	_, err = db.Exec(createTableSQL)
	if err != nil {
		return db, err
	}

	_, err = db.Exec(`INSERT INTO cursor(time_us) SELECT 0 WHERE NOT EXISTS(SELECT * FROM cursor)`)
	if err != nil {
		return db, err
	}

	createTableSQL = `
	CREATE TABLE IF NOT EXISTS post_mentions (
		did TEXT NOT NULL,
		rkey TEXT NOT NULL,
		time_us INTEGER NOT NULL,
		json TEXT NOT NULL,
		processed BOOLEAN NOT NULL DEFAULT FALSE,
		PRIMARY KEY(did, rkey)
	);`
	_, err = db.Exec(createTableSQL)
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
