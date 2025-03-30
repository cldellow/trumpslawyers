package main

import (
	"database/sql"
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


