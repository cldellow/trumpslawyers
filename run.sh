#!/bin/bash
LOGFILE="scraper.log"

while true; do
  echo "Starting app at $(date)" >> "$LOGFILE"
  go run *.go >> "$LOGFILE" 2>&1
  echo "App crashed at $(date), restarting in 5 seconds..." >> "$LOGFILE"
  sleep 5
done
