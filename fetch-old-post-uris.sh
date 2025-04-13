#!/usr/bin/env bash

set -euo pipefail

if [ $# -ne 1 ]; then
  echo "Usage: $0 <DID>"
  exit 1
fi

DID="$1"
ENDPOINT="https://public.api.bsky.app/xrpc/app.bsky.feed.getAuthorFeed"
CURSOR=""
CUTOFF_DATE="2025-01-20T00:00:00Z"

while :; do
  URL="${ENDPOINT}?actor=${DID}&limit=100"
  if [ -n "$CURSOR" ]; then
    URL="${URL}&cursor=${CURSOR}"
  fi

  RESPONSE=$(curl -s "$URL")

  echo "$RESPONSE" | jq -r \
    --arg cutoff "$CUTOFF_DATE" '
      .feed[]
      | select(.post.indexedAt > $cutoff)
      | .post.uri
    '

  # Break if there are no more posts
  POST_COUNT=$(echo "$RESPONSE" | jq '.feed | length')
  if [ "$POST_COUNT" -eq 0 ]; then
    break
  fi

  # Update cursor
  CURSOR=$(echo "$RESPONSE" | jq -r '.cursor')

  # Stop if the oldest post is older than cutoff
  #OLDEST_DATE=$(echo "$RESPONSE" | jq -r '.feed[-1].post.indexedAt // empty')
  OLDEST_DATE="$CURSOR"
  if [[ "$OLDEST_DATE" < "$CUTOFF_DATE" ]]; then
    break
  fi
done
