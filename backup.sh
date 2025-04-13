#!/bin/bash
set -euo pipefail

# Paths
DB_PATH="doj47.sqlite"
BACKUP_NAME="doj47-$(date +%Y%m%d-%H%M%S).sqlite"
TEMP_BACKUP="/tmp/$BACKUP_NAME"

cleanup() {
     rm -f "${TEMP_BACKUP}"*
}
trap cleanup EXIT

cd "$(dirname "$0")"

# Step 1: Use sqlite3 to safely create a backup
sqlite3 "$DB_PATH" ".backup '$TEMP_BACKUP'"

zstd "${TEMP_BACKUP}"

# Step 2: Upload the backup to R2 using rclone
rclone copyto "$TEMP_BACKUP".zst "r2:/doj47/backups/${BACKUP_NAME}.zst" --s3-no-check-bucket
rclone copyto "$TEMP_BACKUP".zst "r2:/doj47/doj47.sqlite.zst" --s3-no-check-bucket

echo "Backup completed and uploaded to r2://doj47/${BACKUP_NAME}.zst"
