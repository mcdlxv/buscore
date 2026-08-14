#!/usr/bin/env bash
set -euo pipefail
[ -z "${1:-}" ] && { echo "usage: restore.sh backups/file.sql.gz"; exit 1; }
gunzip -c "$1" | docker compose exec -T db psql -U bus -d buscore
echo "Restored from $1"
