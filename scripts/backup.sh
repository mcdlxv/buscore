#!/usr/bin/env bash
set -euo pipefail
mkdir -p backups
FILE="backups/buscore-$(date +%Y%m%d-%H%M%S).sql.gz"
docker compose exec -T db pg_dump -U bus buscore | gzip > "$FILE"
echo "Backup: $FILE"
