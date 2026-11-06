#!/bin/bash
set -euo pipefail

# Wrapper for code/consulter_copier.py to process USB disks and write a per-disk log.
# Usage:
#   CONSULTER_DB_DSN="dbname=scrapper" ./run_usb_copy.sh
# or export CONSULTER_DB_DSN first.

DB_DSN=${CONSULTER_DB_DSN:-}
if [[ -z "$DB_DSN" ]]; then
  echo "Error: CONSULTER_DB_DSN is not set. Export it before running the script."
  echo "Example: export CONSULTER_DB_DSN='dbname=scrapper'"
  exit 1
fi

PYTHON="/home/uzr/v_envs/Scrapper/bin/python"
CONSULTER="/home/uzr/code/consulter_copier.py"
OUTPUT_ROOT="/home/uzr/data_full"
LOG_FILE="/home/uzr/code/usb_copy_run.log"
MOUNT_ROOT="/media/$(whoami)"

mkdir -p "$(dirname "$LOG_FILE")"
mkdir -p "$OUTPUT_ROOT"

exec >>"$LOG_FILE" 2>&1

echo "========================================"
echo "USB copy run started: $(date +'%F %T')"
echo "DB DSN: ${DB_DSN}"
echo "Output root: ${OUTPUT_ROOT}"
echo "Log file: ${LOG_FILE}"
echo "Mount root: ${MOUNT_ROOT}"
echo ""

if [[ ! -d "$MOUNT_ROOT" ]]; then
  echo "ERROR: mount root does not exist: ${MOUNT_ROOT}"
  exit 1
fi

mapfile -t MOUNT_PATHS < <(find "$MOUNT_ROOT" -maxdepth 1 -mindepth 1 -type d | sort)
if [[ ${#MOUNT_PATHS[@]} -eq 0 ]]; then
  echo "No mounted disks found under ${MOUNT_ROOT}."
  exit 1
fi

sanitize_disk_id() {
  local label="$1"
  local sanitized
  sanitized=$(basename "$label" | tr '[:lower:]' '[:upper:]' | sed 's/[^A-Z0-9]/_/g' | sed 's/^_*//; s/_*$//; s/_\+/_/g')
  if [[ -z "$sanitized" ]]; then
    sanitized="DISK_$2"
  fi
  echo "$sanitized"
}

for i in "${!MOUNT_PATHS[@]}"; do
  mount_path="${MOUNT_PATHS[$i]}"
  disk_id=$(sanitize_disk_id "$mount_path" "$((i + 1))")

  echo "---"
  echo "Processing disk: ${disk_id}"
  echo "Mount path: ${mount_path}"
  echo "Start: $(date +'%F %T')"

  if [[ ! -d "$mount_path" ]]; then
    echo "ERROR: mount path does not exist: ${mount_path}"
    continue
  fi

  set +e
  PYTHONUNBUFFERED=1 "$PYTHON" "$CONSULTER" "$mount_path" \
    --disk-id "$disk_id" \
    --output-path "$OUTPUT_ROOT" \
    --db-dsn "$DB_DSN"
  rc=$?
  set -e

  echo "Exit code: $rc"
  echo "End: $(date +'%F %T')"
  echo ""
done

echo "USB copy run finished: $(date +'%F %T')"
echo "========================================"
