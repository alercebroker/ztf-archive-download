#!/usr/bin/env bash
set -euo pipefail

usage() {
    echo "Usage: $0 <md5sums_file> <tar_dir> [jobs]"
    exit 1
}

[[ $# -lt 2 || $# -gt 3 ]] && usage

MD5FILE="$1"
TARDIR="$2"
JOBS="${3:-$(nproc)}"

[[ -f "$MD5FILE" ]] || { echo "ERROR: $MD5FILE not found"; exit 1; }
[[ -d "$TARDIR" ]] || { echo "ERROR: $TARDIR not found"; exit 1; }

# Build normalized checksum file (strip any leading path from filenames)
TMPFILE=$(mktemp)
FILTERED=$(mktemp)
BATCHDIR=$(mktemp -d)
PROGRESSDIR=$(mktemp -d)
trap 'rm -f "$TMPFILE" "$FILTERED"; rm -rf "$BATCHDIR" "$PROGRESSDIR"' EXIT

while IFS= read -r line || [[ -n "$line" ]]; do
    [[ -z "$line" ]] && continue
    hash=$(awk '{print $1}' <<< "$line")
    fname=$(awk '{print $2}' <<< "$line" | xargs basename)
    echo "$hash  $fname"
done < "$MD5FILE" > "$TMPFILE"

# Detect missing: in checksum file but not in dir
> missing.txt
while IFS= read -r line; do
    fname=$(awk '{print $2}' <<< "$line")
    if [[ ! -f "$TARDIR/$fname" ]]; then
        echo "WARN: in checksum file but not in dir: $fname"
        echo "$fname" >> missing.txt
    fi
done < "$TMPFILE"

# Warn: tar.gz files in dir with no checksum entry
declare -A in_checksums
while IFS= read -r line; do
    fname=$(awk '{print $2}' <<< "$line")
    in_checksums["$fname"]=1
done < "$TMPFILE"

for f in "$TARDIR"/*.tar.gz; do
    [[ -e "$f" ]] || continue
    fname=$(basename "$f")
    [[ -v in_checksums["$fname"] ]] || echo "WARN: in dir but not in checksum file: $fname"
done

# Filter to files present in both
while IFS= read -r line; do
    fname=$(awk '{print $2}' <<< "$line")
    [[ -f "$TARDIR/$fname" ]] && echo "$line"
done < "$TMPFILE" > "$FILTERED"

TOTAL=$(wc -l < "$FILTERED")
echo ""
echo "Running checksums on $TOTAL files using $JOBS jobs..."

split -n "l/$JOBS" "$FILTERED" "$BATCHDIR/batch_"

# Background progress printer
(
    while true; do
        n=$(find "$PROGRESSDIR" -maxdepth 1 -type f | wc -l)
        printf '\r[%d/%d]' "$n" "$TOTAL"
        sleep 0.3
    done
) &
PROGRESS_PID=$!

pids=()
shopt -s nullglob
for batch in "$BATCHDIR"/batch_*; do
    BATCH_OK="$BATCHDIR/ok_$(basename "$batch")"
    BATCH_FAIL="$BATCHDIR/fail_$(basename "$batch")"
    (
        while IFS= read -r line; do
            fname=$(awk '{print $2}' <<< "$line")
            if (cd "$TARDIR" && printf '%s\n' "$line" | md5sum --check --quiet 2>/dev/null); then
                echo "$fname" >> "$BATCH_OK"
            else
                echo "$fname" >> "$BATCH_FAIL"
            fi
            mktemp -p "$PROGRESSDIR" > /dev/null
        done < "$batch"
    ) &
    pids+=($!)
done

for pid in "${pids[@]}"; do
    wait "$pid" || true
done

kill "$PROGRESS_PID" 2>/dev/null || true
wait "$PROGRESS_PID" 2>/dev/null || true
printf '\r[%d/%d]\n' "$TOTAL" "$TOTAL"

# Merge batch results into output files
> ok.txt
> corrupted.txt
for f in "$BATCHDIR"/ok_batch_*; do
    cat "$f" >> ok.txt
done
for f in "$BATCHDIR"/fail_batch_*; do
    cat "$f" >> corrupted.txt
done

OK_COUNT=$(wc -l < ok.txt)
CORRUPTED_COUNT=$(wc -l < corrupted.txt)
MISSING_COUNT=$(wc -l < missing.txt)

echo "OK: $OK_COUNT  |  Corrupted: $CORRUPTED_COUNT  |  Missing: $MISSING_COUNT"
echo "Results written to ok.txt, corrupted.txt, missing.txt"

[[ "$CORRUPTED_COUNT" -eq 0 ]] || { echo "ERROR: $CORRUPTED_COUNT checksum(s) failed"; exit 1; }
