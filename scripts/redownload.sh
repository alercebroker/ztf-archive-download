#!/usr/bin/env bash
set -euo pipefail

BASE_URL="https://ztf.uw.edu/alerts/public"

usage() { echo "Usage: $0 <md5sums_file> <tar_dir> [jobs]"; exit 1; }

[[ $# -lt 2 || $# -gt 3 ]] && usage

MD5FILE="$1"
TARDIR="$2"
JOBS="${3:-2}"

[[ -f "$MD5FILE" ]] || { echo "ERROR: $MD5FILE not found"; exit 1; }
[[ -d "$TARDIR" ]] || { echo "ERROR: $TARDIR not found"; exit 1; }
[[ -f corrupted.txt || -f missing.txt ]] || {
    echo "ERROR: neither corrupted.txt nor missing.txt found"; exit 1
}

touch done.txt

WORKLIST=$(mktemp)
LOCKFILE=$(mktemp)
trap 'rm -f "$WORKLIST" "$LOCKFILE"' EXIT

# Normalized checksum lookup: fname -> hash
declare -A CHECKSUMS
while IFS= read -r line || [[ -n "$line" ]]; do
    [[ -z "$line" ]] && continue
    hash=$(awk '{print $1}' <<< "$line")
    fname=$(awk '{print $2}' <<< "$line" | xargs basename)
    CHECKSUMS["$fname"]="$hash"
done < "$MD5FILE"

# Load already-done set for O(1) skip checks
declare -A DONE
while IFS= read -r f; do DONE["$f"]=1; done < done.txt

# Combined worklist from corrupted + missing, deduplicated
{
    [[ -f corrupted.txt ]] && cat corrupted.txt || true
    [[ -f missing.txt ]]   && cat missing.txt   || true
} | sort -u > "$WORKLIST"

TOTAL=$(wc -l < "$WORKLIST")
NDONE=$(wc -l < done.txt | tr -d ' ')
echo "Files to process: $TOTAL  |  Already done: $NDONE"

# ── helpers ──────────────────────────────────────────────────────────────────

verify() {
    echo "$2  $1" | (cd "$TARDIR" && md5sum --check --quiet 2>/dev/null)
}

mark_done() {
    (flock 9; echo "$1" >> done.txt) 9>"$LOCKFILE"
}

worker() {
    local fname="$1" hash="$2"
    local dest="$TARDIR/$fname"
    local url="$BASE_URL/$fname"

    # Attempt 1: -C - resumes a partial file or starts fresh
    if curl -L -C - --fail -s -S -o "$dest" "$url"; then
        if verify "$fname" "$hash"; then
            mark_done "$fname"
            echo "OK   $fname"
            return 0
        fi
        echo "WARN: bad checksum on attempt 1 for $fname — retrying fresh"
    else
        echo "WARN: curl error on attempt 1 for $fname — retrying fresh"
    fi

    # Attempt 2: clean slate
    rm -f "$dest"
    if curl -L --fail -s -S -o "$dest" "$url"; then
        if verify "$fname" "$hash"; then
            mark_done "$fname"
            echo "OK   $fname"
            return 0
        fi
        echo "ERROR: bad checksum on attempt 2 for $fname"
    else
        echo "ERROR: curl error on attempt 2 for $fname"
    fi

    return 1
}

# ── job pool ─────────────────────────────────────────────────────────────────

pids=()

# Wait for any one worker; remove it from pids; return its exit code.
drain_one() {
    wait -n "${pids[@]}"
    local rc=$?
    local new=()
    for p in "${pids[@]}"; do
        kill -0 "$p" 2>/dev/null && new+=("$p")
    done
    pids=("${new[@]+"${new[@]}"}")
    return $rc
}

abort() {
    echo "FATAL: $1 — stopping all downloads"
    if [[ ${#pids[@]} -gt 0 ]]; then
        kill "${pids[@]}" 2>/dev/null || true
        wait "${pids[@]}" 2>/dev/null || true
    fi
    exit 1
}

# ── main loop ────────────────────────────────────────────────────────────────

while IFS= read -r fname; do
    [[ -z "$fname" ]] && continue
    [[ -v DONE["$fname"] ]] && continue

    hash="${CHECKSUMS[$fname]:-}"
    [[ -n "$hash" ]] || abort "no checksum entry for $fname"

    worker "$fname" "$hash" &
    pids+=($!)

    if [[ ${#pids[@]} -ge $JOBS ]]; then
        drain_one || abort "download failed for one or more files"
    fi
done < "$WORKLIST"

while [[ ${#pids[@]} -gt 0 ]]; do
    drain_one || abort "download failed for one or more files"
done

echo "All done."
