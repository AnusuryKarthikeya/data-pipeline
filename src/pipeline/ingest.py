"""Incremental ingestion of GitHub public events into a raw landing zone."""
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

# Resolve paths relative to this file so the script works from any directory.
# ingest.py is at src/pipeline/ingest.py, so parents[2] is the project root.
ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = ROOT / "data" / "raw"
STATE_FILE = ROOT / "data" / "state" / "watermark.json"

API_URL = "https://api.github.com/events"
HEADERS = {
    "User-Agent": "data-pipeline-learning",
    "Accept": "application/vnd.github+json",
}
MAX_PAGES = 10  # GitHub limits how deep you can page; this is plenty for learning


def load_watermark() -> int:
    """Return the highest event id we've already ingested (0 on first run)."""
    if STATE_FILE.exists():
        return int(json.loads(STATE_FILE.read_text())["last_event_id"])
    return 0


def save_watermark(last_event_id: int) -> None:
    """Persist the new high-water mark so the next run knows where to resume."""
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps({"last_event_id": last_event_id}))


def fetch_page(page: int) -> list[dict]:
    """Fetch one page of events. Returns [] when no more pages are available."""
    resp = requests.get(API_URL, headers=HEADERS, params={"per_page": 100, "page": page})
    # GitHub's /events endpoint only exposes the ~300 most recent events.
    # Paging past that limit returns 422 — treat it as "end of data" and stop.
    if resp.status_code == 422:
        return []
    resp.raise_for_status()  # still raise on genuine errors (e.g. rate limit, 403)
    return resp.json()

def ingest() -> None:
    watermark = load_watermark()
    print(f"Starting ingestion. Last seen event id: {watermark}")

    new_events = []
    for page in range(1, MAX_PAGES + 1):
        events = fetch_page(page)
        if not events:
            break  # no more data available

        # Keep only events strictly newer than our watermark.
        fresh = [e for e in events if int(e["id"]) > watermark]
        new_events.extend(fresh)

        # If this page already contained events we've seen, we've caught up.
        if len(fresh) < len(events):
            break

        time.sleep(1)  # be polite to the API and respect rate limits

    if not new_events:
        print("No new events since last run. Nothing to write.")
        return

    # Land raw events as newline-delimited JSON (one JSON object per line),
    # one file per run, named with a UTC timestamp.
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_file = RAW_DIR / f"events_{stamp}.jsonl"
    with out_file.open("w", encoding="utf-8") as f:
        for event in new_events:
            f.write(json.dumps(event) + "\n")

    # Advance the watermark to the highest id we just ingested.
    new_watermark = max(int(e["id"]) for e in new_events)
    save_watermark(new_watermark)

    print(f"Wrote {len(new_events)} new events to {out_file.name}")
    print(f"New watermark: {new_watermark}")


if __name__ == "__main__":
    ingest()