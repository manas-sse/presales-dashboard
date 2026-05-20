#!/usr/bin/env python3
"""
Pre-sales Dashboard ETL — Phase 1
Authenticates via Metabase username/password, fetches card results,
aggregates by city/stage/cohort, writes data/city_stage.json.
"""

import os
import json
import requests
from datetime import datetime, date, timezone
from collections import defaultdict
import re
from dotenv import load_dotenv

load_dotenv()

# ── CONFIG ────────────────────────────────────────────────────────────────────
METABASE_URL = os.environ["METABASE_URL"].rstrip("/")
USERNAME     = os.environ["METABASE_USERNAME"]
PASSWORD     = os.environ["METABASE_PASSWORD"]
CARD_ID      = int(os.environ.get("METABASE_CARD_ID", "2557"))
OUTPUT_FILE  = "data/city_stage.json"
TIMEOUT_S    = 180
# ─────────────────────────────────────────────────────────────────────────────


def get_session_token() -> str:
    """Exchange username/password for a Metabase session token."""
    r = requests.post(
        f"{METABASE_URL}/api/session",
        json={"username": USERNAME, "password": PASSWORD},
        timeout=30,
    )
    r.raise_for_status()
    token = r.json()["id"]
    print(f"      Session token obtained.")
    return token


def fetch_card_results(token: str) -> list:
    """Fetch full results via export endpoint — bypasses 2000 row cap."""
    headers = {"X-Metabase-Session": token}
    print(f"      Exporting card {CARD_ID} (full dataset, no row cap)...")
    r = requests.post(
        f"{METABASE_URL}/api/card/{CARD_ID}/query/json",
        headers=headers,
        json={"ignore_cache": False},
        timeout=TIMEOUT_S,
    )
    r.raise_for_status()
    rows = r.json()   # returns a list of dicts directly — no cols/rows wrapper
    print(f"      {len(rows):,} rows returned.")
    return rows


def parse_date(raw) -> date | None:
    """Parse MM/DD/YYYY or YYYY-MM-DD or ISO datetime strings."""
    if not raw:
        return None
    s = str(raw).strip()
    try:
        if re.match(r"\d{2}/\d{2}/\d{4}", s):
            m, d, y = s[:10].split("/")
            return date(int(y), int(m), int(d))
        return date.fromisoformat(s[:10])
    except Exception:
        return None


def aggregate(records: list) -> list:
    """Group rows by city/status/stage/cluster/cohort and count leads."""
    buckets = defaultdict(int)

    for r in records:
        city        = r.get("City")        or r.get("city")        or "Unknown"
        lead_status = r.get("Lead status") or r.get("lead_status") or "Unknown"
        lead_stage  = r.get("Lead stage")  or r.get("lead_stage")  or "Unknown"
        cluster     = r.get("Cluster")     or r.get("cluster")     or "Unknown"
        created_raw = r.get("Creation Date") or r.get("createdAt") or ""

        cohort_week  = "Unknown"
        cohort_month = "Unknown"

        d = parse_date(created_raw)
        if d:
            iso = d.isocalendar()
            week_start   = date.fromisocalendar(iso[0], iso[1], 1)
            cohort_week  = week_start.strftime("%Y-%m-%d")
            cohort_month = d.strftime("%Y-%m-01")

        buckets[(city, lead_status, lead_stage, cluster, cohort_week, cohort_month)] += 1

    aggregated = [
        {
            "city":         k[0],
            "lead_status":  k[1],
            "lead_stage":   k[2],
            "cluster":      k[3],
            "cohort_week":  k[4],
            "cohort_month": k[5],
            "lead_count":   v,
        }
        for k, v in buckets.items()
    ]

    aggregated.sort(key=lambda x: (x["cohort_week"], x["city"], x["lead_stage"]),
                    reverse=False)
    aggregated.sort(key=lambda x: x["cohort_week"], reverse=True)

    print(f"      Aggregated into {len(aggregated):,} buckets.")
    return aggregated


def build_output(records: list) -> dict:
    total        = sum(r["lead_count"] for r in records)
    cities       = sorted({r["city"]       for r in records if r["city"]       != "Unknown"})
    stages       = sorted({r["lead_stage"] for r in records if r["lead_stage"] != "Unknown"})
    statuses     = sorted({r["lead_status"] for r in records})
    cohort_weeks = sorted({r["cohort_week"] for r in records
                           if r["cohort_week"] != "Unknown"}, reverse=True)
    return {
        "meta": {
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "total_leads":  total,
            "cities":       cities,
            "stages":       stages,
            "statuses":     statuses,
            "cohort_weeks": cohort_weeks[:12],
        },
        "records": records,
    }


def main():
    print("[1/4] Authenticating with Metabase...")
    token = get_session_token()

    print("[2/4] Fetching card results...")
    raw_rows = fetch_card_results(token)

    print("[3/4] Aggregating...")
    aggregated = aggregate(raw_rows)

    print("[4/4] Writing output...")
    os.makedirs("data", exist_ok=True)
    output = build_output(aggregated)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2, default=str)

    print(f"\n  Done. {OUTPUT_FILE} written.")
    print(f"  Total leads : {output['meta']['total_leads']:,}")
    print(f"  Cities found: {', '.join(output['meta']['cities'])}")
    print(f"  Stages found: {', '.join(output['meta']['stages'])}")


if __name__ == "__main__":
    main()
