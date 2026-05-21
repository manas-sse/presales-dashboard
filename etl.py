#!/usr/bin/env python3
"""
Pre-sales Dashboard ETL — Phase 2
Generates:
  data/city_stage.json      → cluster × status × stage aggregation
  data/call_attempts.json   → cluster × status × stage with daily/overdue/attempts breakdown
"""

import os, re, json, requests
from datetime import datetime, date, timezone, timedelta
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()

METABASE_URL = os.environ["METABASE_URL"].rstrip("/")
USERNAME     = os.environ["METABASE_USERNAME"]
PASSWORD     = os.environ["METABASE_PASSWORD"]
CARD_ID      = int(os.environ.get("METABASE_CARD_ID", "2557"))
TIMEOUT_S    = 240


def get_session_token() -> str:
    r = requests.post(
        f"{METABASE_URL}/api/session",
        json={"username": USERNAME, "password": PASSWORD},
        timeout=30,
    )
    r.raise_for_status()
    print("      Session token obtained.")
    return r.json()["id"]


def fetch_card_results(token: str) -> list:
    headers = {"X-Metabase-Session": token}
    print(f"      Exporting card {CARD_ID} (full dataset)...")
    r = requests.post(
        f"{METABASE_URL}/api/card/{CARD_ID}/query/json",
        headers=headers,
        json={"ignore_cache": False},
        timeout=TIMEOUT_S,
    )
    r.raise_for_status()
    rows = r.json()
    print(f"      {len(rows):,} rows returned.")
    return rows


# ── DATE HELPERS ──────────────────────────────────────────────────────────────
def parse_date_any(raw):
    """Parse 'MM/DD/YYYY HH:MM:SS' or 'YYYY-MM-DD' to a date object."""
    if not raw: return None
    s = str(raw).strip()
    try:
        if "/" in s:
            m, d, y = s.split(" ")[0].split("/")
            return date(int(y), int(m), int(d))
        return date.fromisoformat(s[:10])
    except Exception:
        return None


def today_ist():
    return (datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)).date()


# ── CITY × STAGE AGGREGATION (existing logic, unchanged) ──────────────────────
def aggregate(records: list) -> list:
    buckets = defaultdict(int)
    for r in records:
        city        = r.get("City")        or r.get("city")        or "Unknown"
        lead_status = r.get("Lead status") or r.get("lead_status") or "Unknown"
        lead_stage  = r.get("Lead stage")  or r.get("lead_stage")  or "Unknown"
        cluster     = r.get("Cluster")     or r.get("cluster")     or "Unknown"
        created_raw = r.get("Creation Date") or r.get("createdAt") or ""

        cohort_week  = "Unknown"
        cohort_month = "Unknown"
        d = parse_date_any(created_raw)
        if d:
            iso = d.isocalendar()
            week_start  = date.fromisocalendar(iso[0], iso[1], 1)
            cohort_week  = week_start.strftime("%Y-%m-%d")
            cohort_month = d.strftime("%Y-%m-01")

        buckets[(city, lead_status.strip(), lead_stage.strip(),
                 cluster, cohort_week, cohort_month)] += 1

    out = [
        {"city": k[0], "lead_status": k[1], "lead_stage": k[2],
         "cluster": k[3], "cohort_week": k[4], "cohort_month": k[5],
         "lead_count": v}
        for k, v in buckets.items()
    ]
    out.sort(key=lambda x: x["cohort_week"], reverse=True)
    print(f"      Aggregated into {len(out):,} buckets.")
    return out


def build_city_stage_output(records: list) -> dict:
    total        = sum(r["lead_count"] for r in records)
    cities       = sorted({r["city"]        for r in records if r["city"]       != "Unknown"})
    stages       = sorted({r["lead_stage"]  for r in records if r["lead_stage"] != "Unknown"})
    statuses     = sorted({r["lead_status"] for r in records})
    cohort_weeks = sorted({r["cohort_week"] for r in records if r["cohort_week"] != "Unknown"}, reverse=True)
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


# ── CALL ATTEMPTS AGGREGATION (new) ───────────────────────────────────────────
def build_call_attempts_output(raw_rows: list) -> dict:
    """
    Aggregates by (cluster, lead_status, lead_stage) producing:
      - total: count of leads in bucket
      - updated_today: leads with Updated At = today (IST)
      - scheduled_today: leads with Reshedule Date = today (IST)
      - overdue: leads with Reshedule Date < today (IST)
      - by_attempts: {attempt_count: lead_count, ...}
    """
    today = today_ist()
    print(f"      Building call_attempts (as of IST {today})...")

    buckets = defaultdict(lambda: {
        "total": 0, "updated_today": 0, "scheduled_today": 0, "overdue": 0,
        "by_attempts": defaultdict(int),
    })
    max_attempts = 0

    for r in raw_rows:
        cluster = (r.get("Cluster") or "Unknown").strip()
        status  = (r.get("Lead status") or "Unknown").strip()
        stage   = (r.get("Lead stage") or "Unknown").strip()
        key = (cluster, status, stage)
        b = buckets[key]
        b["total"] += 1

        # Call attempts
        try:
            attempts = int(r.get("call_attempts_lrm") or 0)
        except (ValueError, TypeError):
            attempts = 0
        attempts = max(0, min(attempts, 50))  # clip outliers
        b["by_attempts"][attempts] += 1
        if attempts > max_attempts:
            max_attempts = attempts

        # Updated today
        u = parse_date_any(r.get("Updated At"))
        if u == today:
            b["updated_today"] += 1

        # Reschedule today / overdue
        rs = parse_date_any(r.get("Reshedule Date"))
        if rs:
            if rs == today:
                b["scheduled_today"] += 1
            elif rs < today:
                b["overdue"] += 1

    records = []
    for (cluster, status, stage), b in buckets.items():
        records.append({
            "cluster":         cluster,
            "lead_status":     status,
            "lead_stage":      stage,
            "total":           b["total"],
            "updated_today":   b["updated_today"],
            "scheduled_today": b["scheduled_today"],
            "overdue":         b["overdue"],
            "by_attempts":     dict(b["by_attempts"]),
        })

    clusters = sorted({r["cluster"] for r in records if r["cluster"] != "Unknown"})
    statuses = sorted({r["lead_status"] for r in records if r["lead_status"] != "Unknown"})
    stages   = sorted({r["lead_stage"] for r in records if r["lead_stage"] != "Unknown"})

    print(f"      {len(records):,} buckets · max attempts seen: {max_attempts}")
    return {
        "meta": {
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "as_of_date":   today.strftime("%Y-%m-%d"),
            "max_attempts": max_attempts,
            "clusters":     clusters,
            "statuses":     statuses,
            "stages":       stages,
        },
        "records": records,
    }


def main():
    print("[1/5] Authenticating with Metabase...")
    token = get_session_token()

    print("[2/5] Fetching card results...")
    raw_rows = fetch_card_results(token)

    print("[3/5] Aggregating city × stage...")
    aggregated = aggregate(raw_rows)

    print("[4/5] Aggregating call attempts & overdue...")
    call_data = build_call_attempts_output(raw_rows)

    print("[5/5] Writing output files...")
    os.makedirs("data", exist_ok=True)

    city_stage = build_city_stage_output(aggregated)
    with open("data/city_stage.json", "w") as f:
        json.dump(city_stage, f, indent=2, default=str)
    print(f"      city_stage.json     — {city_stage['meta']['total_leads']:,} leads")

    with open("data/call_attempts.json", "w") as f:
        json.dump(call_data, f, indent=2, default=str)
    print(f"      call_attempts.json  — {len(call_data['records']):,} buckets")

    print("\n  Done.")


if __name__ == "__main__":
    main()
