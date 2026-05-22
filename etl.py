#!/usr/bin/env python3
"""
Pre-sales Dashboard ETL — Phase 3
Generates:
  data/city_stage.json      → cluster × status × stage (lead snapshot)
  data/call_attempts.json   → cluster × status × stage with attempt buckets
  data/daily_movement.json  → stage/status transitions per day per cluster × LRM
  data/eod_position.json    → end-of-day position per lead per day
  data/lrm_performance.json → per-LRM aggregates
  data/tat_stats.json       → TAT distributions for events
"""

import os, re, json, requests, statistics
from datetime import datetime, date, timezone, timedelta
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()

# ── VALID CLUSTER LIST ───────────────────────────────────────────────────────
VALID_CLUSTERS = [
    "Ahmedabad","Surat","Bangalore","Hyderabad","Amravati",
    "Nagpur","Aurangabad","Nashik","Pune","Kolhapur",
    "Jalgaon","Solapur","Ahilyanagar","Bhopal","Gwalior",
    "Indore","Jabalpur","Jaipur","Kanpur","Lucknow",
    "Varanasi","Agra","Bareilly","Meerut","Delhi",
    "Ghaziabad","Noida","Gurgaon","Faridabad","Chennai",
    "Coimbatore","Vijayawada",
]

# Known alternate spellings → canonical name.
# Add more here as you discover them in the data.
CLUSTER_ALIASES = {
    r"bengaluru|banglore|bangaluru|bangalore\s+city|bangalore\s+karnataka": "Bangalore",
    r"gurugram":                            "Gurgaon",
    r"ahilya\s*nagar|ahmadnagar|ahmednagar|ahilyanagar": "Ahilyanagar",
    r"chhatrapati\s*sambhaji\s*nagar|sambhajinagar|sambhaji\s*nagar|csn": "Aurangabad",
    r"gaziabad|gzb":                        "Ghaziabad",
    r"navi\s*mumbai|thane|mumbai":          "Invalid",   # not in cluster list
    r"new\s*delhi|north\s*delhi|south\s*delhi|east\s*delhi|west\s*delhi": "Delhi",
}

# Pre-compile: (pattern, canonical) list, checked in order
_ALIAS_RE = [(re.compile(p, re.IGNORECASE), c) for p, c in CLUSTER_ALIASES.items()]

# Fast lookup: lowercase canonical name → canonical name
_CLUSTER_LOWER = {c.lower(): c for c in VALID_CLUSTERS}


def normalise_cluster(raw: str) -> str:
    """
    Returns the canonical cluster name or 'Invalid'.
    Steps:
      1. Alias regex patterns (handles known misspellings / merged names)
      2. Case-insensitive exact match against VALID_CLUSTERS
      3. 'Invalid' if nothing matches
    """
    if not raw:
        return "Invalid"
    v = str(raw).strip()

    # Step 1 — alias patterns
    for pattern, canonical in _ALIAS_RE:
        if pattern.fullmatch(v):
            return canonical          # could be "Invalid" for explicit exclusions

    # Step 2 — case-insensitive exact match
    canon = _CLUSTER_LOWER.get(v.lower())
    if canon:
        return canon

    return "Invalid"

METABASE_URL    = os.environ["METABASE_URL"].rstrip("/")
USERNAME        = os.environ["METABASE_USERNAME"]
PASSWORD        = os.environ["METABASE_PASSWORD"]
CARD_ID         = int(os.environ.get("METABASE_CARD_ID", "2557"))
AUDIT_CARD_ID   = int(os.environ.get("METABASE_AUDIT_CARD_ID", "3227"))
TIMEOUT_S       = 300


# ── METABASE AUTH + FETCH ────────────────────────────────────────────────────
def get_session_token() -> str:
    r = requests.post(
        f"{METABASE_URL}/api/session",
        json={"username": USERNAME, "password": PASSWORD},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["id"]


def fetch_card(token: str, card_id: int) -> list:
    r = requests.post(
        f"{METABASE_URL}/api/card/{card_id}/query/json",
        headers={"X-Metabase-Session": token},
        json={"ignore_cache": False},
        timeout=TIMEOUT_S,
    )
    r.raise_for_status()
    return r.json()


# ── DATE HELPERS ─────────────────────────────────────────────────────────────
def parse_date_any(raw):
    """Parse 'MM/DD/YYYY HH:MM:SS' or 'YYYY-MM-DD' → date."""
    if not raw: return None
    s = str(raw).strip()
    try:
        if "/" in s:
            m, d, y = s.split(" ")[0].split("/")
            return date(int(y), int(m), int(d))
        return date.fromisoformat(s[:10])
    except Exception:
        return None


def parse_dt_iso(raw):
    """Parse audit createdAt timestamp → datetime (UTC, naive).
    Handles:
      - ISO:           2026-05-21T11:07:48Z  (new standardised format)
      - ISO w/ micros: 2025-12-21T12:06:30.74Z
      - Metabase UI:   December 21, 2025, 3:03 PM  (old format, kept as fallback)
    """
    if not raw: return None
    s = str(raw).strip()
    # ISO variants
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        pass
    # Metabase human-readable: "December 21, 2025, 3:03 PM"
    for fmt in ("%B %d, %Y, %I:%M %p", "%B %d, %Y, %I:%M:%S %p"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def today_ist():
    return (datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)).date()


# ════════════════════════════════════════════════════════════════════════════
# CARD 2557 — LEAD SNAPSHOT (city_stage + call_attempts)
# ════════════════════════════════════════════════════════════════════════════
def aggregate_city_stage(records: list) -> list:
    buckets = defaultdict(int)
    for r in records:
        city        = r.get("City") or "Unknown"
        lead_status = (r.get("Lead status") or "Unknown").strip()
        lead_stage  = (r.get("Lead stage")  or "Unknown").strip()
        cluster     = normalise_cluster(r.get("Cluster") or "")
        created_raw = r.get("Creation Date") or ""
        cohort_week, cohort_month, creation_date = "Unknown", "Unknown", "Unknown"
        d = parse_date_any(created_raw)
        if d:
            iso = d.isocalendar()
            cohort_week    = date.fromisocalendar(iso[0], iso[1], 1).strftime("%Y-%m-%d")
            cohort_month   = d.strftime("%Y-%m-01")
            creation_date  = d.strftime("%Y-%m-%d")
        buckets[(city, lead_status, lead_stage, cluster, cohort_week, cohort_month, creation_date)] += 1
    return [
        {"city": k[0], "lead_status": k[1], "lead_stage": k[2],
         "cluster": k[3], "cohort_week": k[4], "cohort_month": k[5],
         "creation_date": k[6], "lead_count": v}
        for k, v in buckets.items()
    ]


def build_city_stage_output(records: list) -> dict:
    total = sum(r["lead_count"] for r in records)
    return {
        "meta": {
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "total_leads":  total,
            "cities":       sorted({r["city"] for r in records if r["city"] != "Unknown"}),
            "stages":       sorted({r["lead_stage"] for r in records if r["lead_stage"] != "Unknown"}),
            "statuses":     sorted({r["lead_status"] for r in records}),
            "cohort_weeks": sorted({r["cohort_week"] for r in records if r["cohort_week"] != "Unknown"}, reverse=True)[:12],
        },
        "records": records,
    }


def build_call_attempts_output(raw_rows: list) -> dict:
    today = today_ist()
    buckets = defaultdict(lambda: {
        "total": 0, "updated_today": 0, "scheduled_today": 0, "overdue": 0,
        "by_attempts": defaultdict(int),
    })
    max_attempts = 0
    for r in raw_rows:
        key = (normalise_cluster(r.get("Cluster") or ""),
               (r.get("Lead status") or "Unknown").strip(),
               (r.get("Lead stage")  or "Unknown").strip())
        b = buckets[key]
        b["total"] += 1
        try:    attempts = int(r.get("call_attempts_lrm") or 0)
        except: attempts = 0
        attempts = max(0, min(attempts, 50))
        b["by_attempts"][attempts] += 1
        if attempts > max_attempts: max_attempts = attempts
        if parse_date_any(r.get("Updated At")) == today:    b["updated_today"]   += 1
        rs = parse_date_any(r.get("Reshedule Date"))
        if rs == today: b["scheduled_today"] += 1
        elif rs and rs < today: b["overdue"] += 1
    records = [{"cluster": k[0], "lead_status": k[1], "lead_stage": k[2],
                "total": b["total"], "updated_today": b["updated_today"],
                "scheduled_today": b["scheduled_today"], "overdue": b["overdue"],
                "by_attempts": dict(b["by_attempts"])} for k, b in buckets.items()]
    return {
        "meta": {
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "as_of_date":   today.strftime("%Y-%m-%d"),
            "max_attempts": max_attempts,
            "clusters":     sorted({r["cluster"] for r in records if r["cluster"] != "Unknown"}),
            "statuses":     sorted({r["lead_status"] for r in records}),
            "stages":       sorted({r["lead_stage"] for r in records}),
        },
        "records": records,
    }


# ════════════════════════════════════════════════════════════════════════════
# CARD 3227 — AUDIT LOG AGGREGATIONS
# ════════════════════════════════════════════════════════════════════════════

def normalise_audit_rows(audit_raw: list) -> list:
    """Sort audit by lead_id, createdAt asc — required for transition + attempt detection."""
    cleaned = []
    for r in audit_raw:
        lid = r.get("lead_id")
        if not lid: continue
        ts = parse_dt_iso(r.get("createdAt"))
        if not ts: continue
        try:    n_attempts = int(r.get("call_attempts_lrm") or 0)
        except: n_attempts = 0
        # meeting_schedule_first_time: Metabase UI format = IST naive datetime.
        # Subtract 5:30 to convert to UTC naive so it's comparable with ts (createdAt UTC).
        raw_mtg = r.get("meeting_schedule_first_time")
        meeting_ts = parse_dt_iso(raw_mtg)
        if meeting_ts:
            meeting_ts = meeting_ts - timedelta(hours=5, minutes=30)

        cleaned.append({
            "lead_id":    lid,
            "ts":         ts,
            "stage":      (r.get("stage")   or "Unknown").strip(),
            "status":     (r.get("status")  or "Unknown").strip(),
            "activity":   r.get("activity") or "",
            "output":     r.get("output") or "",
            "lrm":        r.get("LRM Email") or "",
            "cluster":    normalise_cluster(r.get("site_address_cluster") or ""),
            "updated_by": r.get("status_stage_updated_by") or "",
            "n_attempts": n_attempts,
            "meeting_ts": meeting_ts,   # UTC naive datetime or None
        })
    cleaned.sort(key=lambda x: (x["lead_id"], x["ts"]))
    return cleaned


def build_daily_movement(audit_sorted: list, lead_creation: dict) -> dict:
    """
    Per (date, cluster, lrm, from_stage, to_stage):
      - count of transitions
    Also tracks status-level transitions (from_status -> to_status).
    Date is IST (audit createdAt + 5:30 hrs).
    """
    transitions_stage  = defaultdict(int)
    transitions_status = defaultdict(int)

    by_lead = defaultdict(list)
    for r in audit_sorted:
        by_lead[r["lead_id"]].append(r)

    for lid, events in by_lead.items():
        prev = None
        for ev in events:
            ev_date = (ev["ts"] + timedelta(hours=5, minutes=30)).date().strftime("%Y-%m-%d")
            cluster = ev["cluster"]
            lrm     = ev["lrm"] or "Unknown"
            if prev is not None:
                if prev["stage"] != ev["stage"]:
                    transitions_stage[(ev_date, cluster, lrm,
                                       prev["stage"], ev["stage"])] += 1
                if prev["status"] != ev["status"]:
                    transitions_status[(ev_date, cluster, lrm,
                                        prev["status"], ev["status"])] += 1
            prev = ev

    stage_records = [
        {"date": k[0], "cluster": k[1], "lrm": k[2],
         "from_stage": k[3], "to_stage": k[4], "count": v}
        for k, v in transitions_stage.items()
    ]
    status_records = [
        {"date": k[0], "cluster": k[1], "lrm": k[2],
         "from_status": k[3], "to_status": k[4], "count": v}
        for k, v in transitions_status.items()
    ]
    stage_records.sort(key=lambda x: (x["date"], x["cluster"]), reverse=True)
    status_records.sort(key=lambda x: (x["date"], x["cluster"]), reverse=True)

    return {
        "meta": {
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "total_stage_transitions":  sum(r["count"] for r in stage_records),
            "total_status_transitions": sum(r["count"] for r in status_records),
        },
        "stage":  stage_records,
        "status": status_records,
    }


def build_eod_position(audit_sorted: list) -> dict:
    """
    For each (date, lead_id) find the LAST event of that day → that's the
    end-of-day position. Then aggregate to (date, cluster, lrm, stage, status).
    """
    by_lead_day = {}   # (lead_id, date) → last event
    for r in audit_sorted:
        d = (r["ts"] + timedelta(hours=5, minutes=30)).date().strftime("%Y-%m-%d")
        key = (r["lead_id"], d)
        if key not in by_lead_day or r["ts"] > by_lead_day[key]["ts"]:
            by_lead_day[key] = r

    buckets = defaultdict(int)
    for (lid, d), ev in by_lead_day.items():
        buckets[(d, ev["cluster"], ev["lrm"] or "Unknown",
                 ev["stage"], ev["status"])] += 1

    records = [
        {"date": k[0], "cluster": k[1], "lrm": k[2],
         "stage": k[3], "status": k[4], "count": v}
        for k, v in buckets.items()
    ]
    records.sort(key=lambda x: x["date"], reverse=True)

    return {
        "meta": {
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "note": "End-of-day position: last audit event per lead per day",
            "total_records": len(records),
        },
        "records": records,
    }


def build_lrm_performance(audit_sorted: list) -> dict:
    """
    Per (date, cluster, lrm):
      - calls_attempted   (rows where call_attempts_lrm incremented)
      - leads_touched     (distinct leads with any event from this LRM)
      - stage_movements   (rows where stage changed compared to prev event)
      - status_movements  (rows where status changed compared to prev event)
    """
    by_lead = defaultdict(list)
    for r in audit_sorted:
        by_lead[r["lead_id"]].append(r)

    bucket = defaultdict(lambda: {"calls": 0, "leads": set(),
                                  "stage_moves": 0, "status_moves": 0})

    for lid, events in by_lead.items():
        prev = None
        for ev in events:
            d = (ev["ts"] + timedelta(hours=5, minutes=30)).date().strftime("%Y-%m-%d")
            lrm = ev["lrm"] or "Unknown"
            cluster = ev["cluster"]
            k = (d, cluster, lrm)
            # leads_touched = unique leads updated BY the LRM (not system/SC updates)
            if ev["updated_by"] == "LRM":
                bucket[k]["leads"].add(lid)
            if prev is not None and ev["n_attempts"] > prev["n_attempts"]:
                bucket[k]["calls"] += 1
            if prev is not None:
                if prev["stage"]  != ev["stage"]:  bucket[k]["stage_moves"]  += 1
                if prev["status"] != ev["status"]: bucket[k]["status_moves"] += 1
            prev = ev

    records = []
    for (d, cluster, lrm), v in bucket.items():
        records.append({
            "date":     d,
            "cluster":  cluster,
            "lrm":      lrm,
            "calls":            v["calls"],
            "leads_touched":    len(v["leads"]),
            "stage_movements":  v["stage_moves"],
            "status_movements": v["status_moves"],
        })
    records.sort(key=lambda x: (x["date"], -x["calls"]), reverse=True)

    lrm_set = sorted({r["lrm"] for r in records if r["lrm"] != "Unknown"})
    return {
        "meta": {
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "lrm_count":    len(lrm_set),
            "lrms":         lrm_set,
            "date_range":   {
                "min": min((r["date"] for r in records), default=None),
                "max": max((r["date"] for r in records), default=None),
            },
        },
        "records": records,
    }


def build_tat_stats(audit_sorted: list, lead_creation: dict) -> dict:
    """
    Computes TAT in fractional HOURS for each measure per lead:
      - tat_first_call : lead creation datetime → createdAt of first call audit event
      - tat_gaps       : list of hour-gaps between each consecutive pair of call events
      - tat_to_meeting : lead creation datetime → meeting_schedule_first_time (from card 3227)
      - tat_to_won     : lead creation datetime → createdAt of first "Order Confirmed" event

    lead_creation values: UTC naive datetimes (card 2557 Creation Date, ISO UTC).
    ev["ts"]            : UTC naive datetimes (card 3227 createdAt, ISO UTC).
    ev["meeting_ts"]    : UTC naive datetimes (meeting_schedule_first_time, IST → UTC adjusted).

    Call detection: n_attempts increments between consecutive events for the same lead.
    prev_n starts at 0 — pre-audit calls (n_attempts already > 0 on first event) are excluded.

    UI displays: < 1h → minutes, 1–24h → hours, > 24h → days.
    """
    by_lead = defaultdict(list)
    for r in audit_sorted:
        by_lead[r["lead_id"]].append(r)

    records = []
    for lid, events in by_lead.items():
        if not events: continue

        # Lead creation datetime (UTC naive)
        cdate_dt = lead_creation.get(lid)
        # IST date string for filtering on the dashboard (display only)
        cdate_ist_str = (
            (cdate_dt + timedelta(hours=5, minutes=30)).date().strftime("%Y-%m-%d")
            if cdate_dt else None
        )

        # Cluster: most recent non-Invalid value, fallback to first event's cluster
        cluster = next(
            (ev["cluster"] for ev in reversed(events) if ev["cluster"] != "Invalid"),
            events[0]["cluster"]
        )

        # LRM: most common LRM email across all audit events for this lead
        lrm_counter = defaultdict(int)
        for ev in events:
            if ev["lrm"]: lrm_counter[ev["lrm"]] += 1
        lrm = max(lrm_counter, key=lrm_counter.get) if lrm_counter else "Unknown"

        # ── TAT 1 & 2: call attempt events ───────────────────────────────────
        # A call event = audit row where call_attempts_lrm increments.
        # prev_n = 0: first event with n_attempts already > 0 is NOT a new call.
        call_times = []   # UTC naive datetimes of each call event
        prev_n = 0
        for ev in events:
            if ev["n_attempts"] > prev_n and ev["n_attempts"] > 0:
                call_times.append(ev["ts"])
                prev_n = ev["n_attempts"]

        # TAT 1: creation datetime → createdAt of first call event (hours)
        tat_first_call = None
        if cdate_dt and call_times:
            tat_first_call = round(
                (call_times[0] - cdate_dt).total_seconds() / 3600, 2
            )

        # TAT 2: gap between each consecutive pair of call events (hours)
        # Gap = createdAt of call[i] − createdAt of call[i-1]
        tat_gaps = []
        for i in range(1, len(call_times)):
            gap_h = (call_times[i] - call_times[i - 1]).total_seconds() / 3600
            if 0 <= gap_h <= 365 * 24:   # exclude data errors > 1 year
                tat_gaps.append(round(gap_h, 2))

        # ── TAT 3: creation → first meeting scheduled (hours) ────────────────
        # Uses meeting_schedule_first_time column (already UTC-adjusted in normalise_audit_rows).
        # All rows for the same lead carry the same value; take the first non-None.
        tat_to_meeting = None
        if cdate_dt:
            meeting_ts = next(
                (ev["meeting_ts"] for ev in events if ev.get("meeting_ts") is not None),
                None
            )
            if meeting_ts:
                tat_to_meeting = round(
                    (meeting_ts - cdate_dt).total_seconds() / 3600, 2
                )

        # ── TAT 4: creation → Order Confirmed (hours) ────────────────────────
        # First audit event where stage == "Order Confirmed".
        tat_to_won = None
        if cdate_dt:
            for ev in events:
                if ev["stage"] == "Order Confirmed":
                    tat_to_won = round(
                        (ev["ts"] - cdate_dt).total_seconds() / 3600, 2
                    )
                    break

        # first_call_date in IST (display only)
        first_call_ist = (
            (call_times[0] + timedelta(hours=5, minutes=30)).date().strftime("%Y-%m-%d")
            if call_times else None
        )

        records.append({
            "lead_id":         lid,
            "cluster":         cluster,
            "lrm":             lrm,
            "creation_date":   cdate_ist_str,
            "first_call_date": first_call_ist,
            "total_calls":     len(call_times),
            "tat_first_call":  tat_first_call,
            "tat_gaps":        tat_gaps,
            "tat_to_meeting":  tat_to_meeting,
            "tat_to_won":      tat_to_won,
        })

    return {
        "meta": {
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "total_leads":  len(records),
            "note": (
                "TAT in fractional hours. "
                "UI displays: < 1h as minutes, < 24h as hours, >= 24h as days."
            ),
        },
        "records": records,
    }

def build_lrm_conversion(audit_sorted: list) -> dict:
    """
    Per (lead_id, lrm): dates when this LRM made a call + first meeting date for the lead.
    Dashboard uses this to compute: of leads called by LRM X in period [A,B],
    how many also had a meeting scheduled in [A,B]?
    Meeting stages: Meeting Scheduled (BD), Meeting Confirmed - Customer Home
    """
    MEETING_STAGES = {"Meeting Scheduled (BD)", "Meeting Confirmed - Customer Home"}

    by_lead = defaultdict(list)
    for r in audit_sorted:
        by_lead[r["lead_id"]].append(r)

    records = []
    for lid, events in by_lead.items():
        # Find first meeting date for this lead (IST)
        meeting_date = None
        for ev in events:
            if ev["stage"] in MEETING_STAGES:
                meeting_date = (ev["ts"] + timedelta(hours=5, minutes=30)).date().strftime("%Y-%m-%d")
                break

        # Collect call events grouped by LRM (call = n_attempts incremented)
        by_lrm: dict = defaultdict(lambda: {"cluster": "Invalid", "call_dates": set()})
        prev = None
        for ev in events:
            if prev is not None and ev["n_attempts"] > prev["n_attempts"]:
                lrm = ev["lrm"] or "Unknown"
                d_ist = (ev["ts"] + timedelta(hours=5, minutes=30)).date().strftime("%Y-%m-%d")
                by_lrm[lrm]["call_dates"].add(d_ist)
                by_lrm[lrm]["cluster"] = ev["cluster"]
            prev = ev

        for lrm, v in by_lrm.items():
            if not v["call_dates"]:
                continue
            records.append({
                "lead_id":      lid,
                "lrm":          lrm,
                "cluster":      v["cluster"],
                "call_dates":   sorted(v["call_dates"]),
                "meeting_date": meeting_date,
            })

    return {
        "meta": {
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "total_records": len(records),
            "note": "One record per (lead, LRM). call_dates = IST dates of call attempts. meeting_date = first meeting-stage date for the lead.",
        },
        "records": records,
    }


# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════
def main():
    os.makedirs("data", exist_ok=True)

    print("[1/7] Authenticating with Metabase...")
    token = get_session_token()

    print("[2/7] Fetching lead data (card 2557)...")
    leads_raw = fetch_card(token, CARD_ID)
    print(f"      {len(leads_raw):,} rows")

    print("[3/7] Fetching audit log (card 3227)...")
    audit_raw = fetch_card(token, AUDIT_CARD_ID)
    print(f"      {len(audit_raw):,} rows")

    # Build lead_id → creation datetime map (UTC naive, from card 2557 Creation Date).
    # Creation Date is now ISO UTC (2026-05-22T08:10:05Z), so parse as full datetime.
    # Stored as datetime so build_tat_stats can compute exact hour-level TAT.
    lead_creation = {}
    for r in leads_raw:
        lid = r.get("Lead Id")
        if not lid: continue
        cd_dt = parse_dt_iso(r.get("Creation Date"))   # UTC naive datetime
        if cd_dt: lead_creation[lid] = cd_dt

    print("[4/7] Aggregating lead snapshot...")
    city_stage = build_city_stage_output(aggregate_city_stage(leads_raw))
    with open("data/city_stage.json", "w") as f:
        json.dump(city_stage, f, indent=2, default=str)
    print(f"      city_stage.json — {city_stage['meta']['total_leads']:,} leads")

    call_attempts = build_call_attempts_output(leads_raw)
    with open("data/call_attempts.json", "w") as f:
        json.dump(call_attempts, f, indent=2, default=str)
    print(f"      call_attempts.json — {len(call_attempts['records']):,} buckets")

    print("[5/7] Normalising audit log...")
    audit_sorted = normalise_audit_rows(audit_raw)
    print(f"      {len(audit_sorted):,} clean audit events")

    print("[6/7] Building daily movement + EOD position...")
    dm = build_daily_movement(audit_sorted, lead_creation)
    with open("data/daily_movement.json", "w") as f:
        json.dump(dm, f, indent=2, default=str)
    print(f"      daily_movement.json — {dm['meta']['total_stage_transitions']:,} stage, "
          f"{dm['meta']['total_status_transitions']:,} status transitions")

    eod = build_eod_position(audit_sorted)
    with open("data/eod_position.json", "w") as f:
        json.dump(eod, f, indent=2, default=str)
    print(f"      eod_position.json — {eod['meta']['total_records']:,} rows")

    print("[7/7] Building LRM performance + TAT...")
    lrm = build_lrm_performance(audit_sorted)
    with open("data/lrm_performance.json", "w") as f:
        json.dump(lrm, f, indent=2, default=str)
    print(f"      lrm_performance.json — {lrm['meta']['lrm_count']} LRMs · {len(lrm['records']):,} rows")

    tat = build_tat_stats(audit_sorted, lead_creation)
    with open("data/tat_stats.json", "w") as f:
        json.dump(tat, f, indent=2, default=str)
    print(f"      tat_stats.json — {tat['meta']['total_leads']:,} lead-level TAT records")

    conv = build_lrm_conversion(audit_sorted)
    with open("data/lrm_conversion.json", "w") as f:
        json.dump(conv, f, indent=2, default=str)
    print(f"      lrm_conversion.json — {conv['meta']['total_records']:,} lead-LRM records")

    print("\n  All done.")


if __name__ == "__main__":
    main()
