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
_IST = timedelta(hours=5, minutes=30)

def parse_dt(raw) -> datetime:
    """
    Parse any raw timestamp from card 2557 or 3227 → UTC naive datetime.

    ISO UTC (Z suffix):           strip timezone offset → UTC naive
    Metabase UI (IST display):    parse as-is then subtract 5:30 → UTC naive
    Legacy MM/DD/YYYY [HH:MM:SS]: treat as IST → subtract 5:30 → UTC naive

    Metabase renders human-readable timestamps in the instance timezone (IST).
    ISO timestamps already carry Z = UTC. We normalise everything to UTC naive
    so all arithmetic in the pipeline is timezone-consistent.
    datetime-to-date conversion is only done at display/output level.
    """
    if not raw: return None
    s = str(raw).strip()
    # ISO variants (2026-05-22T08:10:05Z or with microseconds / +offset)
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        pass
    # Metabase UI (IST): "May 22, 2026, 8:08 AM" or with seconds
    for fmt in ("%B %d, %Y, %I:%M %p", "%B %d, %Y, %I:%M:%S %p"):
        try:
            return datetime.strptime(s, fmt) - _IST   # IST → UTC
        except ValueError:
            continue
    # Legacy MM/DD/YYYY HH:MM:SS or MM/DD/YYYY (treat as IST midnight)
    if "/" in s:
        try:
            parts = s.split()
            mo, dy, yr = parts[0].split("/")
            if len(parts) > 1 and ":" in parts[1]:
                hr, mi, sc = parts[1].split(":")
                ist = datetime(int(yr), int(mo), int(dy), int(hr), int(mi), int(sc.split(".")[0]))
            else:
                ist = datetime(int(yr), int(mo), int(dy))
            return ist - _IST   # IST → UTC
        except Exception:
            pass
    return None


def ist_date_str(dt: datetime) -> str:
    """UTC naive datetime → IST calendar date string 'YYYY-MM-DD'. Display use only."""
    return (dt + _IST).date().strftime("%Y-%m-%d")


def today_ist():
    return (datetime.now(timezone.utc) + _IST).date()


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
        dt = parse_dt(created_raw)
        if dt:
            ist = (dt + _IST).date()   # UTC → IST date (display only)
            iso = ist.isocalendar()
            cohort_week    = date.fromisocalendar(iso[0], iso[1], 1).strftime("%Y-%m-%d")
            cohort_month   = ist.strftime("%Y-%m-01")
            creation_date  = ist.strftime("%Y-%m-%d")
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
        _upd = parse_dt(r.get("Updated At"))
        if _upd and (_upd + _IST).date() == today: b["updated_today"] += 1
        _rs = parse_dt(r.get("Reshedule Date"))
        rs_date = (_rs + _IST).date() if _rs else None
        if rs_date == today: b["scheduled_today"] += 1
        elif rs_date and rs_date < today: b["overdue"] += 1
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
        ts = parse_dt(r.get("createdAt"))   # UTC naive datetime
        if not ts: continue
        try:    n_attempts = int(r.get("call_attempts_lrm") or 0)
        except: n_attempts = 0
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
        })
    cleaned.sort(key=lambda x: (x["lead_id"], x["ts"]))
    return cleaned


def build_daily_movement(audit_sorted: list, lead_creation: dict) -> dict:
    """
    Per (date, cluster, lrm, from_stage, to_stage):
      - count of transitions
    Also tracks status-level transitions (from_status -> to_status).
    Also tracks "touches" — audit events where stage AND status did NOT change
    (call attempted with no transition, or updated_at changed with no transition).
    Date is IST (audit createdAt + 5:30 hrs).
    """
    transitions_stage  = defaultdict(int)
    transitions_status = defaultdict(int)
    # touches: (date, cluster, lrm) → {calls, updates}
    touches = defaultdict(lambda: {"calls": 0, "updates": 0})

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
                stage_changed  = prev["stage"]  != ev["stage"]
                status_changed = prev["status"] != ev["status"]
                call_made      = ev["n_attempts"] > prev["n_attempts"]
                if stage_changed:
                    transitions_stage[(ev_date, cluster, lrm,
                                       prev["stage"], ev["stage"])] += 1
                if status_changed:
                    transitions_status[(ev_date, cluster, lrm,
                                        prev["status"], ev["status"])] += 1
                if not stage_changed and not status_changed:
                    # A touch with no transition — call attempt or silent update
                    key = (ev_date, cluster, lrm)
                    if call_made:
                        touches[key]["calls"]   += 1
                    else:
                        touches[key]["updates"] += 1
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
    touch_records = [
        {"date": k[0], "cluster": k[1], "lrm": k[2],
         "calls_no_transition": v["calls"],
         "updates_no_transition": v["updates"]}
        for k, v in touches.items()
    ]
    stage_records.sort(key=lambda x: (x["date"], x["cluster"]), reverse=True)
    status_records.sort(key=lambda x: (x["date"], x["cluster"]), reverse=True)
    touch_records.sort(key=lambda x: (x["date"], x["cluster"]), reverse=True)

    return {
        "meta": {
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "total_stage_transitions":  sum(r["count"] for r in stage_records),
            "total_status_transitions": sum(r["count"] for r in status_records),
            "total_touches":            sum(r["calls_no_transition"] + r["updates_no_transition"]
                                           for r in touch_records),
            "note": (
                "stage/status: stage/status-level transitions. "
                "touches: events with no stage/status change — calls_no_transition = "
                "call attempted (n_attempts incremented); "
                "updates_no_transition = any other audit event with no state change."
            ),
        },
        "stage":   stage_records,
        "status":  status_records,
        "touches": touch_records,
    }


def build_eod_position(audit_sorted: list) -> dict:
    """
    For each (date, lead_id) find:
      - from_stage/from_status: position at START of that day
        (= previous day's last event, or first event of the day if no prior data)
      - to_stage/to_status: position at END of that day (last event)
    Then aggregate to (date, cluster, lrm, from_stage, to_stage, from_status, to_status).
    """
    by_lead: dict = defaultdict(list)
    for r in audit_sorted:
        by_lead[r["lead_id"]].append(r)

    buckets = defaultdict(int)

    for lid, events in by_lead.items():
        # Group events by IST date
        by_day: dict = defaultdict(list)
        for ev in events:
            d = (ev["ts"] + timedelta(hours=5, minutes=30)).date().strftime("%Y-%m-%d")
            by_day[d].append(ev)

        sorted_days = sorted(by_day.keys())
        prev_last_ev = None

        for d in sorted_days:
            day_evs = sorted(by_day[d], key=lambda x: x["ts"])
            last_ev  = day_evs[-1]

            # from = previous day's end-of-day state; if first day seen, use first
            # event of today (no movement = from == to for that lead)
            if prev_last_ev is not None:
                from_stage  = prev_last_ev["stage"]
                from_status = prev_last_ev["status"]
            else:
                from_stage  = day_evs[0]["stage"]
                from_status = day_evs[0]["status"]

            cluster = last_ev["cluster"]
            lrm     = last_ev["lrm"] or "Unknown"

            buckets[(d, cluster, lrm,
                     from_stage,  last_ev["stage"],
                     from_status, last_ev["status"])] += 1

            prev_last_ev = last_ev

    records = [
        {"date": k[0], "cluster": k[1], "lrm": k[2],
         "from_stage":  k[3], "to_stage":  k[4],
         "from_status": k[5], "to_status": k[6], "count": v}
        for k, v in buckets.items()
    ]
    records.sort(key=lambda x: x["date"], reverse=True)

    return {
        "meta": {
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "note": (
                "End-of-day position per lead per day. "
                "from_* = start-of-day state (previous day's last event). "
                "to_* = end-of-day state (last event of that day). "
                "When from == to, the lead did not change state that day."
            ),
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


def build_tat_stats(audit_sorted: list, lead_creation: dict, lead_ms_dates: dict = None) -> dict:
    """
    Computes TAT in fractional HOURS for each measure per lead.
    lead_creation values are UTC naive datetimes; ev["ts"] are UTC naive datetimes.
    All arithmetic is datetime − datetime → timedelta → total_seconds() / 3600.
    IST date strings (creation_date, first_call_date) are derived at output time only.

    Metrics:
      tat_first_call : creation → createdAt of first call event (hours)
      tat_gaps       : list of hours between each consecutive pair of call events
      tat_to_meeting : creation → Meeting Schedule Date from card 2557 (hours)
      tat_to_booked  : creation → createdAt of first booked-stage event (hours)
      tat_to_won     : creation → createdAt of first Order Confirmed event (hours)

    Call detection: n_attempts increments between consecutive events.
    prev_n starts at the first event's n_attempts (pre-audit calls are excluded).

    lead_ms_dates: dict of lead_id → Meeting Schedule Date UTC naive datetime
    (from card 2557 — more reliable than audit stage detection for this metric).
    """
    by_lead = defaultdict(list)
    for r in audit_sorted:
        by_lead[r["lead_id"]].append(r)

    STAGE_TARGETS = {
        "first_booked":  {"Booking Processing", "Booking Pending by Cx", "Booking Pending by ZSM"},
        "first_won":     {"Order Confirmed"},
    }

    records = []
    for lid, events in by_lead.items():
        if not events: continue

        # Lead creation UTC datetime
        cdate_dt = lead_creation.get(lid)

        # Cluster: most recent non-Invalid value, fallback to first event
        cluster = next(
            (ev["cluster"] for ev in reversed(events) if ev["cluster"] != "Invalid"),
            events[0]["cluster"]
        )

        # LRM: most common email across all events for this lead
        lrm_counter = defaultdict(int)
        for ev in events:
            if ev["lrm"]: lrm_counter[ev["lrm"]] += 1
        lrm = max(lrm_counter, key=lrm_counter.get) if lrm_counter else "Unknown"

        # Call events: n_attempts increments (prev_n = first event's count)
        call_times = []   # UTC naive datetimes
        prev_n = None
        for ev in events:
            n = ev["n_attempts"]
            if prev_n is not None and n > prev_n:
                call_times.append(ev["ts"])
            prev_n = n

        # TAT 1: creation → first call (hours)
        tat_first_call = None
        if cdate_dt and call_times:
            tat_first_call = round((call_times[0] - cdate_dt).total_seconds() / 3600, 2)

        # TAT 2: gap between each consecutive pair of call events (hours)
        tat_gaps = []
        for i in range(1, len(call_times)):
            gap_h = (call_times[i] - call_times[i - 1]).total_seconds() / 3600
            if 0 <= gap_h <= 365 * 24:
                tat_gaps.append(round(gap_h, 2))

        # TAT 3: creation → Meeting Schedule Date (from card 2557)
        tat_to_meeting = None
        if cdate_dt and lead_ms_dates:
            ms_dt_from_lead = lead_ms_dates.get(lid)
            if ms_dt_from_lead:
                tat_to_meeting = round(
                    (ms_dt_from_lead - cdate_dt).total_seconds() / 3600, 2
                )

        # TAT 4–5: creation → first event at each target stage (hours) — from audit
        tat_to_targets = {}
        if cdate_dt:
            for label, target_stages in STAGE_TARGETS.items():
                for ev in events:
                    if ev["stage"] in target_stages:
                        tat_to_targets[label] = round(
                            (ev["ts"] - cdate_dt).total_seconds() / 3600, 2
                        )
                        break

        # Display-only date strings in IST
        creation_date_str   = ist_date_str(cdate_dt) if cdate_dt else None
        first_call_date_str = ist_date_str(call_times[0]) if call_times else None

        records.append({
            "lead_id":         lid,
            "cluster":         cluster,
            "lrm":             lrm,
            "creation_date":   creation_date_str,
            "first_call_date": first_call_date_str,
            "total_calls":     len(call_times),
            "tat_first_call":  tat_first_call,
            "tat_gaps":        tat_gaps,
            "tat_to_meeting":  tat_to_meeting,
            "tat_to_booked":   tat_to_targets.get("first_booked"),
            "tat_to_won":      tat_to_targets.get("first_won"),
        })

    return {
        "meta": {
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "total_leads":  len(records),
            "note": (
                "TAT in fractional hours. "
                "UI displays: <1h as minutes, 1–24h as hours, >=24h as days. "
                "tat_to_meeting uses Meeting Schedule Date from card 2557 (not audit stage events)."
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
# CARD 2557 × 3227 — LRM SNAPSHOT (Leads × LRM sub-tab)
# ════════════════════════════════════════════════════════════════════════════
def build_lrm_snapshot(leads_raw: list, audit_sorted: list, lead_creation: dict) -> dict:
    """
    Per-lead snapshot combining card 2557 (LRM attribution, counts, MS/MD dates)
    and card 3227 (call TATs via n_attempts increments).

    Attribution: LRM Email from card 2557 (not audit).
    Date filter key: creation_date IST — applied client-side.

    TAT values in fractional hours. Dashboard computes avg / P50 / P90.

    Active definition: Lead status NOT IN inactive_statuses AND
                       Lead stage NOT IN inactive_stages.

    Pre-aggregated by (lrm, cluster, creation_date) to keep file size manageable
    (~5-8k rows vs 54k per-lead rows). TAT arrays are concatenated client-side
    when grouping across date buckets.
    """
    INACTIVE_STATUSES = {"Closed - Lost", "Closed - Cold", "Closed - Won", "Lost"}
    INACTIVE_STAGES   = {"Lost in Qualification"}

    # ── Build call TAT lookup from audit (keyed by lead_id) ──────────────────
    # Same prev_n=0 logic as build_tat_stats.
    by_lead_audit = defaultdict(list)
    for ev in audit_sorted:
        by_lead_audit[ev["lead_id"]].append(ev)

    call_tat = {}   # lead_id → {tat_first_call: float|None, tat_gaps: [float]}
    for lid, events in by_lead_audit.items():
        cdate_dt = lead_creation.get(lid)   # UTC naive datetime
        call_times = []
        prev_n = None
        for ev in events:
            n = ev["n_attempts"]
            if prev_n is not None and n > prev_n:
                call_times.append(ev["ts"])
            prev_n = n
        tat_fc = None
        if cdate_dt and call_times:
            tat_fc = round((call_times[0] - cdate_dt).total_seconds() / 3600, 2)
        gaps = []
        for i in range(1, len(call_times)):
            gh = (call_times[i] - call_times[i - 1]).total_seconds() / 3600
            if 0 <= gh <= 365 * 24:
                gaps.append(round(gh, 2))
        call_tat[lid] = {"tat_first_call": tat_fc, "tat_gaps": gaps}

    # ── Aggregate by (lrm, cluster, creation_date_ist) ───────────────────────
    BucketT = lambda: {
        "assigned": 0, "active": 0, "ms": 0, "md": 0,
        "tat_first_call": [], "tat_gaps": [], "tat_to_ms": [], "tat_to_md": [],
    }
    buckets = defaultdict(BucketT)

    for r in leads_raw:
        lid = r.get("Lead Id")
        lrm = (r.get("LRM Email") or "").strip()
        if not lid or not lrm:
            continue

        cluster = normalise_cluster(r.get("Cluster") or "")
        status  = (r.get("Lead status") or "Unknown").strip()
        stage   = (r.get("Lead stage")  or "Unknown").strip()

        cdate_dt = lead_creation.get(lid)   # UTC naive datetime
        if not cdate_dt:
            continue   # no creation timestamp → skip (can't compute any TAT)

        # IST creation date string — display/filter use only
        cdate_ist = ist_date_str(cdate_dt)

        # MS / MD timestamps — must be parsed before is_active check
        ms_dt = parse_dt(r.get("Meeting Schedule Date"))   # UTC naive datetime
        md_dt = parse_dt(r.get("Meeting Done Date"))         # UTC naive datetime

        key = (lrm, cluster, cdate_ist)
        b = buckets[key]
        b["assigned"] += 1

        # Active: exclude closed/lost statuses, inactive stages, AND leads with MS
        is_active = (
            status not in INACTIVE_STATUSES
            and stage not in INACTIVE_STAGES
            and ms_dt is None   # meeting scheduled → no longer pre-sales active
        )
        if is_active:
            b["active"] += 1

        if ms_dt:
            b["ms"] += 1
            tat_ms = round((ms_dt - cdate_dt).total_seconds() / 3600, 2)
            b["tat_to_ms"].append(tat_ms)

        if md_dt:
            b["md"] += 1
            tat_md = round((md_dt - cdate_dt).total_seconds() / 3600, 2)
            b["tat_to_md"].append(tat_md)

        # Call TATs from audit
        ct = call_tat.get(lid, {})
        if ct.get("tat_first_call") is not None:
            b["tat_first_call"].append(ct["tat_first_call"])
        b["tat_gaps"].extend(ct.get("tat_gaps", []))

    records = [
        {
            "lrm":           k[0],
            "cluster":       k[1],
            "date":          k[2],
            "assigned":      v["assigned"],
            "active":        v["active"],
            "ms":            v["ms"],
            "md":            v["md"],
            "tat_first_call": v["tat_first_call"],
            "tat_gaps":       v["tat_gaps"],
            "tat_to_ms":      v["tat_to_ms"],
            "tat_to_md":      v["tat_to_md"],
        }
        for k, v in buckets.items()
    ]

    return {
        "meta": {
            "generated_at":  datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "total_records": len(records),
            "note": (
                "Pre-aggregated by (lrm, cluster, creation_date_ist). "
                "TAT arrays in fractional hours — client concatenates across date range. "
                "Active = not in inactive statuses/stages. "
                "MS/MD timestamps from card 2557 (ISO UTC). "
                "Call TATs from card 3227 (n_attempts increment method)."
            ),
        },
        "records": records,
    }



# ════════════════════════════════════════════════════════════════════════════
# CARD 2557 — LEADS FULL (drill-through)
# ════════════════════════════════════════════════════════════════════════════
def build_leads_full(leads_raw: list) -> dict:
    """
    Per-lead flat snapshot for dashboard drill-through (leads_full.json).
    Lazy-loaded only when the user enables the drill-through master toggle.

    Display columns : lead_id, status, stage, cluster, lrm,
                      creation_date, updated_at, updated_by, _id
      updated_by    : status_stage_updated_by (role: LRM / Solar Consultant / System)
    Filter-only cols: call_attempts, has_ms, has_md

    Standards: cluster via normalise_cluster(); dates via ist_date_str() (IST).
    """
    records = []
    for r in leads_raw:
        _id     = (r.get("_id") or "").strip() or None
        lead_id = (r.get("Lead Id") or "").strip() or None
        cluster = normalise_cluster(r.get("Cluster") or "")

        creation_dt = parse_dt(r.get("Creation Date"))
        updated_dt  = parse_dt(r.get("Updated At"))
        ms_dt       = parse_dt(r.get("Meeting Schedule Date"))
        md_dt       = parse_dt(r.get("Meeting Done Date"))

        try:    call_attempts = int(r.get("call_attempts_lrm") or 0)
        except: call_attempts = 0

        records.append({
            "lead_id":       lead_id,
            "status":        (r.get("Lead status") or "Unknown").strip(),
            "stage":         (r.get("Lead stage")  or "Unknown").strip(),
            "cluster":       cluster,
            "lrm":           (r.get("LRM Email") or "").strip() or None,
            "creation_date": ist_date_str(creation_dt) if creation_dt else None,
            "updated_at":    ist_date_str(updated_dt)  if updated_dt  else None,
            "updated_by":    (r.get("status_stage_updated_by") or "").strip() or None,
            "_id":           _id,
            "call_attempts": call_attempts,
            "has_ms":        ms_dt is not None,
            "has_md":        md_dt is not None,
        })

    return {
        "meta": {
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "total_leads":  len(records),
            "note": (
                "Per-lead snapshot for drill-through. Lazy-loaded on demand. "
                "cluster via normalise_cluster(). Dates in IST. "
                "updated_by = status_stage_updated_by (role). "
                "_id nullable — deep link shows placeholder when None."
            ),
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

    # Build lead_id → creation UTC datetime map (from card 2557 Creation Date).
    # Values are UTC naive datetimes. All TAT calculations use these directly.
    # IST date strings are derived at output time only via ist_date_str().
    lead_creation = {}
    lead_ms_dates = {}   # lead_id → Meeting Schedule Date UTC naive datetime
    for r in leads_raw:
        lid = r.get("Lead Id")
        if not lid: continue
        cd_dt = parse_dt(r.get("Creation Date"))   # UTC naive datetime
        if cd_dt: lead_creation[lid] = cd_dt
        ms_dt = parse_dt(r.get("Meeting Schedule Date"))
        if ms_dt: lead_ms_dates[lid] = ms_dt

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
          f"{dm['meta']['total_status_transitions']:,} status transitions, "
          f"{dm['meta']['total_touches']:,} touches")

    eod = build_eod_position(audit_sorted)
    with open("data/eod_position.json", "w") as f:
        json.dump(eod, f, indent=2, default=str)
    print(f"      eod_position.json — {eod['meta']['total_records']:,} rows (from→to format)")

    print("[7/7] Building LRM performance + TAT...")
    lrm = build_lrm_performance(audit_sorted)
    with open("data/lrm_performance.json", "w") as f:
        json.dump(lrm, f, indent=2, default=str)
    print(f"      lrm_performance.json — {lrm['meta']['lrm_count']} LRMs · {len(lrm['records']):,} rows")

    tat = build_tat_stats(audit_sorted, lead_creation, lead_ms_dates)
    with open("data/tat_stats.json", "w") as f:
        json.dump(tat, f, indent=2, default=str)
    print(f"      tat_stats.json — {tat['meta']['total_leads']:,} lead-level TAT records")

    conv = build_lrm_conversion(audit_sorted)
    with open("data/lrm_conversion.json", "w") as f:
        json.dump(conv, f, indent=2, default=str)
    print(f"      lrm_conversion.json — {conv['meta']['total_records']:,} lead-LRM records")

    snap = build_lrm_snapshot(leads_raw, audit_sorted, lead_creation)
    with open("data/lrm_snapshot.json", "w") as f:
        json.dump(snap, f, separators=(',', ':'), default=str)   # compact — no indent, ~40% smaller
    print(f"      lrm_snapshot.json — {snap['meta']['total_records']:,} (lrm × cluster × date) rows")

    leads_full = build_leads_full(leads_raw)
    with open("data/leads_full.json", "w") as f:
        json.dump(leads_full, f, separators=(',', ':'), default=str)
    print(f"      leads_full.json — {leads_full['meta']['total_leads']:,} leads")

    print("\n  All done.")


if __name__ == "__main__":
    main()
