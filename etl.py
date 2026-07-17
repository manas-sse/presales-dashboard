#!/usr/bin/env python3
"""
Pre-sales Dashboard ETL — Phase 3
Generates:
  data/city_stage.json      → cluster × status × stage (lead snapshot)
  data/call_attempts.json   → cluster × status × stage with attempt buckets
  data/daily_movement.json  → stage/status transitions per day per cluster × LRM
  data/eod_position.json    → end-of-day position per lead per day (aggregated)
  data/eod_leads.json       → per-lead per-day EOD snapshots for multi-day period view
  data/lrm_performance.json → per-LRM aggregates
  data/tat_stats.json       → TAT distributions for events
"""

import os, re, json, time, requests, statistics, calendar
from datetime import datetime, date, timezone, timedelta
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()

# ⚠ Column name NOT independently confirmed against Metabase card 2557 —
# per Manas (July 2026) this IS the correct column. If tat_to_won /
# Control Tower's Order numbers ever come back wrong, check this first.
ORDER_BOOKED_DATE_FIELD = "Order Booked Date"

# ── VALID CLUSTER LIST ───────────────────────────────────────────────────────
# NOTE: "Surat" is discontinued as of June 2026 — no new leads will be created
# there, but historical Surat leads must remain attributed to it (not folded
# into another cluster or dropped to "Invalid"). Kept in the list for that reason.
VALID_CLUSTERS = [
    "Ahmedabad","Surat","Bangalore","Hyderabad","Amravati",
    "Nagpur","Aurangabad","Nashik","Pune","Kolhapur",
    "Jalgaon","Solapur","Ahilyanagar","Bhopal","Gwalior",
    "Indore","Jabalpur","Jaipur","Kanpur","Lucknow",
    "Varanasi","Agra","Bareilly","Meerut","Delhi",
    "Ghaziabad","Noida","Gurgaon","Faridabad","Chennai",
    "Coimbatore","Vijayawada","Kota",
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
    r"kota\s*rajasthan":                    "Kota",
}

# Pre-compile: (pattern, canonical) list, checked in order
_ALIAS_RE = [(re.compile(p, re.IGNORECASE), c) for p, c in CLUSTER_ALIASES.items()]

# Fast lookup: lowercase canonical name → canonical name
_CLUSTER_LOWER = {c.lower(): c for c in VALID_CLUSTERS}

# ── WORKING HOURS CONFIG ──────────────────────────────────────────────────────
# All times in IST.  Adjust these constants to change the definition.
WH_START_H = 10          # Working day starts at 10:00 IST
WH_END_H   = 18         # Working day ends at 18:00 IST (6 PM)
# Python weekday(): 0 = Monday, 1 = Tuesday, … 6 = Sunday.
# Monday is the LRM team's weekoff → working days are Tuesday–Sunday.
WH_WORKING_DAYS: set = {1, 2, 3, 4, 5, 6}   # Tue, Wed, Thu, Fri, Sat, Sun


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

def resolve_cluster(lid: str, event_cluster: str, lead_meta: dict) -> str:
    """
    Returns the card-2557 cluster when valid; falls back to the audit-log cluster.
    Rule: a lead cannot change clusters, so card 2557 is authoritative.
    Exception: card 2557 cluster = 'Invalid' → the LRM may have entered the correct
    cluster in the audit log (site_address_cluster), so use that instead.
    """
    c2557 = lead_meta.get(lid, {}).get("cluster", "Invalid")
    return c2557 if c2557 != "Invalid" else event_cluster


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


def fetch_card(token: str, card_id: int, max_retries: int = 3) -> list:
    """
    Fetch a Metabase saved-question result as a JSON list.

    The audit card (3227) response exceeds 190 MB as data grows.  A single-shot
    r.json() can fail with JSONDecodeError if Metabase's Jetty server, a reverse
    proxy, or a network buffer truncates the response body mid-stream.

    Mitigations applied here:
      • stream=True  — reads the body in 1 MB chunks instead of one huge buffer,
                        reducing peak memory and letting the OS/TCP layer keep up.
      • Separate read timeout (TIMEOUT_S) from connect timeout (30 s), so a slow
        but active transfer is never killed prematurely.
      • Retry with back-off — on JSONDecodeError, wait and try again up to
        max_retries times.  Covers transient Metabase load spikes.

    If all retries fail the original exception is re-raised so the caller sees it.
    If the problem is persistent, the most likely fix is raising MB_JETTY_MAX_POST_SIZE
    in the Metabase server config (or adding a date filter to card 3227).
    """
    url = f"{METABASE_URL}/api/card/{card_id}/query/json"
    headers = {"X-Metabase-Session": token}
    body = {"ignore_cache": False}
    last_exc = None

    for attempt in range(max_retries):
        try:
            r = requests.post(
                url,
                headers=headers,
                json=body,
                timeout=(30, TIMEOUT_S),   # (connect_timeout, read_timeout)
                stream=True,               # don't buffer the whole response at once
            )
            r.raise_for_status()

            # Read in 1 MB chunks; join and parse once fully received
            chunks = []
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    chunks.append(chunk)
            raw = b"".join(chunks)
            return json.loads(raw)

        except (json.JSONDecodeError, requests.exceptions.JSONDecodeError) as exc:
            last_exc = exc
            if attempt < max_retries - 1:
                wait = 15 * (attempt + 1)   # 15 s, 30 s, …
                print(f"      ⚠  Card {card_id}: JSON decode error on attempt "
                      f"{attempt + 1}/{max_retries} "
                      f"(char {getattr(exc, 'pos', '?')}), retrying in {wait} s…")
                time.sleep(wait)
            # else: fall through and re-raise after loop

    raise last_exc


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


def effective_creation_dt_wh(creation_dt_utc: datetime) -> datetime:
    """
    Working-hours-adjusted effective creation datetime.

    Given a UTC naive creation datetime, returns the earliest moment within
    working hours that is ≥ the actual creation time (UTC naive).

    Logic (all arithmetic in IST):
      • If created during working hours on a working day → return unchanged.
      • If created before WH_START_H on a working day → return WH_START_H that day.
      • If created at or after WH_END_H on a working day → return WH_START_H of
        the next working day.
      • If created on a weekoff day (Monday) → return WH_START_H of the next
        working day.

    The returned datetime is UTC naive for TAT arithmetic consistency with the
    rest of the pipeline.  Floor-of-zero (max(0, tat)) is applied at call sites.
    """
    ist_dt = creation_dt_utc + _IST

    def _next_working_start(from_ist: datetime) -> datetime:
        """IST datetime of the start of the next working day after from_ist."""
        d = from_ist.date() + timedelta(days=1)
        while d.weekday() not in WH_WORKING_DAYS:
            d += timedelta(days=1)
        return datetime(d.year, d.month, d.day, WH_START_H, 0, 0)

    wd = ist_dt.weekday()
    if wd not in WH_WORKING_DAYS:
        # Weekoff day → push to next working day's start
        eff_ist = _next_working_start(ist_dt)
    elif ist_dt.hour < WH_START_H:
        # Before working hours → same day's start
        eff_ist = datetime(ist_dt.year, ist_dt.month, ist_dt.day, WH_START_H, 0, 0)
    elif ist_dt.hour >= WH_END_H:
        # After working hours → next working day's start
        eff_ist = _next_working_start(ist_dt)
    else:
        # During working hours — no adjustment
        eff_ist = ist_dt

    return eff_ist - _IST   # IST → UTC naive


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

# ╔══════════════════════════════════════════════════════════════════════════╗
# ║ CALL DETECTION LOGIC — ADJUSTABLE. This is THE single source of truth    ║
# ║ for "did a call happen at this audit event". Every function that counts  ║
# ║ calls or computes call-based TAT reads call_made_calc / call_attempt_    ║
# ║ lrm_calc below — none of them re-derive the rule themselves. If the      ║
# ║ business definition of "a call happened" changes, edit ONLY              ║
# ║ compute_call_attempt_calc(). If the rechurn-truncation rule changes, or  ║
# ║ rechurn ever needs its own dedicated handling (rather than this simple   ║
# ║ truncation), edit ONLY FIRST_CALL_TRUNCATION_STATUSES and                ║
# ║ compute_call_times_and_first_call_tat().                                 ║
# ╚══════════════════════════════════════════════════════════════════════════╝

# Rule (as agreed July 2026): a call is counted at an event when EITHER
#   (a) call_attempts_lrm incremented from the previous event (source truth
#       — actor-unrestricted; the counter itself is already evidence a call
#       was logged, regardless of which audit row happens to carry it), OR
#   (b) the lead's STAGE changed from the previous event AND that change was
#       made BY THE LRM (status_stage_updated_by == "LRM"), even if (a)
#       didn't fire — catches calls that happened but weren't logged in the
#       counter. RESTRICTED to LRM-made changes only — confirmed after
#       testing showed the unrestricted version wrongly counted System/Solar-
#       Consultant-driven stage changes (closures, reopens, SC home-visit
#       progressions) as calls, which inflated counts well beyond intent.
# If raw_delta > 1 (source logged multiple attempts between two audit rows),
# that magnitude is preserved rather than collapsed to 1.
def compute_call_attempt_calc(events: list) -> None:
    """
    Mutates `events` (already sorted by ts, for ONE lead_id) in place, adding:
      call_attempt_lrm_calc  — cumulative corrected call count (same shape as
                                the raw call_attempts_lrm column, but corrected)
      call_made_calc         — bool, whether THIS event is itself a call
    No call is ever attributed to a lead's first-ever audit event (nothing
    to compare it against) — same convention as the old raw-increment logic.
    """
    if not events: return
    events[0]["call_attempt_lrm_calc"] = events[0]["n_attempts"]
    events[0]["call_made_calc"] = False
    for i in range(1, len(events)):
        prev, ev = events[i - 1], events[i]
        raw_delta = ev["n_attempts"] - prev["n_attempts"]
        stage_changed_by_lrm = (ev["stage"] != prev["stage"]) and (ev["updated_by"] == "LRM")
        inc = raw_delta if raw_delta > 0 else (1 if stage_changed_by_lrm else 0)
        ev["call_attempt_lrm_calc"] = prev["call_attempt_lrm_calc"] + inc
        ev["call_made_calc"] = inc > 0

# Rechurn-bleed guard for "first call" TAT specifically (NOT for total call
# counts or call-gap TAT, which intentionally reflect the full lifecycle).
# Problem: a lead created → closed → later reopened, with a NEW call made
# during the reopened cycle, was getting counted as if it were the ORIGINAL
# lead's first call — wrongly inflating/deflating TAT relative to the
# original creation date.
# Fix: once a lead has progressed into one of these "contact clearly
# established" statuses for the first time, no call after that point counts
# toward tat_first_call. If NO call happened before that point, tat_first_call
# is None (the pre-progression call genuinely didn't happen) rather than
# picking up a much-later rechurned call.
# TUNABLE — deliberately excludes Closed-Lost/Closed-Cold/Lost: many of those
# stages (e.g. "Wrong Number", "Not Serviceable") can be reached WITHOUT any
# contact ever happening, so including them here would truncate too early in
# those cases. Revisit this set if that assumption turns out wrong, or if
# rechurn needs full dedicated handling (e.g. splitting a lead's history into
# distinct "cycles" at each close→reopen boundary) instead of this truncation.
FIRST_CALL_TRUNCATION_STATUSES = {"Connected", "Meeting", "Booked", "Closed - Won"}

def compute_call_times_and_first_call_tat(events: list, cdate_dt) -> dict:
    """
    Shared by build_tat_stats() and build_lrm_snapshot() so the call-detection
    rule and the rechurn-truncation rule can never drift between the two —
    edit ONLY this function (and the two constants/helpers above it) to
    change either behaviour everywhere at once.

    events: sorted-by-ts audit events for ONE lead, must already carry
            'call_made_calc' (via compute_call_attempt_calc).
    cdate_dt: lead's creation datetime (UTC naive), or None.

    Returns: {
      call_times:        [ts,...] ALL calls, full lifecycle, untruncated
                          (used for total_calls and tat_gaps — NOT affected
                          by the rechurn guard, by design)
      tat_first_call:     hours, creation → first PRE-TRUNCATION call, or None
      wh_tat_first_call:  same, working-hours-adjusted, floor 0, or None
      first_call_date:    IST date string of that (truncated) first call, or None
      tat_gaps:           [hours,...] between consecutive calls, full lifecycle,
                          gaps > 365 days excluded as outliers
    }
    """
    call_times = [ev["ts"] for ev in events if ev.get("call_made_calc")]

    first_progress_ts = next((ev["ts"] for ev in events if ev["status"] in FIRST_CALL_TRUNCATION_STATUSES), None)
    truncated_calls = [t for t in call_times if not first_progress_ts or t <= first_progress_ts]

    tat_first_call = None
    wh_tat_first_call = None
    first_call_date = None
    if cdate_dt and truncated_calls:
        tat_first_call = round((truncated_calls[0] - cdate_dt).total_seconds() / 3600, 2)
        eff_cdate = effective_creation_dt_wh(cdate_dt)
        wh_tat_first_call = round(max(0.0, (truncated_calls[0] - eff_cdate).total_seconds() / 3600), 2)
        first_call_date = ist_date_str(truncated_calls[0])

    tat_gaps = []
    for i in range(1, len(call_times)):
        gap_h = (call_times[i] - call_times[i - 1]).total_seconds() / 3600
        if 0 <= gap_h <= 365 * 24:
            tat_gaps.append(round(gap_h, 2))

    return {
        "call_times":        call_times,
        "tat_first_call":     tat_first_call,
        "wh_tat_first_call":  wh_tat_first_call,
        "first_call_date":    first_call_date,
        "tat_gaps":           tat_gaps,
    }


def normalise_audit_rows(audit_raw: list) -> list:
    """
    Sort audit by lead_id, createdAt asc — required for transition + attempt
    detection. Also computes call_attempt_lrm_calc / call_made_calc per event
    (see CALL DETECTION LOGIC block above) — every downstream function reads
    these, not the raw n_attempts increment, for call counting.
    """
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
    by_lead_tmp: dict = defaultdict(list)
    for r in cleaned: by_lead_tmp[r["lead_id"]].append(r)
    for lid, evs in by_lead_tmp.items():
        compute_call_attempt_calc(evs)   # mutates evs (and thus `cleaned`) in place
    return cleaned


def build_daily_movement(audit_sorted: list, lead_meta: dict) -> dict:
    """
    Per (date, cluster, lrm, from_stage, to_stage):
      - count of transitions
    Also tracks status-level transitions (from_status -> to_status).
    Also tracks "touches" — audit events where stage AND status did NOT change.
    Date is IST (audit createdAt + 5:30 hrs).
    Cluster resolved via resolve_cluster(): card 2557 authoritative, audit fallback for Invalid.
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
            cluster = resolve_cluster(lid, ev["cluster"], lead_meta)
            lrm     = ev["lrm"] or "Unknown"
            if prev is not None:
                stage_changed  = prev["stage"]  != ev["stage"]
                status_changed = prev["status"] != ev["status"]
                call_made      = ev.get("call_made_calc", False)
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
                "call attempted (call_made_calc — see CALL DETECTION LOGIC block); "
                "updates_no_transition = any other audit event with no state change."
            ),
        },
        "stage":   stage_records,
        "status":  status_records,
        "touches": touch_records,
    }


def build_eod_position(audit_sorted: list, lead_meta: dict) -> dict:
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

            cluster = resolve_cluster(lid, last_ev["cluster"], lead_meta)
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


def build_eod_leads(audit_sorted: list, lead_meta: dict) -> dict:
    """
    Per-lead per-day end-of-day snapshots for multi-day EOD period analysis.
    Compact field names to keep file size manageable:
      lid  = lead_id
      d    = date (YYYY-MM-DD IST)
      cl   = cluster (normalised)
      lrm  = LRM email
      fs   = start-of-day stage  (previous day's last event)
      fst  = start-of-day status (previous day's last event)
      s    = end-of-day stage    (last audit event of the day)
      st   = end-of-day status

    Dashboard usage:
      Single-day  -> use eod_position.json (faster, pre-aggregated).
      Multi-day   -> group by lid, find last record before period start (from-state)
                     and last record within period (to-state), then re-aggregate.
    """
    by_lead: dict = defaultdict(list)
    for r in audit_sorted:
        by_lead[r["lead_id"]].append(r)

    records = []
    for lid, events in by_lead.items():
        by_day: dict = defaultdict(list)
        for ev in events:
            d = (ev["ts"] + timedelta(hours=5, minutes=30)).date().strftime("%Y-%m-%d")
            by_day[d].append(ev)

        sorted_days = sorted(by_day.keys())
        prev_last_ev = None

        for d in sorted_days:
            day_evs = sorted(by_day[d], key=lambda x: x["ts"])
            last_ev = day_evs[-1]

            if prev_last_ev is not None:
                from_stage  = prev_last_ev["stage"]
                from_status = prev_last_ev["status"]
            else:
                from_stage  = day_evs[0]["stage"]
                from_status = day_evs[0]["status"]

            records.append({
                "lid": lid,
                "d":   d,
                "cl":  resolve_cluster(lid, last_ev["cluster"], lead_meta),
                "lrm": last_ev["lrm"] or "Unknown",
                "fs":  from_stage,
                "fst": from_status,
                "s":   last_ev["stage"],
                "st":  last_ev["status"],
            })
            prev_last_ev = last_ev

    return {
        "meta": {
            "generated_at":  datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "total_records": len(records),
            "note": (
                "Per-lead per-day EOD snapshots. Compact keys. "
                "fs/fst = start-of-day state (prev day EOD). "
                "s/st = end-of-day state. "
                "Lazy-loaded only when multi-day EOD period view is active."
            ),
        },
        "records": records,
    }


def build_lrm_performance(audit_sorted: list, lead_meta: dict) -> dict:
    """
    Per (date IST, cluster, lrm):
      - calls          (call_made_calc — see CALL DETECTION LOGIC block; attributed to assigned LRM)
      - leads_touched  (distinct leads with any LRM-initiated update)
      - stage_movements / status_movements (any state change — attributed to assigned LRM)
      - call_lead_ids, touch_lead_ids, stage_move_lead_ids — lead ID sets for drill-through
    Cluster resolved via resolve_cluster(): card 2557 authoritative, audit fallback for Invalid.
    """
    by_lead = defaultdict(list)
    for r in audit_sorted:
        by_lead[r["lead_id"]].append(r)

    bucket = defaultdict(lambda: {
        "calls": 0, "leads": set(),
        "stage_moves": 0, "status_moves": 0,
        "call_ids": set(), "stage_ids": set(),
    })

    for lid, events in by_lead.items():
        prev = None
        for ev in events:
            d       = (ev["ts"] + timedelta(hours=5, minutes=30)).date().strftime("%Y-%m-%d")
            lrm     = ev["lrm"] or "Unknown"
            cluster = resolve_cluster(lid, ev["cluster"], lead_meta)
            k = (d, cluster, lrm)
            # leads_touched = unique leads updated BY the LRM (not system/SC updates)
            if ev["updated_by"] == "LRM":
                bucket[k]["leads"].add(lid)
            if ev.get("call_made_calc"):
                bucket[k]["calls"] += 1
                bucket[k]["call_ids"].add(lid)
            if prev is not None:
                if prev["stage"]  != ev["stage"]:
                    bucket[k]["stage_moves"] += 1
                    bucket[k]["stage_ids"].add(lid)
                if prev["status"] != ev["status"]:
                    bucket[k]["status_moves"] += 1
            prev = ev

    records = []
    for (d, cluster, lrm), v in bucket.items():
        records.append({
            "date":               d,
            "cluster":            cluster,
            "lrm":                lrm,
            "calls":              v["calls"],
            "leads_touched":      len(v["leads"]),
            "stage_movements":    v["stage_moves"],
            "status_movements":   v["status_moves"],
            # Sorted lists for drill-through: exact lead IDs behind each metric
            "call_lead_ids":      sorted(v["call_ids"]),
            "touch_lead_ids":     sorted(v["leads"]),
            "stage_move_lead_ids":sorted(v["stage_ids"]),
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


def build_tat_stats(audit_sorted: list, lead_meta: dict, lead_ms_dates: dict = None, lead_won_dates: dict = None) -> dict:
    """
    Computes TAT in fractional HOURS for each measure per lead.
    lead_meta values carry UTC naive creation_dt, card-2557 cluster, card-2557 lrm.
    All arithmetic is datetime − datetime → timedelta → total_seconds() / 3600.
    IST date strings are derived at output time only.

    Metrics:
      tat_first_call : creation → first call, TRUNCATED at first "contact
                       established" status to guard against rechurn bleed
                       (see FIRST_CALL_TRUNCATION_STATUSES) (hours)
      tat_gaps       : list of hours between each consecutive pair of call
                       events — FULL lifecycle, not truncated
      tat_to_meeting : creation → Meeting Schedule Date from card 2557 (hours).
                       ⚠ CAVEAT (flagged July 2026, not yet resolved): this reads
                       the CURRENT/latest Meeting Schedule Date value, not the
                       FIRST time a meeting was ever scheduled. If a meeting is
                       rescheduled, this TAT silently reflects the most recent
                       schedule date, not the original one. No fix implemented
                       yet — revisit if "time to first scheduling attempt" (as
                       opposed to "time to current scheduled meeting") is needed.
      tat_to_won     : creation → Order Booked Date from card 2557 (hours) when
                       available, else FALLS BACK to createdAt of the first
                       audit event where stage == "Order Confirmed" (the old,
                       sole method). See ORDER_BOOKED_DATE_FIELD below — confirm
                       the exact Metabase column name matches.

    Call detection: call_made_calc (see CALL DETECTION LOGIC block near
    normalise_audit_rows) — increments on raw call_attempts_lrm OR stage change.
    LRM attribution: card 2557 LRM Email (lead_meta["lrm"]).
    Cluster attribution: card 2557 Cluster with Invalid fallback (resolve_cluster).
    lead_ms_dates: dict of lead_id → Meeting Schedule Date UTC naive datetime (from card 2557).
    lead_won_dates: dict of lead_id → Order Booked Date UTC naive datetime (from card 2557),
                    may be None/absent for older leads or if the field isn't populated.
    """
    by_lead = defaultdict(list)
    for r in audit_sorted:
        by_lead[r["lead_id"]].append(r)

    STAGE_TARGETS = {
        "first_won": {"Order Confirmed"},
    }

    records = []
    for lid, events in by_lead.items():
        if not events: continue

        # Lead-level attributes from card 2557 (authoritative)
        meta     = lead_meta.get(lid, {})
        cdate_dt = meta.get("creation_dt")
        lrm      = meta.get("lrm") or "Unknown"

        # Cluster: card 2557 with Invalid fallback to most recent valid audit cluster
        c2557   = meta.get("cluster", "Invalid")
        cluster = c2557 if c2557 != "Invalid" else next(
            (ev["cluster"] for ev in reversed(events) if ev["cluster"] != "Invalid"),
            events[0]["cluster"]
        )

        # Call detection + first-call TAT (incl. rechurn truncation) — shared
        # helper, see FIRST_CALL_TRUNCATION_STATUSES block near normalise_audit_rows.
        fc = compute_call_times_and_first_call_tat(events, cdate_dt)
        call_times      = fc["call_times"]       # full lifecycle, untruncated
        tat_first_call  = fc["tat_first_call"]    # truncated at rechurn guard
        tat_gaps        = fc["tat_gaps"]          # full lifecycle, untruncated

        # TAT 3: creation → Meeting Schedule Date (from card 2557)
        # ⚠ Uses the CURRENT/latest MS date, not the first-ever scheduled one —
        # see caveat in the function docstring above. Not fixed, just flagged.
        tat_to_meeting = None
        if cdate_dt and lead_ms_dates:
            ms_dt_from_lead = lead_ms_dates.get(lid)
            if ms_dt_from_lead:
                tat_to_meeting = round(
                    (ms_dt_from_lead - cdate_dt).total_seconds() / 3600, 2
                )

        # TAT 4–5: creation → first event at each target stage (hours) — from audit
        # (kept as-is: this is now ONLY the fallback path for tat_to_won when
        # lead_won_dates has no entry for this lead — see below)
        tat_to_targets = {}
        if cdate_dt:
            for label, target_stages in STAGE_TARGETS.items():
                for ev in events:
                    if ev["stage"] in target_stages:
                        tat_to_targets[label] = round(
                            (ev["ts"] - cdate_dt).total_seconds() / 3600, 2
                        )
                        break

        # TAT (won): PRIMARY = creation → Order Booked Date (card 2557).
        # FALLBACK = old audit-based method (first "Order Confirmed" stage
        # event) when lead_won_dates has no value for this lead (older leads,
        # or the field not yet populated at ETL run time).
        won_dt_from_lead = lead_won_dates.get(lid) if lead_won_dates else None
        if cdate_dt and won_dt_from_lead:
            tat_to_won = round((won_dt_from_lead - cdate_dt).total_seconds() / 3600, 2)
        else:
            tat_to_won = tat_to_targets.get("first_won")

        # ── WORKING HOURS TAT (separate from calendar TAT above) ─────────────
        # Effective creation = creation time adjusted forward to next working-hour
        # window.  TAT floor = 0 (a call before wh-start on creation day → 0).
        # tat_gaps has no WH equivalent (gap is between two call events, not
        # relative to creation).
        wh_tat_first_call  = fc["wh_tat_first_call"]   # also rechurn-truncated
        wh_tat_to_meeting  = None
        wh_tat_to_won      = None
        if cdate_dt:
            eff_cdate = effective_creation_dt_wh(cdate_dt)

            # WH TAT 3: effective creation → Meeting Schedule Date
            if lead_ms_dates:
                ms_dt_from_lead = lead_ms_dates.get(lid)
                if ms_dt_from_lead:
                    raw = (ms_dt_from_lead - eff_cdate).total_seconds() / 3600
                    wh_tat_to_meeting = round(max(0.0, raw), 2)

            # WH TAT 4: effective creation → Order Booked Date (primary),
            # else effective creation → first Order Confirmed audit event (fallback)
            if won_dt_from_lead:
                raw = (won_dt_from_lead - eff_cdate).total_seconds() / 3600
                wh_tat_to_won = round(max(0.0, raw), 2)
            else:
                for ev in events:
                    if ev["stage"] in STAGE_TARGETS["first_won"]:
                        raw = (ev["ts"] - eff_cdate).total_seconds() / 3600
                        wh_tat_to_won = round(max(0.0, raw), 2)
                        break

        # Display-only date strings in IST
        creation_date_str   = ist_date_str(cdate_dt) if cdate_dt else None
        first_call_date_str = fc["first_call_date"]   # truncated, consistent with tat_first_call

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
            "tat_to_won":      tat_to_won,
            # Working-hours variants (effective creation time, floor 0, no WH gaps)
            "wh_tat_first_call": wh_tat_first_call,
            "wh_tat_to_meeting": wh_tat_to_meeting,
            "wh_tat_to_won":     wh_tat_to_won,
        })

    return {
        "meta": {
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "total_leads":  len(records),
            "note": (
                "TAT in fractional hours. "
                "UI displays: <1h as minutes, 1–24h as hours, >=24h as days. "
                "tat_to_meeting uses Meeting Schedule Date from card 2557 — CAVEAT: this is "
                "the CURRENT/latest MS value, not the first-ever scheduled date; a rescheduled "
                "meeting shifts this TAT. Not fixed, flagged only (July 2026). "
                "tat_to_won: PRIMARY = Order Booked Date (card 2557); FALLBACK = first audit "
                "event at stage 'Order Confirmed' when Order Booked Date is absent for the lead. "
                "Call detection = call_made_calc (raw call_attempts_lrm increment OR stage change), "
                "see CALL DETECTION LOGIC block in etl.py. "
                "tat_first_call/wh_tat_first_call are rechurn-truncated (see FIRST_CALL_TRUNCATION_STATUSES) "
                "— total_calls and tat_gaps are NOT truncated (full lifecycle). "
                "wh_tat_* fields use working-hours-adjusted effective creation time "
                f"(WH_START={WH_START_H}:00–{WH_END_H}:00 IST, Mon weekoff). "
                "wh_tat_* floor is 0 (call before wh-start on creation day). "
                "tat_gaps has no wh equivalent."
            ),
        },
        "records": records,
    }

def build_lrm_conversion(audit_sorted: list, lead_meta: dict) -> dict:
    """
    Per (lead_id, lrm): dates when this LRM made a call + first meeting date for the lead.
    Cluster resolved via resolve_cluster().
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

        # Collect call events grouped by LRM (call = call_made_calc, see
        # CALL DETECTION LOGIC block near normalise_audit_rows)
        by_lrm: dict = defaultdict(lambda: {"cluster": "Invalid", "call_dates": set()})
        for ev in events:
            if ev.get("call_made_calc"):
                lrm = ev["lrm"] or "Unknown"
                d_ist = (ev["ts"] + timedelta(hours=5, minutes=30)).date().strftime("%Y-%m-%d")
                by_lrm[lrm]["call_dates"].add(d_ist)
                by_lrm[lrm]["cluster"] = resolve_cluster(lid, ev["cluster"], lead_meta)

        creation_dt = lead_meta.get(lid, {}).get("creation_dt")
        creation_date_str = creation_dt.date().isoformat() if creation_dt else None

        for lrm, v in by_lrm.items():
            if not v["call_dates"]:
                continue
            records.append({
                "lead_id":       lid,
                "lrm":           lrm,
                "cluster":       v["cluster"],
                "creation_date": creation_date_str,
                "call_dates":    sorted(v["call_dates"]),
                "meeting_date":  meeting_date,
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
def build_lrm_snapshot(leads_raw: list, audit_sorted: list, lead_meta: dict) -> dict:
    """
    Per-lead snapshot combining card 2557 (LRM attribution, counts, MS/MD dates)
    and card 3227 (call TATs via call_made_calc — see CALL DETECTION LOGIC block).

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
    # Uses the SAME shared helper as build_tat_stats — see CALL DETECTION
    # LOGIC block near normalise_audit_rows. Do not re-derive this locally;
    # if it drifts from build_tat_stats, the TAT tab and Leads×LRM tab will
    # silently disagree on "first call" for the same lead.
    by_lead_audit = defaultdict(list)
    for ev in audit_sorted:
        by_lead_audit[ev["lead_id"]].append(ev)

    call_tat = {}   # lead_id → {tat_first_call: float|None, tat_gaps: [float], wh_tat_first_call: float|None}
    for lid, events in by_lead_audit.items():
        cdate_dt = lead_meta.get(lid, {}).get("creation_dt")   # UTC naive datetime
        fc = compute_call_times_and_first_call_tat(events, cdate_dt)
        call_tat[lid] = {
            "tat_first_call":    fc["tat_first_call"],
            "tat_gaps":          fc["tat_gaps"],
            "wh_tat_first_call": fc["wh_tat_first_call"],
        }

    # ── Aggregate by (lrm, cluster, creation_date_ist) ───────────────────────
    BucketT = lambda: {
        "assigned": 0, "active": 0, "ms": 0, "md": 0,
        "tat_first_call": [], "tat_gaps": [], "tat_to_ms": [], "tat_to_md": [],
        # Working-hours TAT arrays (separate from calendar TAT above)
        "wh_tat_first_call": [], "wh_tat_to_ms": [], "wh_tat_to_md": [],
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

        cdate_dt = lead_meta.get(lid, {}).get("creation_dt")   # UTC naive datetime
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

        # Working-hours effective creation datetime (used for wh_tat_* fields)
        eff_cdate_dt = effective_creation_dt_wh(cdate_dt)

        if ms_dt:
            b["ms"] += 1
            tat_ms = round((ms_dt - cdate_dt).total_seconds() / 3600, 2)
            b["tat_to_ms"].append(tat_ms)
            # Working-hours version (floor 0)
            b["wh_tat_to_ms"].append(
                round(max(0.0, (ms_dt - eff_cdate_dt).total_seconds() / 3600), 2)
            )

        if md_dt:
            b["md"] += 1
            tat_md = round((md_dt - cdate_dt).total_seconds() / 3600, 2)
            b["tat_to_md"].append(tat_md)
            # Working-hours version (floor 0)
            b["wh_tat_to_md"].append(
                round(max(0.0, (md_dt - eff_cdate_dt).total_seconds() / 3600), 2)
            )

        # Call TATs from audit (regular + WH)
        ct = call_tat.get(lid, {})
        if ct.get("tat_first_call") is not None:
            b["tat_first_call"].append(ct["tat_first_call"])
        if ct.get("wh_tat_first_call") is not None:
            b["wh_tat_first_call"].append(ct["wh_tat_first_call"])
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
            # Working-hours TAT arrays (separate; use wh toggle in dashboard)
            "wh_tat_first_call": v["wh_tat_first_call"],
            "wh_tat_to_ms":      v["wh_tat_to_ms"],
            "wh_tat_to_md":      v["wh_tat_to_md"],
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
                "Call TATs from card 3227 (call_made_calc method, see CALL DETECTION LOGIC block; "
                "tat_first_call is rechurn-truncated, see FIRST_CALL_TRUNCATION_STATUSES). "
                "wh_tat_* fields use working-hours-adjusted effective creation time "
                f"(WH={WH_START_H}:00–{WH_END_H}:00 IST, Mon weekoff); floor 0."
            ),
        },
        "records": records,
    }


# ════════════════════════════════════════════════════════════════════════════
# CONTROL TOWER — BQL → MS → MD → Order funnel (Referral channel only)
# ════════════════════════════════════════════════════════════════════════════
# Agreed design (July 2026):
#   - "BQL" = Lead Created (relabelled only — no separate BQL qualifying
#     condition implemented yet; revisit if that changes).
#   - Pure card 2557 timestamps. Stage/status AGNOSTIC — a milestone is
#     "reached" purely because its date column is populated, full stop.
#   - HOTO not implemented (deferred). Tier / Sub-Channel not implemented.
#
# BACKFILL rule (strict subset funnel) — used ONLY in the Cohort/MO framing:
#   has_oc = order_booked_dt is not None
#   has_md = (meeting_done_dt is not None) OR has_oc
#   has_ms = (meeting_schedule_dt is not None) OR has_md
# Mirrored in index.html's Lead Data drill filters (ct_ms/ct_md/ct_oc) — if
# this rule ever changes, update BOTH places or the two will silently drift.
#
# EFFORT framing: each stage counted on ITS OWN event date's month, using
# RAW dates only (no backfill — a backfilled milestone has no date of its
# own, so it can't be placed in a specific month/day). Effort %s are pace
# ratios (numerator/denominator are NOT the same lead set), not true
# per-lead conversion — labelled as such in the dashboard.
# FUTURE-MONTH GUARD (added July 2026): meetings/orders are sometimes
# pre-scheduled for a date beyond the current month (e.g. an August meeting
# booked while still in July). Those are EXCLUDED from Effort's monthly
# buckets — a future month isn't a real month to report activity for yet.
# This does NOT affect Cohort/MO (bucketed by creation month, which can't be
# future-dated) or the DoD matrix (current month only, day-level — a future
# DAY within the current month is left as-is; that's real pre-scheduled data,
# not the same problem).
#
# COHORT (MO) framing: a lead created in month X only counts toward month
# X's MS/MD/Order numbers if THAT milestone ALSO falls in month X. Backfill
# still applies, but a backfilled milestone always inherits the same-month-
# ness of whichever later milestone caused the backfill (it must
# chronologically precede it, so it can't be in a later month).
#
# MTD tables: every month (including past ones) is truncated to
# day-of-month <= today's day-of-month, enabling apples-to-apples MTD vs
# LMTD (last-month-to-date) comparison.
#
# DoD matrix: current calendar month only. Row = day lead entered the start
# stage; "count_in" = ALL leads that entered that day (converted or not);
# cells = % of that day's entrants converting on each subsequent day
# (same month only). D1-3%/D1-7% = % converting within 3/7 calendar days
# inclusive of the start day.
def build_control_tower(leads_raw: list) -> dict:
    today = today_ist()
    cur_month = today.strftime("%Y-%m")
    cutoff_day = today.day

    def month_str(d): return d.strftime("%Y-%m")
    def new_bucket(): return {"bql": 0, "ms": 0, "md": 0, "order": 0}

    leads = []
    for r in leads_raw:
        lead_id = (r.get("Lead Id") or "").strip()
        if not lead_id: continue
        cdt = parse_dt(r.get("Creation Date"))
        if not cdt: continue   # every lead must have a creation date; skip malformed rows
        ms_dt = parse_dt(r.get("Meeting Schedule Date"))
        md_dt = parse_dt(r.get("Meeting Done Date"))
        oc_dt = parse_dt(r.get(ORDER_BOOKED_DATE_FIELD))
        leads.append({
            "cluster":  normalise_cluster(r.get("Cluster") or ""),
            "c_date":   (cdt + _IST).date(),
            "ms_date":  (ms_dt + _IST).date() if ms_dt else None,
            "md_date":  (md_dt + _IST).date() if md_dt else None,
            "oc_date":  (oc_dt + _IST).date() if oc_dt else None,
        })

    # ── EFFORT: full-month + MTD, both global and per-city ──────────────────
    effort_month        = defaultdict(new_bucket)          # key: month
    effort_month_city   = defaultdict(new_bucket)          # key: (month, cluster)
    effort_mtd_month      = defaultdict(new_bucket)
    effort_mtd_month_city = defaultdict(new_bucket)

    for L in leads:
        cm = month_str(L["c_date"])
        effort_month[cm]["bql"] += 1
        effort_month_city[(cm, L["cluster"])]["bql"] += 1
        if L["c_date"].day <= cutoff_day:
            effort_mtd_month[cm]["bql"] += 1
            effort_mtd_month_city[(cm, L["cluster"])]["bql"] += 1
        for fld, dt in (("ms", L["ms_date"]), ("md", L["md_date"]), ("order", L["oc_date"])):
            if not dt: continue
            m = month_str(dt)
            if m > cur_month: continue   # future-dated event (e.g. a meeting pre-scheduled for next month) — exclude, not a real month to report on yet
            effort_month[m][fld] += 1
            effort_month_city[(m, L["cluster"])][fld] += 1
            if dt.day <= cutoff_day:
                effort_mtd_month[m][fld] += 1
                effort_mtd_month_city[(m, L["cluster"])][fld] += 1

    # ── COHORT (MO): full-month + MTD, both global and per-city ─────────────
    cohort_month        = defaultdict(new_bucket)
    cohort_month_city   = defaultdict(new_bucket)
    cohort_mtd_month      = defaultdict(new_bucket)
    cohort_mtd_month_city = defaultdict(new_bucket)

    # DoD matrix inputs — current month only, per adjacent stage pair.
    # value = day-of-month the lead converted to the NEXT stage (same month),
    # or None if it entered the start stage this month but never converted
    # (within this month) to the next one.
    dod_bql_ms   = defaultdict(list)   # start_day(entered BQL) -> [conv_day|None]
    dod_ms_md    = defaultdict(list)   # start_day(entered MS)  -> [conv_day|None]
    dod_md_order = defaultdict(list)   # start_day(entered MD)  -> [conv_day|None]

    for L in leads:
        cm = month_str(L["c_date"])
        has_oc = L["oc_date"] is not None and month_str(L["oc_date"]) == cm
        md_same_month = L["md_date"] is not None and month_str(L["md_date"]) == cm
        has_md = md_same_month or has_oc
        ms_same_month = L["ms_date"] is not None and month_str(L["ms_date"]) == cm
        has_ms = ms_same_month or has_md

        cohort_month[cm]["bql"] += 1
        cohort_month_city[(cm, L["cluster"])]["bql"] += 1
        if has_ms: cohort_month[cm]["ms"] += 1; cohort_month_city[(cm, L["cluster"])]["ms"] += 1
        if has_md: cohort_month[cm]["md"] += 1; cohort_month_city[(cm, L["cluster"])]["md"] += 1
        if has_oc: cohort_month[cm]["order"] += 1; cohort_month_city[(cm, L["cluster"])]["order"] += 1

        # MTD: effective date = the actual raw date that made each flag true
        # (its own date if genuinely present this month, else whichever
        # later milestone's date backfilled it).
        oc_eff = L["oc_date"] if has_oc else None
        md_eff = L["md_date"] if md_same_month else (oc_eff if has_md else None)
        ms_eff = L["ms_date"] if ms_same_month else (md_eff if has_ms else None)

        if L["c_date"].day <= cutoff_day:
            cohort_mtd_month[cm]["bql"] += 1
            cohort_mtd_month_city[(cm, L["cluster"])]["bql"] += 1
            if has_ms and ms_eff and ms_eff.day <= cutoff_day:
                cohort_mtd_month[cm]["ms"] += 1; cohort_mtd_month_city[(cm, L["cluster"])]["ms"] += 1
            if has_md and md_eff and md_eff.day <= cutoff_day:
                cohort_mtd_month[cm]["md"] += 1; cohort_mtd_month_city[(cm, L["cluster"])]["md"] += 1
            if has_oc and oc_eff and oc_eff.day <= cutoff_day:
                cohort_mtd_month[cm]["order"] += 1; cohort_mtd_month_city[(cm, L["cluster"])]["order"] += 1

        # DoD matrix — current month only
        if cm == cur_month:
            conv = L["ms_date"].day if ms_same_month else None
            dod_bql_ms[L["c_date"].day].append(conv)
        if L["ms_date"] is not None and month_str(L["ms_date"]) == cur_month:
            conv = L["md_date"].day if (L["md_date"] and month_str(L["md_date"]) == cur_month) else None
            dod_ms_md[L["ms_date"].day].append(conv)
        if L["md_date"] is not None and month_str(L["md_date"]) == cur_month:
            conv = L["oc_date"].day if (L["oc_date"] and month_str(L["oc_date"]) == cur_month) else None
            dod_md_order[L["md_date"].day].append(conv)

    def dod_rows(entries_by_day, days_in_month):
        rows = []
        all_pairs = []   # (start_day, conv_day|None) across every day — for the ALL summary row
        for day in range(1, days_in_month + 1):
            convs = entries_by_day.get(day, [])
            all_pairs.extend((day, c) for c in convs)
            count_in = len(convs)
            converted = [c for c in convs if c is not None]
            n_conv = len(converted)
            cells = defaultdict(int)
            for c in converted: cells[c] += 1
            rows.append({
                "day": day,
                "count_in": count_in,
                "conv_total_pct": round(n_conv / count_in * 100, 1) if count_in else 0,
                "d1_3_pct": round(sum(1 for c in converted if c - day <= 2) / count_in * 100, 1) if count_in else 0,
                "d1_7_pct": round(sum(1 for c in converted if c - day <= 6) / count_in * 100, 1) if count_in else 0,
                "cells": {str(k): round(v / count_in * 100, 1) for k, v in cells.items()} if count_in else {},
            })

        # ALL row: totals across every start-day combined. count_in/conv_total/
        # D1-3/D1-7 are straightforward sums; "cells" here is different from a
        # normal row's cells — it's the % of the TOTAL entrant pool that
        # converted on each ABSOLUTE end-day (a marginal distribution across
        # the whole month), not a per-row relative offset.
        total_count_in = len(all_pairs)
        total_converted = [(d, c) for d, c in all_pairs if c is not None]
        all_cells = defaultdict(int)
        for _, c in total_converted: all_cells[c] += 1
        all_row = {
            "day": "ALL",
            "count_in": total_count_in,
            "conv_total_pct": round(len(total_converted) / total_count_in * 100, 1) if total_count_in else 0,
            "d1_3_pct": round(sum(1 for d, c in total_converted if c - d <= 2) / total_count_in * 100, 1) if total_count_in else 0,
            "d1_7_pct": round(sum(1 for d, c in total_converted if c - d <= 6) / total_count_in * 100, 1) if total_count_in else 0,
            "cells": {str(k): round(v / total_count_in * 100, 1) for k, v in all_cells.items()} if total_count_in else {},
        }
        return {"rows": rows, "all": all_row}

    days_in_cur_month = calendar.monthrange(today.year, today.month)[1]

    def serialize_month_buckets(d):
        return sorted([{"month": k, **v} for k, v in d.items()], key=lambda x: x["month"])

    def serialize_city_buckets(d):
        return sorted([{"month": k[0], "cluster": k[1], **v} for k, v in d.items()], key=lambda x: (x["month"], x["cluster"]))

    return {
        "meta": {
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "current_month": cur_month,
            "cutoff_day": cutoff_day,
            "note": (
                "BQL = Lead Created (relabelled, no separate qualifying condition yet). "
                "Effort = event-date based, RAW dates, no backfill; %s are pace ratios, "
                "not true conversion (numerator/denominator are different lead sets). "
                "Cohort(MO) = same-month-bounded (lead created in month X counts toward "
                "month X's MS/MD/Order only if that milestone ALSO falls in month X), WITH "
                "backfill (see BACKFILL rule in etl.py). MTD tables truncate every month "
                "(including past ones) to day-of-month <= today's day-of-month. "
                "HOTO, Tier, Sub-Channel not implemented — deferred. Referral channel only."
            ),
        },
        "effort": {
            "full_month":      serialize_month_buckets(effort_month),
            "mtd":             serialize_month_buckets(effort_mtd_month),
            "city_full_month": serialize_city_buckets(effort_month_city),
            "city_mtd":        serialize_city_buckets(effort_mtd_month_city),
        },
        "cohort": {
            "full_month":      serialize_month_buckets(cohort_month),
            "mtd":             serialize_month_buckets(cohort_mtd_month),
            "city_full_month": serialize_city_buckets(cohort_month_city),
            "city_mtd":        serialize_city_buckets(cohort_mtd_month_city),
            "dod_matrix": {
                "bql_to_ms":   dod_rows(dod_bql_ms, days_in_cur_month),
                "ms_to_md":    dod_rows(dod_ms_md, days_in_cur_month),
                "md_to_order": dod_rows(dod_md_order, days_in_cur_month),
            },
        },
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
    Filter-only cols: call_attempts, has_ms, has_md, has_oc, is_overdue
    Date cols (raw, for Control Tower drill-through & date-basis filtering):
      ms_date, md_date, oc_date — IST date strings, None if not reached.
      has_ms/has_md/has_oc are RAW (no backfill) — Control Tower's backfill
      rule (see build_control_tower) is applied CLIENT-SIDE when filtering
      Lead Data from a Cohort drill (ct_ms/ct_md/ct_oc in index.html) — if
      that rule changes, update both places.

    Standards: cluster via normalise_cluster(); dates via ist_date_str() (IST).
    """
    today = today_ist()
    records = []
    for r in leads_raw:
        _id     = (r.get("_id") or "").strip() or None
        lead_id = (r.get("Lead Id") or "").strip() or None
        cluster = normalise_cluster(r.get("Cluster") or "")

        creation_dt = parse_dt(r.get("Creation Date"))
        updated_dt  = parse_dt(r.get("Updated At"))
        ms_dt       = parse_dt(r.get("Meeting Schedule Date"))
        md_dt       = parse_dt(r.get("Meeting Done Date"))
        oc_dt       = parse_dt(r.get(ORDER_BOOKED_DATE_FIELD))

        _rs         = parse_dt(r.get("Reshedule Date"))
        rs_date     = (_rs + _IST).date() if _rs else None
        is_overdue  = (rs_date < today) if rs_date else False
        is_scheduled_today = (rs_date == today) if rs_date else False
        is_updated_today   = ((updated_dt + _IST).date() == today) if updated_dt else False

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
            "has_oc":        oc_dt is not None,
            "ms_date":       ist_date_str(ms_dt) if ms_dt else None,
            "md_date":       ist_date_str(md_dt) if md_dt else None,
            "oc_date":       ist_date_str(oc_dt) if oc_dt else None,
            "is_overdue":    is_overdue,
            "is_scheduled_today": is_scheduled_today,
            "is_updated_today":   is_updated_today,
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

    print("[1/8] Authenticating with Metabase...")
    token = get_session_token()

    print("[2/8] Fetching lead data (card 2557)...")
    leads_raw = fetch_card(token, CARD_ID)
    print(f"      {len(leads_raw):,} rows")

    print("[3/8] Fetching audit log (card 3227)...")
    audit_raw = fetch_card(token, AUDIT_CARD_ID)
    print(f"      {len(audit_raw):,} rows")

    # Build lead_id → {creation_dt, cluster, lrm} from card 2557.
    # creation_dt: UTC naive datetime for TAT arithmetic.
    # cluster/lrm: card 2557 is authoritative for all lead-level attribution.
    # resolve_cluster() uses this dict to override audit cluster when card 2557 is valid.
    #
    # ⚠ ORDER_BOOKED_DATE_FIELD — column name NOT independently confirmed against
    # Metabase card 2557 (module-level constant, near VALID_CLUSTERS). If
    # tat_to_won / Control Tower come back wrong, check that constant first.
    lead_meta   = {}
    lead_ms_dates = {}    # lead_id → Meeting Schedule Date UTC naive datetime (CURRENT value, see caveat in build_tat_stats)
    lead_won_dates = {}   # lead_id → Order Booked Date UTC naive datetime (primary source for tat_to_won)
    for r in leads_raw:
        lid = r.get("Lead Id")
        if not lid: continue
        cd_dt  = parse_dt(r.get("Creation Date"))
        ms_dt  = parse_dt(r.get("Meeting Schedule Date"))
        won_dt = parse_dt(r.get(ORDER_BOOKED_DATE_FIELD))
        if cd_dt:
            lead_meta[lid] = {
                "creation_dt": cd_dt,
                "cluster":     normalise_cluster(r.get("Cluster") or ""),
                "lrm":         (r.get("LRM Email") or "").strip(),
            }
        if ms_dt:
            lead_ms_dates[lid] = ms_dt
        if won_dt:
            lead_won_dates[lid] = won_dt

    print("[4/8] Aggregating lead snapshot...")
    city_stage = build_city_stage_output(aggregate_city_stage(leads_raw))
    with open("data/city_stage.json", "w") as f:
        json.dump(city_stage, f, indent=2, default=str)
    print(f"      city_stage.json — {city_stage['meta']['total_leads']:,} leads")

    call_attempts = build_call_attempts_output(leads_raw)
    with open("data/call_attempts.json", "w") as f:
        json.dump(call_attempts, f, indent=2, default=str)
    print(f"      call_attempts.json — {len(call_attempts['records']):,} buckets")

    print("[5/8] Normalising audit log...")
    audit_sorted = normalise_audit_rows(audit_raw)
    print(f"      {len(audit_sorted):,} clean audit events")

    print("[6/8] Building daily movement + EOD position...")
    dm = build_daily_movement(audit_sorted, lead_meta)
    with open("data/daily_movement.json", "w") as f:
        json.dump(dm, f, indent=2, default=str)
    print(f"      daily_movement.json — {dm['meta']['total_stage_transitions']:,} stage, "
          f"{dm['meta']['total_status_transitions']:,} status transitions, "
          f"{dm['meta']['total_touches']:,} touches")

    eod = build_eod_position(audit_sorted, lead_meta)
    with open("data/eod_position.json", "w") as f:
        json.dump(eod, f, indent=2, default=str)
    print(f"      eod_position.json — {eod['meta']['total_records']:,} rows (from→to format)")

    eod_leads = build_eod_leads(audit_sorted, lead_meta)
    with open("data/eod_leads.json", "w") as f:
        json.dump(eod_leads, f, separators=(',', ':'), default=str)   # compact — no indent
    print(f"      eod_leads.json — {eod_leads['meta']['total_records']:,} lead-day records")

    print("[7/8] Building LRM performance + TAT...")
    lrm = build_lrm_performance(audit_sorted, lead_meta)
    with open("data/lrm_performance.json", "w") as f:
        json.dump(lrm, f, indent=2, default=str)
    print(f"      lrm_performance.json — {lrm['meta']['lrm_count']} LRMs · {len(lrm['records']):,} rows")

    tat = build_tat_stats(audit_sorted, lead_meta, lead_ms_dates, lead_won_dates)
    with open("data/tat_stats.json", "w") as f:
        json.dump(tat, f, indent=2, default=str)
    print(f"      tat_stats.json — {tat['meta']['total_leads']:,} lead-level TAT records")

    conv = build_lrm_conversion(audit_sorted, lead_meta)
    with open("data/lrm_conversion.json", "w") as f:
        json.dump(conv, f, indent=2, default=str)
    print(f"      lrm_conversion.json — {conv['meta']['total_records']:,} lead-LRM records")

    snap = build_lrm_snapshot(leads_raw, audit_sorted, lead_meta)
    with open("data/lrm_snapshot.json", "w") as f:
        json.dump(snap, f, separators=(',', ':'), default=str)   # compact — no indent, ~40% smaller
    print(f"      lrm_snapshot.json — {snap['meta']['total_records']:,} (lrm × cluster × date) rows")

    leads_full = build_leads_full(leads_raw)
    with open("data/leads_full.json", "w") as f:
        json.dump(leads_full, f, separators=(',', ':'), default=str)
    print(f"      leads_full.json — {leads_full['meta']['total_leads']:,} leads")

    print("[8/8] Building Control Tower (BQL→MS→MD→Order)...")
    ct = build_control_tower(leads_raw)
    with open("data/control_tower.json", "w") as f:
        json.dump(ct, f, indent=2, default=str)
    print(f"      control_tower.json — current month {ct['meta']['current_month']}, "
          f"cutoff day {ct['meta']['cutoff_day']}")

    print("\n  All done.")


if __name__ == "__main__":
    main()