#!/usr/bin/env python3
"""DEPTH01 collector - the DSE public top-10 order book (market depth), snapshotted through one trading session.

What it does (one GitHub Actions job per day, Sun-Thu, started a little before 10:00 Dhaka):
  1. Exits at once on Friday/Saturday and on a DSE holiday (www.dse.com.bd/holidays). The session hours come only
     from DSE's own session block (api/live/depth session.today opening/closing) - none are built in; DSE not
     giving them stops the day. It exits when DSE says it is not a trading day or the job starts after DSE's close.
  2. Builds the day's universe: equities (assetType EQ) that traded the previous session, from api/live/prices
     (or, when that list is already reset to 0 trades or the job started after the open, from
     api/live/data-archive/day-end), MINUS the going-concern list (www.dse.com.bd/going-concern) MINUS the
     closed-companies list (www.dse.com.bd/companies/closed).
  3. Waits for the open, then walks the universe again and again until the close: one depth request at a time,
     at most one request every 2 s, honest User-Agent. Each snapshot is appended to a CSV as it arrives.
  4. `finalize` (run by the workflow even when the job fails) gzips the CSV and writes a manifest (rows, sha256,
     why it stopped) - the workflow uploads both as one Actions artifact `depth01-YYYY-MM-DD` (90-day retention).

Stored per snapshot (nothing else): session_date, fetch_utc, as_of, phase, trading_code, side, level, price,
quantity, orders. A book with no levels at all is one row with side "none" and level 0, so "empty" is recorded,
not lost. priceStats and minute bars are NOT stored.

Failures: 429/403/5xx/timeouts/bad JSON back off 2 s doubling to 60 s; a request that still fails stops the day
(exit 1, the reason in the manifest). A code DSE answers 404 for is skipped for the rest of the day. No browser
impersonation, no TLS tricks, no tight retry loops.

Standard library only (Python 3.9+). Usage:
    python3 collector.py run --out out                 the daily job
    python3 collector.py run --out out --codes SPCL,GP one pass over those codes, no session gate (test run)
    python3 collector.py universe                      build and print the universe only (no depth calls)
    python3 collector.py finalize --out out            gzip + manifest (prints name=... for the workflow)
"""
from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import html.parser
import io
import http.client
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta, timezone

VERSION = "depth01_collector_v1"
DHAKA = timezone(timedelta(hours=6))
WWW = "https://www.dse.com.bd"
DEPTH_HOST = "https://dse.com.bd"            # the host api/live/depth was measured on (2026-11-09)
DEPTH_PATH = "/api/live/depth?code={code}"
PRICES_PATH = "/api/live/prices"
DAY_END_PATH = "/api/live/data-archive/day-end"
GOING_CONCERN_PATH = "/going-concern"
CLOSED_PATH = "/companies/closed"
HOLIDAYS_PATH = "/holidays"

MIN_GAP_S = 2.0                              # one request every 2 s, one at a time
BACKOFF_S = (2, 4, 8, 16, 32, 60)            # 2 s doubling to 60 s; still failing after these -> stop the day
TIMEOUT_S = 20
NEVER_OPEN_GRACE_S = 30 * 60                 # DSE never reports the session open this long after the open -> stop
CLOSED_CONFIRM_S = 120                       # isOpen False must hold this long after the open was seen -> stop
CSV_COLS = ["session_date", "fetch_utc", "as_of", "phase", "trading_code", "side", "level", "price", "quantity",
            "orders"]
WEEKEND = (4, 5)                             # Friday, Saturday (date.weekday())


class StopDay(RuntimeError):
    """A request kept failing after the full back-off, or a source could not be read: stop for the day."""


class NotFound(RuntimeError):
    """HTTP 404 for one request (a code DSE does not know today)."""


def user_agent() -> str:
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    where = f"https://github.com/{repo}" if repo else "a personal research job"
    return f"DEPTH01-collector/1.0 (+{where}; public market depth; 1 request per 2 s)"


def urllib_fetch(url: str, headers: dict, timeout: float):
    """(status, body). Network errors (timeout, DNS, reset, a body cut short) raise OSError."""
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:              # the error body is never used: not read (it can be cut short)
        e.close()
        return e.code, b""
    except http.client.HTTPException as e:       # IncompleteRead, BadStatusLine: back off like any network fault
        raise OSError(f"{type(e).__name__}: {e}") from e


class Http:
    """One request at a time, >= MIN_GAP_S between request starts, back-off on failure. Clock and sleep injectable."""

    def __init__(self, fetch=urllib_fetch, clock=time.monotonic, sleep=time.sleep, log=print):
        self.fetch, self.clock, self.sleep, self.log = fetch, clock, sleep, log
        self.last = None
        self.requests = 0
        self.failures = 0

    def _wait_gap(self, extra: float = 0.0) -> None:
        if self.last is not None:
            due = self.last + max(MIN_GAP_S, extra)
            now = self.clock()
            if due > now:
                self.sleep(due - now)

    def get(self, url: str, accept: str = "application/json", parse=None):
        headers = {"User-Agent": user_agent(), "Accept": accept}
        wait = 0.0
        for attempt in range(len(BACKOFF_S) + 1):
            self._wait_gap(wait)
            self.last = self.clock()
            self.requests += 1
            try:
                status, body = self.fetch(url, headers, TIMEOUT_S)
                if status == 404:
                    raise NotFound(url)
                if status != 200:
                    raise OSError(f"HTTP {status}")
                return parse(body) if parse else body
            except NotFound:
                raise
            except (OSError, ValueError) as e:      # HTTP 429/403/5xx/other, timeout, network, bad JSON/HTML
                self.failures += 1
                if attempt == len(BACKOFF_S):
                    raise StopDay(f"{url}: still failing after {attempt + 1} tries ({e})") from e
                wait = BACKOFF_S[attempt]
                self.log(f"  {url}: {e}; waiting {wait} s")
        raise StopDay(url)   # not reached

    def json(self, url: str):
        return self.get(url, "application/json", lambda b: json.loads(b.decode("utf-8")))

    def text(self, url: str) -> str:
        return self.get(url, "text/html", lambda b: b.decode("utf-8", "replace"))


# ------------------------------------------------------------------ HTML (stdlib parser)
class _Tables(html.parser.HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.tables, self._stack, self._row, self._cell = [], [], None, None

    def handle_starttag(self, tag, attrs):
        if tag == "table":
            self._stack.append([])
        elif tag == "tr" and self._stack:
            self._row = []
        elif tag in ("td", "th") and self._row is not None:
            self._cell = []

    def handle_endtag(self, tag):
        if tag in ("td", "th") and self._cell is not None and self._row is not None:
            self._row.append(re.sub(r"\s+", " ", "".join(self._cell)).strip())
            self._cell = None
        elif tag == "tr" and self._row is not None and self._stack:
            if self._row:
                self._stack[-1].append(self._row)
            self._row = None
        elif tag == "table" and self._stack:
            self.tables.append(self._stack.pop())

    def handle_data(self, data):
        if self._cell is not None:
            self._cell.append(data.replace("\xa0", " "))


def tables(html_text: str) -> list:
    p = _Tables()
    p.feed(html_text)
    p.close()
    return p.tables


def codes_from_list_page(html_text: str, what: str) -> set:
    """The going-concern / closed-companies page: the largest table, trading code in column 2 (header skipped)."""
    tabs = [t for t in tables(html_text) if len(t) >= 2]
    if not tabs:
        raise StopDay(f"{what}: no table on the page")
    codes = set()
    for row in max(tabs, key=len)[1:]:              # the list itself, not a smaller layout table before it
        if len(row) >= 2 and row[1] and not re.search(r"trading\s*code", row[1], re.I):
            codes.add(row[1].strip().upper())
    if not codes:
        raise StopDay(f"{what}: table has no trading codes in column 2")
    return codes


_MONTHS = {m: i for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july", "august", "september", "october",
     "november", "december"], 1)}
_MONTHS.update({k[:3]: v for k, v in list(_MONTHS.items())})
_MONTHS["sept"] = 9


def _day_month(part: str, default_month=None):
    t = part.replace(",", " ").strip()
    m = re.search(r"\b(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]+)", t)
    if m and m.group(2).lower() in _MONTHS:
        return int(m.group(1)), _MONTHS[m.group(2).lower()]
    m = re.search(r"\b(\d{1,2})(?:st|nd|rd|th)?\b", t)
    if m and default_month:
        return int(m.group(1)), default_month
    raise ValueError(f"cannot read a date from {part!r}")


def expand_date_text(text: str, year: int) -> list:
    """'25 December' -> [d]; '10 - 12 March' / '30 Dec - 01 Jan' -> every day of the range."""
    t = re.sub(r"\s+to\s+", " - ", text.replace("–", "-").replace("—", "-"), flags=re.I)
    parts = [p.strip() for p in t.split("-") if p.strip()]
    if not parts:
        return []
    if len(parts) == 1:
        d, m = _day_month(parts[0])
        return [date(year, m, d)]
    ed, em = _day_month(parts[-1])
    sd, sm = _day_month(parts[0], em)
    start, end = date(year, sm, sd), date(year + (1 if em < sm else 0), em, ed)
    if end < start:
        raise ValueError(f"range ends before it starts: {text!r}")
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def holidays_from_page(html_text: str, default_year: int) -> set:
    """Closed dates from the 'Name of Holidays / Date / Day(s) / No. of Days' table."""
    plain = re.sub(r"<[^>]+>", " ", html_text)
    m = re.search(r"Calendar\s+Year\s*[:\-]?\s*(20\d{2})", plain, re.I) or re.search(r"\bYear\s+(20\d{2})", plain)
    year = int(m.group(1)) if m else default_year
    for tab in tables(html_text):
        if not tab:                                  # an empty layout/wrapper table
            continue
        head = [c.lower() for c in tab[0]]
        name_i = next((i for i, c in enumerate(head) if "holiday" in c), None)
        date_i = next((i for i, c in enumerate(head) if c.strip() == "date"), None)
        if name_i is None or date_i is None:
            continue
        out = set()
        for row in tab[1:]:
            if len(row) > date_i and row[date_i]:
                out.update(expand_date_text(row[date_i], year))
        if out:
            return out
    raise ValueError("holiday table not found")


# ------------------------------------------------------------------ universe
_CODE_KEYS = ("code", "tradingCode", "trading_code", "symbol", "scrip")
_TRADE_KEYS = ("trades", "trade", "totalTrades", "noOfTrades", "numberOfTrades", "no_of_trades")
_ASSET_KEYS = ("assetType", "asset_type")


def rows_as_dicts(payload) -> list:
    """{cols, rows:[[...]]} or {rows:[{...}]} or [{...}] or {<anything>: [{...}]} -> list of dicts."""
    if isinstance(payload, dict):
        cols, rows = payload.get("cols"), payload.get("rows")
        if isinstance(rows, list):
            if cols and rows and isinstance(rows[0], list):
                return [dict(zip(cols, r)) for r in rows]
            return [r for r in rows if isinstance(r, dict)]
        for v in payload.values():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                return v
        return []
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    return []


def _pick(row: dict, keys):
    for k in keys:
        if k in row and row[k] not in (None, ""):
            return row[k]
    return None


def _num(v) -> float:
    try:
        return float(str(v).replace(",", ""))
    except (TypeError, ValueError):
        return 0.0


def traded_codes(rows: list, eq_only: bool) -> set:
    out = set()
    for r in rows:
        code = _pick(r, _CODE_KEYS)
        if not code:
            continue
        if eq_only and str(_pick(r, _ASSET_KEYS) or "").upper() != "EQ":
            continue
        if _num(_pick(r, _TRADE_KEYS)) > 0:
            out.add(str(code).strip().upper())
    return out


def eq_codes(rows: list) -> set:
    return {str(_pick(r, _CODE_KEYS)).strip().upper() for r in rows
            if _pick(r, _CODE_KEYS) and str(_pick(r, _ASSET_KEYS) or "").upper() == "EQ"}


def build_universe(prices, day_end_fetch, going_concern: set, closed: set, after_open: bool):
    """(sorted codes, source label). day_end_fetch() is called only when needed."""
    rows = rows_as_dicts(prices)
    eq = eq_codes(rows)
    if not eq:
        raise StopDay("api/live/prices: no assetType EQ rows")
    traded = traded_codes(rows, eq_only=True)
    source = "prices"
    if after_open or not traded:      # prices already show today (late start) or were reset -> previous session
        de_rows = rows_as_dicts(day_end_fetch())
        traded = traded_codes(de_rows, eq_only=False) & eq
        source = "day-end"
        if not traded:
            raise StopDay("api/live/data-archive/day-end: no traded EQ codes for the previous session")
    uni = sorted(traded - going_concern - closed)
    if not uni:
        raise StopDay("universe is empty after the going-concern / closed filters")
    return uni, source


# ------------------------------------------------------------------ depth
def ladder_rows(payload: dict, code: str, session_date: str, fetch_utc: str):
    """(rows, bad_levels). rows follow CSV_COLS. An empty book -> one 'none' row."""
    sess = payload.get("session")
    phase = str((sess if isinstance(sess, dict) else {}).get("phase") or "")
    base = [session_date, fetch_utc, str(payload.get("asOf") or ""), phase, code]
    out, bad = [], 0
    for side, key in (("bid", "bids"), ("ask", "asks")):
        levels = payload.get(key) or []
        level = 0
        for lv in levels if isinstance(levels, list) else []:
            try:
                price, qty = float(str(lv["price"]).replace(",", "")), int(_num(lv["quantity"]))
                orders = lv.get("orders")
                orders = "" if orders in (None, "") else int(_num(orders))
            except (KeyError, TypeError, ValueError, AttributeError):
                bad += 1
                continue
            level += 1
            out.append(base + [side, level, f"{price:.2f}", qty, orders])
    if not out:
        out.append(base + ["none", 0, "", "", ""])
    return out, bad


def _hhmm(v):
    m = re.search(r"(\d{1,2}):(\d{2})\s*([AaPp][Mm])?", str(v or ""))
    if not m:
        return None
    h, mi = int(m.group(1)), int(m.group(2))
    if m.group(3):
        h = h % 12 + (12 if m.group(3).lower() == "pm" else 0)
    return h, mi


def session_hours(session: dict):
    """(open, close) as (h, m) from DSE's session block (session.today, else session.opening/closing).
    No built-in hours: DSE not giving them stops the day."""
    today = session.get("today") if isinstance(session.get("today"), dict) else {}
    op = _hhmm(today.get("opening")) or _hhmm(session.get("opening"))
    cl = _hhmm(today.get("closing")) or _hhmm(session.get("closing"))
    if not (op and cl) or cl <= op:
        raise StopDay(f"DSE session block gives no usable opening/closing time: {json.dumps(session)[:200]}")
    return op, cl


# ------------------------------------------------------------------ the run
class Collector:
    def __init__(self, out_dir: str, http: Http, now=None, sleep=time.sleep, log=print):
        self.out, self.http, self.sleep, self.log = out_dir, http, sleep, log
        self.now = now or (lambda: datetime.now(timezone.utc))
        self.status = {}

    def dhaka(self) -> datetime:
        return self.now().astimezone(DHAKA)

    def _write_status(self, **kw) -> None:
        self.status.update(kw)
        os.makedirs(self.out, exist_ok=True)
        path = os.path.join(self.out, "status.json")
        with open(path + ".tmp", "w", encoding="utf-8") as f:     # a job killed mid-write keeps the old file
            json.dump(self.status, f, indent=1)
        os.replace(path + ".tmp", path)

    def _csv_path(self, day: str, test: bool) -> str:
        return os.path.join(self.out, f"depth01{'test' if test else ''}-{day}.csv")

    def _sleep_until(self, target: datetime) -> None:
        while True:
            left = (target - self.dhaka()).total_seconds()
            if left <= 0:
                return
            self.sleep(min(left, 300))

    def snapshot(self, code: str, writer, day: str) -> int:
        payload = self.http.json(DEPTH_HOST + DEPTH_PATH.format(code=code))
        fetch_utc = self.now().astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        if not isinstance(payload, dict) or str(payload.get("code", code)).upper() != code:
            self.status["mismatched"] = self.status.get("mismatched", 0) + 1
            return 0
        rows, bad = ladder_rows(payload, code, day, fetch_utc)
        if bad:
            self.status["bad_levels"] = self.status.get("bad_levels", 0) + bad
        for r in rows:
            writer.writerow(r)
        sess = payload.get("session")
        self.last_session = sess if isinstance(sess, dict) else {}
        return len(rows)

    def run(self, codes=None) -> int:
        start = self.dhaka()
        day = start.date().isoformat()
        self._write_status(version=VERSION, day=day, started_utc=self.now().astimezone(timezone.utc).isoformat(),
                           state="running", test=bool(codes), stop_reason="", passes=0, rows=0)
        try:
            return self._run(codes, start, day)
        except (StopDay, NotFound) as e:            # NotFound here = a source page/API gone (404)
            self.log(f"STOPPED for the day: {type(e).__name__}: {e}")
            self._write_status(state="stopped", stop_reason=f"{type(e).__name__}: {e}")
            return 1
        except Exception as e:  # noqa: BLE001  a fault not foreseen: keep what was collected + why, exit 1
            self.log(f"CRASHED: {type(e).__name__}: {e}")
            self._write_status(state="crashed", stop_reason=f"{type(e).__name__}: {e}")
            return 1
        finally:
            self._write_status(requests=self.http.requests, failures=self.http.failures,
                               ended_utc=self.now().astimezone(timezone.utc).isoformat())

    def _exit(self, why: str) -> int:
        self.log(f"no collection today: {why}")
        self._write_status(state="skipped", stop_reason=why)
        return 0

    def _run(self, codes, start: datetime, day: str) -> int:
        if codes:                                   # test run: one pass, no gate
            return self._collect(codes, day, test=True, close_at=None, open_at=None)
        if start.weekday() in WEEKEND:
            return self._exit("Friday/Saturday")
        try:
            hol = holidays_from_page(self.http.text(WWW + HOLIDAYS_PATH), start.year)
            if start.date() in hol:
                return self._exit("DSE holiday (dse.com.bd/holidays)")
        except ValueError as e:                     # page read but not parsable: the session JSON still gates
            self.log(f"WARNING holidays page not parsed ({e}); relying on the session JSON")
        prices = self.http.json(WWW + PRICES_PATH)
        session = None
        for code in sorted(eq_codes(rows_as_dicts(prices))):   # DSE's session block, from the first EQ code answered
            try:
                ans = self.http.json(DEPTH_HOST + DEPTH_PATH.format(code=code))
                session = ans.get("session") if isinstance(ans, dict) else None
                session = session if isinstance(session, dict) else {}   # no usable block -> session_hours stops
                break
            except NotFound:
                continue
        if session is None:
            raise StopDay("api/live/depth answered 404 for every EQ code")
        if session.get("tradingDay") is False:
            return self._exit("DSE session JSON: not a trading day")
        op, cl = session_hours(session)             # hours only from DSE, never built in
        now = self.dhaka()
        open_at = now.replace(hour=op[0], minute=op[1], second=0, microsecond=0)
        close_at = now.replace(hour=cl[0], minute=cl[1], second=0, microsecond=0)
        self._write_status(open=f"{op[0]:02d}:{op[1]:02d}", close=f"{cl[0]:02d}:{cl[1]:02d}")
        if now >= close_at:
            return self._exit(f"after DSE's close ({cl[0]:02d}:{cl[1]:02d})")
        going = codes_from_list_page(self.http.text(WWW + GOING_CONCERN_PATH), "going-concern")
        closed = codes_from_list_page(self.http.text(WWW + CLOSED_PATH), "companies/closed")
        uni, source = build_universe(prices, lambda: self.http.json(WWW + DAY_END_PATH), going, closed,
                                     after_open=self.dhaka() >= open_at)
        self.log(f"universe {len(uni)} codes (source {source}; minus {len(going)} going-concern, "
                 f"{len(closed)} closed); DSE hours {op[0]:02d}:{op[1]:02d}-{cl[0]:02d}:{cl[1]:02d}")
        self._write_status(universe_size=len(uni), universe_source=source)
        self._sleep_until(open_at)
        return self._collect(uni, day, test=False, close_at=close_at, open_at=open_at)

    def _collect(self, codes, day, test, close_at, open_at) -> int:
        path = self._csv_path(day, test)
        new = not os.path.exists(path)
        skip, seen_open, total, closed_since = set(), False, 0, None
        self.last_session = {}
        with open(path, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if new:
                w.writerow(CSV_COLS)
            passes = 0
            while True:
                passes += 1
                for code in codes:
                    if code in skip:
                        continue
                    if close_at is not None and self.dhaka() >= close_at:
                        return self._done("closing time", passes - 1, total)   # full passes
                    try:
                        total += self.snapshot(code, w, day)
                    except NotFound:
                        skip.add(code)
                        self.log(f"  {code}: 404, skipped for the day")
                        continue
                    f.flush()
                    is_open = self.last_session.get("isOpen")
                    if is_open is True:
                        seen_open, closed_since = True, None
                    elif is_open is False and seen_open and not test:
                        closed_since = closed_since or self.dhaka()        # one stray answer does not end the day
                        if (self.dhaka() - closed_since).total_seconds() >= CLOSED_CONFIRM_S:
                            return self._done("DSE reports the session closed", passes, total)
                    if (not test and not seen_open and open_at is not None
                            and (self.dhaka() - open_at).total_seconds() > NEVER_OPEN_GRACE_S):
                        return self._done("DSE never reported the session open", passes, total)
                self._write_status(passes=passes, rows=total)
                if test or len(skip) == len(codes):
                    return self._done("test pass done" if test else "every code 404", passes, total)

    def _done(self, why, passes, total) -> int:
        self.log(f"done: {why}; {passes} pass(es), {total} rows")
        self._write_status(state="done", stop_reason=why, passes=passes, rows=total)
        return 0


# ------------------------------------------------------------------ finalize (always run by the workflow)
def finalize(out_dir: str) -> str:
    """Gzip the day's CSV (complete rows only) + manifest into out/<name>/. Returns the artifact name or ''."""
    if not os.path.isdir(out_dir):
        return ""
    csvs = sorted(f for f in os.listdir(out_dir) if f.endswith(".csv") and f.startswith("depth01"))
    if not csvs:
        return ""
    src = os.path.join(out_dir, csvs[-1])
    name = csvs[-1][:-4]
    st = {}
    sp = os.path.join(out_dir, "status.json")
    if os.path.exists(sp):
        try:
            with open(sp, encoding="utf-8") as f:
                st = json.load(f)
        except (OSError, ValueError) as e:           # unreadable status: the rows still ship, marked incomplete
            st = {"state": "unknown", "stop_reason": f"status.json not readable ({type(e).__name__})"}
    dest_dir = os.path.join(out_dir, name)
    os.makedirs(dest_dir, exist_ok=True)
    gz = os.path.join(dest_dir, name + ".csv.gz")
    rows, dropped = 0, 0
    with open(src, "rb") as f:
        raw = f.read().decode("utf-8", "replace")
    if raw and not raw.endswith("\n"):               # the job was killed mid-line: the last line is not whole
        raw, dropped = raw[:raw.rfind("\n") + 1], 1
    with gzip.open(gz, "wt", newline="", encoding="utf-8") as g:
        w = csv.writer(g)
        rd = csv.reader(io.StringIO(raw, newline=""))
        header = next(rd, None)
        if header is not None and header != CSV_COLS:
            raise SystemExit(f"{src}: unexpected header {header}")
        w.writerow(CSV_COLS)                         # an empty file (killed before the first flush) ships 0 rows
        for r in rd:
            if len(r) == len(CSV_COLS) and r[0] and r[1]:
                w.writerow(r)
                rows += 1
            else:
                dropped += 1                         # a malformed line
    with open(gz, "rb") as f:
        sha = hashlib.sha256(f.read()).hexdigest()
    manifest = {"name": name, "day": name.split("-", 1)[1], "file": name + ".csv.gz", "rows": rows, "sha256": sha,
                "complete": st.get("state") == "done", "dropped_lines": dropped,
                **{k: st.get(k) for k in ("version", "state", "stop_reason", "test", "universe_size",
                                          "universe_source", "passes", "requests", "failures", "mismatched",
                                          "bad_levels", "open", "close", "started_utc", "ended_utc")}}
    with open(os.path.join(dest_dir, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=1)
    return name


def universe_check(http: Http, now: datetime, log=print) -> int:
    """Manual check: both universe paths, list sizes, the first rows - no depth calls, nothing saved."""
    prices = http.json(WWW + PRICES_PATH)
    rows = rows_as_dicts(prices)
    sess = prices.get("session") if isinstance(prices, dict) else None
    log(f"prices: {len(rows)} rows; keys {sorted(rows[0]) if rows else []}; session {json.dumps(sess)[:300]}")
    log(f"prices: EQ {len(eq_codes(rows))}, EQ with trades>0 {len(traded_codes(rows, True))}")
    de = http.json(WWW + DAY_END_PATH)
    de_rows = rows_as_dicts(de)
    log(f"day-end: top-level {type(de).__name__} {list(de)[:12] if isinstance(de, dict) else len(de)}; "
        f"{len(de_rows)} rows; first {json.dumps(de_rows[:2])[:600]}")
    log(f"day-end: codes with trades>0 {len(traded_codes(de_rows, False))}")
    going = codes_from_list_page(http.text(WWW + GOING_CONCERN_PATH), "going-concern")
    closed = codes_from_list_page(http.text(WWW + CLOSED_PATH), "companies/closed")
    try:
        hol = holidays_from_page(http.text(WWW + HOLIDAYS_PATH), now.year)
        log(f"holidays: {len(hol)} closed dates, next {sorted(d for d in hol if d >= now.date())[:3]}")
    except ValueError as e:
        log(f"holidays: NOT parsed ({e})")
    for after_open in (False, True):
        try:
            uni, src = build_universe(prices, lambda: de, going, closed, after_open)
            log(f"universe ({'after' if after_open else 'before'} the open): {len(uni)} codes from {src}; "
                f"first {uni[:10]}")
        except StopDay as e:
            log(f"universe ({'after' if after_open else 'before'} the open): FAILED {e}")
    log(f"going-concern {len(going)}, closed {len(closed)}; requests {http.requests}, failures {http.failures}")
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("cmd", choices=("run", "universe", "finalize"), nargs="?", default="run")
    ap.add_argument("--out", default="out")
    ap.add_argument("--codes", default="", help="comma list: one pass over these codes, no session gate")
    a = ap.parse_args(argv)
    if a.cmd == "finalize":
        name = finalize(a.out)
        print(f"name={name}")
        gho = os.environ.get("GITHUB_OUTPUT")
        if gho:
            with open(gho, "a", encoding="utf-8") as f:
                f.write(f"name={name}\n")
        return 0
    http = Http()
    if a.cmd == "universe":
        try:
            return universe_check(http, datetime.now(timezone.utc).astimezone(DHAKA))
        except (StopDay, NotFound) as e:
            print(f"STOPPED: {type(e).__name__}: {e}")
            return 1
    codes = [c.strip().upper() for c in a.codes.split(",") if c.strip()]
    return Collector(a.out, http).run(codes or None)


if __name__ == "__main__":
    raise SystemExit(main())
