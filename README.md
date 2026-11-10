# depth01

Collects the Dhaka Stock Exchange's public top-10 order book (market depth) through each trading session and
keeps each day as a GitHub Actions artifact. Personal research data. No trading code here.

## What runs
`.github/workflows/depth01.yml` runs `collector.py` once a day, Sunday to Thursday, starting at 09:11 Dhaka (03:11 UTC)
with backup starts every 30 min to 11:41 Dhaka, because GitHub can start a scheduled run hours late. The runs go one
at a time; a backup that starts after a finished day exits at once.
The collector:

- exits at once on Friday and Saturday, on DSE holidays (read from www.dse.com.bd/holidays) and when it starts
  after the close;
- builds the day's list: equities that traded in the previous session (api/live/prices, or
  api/live/data-archive/day-end when that list is already reset or the job starts late), minus the going-concern
  and closed-company lists published by DSE;
- waits for the open, then asks `api/live/depth?code=...` for one code at a time, **at most one request every
  2 seconds**, until the close (hours from DSE's own session data);
- backs off 2 s doubling to 60 s on 429/403/5xx/timeouts, and stops for the day if a request still fails.

Stored per snapshot: session date, fetch time (UTC), DSE's `asOf`, session phase, trading code, side, level,
price, quantity, orders. A book with no orders at all is one row with side `none`.

## Output
One artifact per day, `depth01-YYYY-MM-DD`: `depth01-YYYY-MM-DD.csv.gz` plus `manifest.json` (row count,
sha256, whether the day finished, why it stopped). Kept 90 days; the owner's machine downloads, verifies and then
deletes it. Nothing is committed to the repository.

## Manual runs (Actions -> depth01 -> Run workflow)
- `codes` = `SPCL,GP`: one pass over those codes, no session gate (a reachability test). The artifact is named
  `depth01test-...` so the daily import ignores it.
- `universe_only`: builds and prints the universe from both sources (no depth requests, no artifact).

## Contact
The User-Agent names this repository. Standard library Python only.
