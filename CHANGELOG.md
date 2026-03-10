# Changelog

All notable progress is summarised here from git history.

Scope of this file:
- Time window: 2026-01-03 to 2026-03-10
- Source: commit messages in this repository

## 2026-03

Total commits (so far): 4

### Batch submission and enrichment
- Added KPI enrichment with cache support in batch submission flow.
- Updated `process_dump.py` CFP handling so missing/invalid CFP is recomputed when possible and set to SQL `NULL` when CI/PUE are unavailable.
- Added broader CFP key normalization in conversion logic (including keys such as `CFP(g)` via CFP-pattern matching).
- Added persistent cache files for batch enrichment and summary stats for CFP review/enrichment outcomes.

### Export windowing and operational safety
- Added incremental export watermark support via `scripts/batch_submit_cnr/last_exported.txt`.
- Added optional `--end-time` for batch runs; default end window is UTC yesterday at `23:59:59`.
- Added safeguard to prevent duplicate same-day export windows in regular cron mode.
- Added timestamp-window filtering directly in `mongoexport`.
- Added `publisher_email` filtering directly in `mongoexport` (`$in` from configured email list).

### UI and docs
- Added changelog maintenance updates and dashboard unit visibility updates.
- Fixed image loading in FastAPI/landing integration.

## 2026-02

Total commits: 37

### Dashboards and UI
- Introduced the first Grafana dashboard version, then expanded overview range and improved chart visualisation.
- Improved and reformatted dashboards, including filter handling and no-data behaviour.
- Updated landing page with Grafana links and added GreenDIGIT homepage updates.
- Removed dashboard auto-refresh and adjusted default panels.

### KPI and CIM pipeline
- Added KPI prefetch and fallback behaviour, including docs and follow-up fixes.
- Added `/metrics/me` request parameters and introduced a second `cim-fastapi` instance for downtime handling.
- Activated CIM-FastAPI rollout and added a cap of 1000 for `/metrics/me`.
- Fixed CIM conversion behaviour for `0.0` values.
- Adjusted prefetch behaviour to focus on `now`.

### Data, SQL, and scripts
- Added SQL query helpers and GeoJSON data files.
- Added thresholds for numeric SQL columns and truncation for `SiteName`/ID.
- Added stress test files and reorganised the scripts folder.
- Moved JSON files and applied small script/utility improvements.
- Fixed temporary file writing for KPI cache.

### Auth and deployment
- Added manual auth and password-related updates.
- Added volume module updates and Docker/runtime adjustments related to dashboard rollout.

### Notes
- One dashboard change was reverted (`Revert "Dashboard small improvement."`), indicating iterative tuning.

## 2026-01

Total commits: 23

### CIM and KPI evolution
- Added CIM module and implemented/continued CNR transformation workflow.
- Completed KPI enrichment in the CIM service (initial implementation and follow-up completion).
- Added CIM address handling and KPI fallback improvements.

### API, services, and infrastructure
- Applied endpoint updates for `/v1/`.
- Updated Docker image references to `:latest`.
- Fixed MongoDB initialisation.
- Cleaned service entrypoint logic (`main.py`) and made minor cronjob/example updates.

### Auth and tokens
- Fixed JWT key rotation for cronjob flow, plus minor key-rotation fixes.
- Renamed token scripts and paths.
- Added login page updates to landing page.
- Temporarily disabled auth verification in one commit (later history indicates continued auth adjustments).

### Reporting, metadata, and utilities
- Added MongoDB overview script and moved migration steps.
- Improved MetricsDB metadata export and enrichment outputs.
- Added known partners summary, row aggregation, and status label fixes.
- Minor email-related fixes and image update.

## Commit Activity Snapshot

- 2026-01: 23 commits
- 2026-02: 37 commits
- Total in scope: 60 commits

---
