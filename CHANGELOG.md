# Changelog

All notable progress is summarised here from git history.

Scope of this file:
- Time window: 2026-01-03 to 2026-03-11
- Source: commit messages in this repository

## 2026-03

Total commits (so far): 16

### Metrics records/delete API parity
- Added parallel records APIs for Mongo/CIM and CNR SQL with aligned naming.
- Added `GET /v1/cim-records`, `GET /v1/cim-records/count`, and `POST /v1/cim-db/delete`.
- Added `GET /v1/cnr-records`, `GET /v1/cnr-records/count`, and `POST /v1/cnr-db/delete`.
- Added pagination support via `limit`, `offset`, and `page`, with a capped page size.
- Added recursive Mongo `filter_key[]` matching and delete feedback for unmatched filters.
- Added SQL adapter backend routes to list/count/delete CNR rows filtered by `site_id`, `vo`, `activity`, and time window.
- Added `scripts/example-edit-metrics.sh` with request snippets covering Mongo/CIM and CNR read/delete flows plus validation and pagination checks.
- Documented the current CNR SQL scoping behaviour: endpoints are authenticated, but filtering is presently based on the provided SQL dimensions and time window rather than per-user ownership in SQL.

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
- Expanded CIM FastAPI docs and README notes for the metrics read/delete endpoints and example request script.

### Batch pipeline refinements
- Applied minor updates and improvements to the CNR submission batch flow.
- Refactored filter views (`activity > vo`) in submission/filtering paths.
- Fixed filtering behaviour and corrected `cfp_g` table rounding.
- Added export marker file support during dump processing.

### Auth, rollout, and service integration
- Rolled out updates across all services.
- Added CIM-backed login proxying and set 30-day default interval handling.
- Promoted CIM auth and related proxying updates.

### Dashboard and query performance
- Changed dashboard defaults to avoid loading all data by default.
- Switched to aggregated 15-minute buckets for better performance.
- Added SQL support for creating daily aggregate export/refresh materialized views.

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
- 2026-03: 16 commits (so far)
- Total in scope: 76 commits

---
