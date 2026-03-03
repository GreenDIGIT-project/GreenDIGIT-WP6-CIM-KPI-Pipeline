# Changelog

All notable progress is summarized here from git history.

Scope of this file:
- Time window: 2026-01-03 to 2026-02-26 (last ~2 months)
- Source: commit messages in this repository

## 2026-02

Total commits: 37

### Dashboards and UI
- Introduced the first Grafana dashboard version, then expanded overview range and improved chart visualization.
- Improved and reformatted dashboards, including filter handling and no-data behavior.
- Updated landing page with Grafana links and added GreenDIGIT homepage updates.
- Removed dashboard auto-refresh and adjusted default panels.

### KPI and CIM pipeline
- Added KPI prefetch and fallback behavior, including docs and follow-up fixes.
- Added `/metrics/me` request parameters and introduced a second `cim-fastapi` instance for downtime handling.
- Activated CIM-FastAPI rollout and added a cap of 1000 for `/metrics/me`.
- Fixed CIM conversion behavior for `0.0` values.
- Adjusted prefetch behavior to focus on `now`.

### Data, SQL, and scripts
- Added SQL query helpers and GeoJSON data files.
- Added thresholds for numeric SQL columns and truncation for `SiteName`/ID.
- Added stress test files and reorganized the scripts folder.
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
- Fixed MongoDB initialization.
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

If you want, I can also generate:
1. A full-history changelog (all commits since project start), or
2. A stricter Keep a Changelog format grouped by `Added/Changed/Fixed` per release tag.
