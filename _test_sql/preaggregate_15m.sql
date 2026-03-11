-- 15-minute pre-aggregation for Grafana-heavy queries.
-- Run once, then schedule REFRESH as needed (e.g. every 5-15 minutes).

-- 1) Materialized view
CREATE MATERIALIZED VIEW IF NOT EXISTS monitoring.mv_fact_site_event_15m AS
SELECT
  -- 15-minute bucket (portable expression)
  date_trunc('hour', f.event_start_timestamp)
    + (floor(extract(minute FROM f.event_start_timestamp) / 15) * interval '15 minutes')
    AS bucket_15m,
  f.site_id,
  COALESCE(NULLIF(TRIM(f.owner), ''), 'Unknown') AS vo,
  s.site_type::text AS activity,
  s.description AS site,
  COUNT(*) AS jobs,
  SUM(COALESCE(f.energy_wh, 0)) AS energy_wh,
  SUM(
    COALESCE(
      CASE
        WHEN f.energy_wh IS NOT NULL AND f.pue IS NOT NULL AND f.ci_g IS NOT NULL
          THEN (f.energy_wh / 1000.0) * f.pue * f.ci_g
        ELSE f.cfp_g::double precision
      END,
      0
    )
  ) AS cfp_g
FROM monitoring.fact_site_event f
JOIN monitoring.sites s ON s.site_id = f.site_id
GROUP BY 1, 2, 3, 4, 5;

-- 2) Required for REFRESH MATERIALIZED VIEW CONCURRENTLY
CREATE UNIQUE INDEX IF NOT EXISTS mv_fact_site_event_15m_uq
  ON monitoring.mv_fact_site_event_15m (bucket_15m, site_id, vo);

-- 3) Lookup/perf indexes for dashboard filters
CREATE INDEX IF NOT EXISTS mv_fact_site_event_15m_bucket_idx
  ON monitoring.mv_fact_site_event_15m (bucket_15m);
CREATE INDEX IF NOT EXISTS mv_fact_site_event_15m_activity_idx
  ON monitoring.mv_fact_site_event_15m (activity);
CREATE INDEX IF NOT EXISTS mv_fact_site_event_15m_vo_idx
  ON monitoring.mv_fact_site_event_15m (vo);
CREATE INDEX IF NOT EXISTS mv_fact_site_event_15m_site_idx
  ON monitoring.mv_fact_site_event_15m (site);
