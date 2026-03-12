-- 15-minute pre-aggregation for Grafana-heavy queries.
-- This script recreates the MV schema safely through a temporary object.

DROP MATERIALIZED VIEW IF EXISTS monitoring.mv_fact_site_event_15m_new;

CREATE MATERIALIZED VIEW monitoring.mv_fact_site_event_15m_new AS
WITH detail_grid_by_event AS (
  SELECT
    dg.event_id,
    SUM(COALESCE(dg.ncores, 0)) AS ncores
  FROM monitoring.detail_grid dg
  GROUP BY 1
)
SELECT
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
  ) AS cfp_g,
  SUM(COALESCE(f.work, 0)) AS work,
  SUM(COALESCE(dg.ncores, 0)) AS ncores
FROM monitoring.fact_site_event f
JOIN monitoring.sites s ON s.site_id = f.site_id
LEFT JOIN detail_grid_by_event dg ON dg.event_id = f.event_id
GROUP BY 1, 2, 3, 4, 5;

DROP MATERIALIZED VIEW IF EXISTS monitoring.mv_fact_site_event_15m;
ALTER MATERIALIZED VIEW monitoring.mv_fact_site_event_15m_new RENAME TO mv_fact_site_event_15m;

-- Required for REFRESH MATERIALIZED VIEW CONCURRENTLY
CREATE UNIQUE INDEX mv_fact_site_event_15m_uq
  ON monitoring.mv_fact_site_event_15m (bucket_15m, site_id, vo);

-- Lookup/perf indexes for dashboard filters
CREATE INDEX mv_fact_site_event_15m_bucket_idx
  ON monitoring.mv_fact_site_event_15m (bucket_15m);
CREATE INDEX mv_fact_site_event_15m_activity_idx
  ON monitoring.mv_fact_site_event_15m (activity);
CREATE INDEX mv_fact_site_event_15m_vo_idx
  ON monitoring.mv_fact_site_event_15m (vo);
CREATE INDEX mv_fact_site_event_15m_site_idx
  ON monitoring.mv_fact_site_event_15m (site);
