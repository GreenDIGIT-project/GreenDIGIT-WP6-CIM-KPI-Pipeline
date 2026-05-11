-- Public dashboard projection over the existing 15-minute aggregate.
-- This creates only the public/anonymised materialized views; it does not rebuild
-- monitoring.mv_fact_site_event_15m_base.

DROP VIEW IF EXISTS monitoring.v_public_dashboard_resource_listing;
DROP VIEW IF EXISTS monitoring.v_public_dashboard_15m;
DROP MATERIALIZED VIEW IF EXISTS monitoring.mv_public_dashboard_resource_listing;
DROP MATERIALIZED VIEW IF EXISTS monitoring.mv_public_dashboard_15m;
DROP MATERIALIZED VIEW IF EXISTS monitoring.mv_public_dashboard_resource_selection;

CREATE MATERIALIZED VIEW monitoring.mv_public_dashboard_resource_selection AS
WITH resource_totals AS (
  SELECT
    m.site_id,
    m.activity,
    m.site,
    SUM(COALESCE(m.records, 0)) AS total_records,
    SUM(COALESCE(m.energy_wh, 0)) AS energy_wh,
    SUM(COALESCE(m.cfp_g, 0)) AS cfp_g,
    MIN(m.bucket_15m) AS first_bucket,
    MAX(m.bucket_15m) AS last_bucket
  FROM monitoring.mv_fact_site_event_15m m
  WHERE m.activity IN ('grid', 'cloud', 'network')
  GROUP BY 1, 2, 3
),
grid_ranked AS (
  SELECT
    r.*,
    ROW_NUMBER() OVER (
      ORDER BY r.total_records DESC, r.energy_wh DESC, r.cfp_g DESC, r.site_id
    ) AS selection_rank
  FROM resource_totals r
  WHERE r.activity = 'grid'
),
cloud_ranked AS (
  SELECT
    r.*,
    ROW_NUMBER() OVER (
      ORDER BY
        CASE WHEN r.site ILIKE '%ifca%' THEN 0 ELSE 1 END,
        r.total_records DESC,
        r.energy_wh DESC,
        r.cfp_g DESC,
        r.site_id
    ) AS selection_rank
  FROM resource_totals r
  WHERE r.activity = 'cloud'
),
network_ranked AS (
  SELECT
    r.*,
    ROW_NUMBER() OVER (
      ORDER BY r.total_records DESC, r.energy_wh DESC, r.cfp_g DESC, r.site_id
    ) AS selection_rank
  FROM resource_totals r
  WHERE r.activity = 'network'
),
selected AS (
  SELECT 1 AS public_order, * FROM grid_ranked WHERE selection_rank <= 3
  UNION ALL
  SELECT 2 AS public_order, * FROM cloud_ranked WHERE selection_rank = 1
  UNION ALL
  SELECT 3 AS public_order, * FROM network_ranked WHERE selection_rank = 1
)
SELECT
  site_id,
  activity,
  site,
  CASE
    WHEN activity = 'grid' THEN 'Grid Site ' || CHR(64 + selection_rank::integer)
    WHEN activity = 'cloud' THEN 'Cloud Site A'
    WHEN activity = 'network' THEN 'Network Site A'
    ELSE 'Public Site'
  END AS public_site,
  public_order,
  selection_rank,
  total_records,
  energy_wh,
  cfp_g,
  first_bucket,
  last_bucket
FROM selected;

CREATE UNIQUE INDEX mv_public_dashboard_resource_selection_uq
  ON monitoring.mv_public_dashboard_resource_selection (site_id);
CREATE INDEX mv_public_dashboard_resource_selection_activity_idx
  ON monitoring.mv_public_dashboard_resource_selection (activity);

CREATE MATERIALIZED VIEW monitoring.mv_public_dashboard_15m AS
WITH masked AS (
  SELECT
    m.bucket_15m,
    s.public_site,
    INITCAP(m.activity) AS activity,
    m.vo,
    m.records,
    m.energy_wh,
    m.cfp_g,
    m.work,
    m.ncores,
    m.ci_attached_records,
    m.pue_attached_records,
    m.cfp_attached_records,
    m.zero_cfp_records,
    m.default_pue_records,
    m.cached_ci_records
  FROM monitoring.mv_fact_site_event_15m m
  JOIN monitoring.mv_public_dashboard_resource_selection s
    ON s.site_id = m.site_id
)
SELECT
  bucket_15m,
  'VO ' || DENSE_RANK() OVER (ORDER BY vo) AS public_vo,
  activity,
  public_site,
  SUM(records) AS records,
  SUM(energy_wh) AS energy_wh,
  SUM(cfp_g) AS cfp_g,
  SUM(work) AS work,
  SUM(ncores) AS ncores,
  SUM(ci_attached_records) AS ci_attached_records,
  SUM(pue_attached_records) AS pue_attached_records,
  SUM(cfp_attached_records) AS cfp_attached_records,
  SUM(zero_cfp_records) AS zero_cfp_records,
  SUM(default_pue_records) AS default_pue_records,
  SUM(cached_ci_records) AS cached_ci_records
FROM masked
GROUP BY bucket_15m, vo, activity, public_site;

CREATE UNIQUE INDEX mv_public_dashboard_15m_uq
  ON monitoring.mv_public_dashboard_15m (bucket_15m, public_vo, activity, public_site);
CREATE INDEX mv_public_dashboard_15m_bucket_idx
  ON monitoring.mv_public_dashboard_15m (bucket_15m);
CREATE INDEX mv_public_dashboard_15m_activity_idx
  ON monitoring.mv_public_dashboard_15m (activity);
CREATE INDEX mv_public_dashboard_15m_site_idx
  ON monitoring.mv_public_dashboard_15m (public_site);
CREATE INDEX mv_public_dashboard_15m_vo_idx
  ON monitoring.mv_public_dashboard_15m (public_vo);

CREATE MATERIALIZED VIEW monitoring.mv_public_dashboard_resource_listing AS
SELECT
  m.public_vo,
  m.activity,
  m.public_site,
  TO_CHAR(MIN(m.bucket_15m), 'YYYY-MM-DD') AS active_since,
  TO_CHAR(MAX(m.bucket_15m), 'YYYY-MM-DD') AS last_seen,
  SUM(COALESCE(m.records, 0)) AS total_records,
  ROUND(SUM(COALESCE(m.energy_wh, 0))::numeric, 3) AS energy_wh,
  ROUND(SUM(COALESCE(m.cfp_g, 0))::numeric, 3) AS cfp_g,
  ROUND(SUM(COALESCE(m.work, 0))::numeric, 3) AS work,
  SUM(COALESCE(m.ncores, 0)) AS ncores
FROM monitoring.mv_public_dashboard_15m m
GROUP BY 1, 2, 3;

CREATE UNIQUE INDEX mv_public_dashboard_resource_listing_uq
  ON monitoring.mv_public_dashboard_resource_listing (public_vo, activity, public_site);

CREATE OR REPLACE VIEW monitoring.v_public_dashboard_15m AS
SELECT *
FROM monitoring.mv_public_dashboard_15m;

CREATE OR REPLACE VIEW monitoring.v_public_dashboard_resource_listing AS
SELECT *
FROM monitoring.mv_public_dashboard_resource_listing;
