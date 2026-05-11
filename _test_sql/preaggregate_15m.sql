-- 15-minute pre-aggregation and reporting views for Grafana-heavy queries.

CREATE TABLE IF NOT EXISTS monitoring.event_enrichment_audit (
  event_id BIGINT PRIMARY KEY REFERENCES monitoring.fact_site_event(event_id) ON DELETE CASCADE,
  pue_source TEXT,
  ci_source TEXT,
  cfp_source TEXT,
  cfp_null_reason TEXT,
  used_default_pue BOOLEAN,
  used_cached_ci BOOLEAN,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS monitoring.ingestion_audit (
  audit_id BIGSERIAL PRIMARY KEY,
  audit_ts TIMESTAMPTZ NOT NULL DEFAULT now(),
  publisher_email TEXT,
  caller_email TEXT,
  vo TEXT,
  site TEXT,
  activity TEXT,
  submitted_count INTEGER NOT NULL DEFAULT 0,
  accepted_count INTEGER NOT NULL DEFAULT 0,
  rejected_count INTEGER NOT NULL DEFAULT 0,
  outcome TEXT NOT NULL DEFAULT 'unknown',
  reason TEXT,
  source TEXT NOT NULL DEFAULT 'submit-cim',
  window_start TIMESTAMPTZ,
  window_end TIMESTAMPTZ
);

ALTER TABLE monitoring.ingestion_audit
  ADD COLUMN IF NOT EXISTS vo TEXT;

CREATE INDEX IF NOT EXISTS ingestion_audit_ts_idx
  ON monitoring.ingestion_audit (audit_ts);
CREATE INDEX IF NOT EXISTS ingestion_audit_site_idx
  ON monitoring.ingestion_audit (site);
CREATE INDEX IF NOT EXISTS ingestion_audit_activity_idx
  ON monitoring.ingestion_audit (activity);
CREATE INDEX IF NOT EXISTS ingestion_audit_publisher_idx
  ON monitoring.ingestion_audit (publisher_email);
CREATE INDEX IF NOT EXISTS ingestion_audit_vo_idx
  ON monitoring.ingestion_audit (vo);

CREATE TABLE IF NOT EXISTS monitoring.service_health_probe (
  probe_id BIGSERIAL PRIMARY KEY,
  probe_ts TIMESTAMPTZ NOT NULL DEFAULT now(),
  service_name TEXT NOT NULL,
  target TEXT,
  ok BOOLEAN NOT NULL,
  status_code INTEGER,
  latency_ms INTEGER,
  detail TEXT
);

CREATE INDEX IF NOT EXISTS service_health_probe_ts_idx
  ON monitoring.service_health_probe (probe_ts);
CREATE INDEX IF NOT EXISTS service_health_probe_service_idx
  ON monitoring.service_health_probe (service_name);

CREATE TABLE IF NOT EXISTS monitoring.reporting_excluded_sites (
  site TEXT PRIMARY KEY,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS monitoring.reporting_excluded_vos (
  vo TEXT PRIMARY KEY,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

DROP MATERIALIZED VIEW IF EXISTS monitoring.mv_fact_site_event_15m_new;
DROP VIEW IF EXISTS monitoring.v_reporting_record_listing;
DROP VIEW IF EXISTS monitoring.v_reporting_resource_listing;
DROP VIEW IF EXISTS monitoring.v_public_dashboard_resource_listing;
DROP VIEW IF EXISTS monitoring.v_public_dashboard_15m;
DROP MATERIALIZED VIEW IF EXISTS monitoring.mv_public_dashboard_resource_listing;
DROP MATERIALIZED VIEW IF EXISTS monitoring.mv_public_dashboard_15m;
DROP MATERIALIZED VIEW IF EXISTS monitoring.mv_public_dashboard_resource_selection;
DROP MATERIALIZED VIEW IF EXISTS monitoring.mv_reporting_resource_listing;
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'monitoring'
      AND c.relname = 'mv_fact_site_event_15m'
      AND c.relkind = 'm'
  ) THEN
    EXECUTE 'DROP MATERIALIZED VIEW monitoring.mv_fact_site_event_15m';
  ELSIF EXISTS (
    SELECT 1
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'monitoring'
      AND c.relname = 'mv_fact_site_event_15m'
      AND c.relkind = 'v'
  ) THEN
    EXECUTE 'DROP VIEW monitoring.mv_fact_site_event_15m';
  END IF;
END $$;
DROP MATERIALIZED VIEW IF EXISTS monitoring.mv_fact_site_event_15m_base;

CREATE MATERIALIZED VIEW monitoring.mv_fact_site_event_15m_new AS
WITH detail_grid_by_event AS (
  SELECT
    dg.event_id,
    SUM(COALESCE(dg.ncores, 0)) AS ncores
  FROM monitoring.detail_grid dg
  GROUP BY 1
),
fact_enriched AS (
  SELECT
    date_trunc('hour', f.event_start_timestamp)
      + (floor(extract(minute FROM f.event_start_timestamp) / 15) * interval '15 minutes')
      AS bucket_15m,
    f.event_id,
    f.site_id,
    COALESCE(NULLIF(TRIM(f.owner), ''), 'Unknown') AS vo,
    s.site_type::text AS activity,
    s.description AS site,
    COALESCE(f.energy_wh, 0) AS energy_wh,
    COALESCE(
      CASE
        WHEN f.energy_wh IS NOT NULL AND f.pue IS NOT NULL AND f.ci_g IS NOT NULL
          THEN (f.energy_wh / 1000.0) * f.pue * f.ci_g
        ELSE f.cfp_g::double precision
      END,
      0
    ) AS cfp_g,
    COALESCE(f.work, 0) AS work,
    COALESCE(dg.ncores, 0) AS ncores,
    CASE WHEN f.ci_g IS NOT NULL THEN 1 ELSE 0 END AS ci_attached,
    CASE WHEN f.pue IS NOT NULL THEN 1 ELSE 0 END AS pue_attached,
    CASE
      WHEN (
        CASE
          WHEN f.energy_wh IS NOT NULL AND f.pue IS NOT NULL AND f.ci_g IS NOT NULL
            THEN (f.energy_wh / 1000.0) * f.pue * f.ci_g
          ELSE f.cfp_g::double precision
        END
      ) IS NOT NULL THEN 1 ELSE 0
    END AS cfp_attached,
    CASE
      WHEN COALESCE(
        CASE
          WHEN f.energy_wh IS NOT NULL AND f.pue IS NOT NULL AND f.ci_g IS NOT NULL
            THEN (f.energy_wh / 1000.0) * f.pue * f.ci_g
          ELSE f.cfp_g::double precision
        END,
        0
      ) = 0 THEN 1 ELSE 0
    END AS zero_cfp,
    CASE WHEN COALESCE(eea.used_default_pue, FALSE) THEN 1 ELSE 0 END AS default_pue,
    CASE WHEN COALESCE(eea.used_cached_ci, FALSE) THEN 1 ELSE 0 END AS cached_ci
  FROM monitoring.fact_site_event f
  JOIN monitoring.sites s ON s.site_id = f.site_id
  LEFT JOIN detail_grid_by_event dg ON dg.event_id = f.event_id
  LEFT JOIN monitoring.event_enrichment_audit eea ON eea.event_id = f.event_id
)
SELECT
  bucket_15m,
  site_id,
  vo,
  activity,
  site,
  COUNT(*) AS records,
  SUM(energy_wh) AS energy_wh,
  SUM(cfp_g) AS cfp_g,
  SUM(work) AS work,
  SUM(ncores) AS ncores,
  SUM(ci_attached) AS ci_attached_records,
  SUM(pue_attached) AS pue_attached_records,
  SUM(cfp_attached) AS cfp_attached_records,
  SUM(zero_cfp) AS zero_cfp_records,
  SUM(default_pue) AS default_pue_records,
  SUM(cached_ci) AS cached_ci_records
FROM fact_enriched
GROUP BY 1, 2, 3, 4, 5;

ALTER MATERIALIZED VIEW monitoring.mv_fact_site_event_15m_new RENAME TO mv_fact_site_event_15m_base;

CREATE UNIQUE INDEX mv_fact_site_event_15m_base_uq
  ON monitoring.mv_fact_site_event_15m_base (bucket_15m, site_id, vo);

CREATE INDEX mv_fact_site_event_15m_base_bucket_idx
  ON monitoring.mv_fact_site_event_15m_base (bucket_15m);
CREATE INDEX mv_fact_site_event_15m_base_activity_idx
  ON monitoring.mv_fact_site_event_15m_base (activity);
CREATE INDEX mv_fact_site_event_15m_base_vo_idx
  ON monitoring.mv_fact_site_event_15m_base (vo);
CREATE INDEX mv_fact_site_event_15m_base_site_idx
  ON monitoring.mv_fact_site_event_15m_base (site);

CREATE VIEW monitoring.mv_fact_site_event_15m AS
SELECT
  m.*
FROM monitoring.mv_fact_site_event_15m_base m
WHERE NOT EXISTS (
  SELECT 1
  FROM monitoring.reporting_excluded_sites x
  WHERE LOWER(BTRIM(x.site)) = LOWER(BTRIM(m.site))
)
AND NOT EXISTS (
  SELECT 1
  FROM monitoring.reporting_excluded_vos x
  WHERE LOWER(BTRIM(x.vo)) = LOWER(BTRIM(m.vo))
);

CREATE MATERIALIZED VIEW monitoring.mv_reporting_resource_listing AS
WITH base AS (
  SELECT
    m.vo,
    m.activity,
    m.site,
    COALESCE(
      NULLIF(
        CASE
          WHEN m.site ~ '.*-([A-Z]{2})-.*' THEN regexp_replace(m.site, '.*-([A-Z]{2})-.*', '\1')
          WHEN m.site ~ '.*-([A-Z]{2})$' THEN regexp_replace(m.site, '.*-([A-Z]{2})$', '\1')
          WHEN m.site ~ '.*\.([A-Za-z]{2})$' THEN UPPER(regexp_replace(m.site, '.*\.([A-Za-z]{2})$', '\1'))
          ELSE NULL
        END,
        ''
      ),
      'Unknown'
    ) AS country,
    MIN(m.bucket_15m) AS first_bucket,
    MAX(m.bucket_15m) AS last_bucket,
    COUNT(*) AS active_buckets,
    SUM(COALESCE(m.records, 0)) AS total_records,
    SUM(COALESCE(m.energy_wh, 0)) AS energy_wh,
    SUM(COALESCE(m.cfp_g, 0)) AS cfp_g,
    SUM(COALESCE(m.ncores, 0)) AS total_ncores,
    SUM(COALESCE(m.ci_attached_records, 0)) AS ci_attached_records,
    SUM(COALESCE(m.pue_attached_records, 0)) AS pue_attached_records,
    SUM(COALESCE(m.cfp_attached_records, 0)) AS cfp_attached_records,
    SUM(COALESCE(m.zero_cfp_records, 0)) AS zero_cfp_records,
    SUM(COALESCE(m.default_pue_records, 0)) AS default_pue_records,
    SUM(COALESCE(m.cached_ci_records, 0)) AS cached_ci_records
  FROM monitoring.mv_fact_site_event_15m m
  GROUP BY 1, 2, 3, 4
),
network_data AS (
  SELECT
    COALESCE(NULLIF(TRIM(f.owner), ''), 'Unknown') AS vo,
    s.site_type::text AS activity,
    s.description AS site,
    SUM(COALESCE(dn.amountofdatatransferred, 0)) AS volume_of_data_bytes
  FROM monitoring.fact_site_event f
  JOIN monitoring.sites s ON s.site_id = f.site_id
  LEFT JOIN monitoring.detail_network dn ON dn.event_id = f.event_id
  GROUP BY 1, 2, 3
)
SELECT
  b.vo,
  b.activity,
  b.site,
  b.country,
  TO_CHAR(b.first_bucket, 'YYYY-MM-DD') AS active_since,
  TO_CHAR(b.last_bucket, 'YYYY-MM-DD') AS last_seen,
  ROUND((EXTRACT(EPOCH FROM (b.last_bucket - b.first_bucket)) / 2592000.0)::numeric, 2) AS span_months,
  ROUND(
    (
      100.0 * b.active_buckets
      / NULLIF((EXTRACT(EPOCH FROM (b.last_bucket - b.first_bucket)) / 900.0) + 1, 0)
    )::numeric,
    2
  ) AS continuity_pct,
  TRIM(BOTH ', ' FROM CONCAT(
    CASE WHEN b.energy_wh > 0 THEN 'Energy, ' ELSE '' END,
    CASE WHEN b.cfp_attached_records > 0 THEN 'CO2, ' ELSE '' END,
    CASE WHEN b.ci_attached_records > 0 THEN 'Carbon Intensity, ' ELSE '' END,
    CASE WHEN b.pue_attached_records > 0 THEN 'PUE, ' ELSE '' END
  )) AS metrics_reported,
  b.total_records,
  ROUND(b.energy_wh::numeric, 3) AS energy_wh,
  ROUND(b.cfp_g::numeric, 3) AS cfp_g,
  b.total_ncores,
  COALESCE(nd.volume_of_data_bytes, 0) AS volume_of_data_bytes,
  'sql_only' AS source_db_presence
FROM base b
LEFT JOIN network_data nd
  ON nd.vo = b.vo AND nd.activity = b.activity AND nd.site = b.site;

CREATE UNIQUE INDEX mv_reporting_resource_listing_uq
  ON monitoring.mv_reporting_resource_listing (vo, activity, site);

CREATE OR REPLACE VIEW monitoring.v_reporting_resource_listing AS
SELECT *
FROM monitoring.mv_reporting_resource_listing;

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

CREATE OR REPLACE VIEW monitoring.v_reporting_record_listing AS
SELECT
  m.bucket_15m AS time_bucket,
  m.vo,
  m.activity,
  m.site,
  m.records,
  ROUND(m.energy_wh::numeric, 3) AS energy_wh,
  ROUND(m.cfp_g::numeric, 3) AS cfp_g,
  ROUND(m.work::numeric, 3) AS work,
  m.ncores
FROM monitoring.mv_fact_site_event_15m m;
