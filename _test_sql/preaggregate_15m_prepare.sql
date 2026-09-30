-- Prepare an isolated, disposable build table. Live dashboard objects are not
-- touched until preaggregate_15m.sql performs the final atomic swap.

ALTER TABLE monitoring.fact_site_event ADD COLUMN IF NOT EXISTS publisher_email TEXT;
ALTER TABLE monitoring.fact_site_event ADD COLUMN IF NOT EXISTS group_name TEXT;
CREATE INDEX IF NOT EXISTS fact_site_event_group_idx ON monitoring.fact_site_event(group_name);
CREATE INDEX IF NOT EXISTS idx_detail_grid_event_id ON monitoring.detail_grid(event_id);

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

CREATE TABLE IF NOT EXISTS monitoring.fact_site_event_15m_build (
  bucket_15m TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  site_id INTEGER NOT NULL,
  group_name TEXT,
  vo TEXT NOT NULL,
  activity TEXT NOT NULL,
  site TEXT NOT NULL,
  records BIGINT NOT NULL,
  energy_wh DOUBLE PRECISION NOT NULL,
  cfp_g DOUBLE PRECISION NOT NULL,
  work DOUBLE PRECISION NOT NULL,
  ncores NUMERIC NOT NULL,
  grid_efficiency_sum DOUBLE PRECISION NOT NULL,
  grid_efficiency_count BIGINT NOT NULL,
  ci_sum DOUBLE PRECISION NOT NULL,
  ci_count BIGINT NOT NULL,
  pue_sum DOUBLE PRECISION NOT NULL,
  pue_count BIGINT NOT NULL,
  green_score_sum DOUBLE PRECISION NOT NULL,
  green_score_count BIGINT NOT NULL,
  ci_attached_records BIGINT NOT NULL,
  pue_attached_records BIGINT NOT NULL,
  green_score_records BIGINT NOT NULL,
  cfp_attached_records BIGINT NOT NULL,
  zero_cfp_records BIGINT NOT NULL,
  default_pue_records BIGINT NOT NULL,
  cached_ci_records BIGINT NOT NULL
);

TRUNCATE TABLE monitoring.fact_site_event_15m_build;
