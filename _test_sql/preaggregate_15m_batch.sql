-- Required psql variables: batch_after, batch_through.
-- Each invocation is one bounded transaction and may produce partial rows for
-- a 15-minute group. preaggregate_15m.sql combines those partial rows later.

INSERT INTO monitoring.fact_site_event_15m_build (
  bucket_15m, site_id, group_name, vo, activity, site, records,
  energy_wh, cfp_g, work, ncores,
  grid_efficiency_sum, grid_efficiency_count,
  ci_sum, ci_count, pue_sum, pue_count,
  green_score_sum, green_score_count,
  ci_attached_records, pue_attached_records, green_score_records,
  cfp_attached_records, zero_cfp_records, default_pue_records,
  cached_ci_records
)
WITH batch_fact AS MATERIALIZED (
  SELECT f.*
  FROM monitoring.fact_site_event f
  WHERE f.event_id > :'batch_after'::bigint
    AND f.event_id <= :'batch_through'::bigint
),
detail_grid_by_event AS (
  SELECT
    dg.event_id,
    SUM(COALESCE(dg.ncores, 0)) AS ncores,
    AVG(dg.efficiency::double precision) FILTER (WHERE dg.efficiency IS NOT NULL) AS grid_efficiency
  FROM monitoring.detail_grid dg
  JOIN batch_fact f ON f.event_id = dg.event_id
  GROUP BY dg.event_id
),
fact_enriched AS (
  SELECT
    date_trunc('hour', f.event_start_timestamp)
      + (floor(extract(minute FROM f.event_start_timestamp) / 15) * interval '15 minutes') AS bucket_15m,
    f.site_id,
    f.group_name,
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
    dg.grid_efficiency,
    f.ci_g::double precision AS ci_g,
    f.pue::double precision AS pue,
    CASE
      WHEN dg.grid_efficiency IS NOT NULL AND dg.grid_efficiency > 0
        AND f.ci_g IS NOT NULL AND f.ci_g > 0
        AND f.pue IS NOT NULL AND f.pue > 0
      THEN (dg.grid_efficiency * 360000.0) / (f.pue::double precision * f.ci_g::double precision)
      ELSE NULL
    END AS green_score_s_per_gco2,
    CASE WHEN f.ci_g IS NOT NULL THEN 1 ELSE 0 END AS ci_attached,
    CASE WHEN f.pue IS NOT NULL THEN 1 ELSE 0 END AS pue_attached,
    CASE
      WHEN dg.grid_efficiency IS NOT NULL AND dg.grid_efficiency > 0
        AND f.ci_g IS NOT NULL AND f.ci_g > 0
        AND f.pue IS NOT NULL AND f.pue > 0
      THEN 1 ELSE 0
    END AS green_score_attached,
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
  FROM batch_fact f
  JOIN monitoring.sites s ON s.site_id = f.site_id
  LEFT JOIN detail_grid_by_event dg ON dg.event_id = f.event_id
  LEFT JOIN monitoring.event_enrichment_audit eea ON eea.event_id = f.event_id
)
SELECT
  bucket_15m,
  site_id,
  group_name,
  vo,
  activity,
  site,
  COUNT(*) AS records,
  SUM(energy_wh) AS energy_wh,
  SUM(cfp_g) AS cfp_g,
  SUM(work) AS work,
  SUM(ncores) AS ncores,
  COALESCE(SUM(grid_efficiency) FILTER (WHERE grid_efficiency IS NOT NULL), 0) AS grid_efficiency_sum,
  COUNT(grid_efficiency) AS grid_efficiency_count,
  COALESCE(SUM(ci_g) FILTER (WHERE ci_g IS NOT NULL), 0) AS ci_sum,
  COUNT(ci_g) AS ci_count,
  COALESCE(SUM(pue) FILTER (WHERE pue IS NOT NULL), 0) AS pue_sum,
  COUNT(pue) AS pue_count,
  COALESCE(SUM(green_score_s_per_gco2) FILTER (WHERE green_score_s_per_gco2 IS NOT NULL), 0) AS green_score_sum,
  COUNT(green_score_s_per_gco2) AS green_score_count,
  SUM(ci_attached) AS ci_attached_records,
  SUM(pue_attached) AS pue_attached_records,
  SUM(green_score_attached) AS green_score_records,
  SUM(cfp_attached) AS cfp_attached_records,
  SUM(zero_cfp) AS zero_cfp_records,
  SUM(default_pue) AS default_pue_records,
  SUM(cached_ci) AS cached_ci_records
FROM fact_enriched
GROUP BY 1, 2, 3, 4, 5, 6;

