-- SELECT table_schema, table_name, column_name, data_type, udt_name
-- FROM information_schema.columns
-- WHERE table_schema = 'monitoring'
--      AND table_name IN ('detail_cloud','detail_network','detail_grid')
--      AND column_name IN ('cpuduration_s','wallclocktime_s','amountofdatatransferred','totalcputime_s','scaledcputime_s')
-- ORDER BY table_name, column_name;

-- Quick check.
-- SELECT
--   pg_typeof(cpuduration_s) AS col_type,
--   MIN(cpuduration_s),
--   MAX(cpuduration_s)
-- FROM monitoring.detail_cloud;

-- If we want to switch the col type to BigInt.
-- ALTER TABLE monitoring.detail_cloud
--   ALTER COLUMN cpuduration_s TYPE bigint;


BEGIN;

ALTER TABLE monitoring.detail_cloud
  ALTER COLUMN cpuduration_s TYPE bigint,
  ALTER COLUMN wallclocktime_s TYPE bigint,
  ALTER COLUMN suspendduration_s TYPE bigint;

ALTER TABLE monitoring.detail_grid
  ALTER COLUMN wallclocktime_s TYPE bigint,
  ALTER COLUMN normcputime_s TYPE bigint,
  ALTER COLUMN totalcputime_s TYPE bigint,
  ALTER COLUMN scaledcputime_s TYPE bigint;

ALTER TABLE monitoring.detail_network
  ALTER COLUMN amountofdatatransferred TYPE bigint;

COMMIT;

