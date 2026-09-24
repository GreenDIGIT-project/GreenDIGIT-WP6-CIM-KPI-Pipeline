BEGIN;
ALTER TABLE monitoring.fact_site_event ADD COLUMN IF NOT EXISTS publisher_email TEXT;
ALTER TABLE monitoring.fact_site_event ADD COLUMN IF NOT EXISTS group_name TEXT;
CREATE INDEX IF NOT EXISTS fact_site_event_group_idx ON monitoring.fact_site_event(group_name);
COMMIT;
