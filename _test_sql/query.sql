SELECT
  SUM(COALESCE(f.energy_wh,0))/1000.0
FROM monitoring.fact_site_event f
JOIN monitoring.sites s ON s.site_id = f.site_id
WHERE f.event_start_timestamp BETWEEN now() - interval '90 day' AND now()
  AND COALESCE(NULLIF(TRIM(f.owner),''),'Unknown') LIKE '%'
  AND s.site_type::text LIKE '%'
  AND s.description LIKE '%';
