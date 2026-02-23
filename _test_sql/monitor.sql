SELECT state, count(*)
FROM pg_stat_activity
WHERE datname = :'db_name'
GROUP BY state;

SELECT pid, state, wait_event_type, wait_event, query
FROM pg_stat_activity
WHERE datname = :'db_name' AND state <> 'idle';
