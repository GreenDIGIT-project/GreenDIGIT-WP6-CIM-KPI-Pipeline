- Run these commands in the same shell session so `START`, `END`, `EMAILS`, and `DUMP_BASE` are reused by later steps.

- Count the CNR SQL entries for the replacement window (August 16 and 17)
```bash
set -a; source .env; set +a

START="2026-08-16T00:00:00Z"
END="2026-08-17T23:59:59Z"

PGPASSWORD="$CNR_POSTEGRESQL_PASSWORD" psql \
  -h "$CNR_HOST" -p "${CNR_POSTEGRESQL_PORT:-5432}" \
  -U "$CNR_USER" -d "$CNR_GD_DB" -P pager=off \
  -c "WITH ids AS (
        SELECT event_id
        FROM monitoring.fact_site_event
        WHERE event_start_timestamp <= '$END'::timestamptz
          AND event_end_timestamp >= '$START'::timestamptz
      )
      SELECT COUNT(*) AS fact_rows_to_delete FROM ids;"
```

- Delete the CNR SQL rows for the replacement window (August 16 and 17)
```bash
PGPASSWORD="$CNR_POSTEGRESQL_PASSWORD" psql \
  -h "$CNR_HOST" -p "${CNR_POSTEGRESQL_PORT:-5432}" \
  -U "$CNR_USER" -d "$CNR_GD_DB" -v ON_ERROR_STOP=1 -P pager=off \
  -c "WITH ids AS MATERIALIZED (
        SELECT event_id
        FROM monitoring.fact_site_event
        WHERE event_start_timestamp <= '$END'::timestamptz
          AND event_end_timestamp >= '$START'::timestamptz
      ),
      del_grid AS (
        DELETE FROM monitoring.detail_grid WHERE event_id IN (SELECT event_id FROM ids) RETURNING 1
      ),
      del_cloud AS (
        DELETE FROM monitoring.detail_cloud
        WHERE event_id IN (SELECT event_id FROM ids) OR site_id IN (SELECT event_id FROM ids)
        RETURNING 1
      ),
      del_network AS (
        DELETE FROM monitoring.detail_network WHERE event_id IN (SELECT event_id FROM ids) RETURNING 1
      ),
      del_fact AS (
        DELETE FROM monitoring.fact_site_event WHERE event_id IN (SELECT event_id FROM ids) RETURNING 1
      )
      SELECT
        (SELECT COUNT(*) FROM del_fact) AS deleted_fact,
        (SELECT COUNT(*) FROM del_grid) AS deleted_grid,
        (SELECT COUNT(*) FROM del_cloud) AS deleted_cloud,
        (SELECT COUNT(*) FROM del_network) AS deleted_network;"
```


- Make sure the CNR SQL rows were deleted
```bash
PGPASSWORD="$CNR_POSTEGRESQL_PASSWORD" psql \
  -h "$CNR_HOST" -p "${CNR_POSTEGRESQL_PORT:-5432}" \
  -U "$CNR_USER" -d "$CNR_GD_DB" -P pager=off \
  -c "SELECT COUNT(*) AS remaining_rows
      FROM monitoring.fact_site_event
      WHERE event_start_timestamp <= '$END'::timestamptz
        AND event_end_timestamp >= '$START'::timestamptz;"
```

- Delete and replace the local dump files for August 16 and 17
```bash
DUMP_BASE="/~/data/1786838400_1787011199_dump"
rm -rf "$DUMP_BASE"
mkdir -p "$DUMP_BASE"

# Publisher filter (CSV); users.db is authoritative unless EMAILS is already set.
if [[ -n "${EMAILS:-}" ]]; then
  :
elif [[ -f "_auth_server/users.db" ]]; then
  EMAILS="$(python3 _auth_server/role_admin.py publish-emails)"
else
  echo "Error: EMAILS not set and _auth_server/users.db not found." >&2
  exit 1
fi

EMAILS_JSON_ARRAY=$(
  printf '%s' "$EMAILS" | awk -F',' '
    BEGIN { printf "[" }
    {
      for (i=1; i<=NF; i++) {
        gsub(/^[ \t]+|[ \t]+$/, "", $i)
        if ($i != "") {
          if (n++) printf ","
          gsub(/"/, "\\\"", $i)
          printf "\"%s\"", $i
        }
      }
    }
    END { printf "]" }
  '
)

MONGO_QUERY="{\"timestamp\":{\"\$gte\":\"$START\",\"\$lte\":\"$END\"},\"publisher_email\":{\"\$in\":$EMAILS_JSON_ARRAY}}"

docker compose exec -T metrics-db \
     mongoexport --db metricsdb --collection metrics \
     --query "$MONGO_QUERY" \
     --type=json --out /dump/metrics.jsonl

mkdir -p "$DUMP_BASE/01_mongo"
docker cp "$(docker compose ps -q metrics-db):/dump/metrics.jsonl" "$DUMP_BASE/01_mongo/"
```

- Convert the Mongo export into CNR envelopes
```bash
mkdir -p "$DUMP_BASE/02_dump_processed/"

./bin/python ./scripts/batch_submit_cnr/process_dump.py "$DUMP_BASE/01_mongo/metrics.jsonl" \
  --emails "$EMAILS" \
  --out-dir "$DUMP_BASE/02_dump_processed" \
  --cache-granularity-s 86400
```

- Resubmit the replacement envelopes into CNR SQL
```bash
source bin/activate
pip install -q psycopg2-binary==2.9.10

python3 scripts/batch_submit_cnr/load_envelopes_direct_cnr.py \
  "$DUMP_BASE"/02_dump_processed/envelopes_*.jsonl \
  --batch-size 5000
```

- Check the reloaded CNR SQL rows for August 16 and 17
```bash
PGPASSWORD="$CNR_POSTEGRESQL_PASSWORD" psql \
  -h "$CNR_HOST" -p "${CNR_POSTEGRESQL_PORT:-5432}" \
  -U "$CNR_USER" -d "$CNR_GD_DB" -P pager=off \
  -c "SELECT COUNT(*) AS reloaded_rows
      FROM monitoring.fact_site_event
      WHERE event_start_timestamp <= '$END'::timestamptz
        AND event_end_timestamp >= '$START'::timestamptz;"
```

- Reconstruct the materialized views
```bash
./scripts/pre_aggregate_sql.sh
```
