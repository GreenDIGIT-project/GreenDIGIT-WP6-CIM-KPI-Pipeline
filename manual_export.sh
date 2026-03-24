LAST_EXPORTED_FILE="scripts/batch_submit_cnr/last_exported.txt"
START="2026-01-01T23:59:59Z"
END="2026-03-24T23:59:59Z"
EMAILS_JSON_ARRAY="[\"biagio.peccerillo@isti.cnr.it\",\"jerome.pansanel@iphc.cnrs.fr\"]"
MONGO_QUERY="{\"timestamp\":{\"\$gte\":\"$START\",\"\$lte\":\"$END\"},\"publisher_email\":{\"\$in\":$EMAILS_JSON_ARRAY}}"

DUMP_BASE="dump_temp"
mkdir -p $DUMP_BASE

docker compose exec -T metrics-db \
     mongoexport --db metricsdb --collection metrics \
     --query "$MONGO_QUERY" \
     --type=json --out /dump/metrics.jsonl

mkdir -p "$DUMP_BASE/01_mongo"
docker cp "$(docker compose ps -q metrics-db):/dump/metrics.jsonl" "$DUMP_BASE/01_mongo/"

mkdir -p "$DUMP_BASE/02_dump_processed/"
# 2) Convert Mongo export -> CNR envelopes JSONL (filtered) (CIM-compatible)
./bin/python ./scripts/batch_submit_cnr/process_dump.py "$DUMP_BASE/01_mongo/metrics.jsonl" \
  --emails "$EMAILS" \
  --out-dir "$DUMP_BASE/02_dump_processed" \
  --cache-granularity-s 86400

source bin/activate
pip install -q psycopg2-binary==2.9.10

python3 scripts/batch_submit_cnr/load_envelopes_direct_cnr.py \
  "$DUMP_BASE"/02_dump_processed/envelopes_*.jsonl \
  --batch-size 5000

printf "%s\n" "$END" > "$LAST_EXPORTED_FILE"
echo "[batch_submit_cnr] Updated $LAST_EXPORTED_FILE to $END"

./scripts/pre_aggregate_sql.sh