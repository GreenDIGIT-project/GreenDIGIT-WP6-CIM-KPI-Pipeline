curl -sS -X POST 'http://localhost:8000/gd-cim-api/v1/submit-cim' \
  -H "Authorization: Bearer $JWT_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"publisher_email":"atsareg@in2p3.fr","start":"2026-02-06T00:00:00Z","end":"2026-02-08T00:00:00Z","limit_docs":10}'

# To automatically extract the event_ids

curl -sS -X POST 'http://localhost:8000/gd-cim-api/v1/submit-cim' \
  -H "Authorization: Bearer $JWT_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"publisher_email":"atsareg@in2p3.fr","start":"2026-02-06T00:00:00Z","end":"2026-02-08T00:00:00Z","limit_docs":10}' \
| jq -r '.cim_response.results[].cnr_response.event_id'

curl -sS 'http://localhost:8033/get-cnr-entry/568339' | jq

# Small request
curl -sS -X POST 'http://localhost:8000/gd-cim-api/v1/submit-cim' \
  -H "Authorization: Bearer $JWT_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"publisher_email":"atsareg@in2p3.fr","start":"2026-01-16T12:00:00Z","end":"2026-01-16T12:05:00Z","limit_docs":500}'

START="2025-09-01T00:00:00Z"
END="2025-12-31T23:59:59Z"
t="$START"

while [[ "$t" < "$END" ]]; do
  t2="$(date -u -d "$t + 15 minutes" +%Y-%m-%dT%H:%M:%SZ)"
  [[ "$t2" > "$END" ]] && t2="$END"

  echo "window $t -> $t2" >&2 # For logs

  curl -sS -X POST 'http://localhost:8000/gd-cim-api/v1/submit-cim' \
    -H "Authorization: Bearer $JWT_TOKEN" \
    -H 'Content-Type: application/json' \
    -d "{\"publisher_email\":\"atsareg@in2p3.fr\",\"start\":\"$t\",\"end\":\"$t2\",\"end_inclusive\":false,\"limit_docs\":1000}"

  echo
  t="$t2"
done

# Until Feb 07
# const start = new Date();
# start.setUTCMonth(start.getUTCMonth() - 6);

# const res = db.metrics.aggregate([
#   { $project: {
#       publisher_email: 1,
#       timestamp: 1,
#       ts: { $dateFromString: { dateString: "$timestamp", onError: null, onNull: null } }
#   }},
#   { $match: { ts: { $ne: null, $gte: start } } },
#   { $sort: { ts: 1 } },

#   { $setWindowFields: {
#       sortBy: { ts: 1 },
#       output: {
#         count_30m: {
#           $count: {},
#           window: { range: [-30, 0], unit: "minute" }
#         }
#       }
#   }},

#   { $sort: { count_30m: -1 } },
#   { $limit: 1 },
#   { $project: { _id: 0, ts: 1, timestamp: 1, publisher_email: 1, count_30m: 1 } }
# ], { allowDiskUse: true }).toArray();

# printjson(res[0]);

# {
#   timestamp: '2026-02-08T08:08:31.837506+00:00',
#   publisher_email: 'atsareg@in2p3.fr',
#   ts: ISODate('2026-02-08T08:08:31.837Z'),
#   count_30m: 9985
# }