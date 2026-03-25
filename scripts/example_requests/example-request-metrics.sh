#!/bin/bash

set -a; source .env; set +a

# Retrieve the JWT token
JWT_TOKEN=$(curl -G "https://greendigit-cim.sztaki.hu/gd-cim-api/v1/token" \
  --data-urlencode "email=goncalo.ferreira@student.uva.nl" \
  --data-urlencode "password=gongon" | jq -r ".access_token")

# Example submission
curl -X POST https://greendigit-cim.sztaki.hu/gd-cim-api/v1/submit \
  -H "Authorization: Bearer $JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d @_test_requests/01_raw_dirac.json

# Example /metrics/me with all optional parameters (site, time_window, limit)
# time_window format: "<start>--<end>" in ISO-8601 UTC
curl -G "https://greendigit-cim.sztaki.hu/gd-cim-api/v1/metrics/me" \
  -H "Authorization: Bearer $JWT_TOKEN" \
  --data-urlencode "site=EGI.SARA.nl" \
  --data-urlencode "time_window=2020-01-01T00:00:00Z--2030-01-01T00:00:00Z" \
  --data-urlencode "limit=5000"

# db.metrics.find({ publisher_email: "goncalo.ferreira@student.uva.nl" });
# # MongoDB delete
# db.metrics.deleteOne({ _id: ObjectId("693bd40d13276a55fc9c2c38") });

# curl -X POST https://greendigit-cim.sztaki.hu/gd-kpi-api/v1/submit \
#   -H "Authorization: Bearer $TOKEN" \
#   -H "Content-Type: application/json" \
#   -d @_test_requests/01_raw.json
