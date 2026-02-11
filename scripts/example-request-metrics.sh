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
  -d @_test_requests/01_raw.json

# Example DIRAC submission (stored in metrics-db-dirac)
curl -X POST https://greendigit-cim.sztaki.hu/gd-cim-api/v1/submit-dirac \
  -H "Authorization: Bearer $JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d @_test_requests/01_raw_dirac.json

# Example DIRAC delete by site and time window (URL-encoded timestamps)
SITE="<example_site>" # Just as example
START_END_ENC="2020-01-01T00%3A00%3A00Z--2023-02-01T00%3A00%3A00Z"
curl -X DELETE "https://greendigit-cim.sztaki.hu/gd-cim-api/v1/delete-dirac/$SITE/$START_END_ENC" \
  -H "Authorization: Bearer $JWT_TOKEN"
# Response:
# {"ok":true,"publisher_email":"goncalo.ferreira@student.uva.nl","site":"EGI.SARA.nl","start":"2020-01-01T00:00:00.000000+00:00","end":"2023-02-01T00:00:00.000000+00:00","deleted_count":1,"time_window_candidates":1,"remaining_count":0}

# db.metrics.find({ publisher_email: "goncalo.ferreira@student.uva.nl" });
# # MongoDB delete
# db.metrics.deleteOne({ _id: ObjectId("693bd40d13276a55fc9c2c38") });

# curl -X POST https://greendigit-cim.sztaki.hu/gd-kpi-api/v1/submit \
#   -H "Authorization: Bearer $TOKEN" \
#   -H "Content-Type: application/json" \
#   -d @_test_requests/01_raw.json
