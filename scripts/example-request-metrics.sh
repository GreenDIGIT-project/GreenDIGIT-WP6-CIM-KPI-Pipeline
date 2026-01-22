#!/bin/bash

# Retrieve the JWT token
JWT_TOKEN=$(curl -X GET "https://greendigit-cim.sztaki.hu/gd-cim-api/get-token" \
  -H "Content-Type: application/json" \
  -d "{ \"email\": \"goncalo.ferreira@student.uva.nl\", \"password\": \"gongon\"}" | jq -r ".access_token")

# Example submission
curl -X POST https://greendigit-cim.sztaki.hu/gd-cim-api/submit \
  -H "Authorization: Bearer $JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d @_test_requests/01_raw.json

# db.metrics.find({ publisher_email: "goncalo.ferreira@student.uva.nl" });
# # MongoDB delete
# db.metrics.deleteOne({ _id: ObjectId("693bd40d13276a55fc9c2c38") });

# curl -X POST https:///greendigit-cim.sztaki.hu/gd-cim-api/submit \
#   -H "Authorization: Bearer $TOKEN" \
#   -H "Content-Type: application/json" \
#   -d @_test_requests/01_raw.json