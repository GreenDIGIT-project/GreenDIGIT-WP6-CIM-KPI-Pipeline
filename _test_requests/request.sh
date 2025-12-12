TOKEN="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJnb25jYWxvLmZlcnJlaXJhQHN0dWRlbnQudXZhLm5sIiwiaXNzIjoiZ3JlZW5kaWdpdC1sb2dpbi11dmEiLCJpYXQiOjE3NjU1NDk5MTAsIm5iZiI6MTc2NTU0OTkxMCwiZXhwIjoxNzY1NjM2MzEwfQ.Q5apoy7TOnufQRDco9vXbk8SKO0UH1NuhdJHpipHlkI"

# First submission to AuthServer.
curl -X POST https://greendigit-cim.sztaki.hu/gd-cim-api/submit \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d @_test_requests/01_raw.json

# db.metrics.find({ publisher_email: "goncalo.ferreira@student.uva.nl" });
# # MongoDB delete
# db.metrics.deleteOne({ _id: ObjectId("693bd40d13276a55fc9c2c38") });

# curl -X POST https:///mc-a4.lab.uvalight.net/gd-cim-api/submit \
#   -H "Authorization: Bearer $TOKEN" \
#   -H "Content-Type: application/json" \
#   -d @_test_requests/01_raw.json
