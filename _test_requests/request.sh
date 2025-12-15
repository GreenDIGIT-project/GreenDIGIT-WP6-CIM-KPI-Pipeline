TOKEN="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJnb25jYWxvLmZlcnJlaXJhQHN0dWRlbnQudXZhLm5sIiwiaXNzIjoiZ3JlZW5kaWdpdC1sb2dpbi11dmEiLCJpYXQiOjE3NjU3ODM4NzcsIm5iZiI6MTc2NTc4Mzg3NywiZXhwIjoxNzY1ODcwMjc3fQ.DtWmHu0TY3qcyLse3AJMBXKms0WdrlHH4_nto3e9F6Y"

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
#   -d @_test_requests/01_raw.json12