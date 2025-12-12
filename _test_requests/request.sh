TOKEN="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJnb25jYWxvLmZlcnJlaXJhQHN0dWRlbnQudXZhLm5sIiwiaXNzIjoiZ3JlZW5kaWdpdC1sb2dpbi11dmEiLCJpYXQiOjE3NjU1Mjc3ODQsIm5iZiI6MTc2NTUyNzc4NCwiZXhwIjoxNzY1NjE0MTg0fQ.9k4RpwEjtCuZGvFaXaLuJOndLWwNQUqhiihxzzh93bc"

# First submission to AuthServer.
curl -X POST https://greendigit-cim.sztaki.hu/gd-cim-api/submit \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d @_test_requests/01_raw.json