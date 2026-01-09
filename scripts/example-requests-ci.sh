JWT_TOKEN=''

curl -X POST "http://localhost:8011/ci" \
  -H "Authorization: Bearer $JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -H "aggregate: true" \
  -d '{"lat":45.071,"lon":7.652,"start":"2024-05-01T10:30:00Z","end":"2024-05-01T13:30:00Z","pue":1.7}'


WATTNET_TOKEN=''

curl -s -X GET "https://api.wattnet.eu/v1/footprints?lat=45.071&lon=7.652&footprint_type=carbon&start=2024-05-01T10:30:00Z&end=2024-05-01T13:30:00Z&aggregate=false" \
  -H "Authorization: Bearer $WATTNET_TOKEN" \
  -H "Accept: application/json" \
  -H "aggregate: true"

  
JWT_TOKEN='eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJnb25jYWxvLmZlcnJlaXJhQHN0dWRlbnQudXZhLm5sIiwiaXNzIjoiZ3JlZW5kaWdpdC1sb2dpbi11dmEiLCJpYXQiOjE3Njc5MTMyNjYsIm5iZiI6MTc2NzkxMzI2NiwiZXhwIjoxNzY3OTk5NjY2fQ.J5kIIl55a8HXKOXIW5zIHIaCP3jqaF40pjHA7F605zg'

curl -v -H "Authorization: Bearer $JWT_TOKEN" "https://mc-a4.lab.uvalight.net/gd-cim-api/verify_token"
