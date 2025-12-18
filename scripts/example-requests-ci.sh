TOKEN="$TOKEN"

curl -X POST "http://localhost:8011/ci" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "aggregate: true" \
  -d '{
    "lat": 52.3702,
    "lon": 4.8952,
    "pue": 1.7,
    "energy_wh": 1000,
    "metric_id": "example-metric"
  }'

WATTNET_TOKEN="$WATTNET_TOKEN"

curl -s -X GET "https://api.wattnet.eu/v1/footprints?lat=45.071&lon=7.652&footprint_type=carbon&start=2024-05-01T10:30:00Z&end=2024-05-01T13:30:00Z&aggregate=true" \
  -H "Authorization: Bearer $WATTNET_TOKEN" \
  -H "Accept: application/json" \
  -H "aggregate: true"

  