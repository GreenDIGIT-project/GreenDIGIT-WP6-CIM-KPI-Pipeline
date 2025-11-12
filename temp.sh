# 1) fetch PUE and location metadata
curl -sS http://localhost:8011/get-pue \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"site_name":"AEGIS01-IPB-SCL"}'

# 2) fetch carbon intensity using the coordinates/PUE from the first call
curl -sS http://localhost:8011/get-ci \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"lat":44.8176,"lon":20.4569,"pue":1.4,"energy_wh":2800}'

WATTNET_TOKEN="eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJubkxEVEtJVkJBN3o5N3Zrbi1RWE5zQXFwSUNVQlQtam5fZ25mX2Z4TWdZIn0.eyJleHAiOjE3NjIyOTcyNjgsImlhdCI6MTc2MjIxMDg2OCwianRpIjoidHJydGNjOmQ2NWQwOTY0LTY4MjQtZjRkMi0yMDA5LTg5OTg2OWU5MjNiNCIsImlzcyI6Imh0dHBzOi8vYXV0aC53YXR0bmV0LmV1L3JlYWxtcy93YXR0bmV0IiwiYXVkIjpbIm9hdXRoMi1wcm94eSIsImFjY291bnQiXSwic3ViIjoiOTQ0NWUzODYtOGQzOS00ZDYxLTlmYzctZWEwMmIzNGY0MTEwIiwidHlwIjoiQmVhcmVyIiwiYXpwIjoib2F1dGgyLXByb3h5IiwiYWNyIjoiMSIsImFsbG93ZWQtb3JpZ2lucyI6WyJodHRwczovL2FwaS53YXR0bmV0LmV1Il0sInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJkZWZhdWx0LXJvbGVzLXdhdHRwcmludCIsIm9mZmxpbmVfYWNjZXNzIiwidW1hX2F1dGhvcml6YXRpb24iXX0sInJlc291cmNlX2FjY2VzcyI6eyJhY2NvdW50Ijp7InJvbGVzIjpbIm1hbmFnZS1hY2NvdW50IiwibWFuYWdlLWFjY291bnQtbGlua3MiLCJ2aWV3LXByb2ZpbGUiXX19LCJzY29wZSI6ImVtYWlsIHByb2ZpbGUiLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwiY2xpZW50SG9zdCI6IjE3Mi4xNi4zNS4xMjEiLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJzZXJ2aWNlLWFjY291bnQtb2F1dGgyLXByb3h5IiwiY2xpZW50QWRkcmVzcyI6IjE3Mi4xNi4zNS4xMjEiLCJjbGllbnRfaWQiOiJvYXV0aDItcHJveHkifQ.10uxdOeQucMw_SNQzoOWAvI__ee8n1UoD5hG01JCpxi0slIdyoUi4fQrJIh6urgZzjbV9Y29MBpw_h7lwUwsiHsf_mQ-qQ5TtNMjR-oEasYdPawJJGSOu_jIXKyZ0gg8wDMw3r0n1sb9cGy_f18Dd9W2aYrG3J_gS5yOyfh9SpW6QFjvr3n--c72a3J5ddhJvb7mRWxgn2VuFlj9-Inovzd-3UUjVgv-zPuxgOPF0aiQ08btGzqrlPnIGsYbvu6Ghg0kNSqMHmtppgK_vHMZGimUbiLjTBPMBnq_NqToDpGU_L_5g5ACGOgbNwUnZgsGfWP7WXjHKLK1DIKEa0YJUg"
curl -sS -D - \
  "https://api.wattnet.eu/v1/footprints?lat=44.8176&lon=20.4569&footprint_type=carbon&start=2025-11-03T13:53:32Z&end=2025-11-03T16:53:32Z&aggregate=true" \
  -H "Authorization: Bearer $WATTNET_TOKEN" \
  -H "Accept: application/json" \
  -o ./wattnet-response.json

curl -sS "https://api.wattnet.eu/v1/footprints?lat=44.8176&lon=20.4569&start=2024-06-01T11:00:00Z&end=2024-06-01T14:00:00Z&footprint_type=carbon&aggregate=true" \
  -H "Authorization: Bearer $WATTNET_TOKEN" \
  -H "Accept: application/json"


TOKEN="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJnb25jYWxvLmZlcnJlaXJhQHN0dWRlbnQudXZhLm5sIiwiaXNzIjoiZ3JlZW5kaWdpdC1sb2dpbi11dmEiLCJpYXQiOjE3NjIyMTA4NjcsIm5iZiI6MTc2MjIxMDg2NywiZXhwIjoxNzYyMjk3MjY3fQ.H-HS3OXk_WhWlpnLDwNoJ42EpWN-c1T-0_h44MvcKFg"

curl -sS "https://mc-a4.lab.uvalight.net/gd-ci-api/ci" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"lat":52.0,"lon":5.0,"pue":1.4}'

curl -sS https://mc-a4.lab.uvalight.net/gd-ci-api/pue \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"site_name":"IFCA-LCG2"}'

IFCA-LCG2
CIEMAT-LCG2
CESGA
PIC