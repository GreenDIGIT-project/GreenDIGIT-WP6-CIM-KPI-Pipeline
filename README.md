## 🌱🌍♻️ GreenDIGIT WP6.1 CIM & KPI Pipeline

### GreenDIGIT Main Page: https://greendigit-cim.sztaki.hu

### GreenDIGIT GitHub Organisation: https://github.com/GreenDIGIT-project

### Overview

This is a configuration repository to spin-up the pipeline used in WP6 to ingest, process and publish metrics using CIM unified namespaces and integrated .

Related repositories:
- [GreenDIGIT-project](https://github.com/GreenDIGIT-project)
- [GreenDIGIT-CIM](https://github.com/g-uva/GreenDIGIT-CIM)
- [GreenDIGIT-AuthServer](https://github.com/g-uva/GreenDIGIT-AuthServer)
- [GreenDIGIT-SQLAdapter](https://github.com/g-uva/GreenDIGIT-SQLAdapter)
- [GreenDIGIT-KPIService](https://github.com/g-uva/GreenDIGIT-WP6-KPI-Service)

*This work is funded from the European Union’s Horizon Europe research and innovation programme through the [GreenDIGIT project](https://greendigit-project.eu/), under the grant agreement No. [101131207](https://cordis.europa.eu/project/id/101131207)*.

<div style="display:flex;align-items:center;width:100%;">
  <img src="static/EN-Funded-by-the-EU-POS-2.png" alt="EU Logo" width="250px">
  <img src="static/cropped-GD_logo.png" alt="GreenDIGIT Logo" width="110px" style="margin-right:100px">
</div>


## To install on-premises
1. Create a `.env` file (minimum required keys)

```env
# Auth server
JWT_GEN_SEED_TOKEN=<generate-a-strong-random-secret>
JWT_TOKEN=<service-token-for-internal-calls>
CNR_INTERNAL_TOKEN=<independent-random-token-for-auth-to-sql-adapter>

# CI provider credentials
CI_PROVIDER=wattnet
WATTNET_EMAIL=<wattnet-account-email>
WATTNET_PASSWORD=<wattnet-account-password>
ELECTRICITYMAPS_TOKEN=<electricitymaps-token>

# CNR SQL adapter / Grafana datasource
CNR_HOST=<postgres-host>
CNR_USER=<postgres-user>
CNR_POSTEGRESQL_PASSWORD=<postgres-password>
CNR_GD_DB=<postgres-db-name>
CNR_SQL_FORWARD_URL=http://sql-adapter:8033/cnr-sql-adapter
CNR_PUBLIC_USER=<restricted-public-dashboard-postgres-user>
CNR_PUBLIC_PASSWORD=<restricted-public-dashboard-postgres-password>

# Grafana admin
GRAFANA_ADMIN_USER=<grafana-admin-user>
GRAFANA_ADMIN_PASSWORD=<grafana-admin-password>
GRAFANA_PUBLIC_ADMIN_USER=<public-grafana-admin-user>
GRAFANA_PUBLIC_ADMIN_PASSWORD=<public-grafana-admin-password>

# Public landing page links
PUBLIC_DASHBOARD_PATH=/public-dashboards
PUBLIC_DASHBOARD_URL=/public-dashboards
METRICS_FORM_URL=https://forms.gle/uYvEBGPvaiGW1rDDA
EGI_FEDERATION_REGISTRY_URL=https://aai.egi.eu/auth/realms/id/account/#/enroll?groupPath=/vo.greendigit.egi.eu

# EGI Check-in OIDC login
EGI_OIDC_ISSUER=https://aai.egi.eu/auth/realms/egi
EGI_OIDC_CLIENT_ID=<client-id-from-egi-federation-registry>
EGI_OIDC_CLIENT_SECRET=<client-secret-if-issued>
EGI_OIDC_REDIRECT_URI=https://greendigit-cim.sztaki.hu/auth/callback
EGI_OIDC_SCOPE="openid email profile eduperson_entitlement"
EGI_REQUIRED_ENTITLEMENT=<exact-entitlement-issued-by-egi-check-in>
# Optional alternative/additional check when EGI releases a groups claim:
EGI_REQUIRED_GROUP=<exact-group-issued-by-egi-check-in>
```

For the public Grafana instance, prefer a restricted PostgreSQL user that can only
read `monitoring.v_public_dashboard_15m` and
`monitoring.v_public_dashboard_resource_listing`. If `CNR_PUBLIC_USER` exists,
`scripts/pre_aggregate_sql.sh` grants those read permissions during refresh.

2. Install Nginx + TLS certificate and use this reverse-proxy example

```nginx
server {
    listen 80;
    server_name greendigit-cim.sztaki.hu;
    return 301 https://$host$request_uri;
}

server {
    listen 443 ssl http2;
    server_name greendigit-cim.sztaki.hu;

    ssl_certificate /etc/letsencrypt/live/greendigit-cim.sztaki.hu/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/greendigit-cim.sztaki.hu/privkey.pem;

    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;

    location /gd-cim-api/ {
        proxy_pass http://127.0.0.1:8000/;
    }

    location /gd-kpi-api/ {
        proxy_pass http://127.0.0.1:8011/;
    }

    location /cnr-sql-adapter {
        proxy_pass http://127.0.0.1:8033/cnr-sql-adapter;
    }

    location = / {
        proxy_pass http://127.0.0.1:8044/;
    }

    location /landing {
        proxy_pass http://127.0.0.1:8044/landing;
    }

    location /auth/ {
        proxy_pass http://127.0.0.1:8044/auth/;
    }

    location = /public-dashboards {
        proxy_pass http://127.0.0.1:8044/public-dashboards;
    }

    location /public-dashboards/ {
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_pass http://127.0.0.1:8044/public-dashboards/;
    }

    location /metricsdb-dashboard/v1/charts/ {
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_pass http://127.0.0.1:8044/metricsdb-dashboard/v1/charts/;
    }
}
```

3. Install Docker and start services

```bash
docker compose up -d --build
```

## API notes

The CIM FastAPI documentation is exposed at `/gd-cim-api/v1/docs`.

Metrics read/delete endpoints currently available:

- `GET /gd-cim-api/v1/cim-records` lists raw records stored in the internal MongoDB for the authenticated user.
- `GET /gd-cim-api/v1/cim-records/count` counts those internal MongoDB records.
- `POST /gd-cim-api/v1/cim-db/delete` deletes internal MongoDB records for the authenticated user within a time window and matching `filter_key` expressions.
- `GET /gd-cim-api/v1/cnr-records` lists CNR SQL records filtered by `site_id`, `vo`, `activity`, and time window.
- `GET /gd-cim-api/v1/cnr-records/count` counts those CNR SQL records.
- `POST /gd-cim-api/v1/cnr-db/delete` is disabled.

Example request snippets are available in `scripts/example-edit-metrics.sh` and `scripts/example_requests/example-request-metrics.sh`.

Notes:

- The internal MongoDB endpoints are scoped to the authenticated user via `publisher_email`.
- The CNR SQL endpoints are authenticated, but the current SQL filtering is based on the supplied dimensions (`site_id`, `vo`, `activity`, time window). They are not yet enforced by user ownership in SQL.

## Partner onboarding

We use this checklist when adding a new partner that will submit metrics and view the GreenDIGIT dashboards.

1. Collect the partner email address

We ask the partner which email they will use for API access and dashboard login. We use the same email in the allowlist files and tell the partner to register or request a token with that exact address.

2. Add the email to the correct allowlist

There are two plain-text allowlists in this repository:

- `submit_emails.txt` grants the `publish` role. This allows the user to call `POST /gd-cim-api/v1/submit` and includes their submitted metrics in the nightly publication flow to the CNR MetricsDB.
- `dashboards_emails.txt` grants the `dashboards_view` role. This allows the user to access the private Grafana dashboards at `/metricsdb-dashboard/v1/charts/`.

Most data-providing partners need both roles, so add their email to both files:

```bash
partner_email="partner@example.org"
printf '%s\n' "$partner_email" >> submit_emails.txt
printf '%s\n' "$partner_email" >> dashboards_emails.txt
```

The auth service creates the local account and assigns the matching roles the first time the partner logs in or requests a token. If the account already exists and needs immediate access, grant the roles directly:

```bash
scripts/manage-user-role.sh add partner@example.org publish
scripts/manage-user-role.sh add partner@example.org dashboards_view
```

3. Ask the partner to get an API token

The partner chooses their own password on first token request. The token is valid for 24 hours.

```bash
export GREEN_DIGIT_BASE="https://greendigit-cim.sztaki.hu"
export CIM_EMAIL="partner@example.org"
export CIM_PASSWORD="<partner-chosen-password>"

export JWT_TOKEN="$(
  curl -sS -G "$GREEN_DIGIT_BASE/gd-cim-api/v1/token" \
    --data-urlencode "email=$CIM_EMAIL" \
    --data-urlencode "password=$CIM_PASSWORD" \
  | jq -r '.access_token'
)"
```

4. Submit metrics

Submit a JSON metric object with the Bearer token. `group` is required and is
validated against the publisher's current server-side memberships. The server
continues to derive `publisher_email` from the token; a payload value cannot
override it.
Exporters that expose command-line options should require
`--group="greendigit"` and serialize that value into the top-level JSON
`group` field shown below.

```bash
curl -sS -X POST "$GREEN_DIGIT_BASE/gd-cim-api/v1/submit" \
  -H "Authorization: Bearer $JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
      "group": "greendigit",
      "SiteName": "IFCA-LCG2",
      "EnergyWh": 82.79,
      "Work": 96.58,
      "StartExecTime": "2025-09-15T18:00:01Z",
      "EndExecTime": "2025-09-16T00:00:01Z",
      "Status": "running",
      "Owner": "openrisknet.org",
      "ExecUnitID": "77666a0e-5aac-409d-befd-e427386b554b",
      "WallClockTime_s": 15853,
      "CpuDuration_s": 7996,
      "CloudType": "openstack",
      "CloudComputeService": "ifca"
    }'
```

5. Fetch PUE and carbon intensity

Use the KPI API with the same token. Fetch PUE by site name:

```bash
curl -sS -X POST "$GREEN_DIGIT_BASE/gd-kpi-api/v1/pue" \
  -H "Authorization: Bearer $JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{ "site_name": "IFCA-LCG2" }'
```

Fetch carbon intensity (CI) for a location and time window. Include `energy_wh` when you also want the API to calculate carbon footprint:

```bash
curl -sS -X POST "$GREEN_DIGIT_BASE/gd-kpi-api/v1/ci" \
  -H "Authorization: Bearer $JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -H "aggregate: true" \
  -d '{
    "lat": 43.471,
    "lon": -3.799,
    "start": "2025-09-15T18:00:01Z",
    "end": "2025-09-16T00:00:01Z",
    "pue": 1.5,
    "energy_wh": 82.79
  }'
```

The CI response includes `ci_gco2_per_kwh`, `pue`, `effective_ci_gco2_per_kwh`, and, when `energy_wh` was provided, `cfp_g` and `cfp_kg`.

6. Show the dashboards

Send the partner to the GreenDIGIT landing page:

- Main page: https://greendigit-cim.sztaki.hu
- Private Grafana dashboards: https://greendigit-cim.sztaki.hu/metricsdb-dashboard/v1/charts/
- Public dashboards: https://greendigit-cim.sztaki.hu/public-dashboards

For private Grafana access, the partner must have the `dashboards_view` role. They can log in through the dashboard login page using the same email and password they used for the API token. If EGI Check-in is configured for the deployment, they can also use the EGI dashboard login flow, provided their EGI account releases the configured GreenDIGIT entitlement or group.

## User management and role access

`_auth_server/users.db` is the source of truth for role-based access:

- `publish` allows `POST /gd-cim-api/v1/submit` and inclusion in the nightly CNR publication run by `scripts/batch_submit_cnr/batch_submit_cnr.sh`.
- `dashboards_view` allows private Grafana access at `/metricsdb-dashboard/v1/charts/`.
- `admin` allows platform-wide role and group administration.

Roles grant capabilities; groups scope metric visibility. `public` is the
default membership and dashboard-list users are bootstrapped into `greendigit`.
Group super-users can change membership only in groups they supervise.

The allowlist files are used to grant default roles:

- `submit_emails.txt` allows first registration/login and grants `publish`.
- `dashboards_emails.txt` allows first registration/login and grants `dashboards_view`.

For a new upload/publish user, add the email to `submit_emails.txt`. For a dashboard-only user, add the email to `dashboards_emails.txt`. If the user needs both capabilities, add the email to both files. On first successful login or token request, the auth service creates the user in `_auth_server/users.db` and grants the roles from these files.

EGI Check-in dashboard access is validated separately in the Grafana auth proxy.
Set `EGI_REQUIRED_ENTITLEMENT` to the exact entitlement value released by Check-in
for the GreenDIGIT/EIMPS dashboard role. If Check-in releases a `groups` claim
instead, set `EGI_REQUIRED_GROUP`. When either variable is set, `/auth/callback`
rejects users missing that claim before creating the local dashboard session. If
both variables are empty, EGI login fails closed with a configuration error.

For an existing user, adding the email to one of these files grants the matching role the next time that user successfully logs in or requests a token. You can also grant a role immediately with the management script:

```bash
scripts/manage-user-role.sh add user@example.org publish
```

Use the script for immediate manual role changes:

```bash
scripts/manage-user-role.sh add user@example.org dashboards_view
scripts/manage-user-role.sh remove user@example.org publish
scripts/manage-user-role.sh list user@example.org
scripts/manage-user-role.sh group create example-partner
scripts/manage-user-role.sh group add-user example-partner user@example.org
scripts/manage-user-role.sh group promote-super example-partner user@example.org
scripts/manage-user-role.sh user show user@example.org
scripts/manage-user-role.sh role add admin@example.org admin
```

The script only works for users that already exist in `_auth_server/users.db`. If it prints `User not found in users.db`, add the email to `submit_emails.txt`, `dashboards_emails.txt`, or both, and have the user log in once with their chosen password.

Bootstrap existing users once, or re-run idempotently:

```bash
scripts/bootstrap-user-roles.sh
```

Removing an email from either file does not remove an existing database role. Use `scripts/manage-user-role.sh remove ...` to revoke a role from an existing user.

## Private metric-group deployment

Do not run these commands against production until the dry-run counts and
ambiguous publishers have been reviewed. Back up SQLite, MongoDB and PostgreSQL
first.

```bash
# 1. Idempotent SQLite schema/default groups/allowlist membership
scripts/manage-user-role.sh bootstrap

# 2. Add the CNR columns (DDL only; no row backfill)
PGPASSWORD="$CNR_POSTEGRESQL_PASSWORD" psql \
  -h "$CNR_HOST" -p "${CNR_POSTEGRESQL_PORT:-5432}" \
  -U "$CNR_USER" -d "$CNR_GD_DB" -v ON_ERROR_STOP=1 \
  -f migration/metric_groups_postgres.sql

# 3. Report proposed MongoDB and PostgreSQL assignments
python3 migration/backfill_metric_groups.py

# 4. After resolving every ambiguous publisher, apply restartably
python3 migration/backfill_metric_groups.py --apply

# 5. Rebuild aggregates; public views now include only group_name = 'public'
scripts/pre_aggregate_sql.sh
```

Set the same strong `CNR_INTERNAL_TOKEN` on the auth API and SQL adapter and
restart those services. The SQL query API rejects direct callers, accepts only
server-resolved group lists, and treats ungrouped legacy rows as invisible.
Membership/role changes reach the Grafana proxy within
`AUTH_VERIFY_CACHE_TTL_S` (30 seconds by default).

Important: the repository's legacy private Grafana datasource connects
directly to PostgreSQL with a shared account. A shared connection has no trusted
viewer identity and therefore cannot enforce per-user groups. It must not be
used for private deployment. Route private dashboard queries/exports through
the group-aware `/v1/cnr-records` service (or deploy PostgreSQL RLS with a
trusted per-request session context) before enabling private dashboards. The
separate public Grafana remains limited to the deliberately anonymised public
views and restricted `CNR_PUBLIC_USER`.

## License

This repository is licensed under the [Apache License 2.0](LICENSE).

## Contact & Questions
**Contact:**  
For questions or to request access, please contact the GreenDIGIT UvA team:
- Gonçalo Ferreira: g.j.teixeiradepinhoferreira@uva.nl
