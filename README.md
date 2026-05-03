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
ADMIN_EMAILS=admin@example.org
JWT_TOKEN=<service-token-for-internal-calls>

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

# Grafana admin
GRAFANA_ADMIN_USER=<grafana-admin-user>
GRAFANA_ADMIN_PASSWORD=<grafana-admin-password>
```

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

## License

This repository is licensed under the [Apache License 2.0](LICENSE).

## Contact & Questions
**Contact:**  
For questions or to request access, please contact the GreenDIGIT UvA team:
- Gonçalo Ferreira: g.j.teixeiradepinhoferreira@uva.nl
