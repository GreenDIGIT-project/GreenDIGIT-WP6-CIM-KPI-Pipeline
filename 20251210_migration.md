## Migration notes SZTAKI 2025

### MongoDB Option 01 (dump DB and copy/paste to ne server)
> Establishing a connection between 145.100.131.14​ (UvA) and 193.225.251.153​ (SZTAKI).
```bash
# metricsdb
docker compose exec -it metrics-db \
  mongodump --db metricsdb --out /dump

# For the users we can directly copy the `users.db`
```
```bash
# Copy dumps outside of the container and to the new server
docker compose cp metrics-db:/dump ./mongo-dump # remove it afterwards.

# At the target server (SZTAKI)
# Copy the Dump
scp -r goncalo@mc-a4.lab.uvalight.net:/home/goncalo/GreenDIGIT-WP6-CIM-KPI-Pipeline/mongo-dump ./mongo-dump

# Copy the users.db directly (from the previous step)
scp goncalo@mc-a4.lab.uvalight.net:/home/goncalo/GreenDIGIT-WP6-CIM-KPI-Pipeline/users.db ./users.db
```
```bash
# On the new server (at the repo's root)
docker compose cp ./mongo-dump metrics-db:/dump

docker compose exec -it metrics-db \
  mongorestore --db metricsdb /dump/metricsdb

# For the users.db just spin-up cim-fastapi again.
```

### MongoDB Option 02 (Cross-Server Failover/Replica)
This one is the "best" option.
- Generate mongo-keyfile
- Set .env for host and role for MongoDB
```bash

```