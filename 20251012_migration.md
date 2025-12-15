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
- Generate mongo-keyfile, save it to `.cert/mongo-keyfile`.
<!-- - Set .env for host and role for MongoDB -->
- Set up `docker compose up -d --build metrics-db` in the new server.
- Set up `docker compose up -d --build` on the legacy server
- Once the replica is done, change priority inside the DB.
```bash
cfg = rs.conf();
cfg.members[0].priority = 1;   // legacy now less preferred
cfg.members[1].priority = 2;   // new becomes preferred
rs.reconfig(cfg);
rs.stepDown(60);               // legacy steps down for 60s

# Check on the new
rs.status();
```


### Publisher (watching changes from inside of the CIM-MetricsDB)
Add a `_publisher/publisher.py` from inside of `cim-fastapi` to get a configurable module.