#!/bin/bash

cd /home/ubuntu/GreenDIGIT-WP6-CIM-KPI-Pipeline
openssl rand -base64 756 > .cert/mongo-keyfile
chmod 400 .cert/mongo-keyfile

# # This generates the secrets and certification for distributed communication between servers

# # 1) Run this on each server.
# LOCAL_CERT="./.cert"
# LOCAL_SECRET="./.secrets"

# mkdir -p $LOCAL_CERT $LOCAL_SECRET
# openssl req -newkey rsa:4096 -nodes -keyout "$LOCAL_CERT/mongo.key" -x509 -days 365 \
#   -out "$LOCAL_CERT/mongo.crt" -subj "/CN=<hostname>"
# cat "$LOCAL_CERT/mongo.crt" "$LOCAL_CERT/mongo.key" > "$LOCAL_CERT/mongo.pem"
# cp  "$LOCAL_CERT/mongo.crt" "$LOCAL_CERT/ca.pem"
# chmod 600 "$LOCAL_CERT/mongo.key" "$LOCAL_CERT/mongo.pem" "$LOCAL_CERT/ca.pem"

# # shared RS keyFile (generate once, copy to the other host)
# openssl rand -base64 756 > "$LOCAL_SECRET/mongo-keyfile"
# chmod 600 "$LOCAL_SECRET/mongo-keyfile"

# # 2) Then we must bundle and use the bundle-ca.pem:
# # Rename each of the mongo.crt -> serverA.mongo.crt
# # ... then:

# sudo cat "$LOCAL_CERT/serverA.mongo.crt" "$LOCAL_CERT/serverB.mongo.crt" > "$LOCAL_CERT/ca-bundle.pem"

# # 3) Ensure both the ca-bundle.pem and the ./.secrets/mongo-key are the same in both servers.
# # This is done manually for the moment, unfortunately. :)

# # 4) Change permissions and ownership of the certs/mongo-key
# sudo chmod +x ./.secrets/* ./.cert/*
# sudo chown 999:999 ./.secrets/mongo-keyfile