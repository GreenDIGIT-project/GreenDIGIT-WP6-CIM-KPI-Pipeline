#!/bin/bash

# This generates the secrets and certification for distributed communication between servers

LOCAL_CERT="./.cert"
LOCAL_SECRET="./.secrets"

mkdir -p $LOCAL_CERT $LOCAL_SECRET
openssl req -newkey rsa:4096 -nodes -keyout "$LOCAL_CERT/mongo.key" -x509 -days 365 \
  -out "$LOCAL_CERT/mongo.crt" -subj "/CN=<hostname>"
cat "$LOCAL_CERT/mongo.crt" "$LOCAL_CERT/mongo.key" > "$LOCAL_CERT/mongo.pem"
cp  "$LOCAL_CERT/mongo.crt" "$LOCAL_CERT/ca.pem"
chmod 600 "$LOCAL_CERT/mongo.key" "$LOCAL_CERT/mongo.pem" "$LOCAL_CERT/ca.pem"

# shared RS keyFile (generate once, copy to the other host)
openssl rand -base64 756 > "$LOCAL_SECRET/mongo-keyfile"
chmod 600 "$LOCAL_SECRET/mongo-keyfile"
