#!/bin/bash
export TARGET="gd-sztaki-uva"

# Create / cd dir.
mkdir -p .cert/sztaki-uva
cd .cert/sztaki-uva

# Generate a private key
openssl genrsa -out "$TARGET.key" 4096
chmod 600 "$TARGET.key"

# Generate the public key
openssl req -new -key "$TARGET.key" -out "$TARGET.csr"

# Getting the information back.
openssl req -in $TARGET.csr -noout -text

# DN information from the CSR
openssl req -in "./$TARGET.csr" -noout -subject -nameopt compat

# For the emitted cert
openssl x509 -in .cert/"<cert_name>".pem -noout -subject -nameopt compat
 