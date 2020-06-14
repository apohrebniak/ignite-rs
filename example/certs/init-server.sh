#!/bin/bash

PASSWORD=password &&

openssl genrsa -out server.key 2048 &&

echo "
[ req ]
default_bits = 2048
prompt = no
default_md = sha256
distinguished_name = dn
req_extensions = v3_req

[ dn ]
CN=localhost

[ v3_req ]
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
IP.1 = 127.0.0.1
" > csr.conf &&

openssl req -new -key server.key -out server.csr -config csr.conf &&

openssl x509 -req \
  -CA ca.crt \
  -CAkey ca.key \
  -CAcreateserial \
  -in server.csr \
  -days 3650 \
  -out server.crt \
  -extensions v3_req \
  -extfile csr.conf &&

openssl pkcs12 \
  -export \
  -inkey server.key \
  -in server.crt \
  -out server.p12 \
  -passout pass:${PASSWORD} &&

keytool \
  -keystore server.keystore.jks \
  -alias CARoot \
  -import \
  -file ca.crt \
  -storepass ${PASSWORD} \
  -noprompt &&

keytool \
  -destkeystore server.keystore.jks \
  -deststorepass ${PASSWORD} \
  -importkeystore \
  -srckeystore server.p12 \
  -srcstoretype PKCS12 \
  -srcstorepass ${PASSWORD} \
  -destkeypass ${PASSWORD} &&

keytool \
  -keystore server.truststore.jks \
  -alias CARoot \
  -import \
  -file ca.crt \
  -storepass ${PASSWORD} \
  -noprompt &&

echo "SUCCESS"
