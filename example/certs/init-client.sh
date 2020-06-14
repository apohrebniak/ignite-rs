#!/bin/sh

openssl genrsa -out client.key 2048 &&

echo "
[ req ]
prompt = no
distinguished_name = dn
req_extensions = v3_req

[ dn ]
CN=localhost

[ v3_req ]
subjectAltName = @alt_names

[alt_names]
[alt_names]
DNS.1 = localhost
IP.1 = 127.0.0.1
IP.1 = 127.0.0.1
" > csr.conf &&

openssl req -new -key client.key -out client.csr -config csr.conf &&

openssl x509 -req \
  -CA ca.crt \
  -CAkey ca.key \
  -CAcreateserial \
  -in client.csr \
  -days 365 \
  -out client.crt \
  -extensions v3_req \
  -extfile csr.conf &&

cat client.key client.crt > client.pem &&

echo "SUCCESS"
