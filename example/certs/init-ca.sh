#!/bin/bash

openssl genrsa -out ca.key 2048 &&

openssl req -new -key ca.key -out ca.csr &&

openssl req -x509 -new -key ca.key -sha256 -days 1024 -out ca.crt &&

echo "SUCCESS"
