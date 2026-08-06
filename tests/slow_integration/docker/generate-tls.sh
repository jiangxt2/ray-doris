#!/usr/bin/env bash
set -euo pipefail

target=${1:?TLS target directory is required}
umask 077
mkdir -p "${target}"

openssl genrsa -out "${target}/ca.key" 2048
openssl req \
  -x509 \
  -new \
  -sha256 \
  -days 2 \
  -key "${target}/ca.key" \
  -out "${target}/ca.pem" \
  -subj "/CN=ray-doris slow integration CA" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign"

openssl genrsa -out "${target}/fe.key" 2048
openssl req \
  -new \
  -sha256 \
  -key "${target}/fe.key" \
  -out "${target}/fe.csr" \
  -subj "/CN=flight-proxy"

{
  echo "basicConstraints=critical,CA:FALSE"
  echo "keyUsage=critical,digitalSignature,keyEncipherment"
  echo "extendedKeyUsage=serverAuth"
  echo "subjectAltName=DNS:flight-proxy,DNS:fe,IP:172.31.128.2,IP:172.31.128.6"
} > "${target}/fe.ext"

openssl x509 \
  -req \
  -sha256 \
  -days 2 \
  -in "${target}/fe.csr" \
  -CA "${target}/ca.pem" \
  -CAkey "${target}/ca.key" \
  -CAcreateserial \
  -extfile "${target}/fe.ext" \
  -out "${target}/fe.pem"

openssl pkcs12 -export -name doris_ssl_certificate -inkey "${target}/fe.key" \
  -in "${target}/fe.pem" -certfile "${target}/ca.pem" -out "${target}/fe.p12" \
  -passout pass:doris
openssl pkcs12 -export -nokeys -name doris_ssl_ca -in "${target}/ca.pem" \
  -out "${target}/mysql-ca.p12" -passout pass:doris
cat "${target}/fe.pem" "${target}/fe.key" > "${target}/ingress.pem"
chmod 0644 \
  "${target}/ca.pem" \
  "${target}/fe.p12" \
  "${target}/ingress.pem" \
  "${target}/mysql-ca.p12"
