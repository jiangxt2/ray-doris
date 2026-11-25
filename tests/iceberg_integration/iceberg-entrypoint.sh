#!/usr/bin/env bash
set -euo pipefail
target=minio
if [[ "${ICEBERG_WAREHOUSE}" == hdfs:* || -n "${ICEBERG_VENDING:-}" ]]; then
  for i in {1..180}; do
    (echo >/dev/tcp/iceberg-s3/9001) >/dev/null 2>&1 && break
    sleep 1
  done
fi
if [[ "${ICEBERG_WAREHOUSE}" == hdfs:* ]]; then
  target=hms
fi
for i in {1..180}; do
  (echo >"/dev/tcp/${target}/9000") >/dev/null 2>&1 && break
  sleep 1
done
ln -s /opt/gravitino/iceberg-bundles/gravitino-iceberg-aws-bundle-1.3.1.jar /opt/gravitino/iceberg-rest-server/libs/aws-bundle.jar
ln -s /opt/gravitino/jdbc-drivers/postgresql-42.7.11.jar /opt/gravitino/iceberg-rest-server/libs/postgresql.jar
cat >> /opt/gravitino/conf/gravitino-iceberg-rest-server.conf <<EOF
gravitino.iceberg-rest.catalog-backend = jdbc
gravitino.iceberg-rest.uri = jdbc:postgresql://postgres:5432/iceberg
gravitino.iceberg-rest.jdbc-driver = org.postgresql.Driver
gravitino.iceberg-rest.jdbc-user = ${ICEBERG_USER}
gravitino.iceberg-rest.jdbc-password = ${ICEBERG_PASSWORD}
gravitino.iceberg-rest.jdbc-initialize = true
gravitino.iceberg-rest.warehouse = ${ICEBERG_WAREHOUSE}
gravitino.iceberg-rest.io-impl = ${ICEBERG_IO}
gravitino.iceberg-rest.s3-endpoint = http://minio:9000
gravitino.iceberg-rest.s3-path-style-access = true
gravitino.iceberg-rest.s3-region = us-east-1
gravitino.iceberg-rest.s3-access-key-id = ${ICEBERG_USER}
gravitino.iceberg-rest.s3-secret-access-key = ${ICEBERG_PASSWORD}
EOF
if [[ -n "${ICEBERG_VENDING:-}" ]]; then
  printf 'gravitino.iceberg-rest.credential-providers = %s\n' "${ICEBERG_VENDING}" >> /opt/gravitino/conf/gravitino-iceberg-rest-server.conf
fi
exec /opt/gravitino/bin/gravitino-iceberg-rest-server.sh run
