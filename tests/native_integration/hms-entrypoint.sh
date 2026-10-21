#!/usr/bin/env bash
set -euo pipefail
# The cached image's S3A template uses virtual hosting by default. MinIO is a
# single service name, so bucket-prefixed DNS names are not resolvable here.
sed -i '/<\/configuration>/i\
  <property><name>fs.s3a.path.style.access</name><value>true</value></property>\
  <property><name>fs.s3a.connection.ssl.enabled</name><value>false</value></property>\
  <property><name>fs.s3a.connection.establish.timeout</name><value>5000</value></property>\
  <property><name>fs.s3a.connection.timeout</name><value>5000</value></property>' \
  "${HIVE_TMP_CONF_DIR}/hive-site.xml"
exec bash /usr/local/sbin/start.sh
