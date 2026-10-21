#!/usr/bin/env bash
set -euo pipefail
export CLASSPATH="$(/opt/hadoop/bin/hadoop classpath --glob)"
exec "$@"
