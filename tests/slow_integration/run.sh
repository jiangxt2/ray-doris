#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
compose_file="${script_dir}/docker-compose.yml"
log_dir=${RAY_DORIS_SLOW_LOG_DIR:-/tmp/ray-doris-slow-it-logs}
pytest_log="${log_dir}/pytest.log"
compose_log="${log_dir}/compose.log"
result_file="${log_dir}/slow-result.json"
compose=(docker compose -f "${compose_file}")
pytest_pid=
profile=${RAY_DORIS_SLOW_PROFILE:-full}
RAY_DORIS_SLOW_COMMIT_SHA=${GITHUB_SHA:-$(git -C "${script_dir}/../.." rev-parse HEAD)}
RAY_DORIS_SLOW_RUN_ID=${GITHUB_RUN_ID:-1}
RAY_DORIS_SLOW_PROFILE=${profile}
export RAY_DORIS_SLOW_COMMIT_SHA RAY_DORIS_SLOW_PROFILE RAY_DORIS_SLOW_RUN_ID

if [[ -z "${RAY_DORIS_READER_PASSWORD:-}" ]]; then
  RAY_DORIS_READER_PASSWORD=$(openssl rand -hex 24)
  export RAY_DORIS_READER_PASSWORD
fi

if [[ "${profile}" != "full" && "${profile}" != "core" ]]; then
  echo "RAY_DORIS_SLOW_PROFILE must be 'full' or 'core'." >&2
  exit 1
fi

mkdir -p "${log_dir}"

if [[ -z "${RAY_BASE_IMAGE:-}" ]] &&
  docker image inspect ray-cluster:2.55.1 >/dev/null 2>&1; then
  export RAY_BASE_IMAGE=ray-cluster:2.55.1
fi

cleanup() {
  status=$?
  trap - EXIT
  set +e
  "${compose[@]}" ps -a >> "${compose_log}" 2>&1
  "${compose[@]}" logs --no-color >> "${compose_log}" 2>&1
  "${compose[@]}" down -v --rmi local >> "${compose_log}" 2>&1
  echo "Slow integration logs: ${log_dir}"
  exit "${status}"
}
trap cleanup EXIT

existing=$(
  docker ps -aq \
    --filter "label=com.docker.compose.project=ray-doris-it"
)
if [[ -n "${existing}" ]]; then
  echo "The ray-doris-it Compose project already exists; refusing to reuse it." >&2
  exit 1
fi

wait_for_service_log() {
  service=$1
  log_file=$2
  text=$3
  deadline=$((SECONDS + 300))
  until "${compose[@]}" exec -T "${service}" \
    bash -c 'test -f "$1" && grep -Fq "$2" "$1"' -- "${log_file}" "${text}"; do
    if ((SECONDS >= deadline)); then
      echo "Timed out waiting for ${service} log marker: ${text}" >&2
      return 1
    fi
    sleep 2
  done
}

wait_for_marker() {
  marker=$1
  timeout_seconds=$2
  deadline=$((SECONDS + timeout_seconds))
  until "${compose[@]}" exec -T ray-head test -s "${marker}"; do
    if [[ -n "${pytest_pid}" ]] && ! kill -0 "${pytest_pid}" 2>/dev/null; then
      wait "${pytest_pid}"
      return 1
    fi
    if ((SECONDS >= deadline)); then
      echo "Timed out waiting for test marker ${marker}" >&2
      return 1
    fi
    sleep 2
  done
}

"${compose[@]}" up -d --build

wait_for_service_log \
  fe \
  /opt/apache-doris/fe/log/fe.log \
  "Arrow Flight SQL service is started"
for service in be-1 be-2 be-3; do
  wait_for_service_log \
    "${service}" \
    /opt/apache-doris/be/log/be.INFO \
    "Arrow Flight Service bind to host"
done

if [[ "${profile}" == "core" ]]; then
  "${compose[@]}" exec -T ray-head \
    python -m pytest \
    -m slow_integration \
    tests/slow_integration/test_distributed_cluster.py::test_mysql_read_executes_on_all_ray_workers \
    -vv -s 2>&1 | tee -a "${pytest_log}"
  exit 0
fi

(
  "${compose[@]}" exec -T ray-head \
    python -m pytest \
    -m slow_integration \
    tests/slow_integration/test_distributed_cluster.py \
    -vv -s 2>&1 | tee -a "${pytest_log}"
) &
pytest_pid=$!

wait_for_marker /state/ray-read-started 7200
"${compose[@]}" kill ray-worker-1

wait_for_marker /state/be-failure-ready 600
"${compose[@]}" kill be-1

wait "${pytest_pid}"
pytest_pid=

"${compose[@]}" exec -T ray-head \
  python tests/slow_integration/write_result.py
"${compose[@]}" cp ray-head:/state/slow-result.json "${result_file}"
