#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
compose_file="${script_dir}/docker-compose.yml"
log_dir=${RAY_DORIS_SLOW_LOG_DIR:-/tmp/ray-doris-slow-it-logs}
pytest_log="${log_dir}/pytest.log"
compose_log="${log_dir}/compose.log"
result_file="${log_dir}/slow-result.json"
pytest_pid=
profile=${RAY_DORIS_SLOW_PROFILE:-full}
RAY_DORIS_SLOW_COMMIT_SHA=${RAY_DORIS_SLOW_COMMIT_SHA:-${GITHUB_SHA:-$(git -C "${script_dir}/../.." rev-parse HEAD)}}
RAY_DORIS_SLOW_RUN_ID=${RAY_DORIS_SLOW_RUN_ID:-${GITHUB_RUN_ID:-1}}
RAY_DORIS_SLOW_PROFILE=${profile}
RAY_DORIS_SLOW_IMAGE_TAG=${RAY_DORIS_SLOW_IMAGE_TAG:-"run-${RAY_DORIS_SLOW_RUN_ID}-$$"}
export RAY_DORIS_SLOW_COMMIT_SHA RAY_DORIS_SLOW_PROFILE RAY_DORIS_SLOW_RUN_ID
export RAY_DORIS_SLOW_IMAGE_TAG

if [[ ! "${RAY_DORIS_SLOW_IMAGE_TAG}" =~ ^[A-Za-z0-9_.-]+$ ]]; then
  echo "RAY_DORIS_SLOW_IMAGE_TAG contains unsupported characters." >&2
  exit 1
fi
if [[ ! "${RAY_DORIS_SLOW_COMMIT_SHA}" =~ ^[0-9a-f]{40}$ ]]; then
  echo "RAY_DORIS_SLOW_COMMIT_SHA must be a full lowercase commit SHA." >&2
  exit 1
fi

ray_image="ray-doris-it-ray:${RAY_DORIS_SLOW_IMAGE_TAG}"
fe_image="ray-doris-it-fe:${RAY_DORIS_SLOW_IMAGE_TAG}"
be_image="ray-doris-it-be:${RAY_DORIS_SLOW_IMAGE_TAG}"
export RAY_DORIS_RAY_IMAGE=${ray_image}
export RAY_DORIS_FE_IMAGE=${fe_image}
export RAY_DORIS_BE_IMAGE=${be_image}

compose=(
  env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY
  -u http_proxy -u https_proxy -u all_proxy
  docker compose --project-name ray-doris-it -f "${compose_file}"
)

docker_cmd() {
  env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
    -u http_proxy -u https_proxy -u all_proxy docker "$@"
}

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
  docker_cmd image inspect ray-cluster:2.58.0 >/dev/null 2>&1; then
  export RAY_BASE_IMAGE=ray-cluster:2.58.0
fi
if [[ -z "${RAY_BASE_IMAGE:-}" ]]; then
  export RAY_BASE_IMAGE=rayproject/ray:2.58.0-py312-cpu
fi

baseline_project_dangling_ids=$(
  docker_cmd image ls --filter label=com.docker.compose.project=ray-doris-it --filter dangling=true -q
)

cleanup() {
  status=$?
  trap - EXIT
  set +e
  {
    "${compose[@]}" ps -a
    "${compose[@]}" logs --no-color
    "${compose[@]}" down -v
  } >> "${compose_log}" 2>&1
  for image in "${ray_image}" "${fe_image}" "${be_image}"; do
    image_id=$(docker_cmd image inspect --format '{{.Id}}' "${image}" 2>/dev/null || true)
    if [[ -n "${image_id}" ]]; then
      docker_cmd image rm "${image}" >> "${compose_log}" 2>&1 || true
    fi
  done
  current_project_dangling_ids=$(
    docker_cmd image ls --filter label=com.docker.compose.project=ray-doris-it --filter dangling=true -q
  )
  while read -r image_id; do
    [[ -z "${image_id}" ]] && continue
    if [[ " ${baseline_project_dangling_ids} " == *" ${image_id} "* ]]; then
      continue
    fi
    docker_cmd image inspect --format 'dangling_id={{.Id}} created={{.Created}} labels={{json .Config.Labels}}' \
      "${image_id}" >> "${compose_log}" 2>&1 || true
    docker_cmd image rm "${image_id}" >> "${compose_log}" 2>&1 || true
  done <<< "${current_project_dangling_ids}"
  docker_cmd image ls --filter dangling=true >> "${compose_log}" 2>&1
  docker_cmd system df >> "${compose_log}" 2>&1
  echo "Slow integration logs: ${log_dir}"
  exit "${status}"
}

existing=$(
  docker_cmd ps -aq \
    --filter "label=com.docker.compose.project=ray-doris-it"
)
existing_named_containers=$(
  docker_cmd ps -aq --filter "name=ray-doris-it-"
)
existing_networks=$(
  docker_cmd network ls -q \
    --filter "label=com.docker.compose.project=ray-doris-it"
)
existing_volumes=$(
  docker_cmd volume ls -q \
    --filter "label=com.docker.compose.project=ray-doris-it"
)
existing_named_networks=$(
  docker_cmd network ls --format '{{.Name}}' | awk '$0 ~ /^ray-doris-it(_|$)/'
)
existing_named_volumes=$(
  docker_cmd volume ls --format '{{.Name}}' | awk '$0 ~ /^ray-doris-it(_|$)/'
)
if [[ -n "${existing}" || -n "${existing_named_containers}" ||
  -n "${existing_networks}" || -n "${existing_volumes}" ||
  -n "${existing_named_networks}" || -n "${existing_named_volumes}" ]]; then
  echo "The ray-doris-it Compose project already exists; refusing to reuse it." >&2
  exit 1
fi

for image in "${ray_image}" "${fe_image}" "${be_image}"; do
  if docker_cmd image inspect "${image}" >/dev/null 2>&1; then
    echo "The per-run image already exists: ${image}" >&2
    exit 1
  fi
done

trap cleanup EXIT

wait_for_service_log() {
  service=$1
  log_file=$2
  text=$3
  deadline=$((SECONDS + 300))
  until "${compose[@]}" exec -T "${service}" \
    bash -c "test -f \"\$1\" && grep -Fq \"\$2\" \"\$1\"" -- "${log_file}" "${text}"; do
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

{
  docker_cmd image ls --filter dangling=true
  docker_cmd system df
} >> "${compose_log}" 2>&1

"${compose[@]}" pull flight-proxy >> "${compose_log}" 2>&1
"${compose[@]}" build ray-head fe be-1 2>&1 | tee -a "${compose_log}"

RAY_DORIS_SLOW_RAY_IMAGE_ID=$(docker_cmd image inspect --format '{{.Id}}' "${ray_image}")
RAY_DORIS_SLOW_DORIS_FE_IMAGE_ID=$(docker_cmd image inspect --format '{{.Id}}' "${fe_image}")
RAY_DORIS_SLOW_DORIS_BE_IMAGE_ID=$(docker_cmd image inspect --format '{{.Id}}' "${be_image}")
RAY_DORIS_SLOW_FLIGHT_PROXY_IMAGE_ID=$(docker_cmd image inspect --format '{{.Id}}' haproxy:3.2.21-alpine)
RAY_DORIS_SLOW_RAY_BASE_IMAGE_ID=$(docker_cmd image inspect --format '{{.Id}}' "${RAY_BASE_IMAGE}")
export RAY_DORIS_SLOW_RAY_IMAGE_ID RAY_DORIS_SLOW_DORIS_FE_IMAGE_ID
export RAY_DORIS_SLOW_DORIS_BE_IMAGE_ID RAY_DORIS_SLOW_FLIGHT_PROXY_IMAGE_ID
export RAY_DORIS_SLOW_RAY_BASE_IMAGE_ID

{
  docker_cmd image inspect --format "image=ray tag=${ray_image} id={{.Id}} created={{.Created}}" "${ray_image}"
  docker_cmd image inspect --format "image=doris-fe tag=${fe_image} id={{.Id}} created={{.Created}}" "${fe_image}"
  docker_cmd image inspect --format "image=doris-be tag=${be_image} id={{.Id}} created={{.Created}}" "${be_image}"
  docker_cmd image inspect --format 'image=flight-proxy tag=haproxy:3.2.21-alpine id={{.Id}} created={{.Created}}' haproxy:3.2.21-alpine
  docker_cmd image inspect --format "image=ray-base tag=${RAY_BASE_IMAGE} id={{.Id}} created={{.Created}}" "${RAY_BASE_IMAGE}"
} >> "${compose_log}" 2>&1

"${compose[@]}" up -d --no-build 2>&1 | tee -a "${compose_log}"

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
