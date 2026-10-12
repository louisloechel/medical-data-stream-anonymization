#!/usr/bin/env bash
# End-to-end smoke test for the running stack.
#
# Checks: Kafka healthy and topics present; producer writing to raw-topic;
# both Flink jobs RUNNING (exactly once); records flowing into processed-topic
# and prink-topic; consumer metrics on :8000; all Prometheus targets up; Grafana
# healthy with the provisioned datasource and dashboards; host ports published.
#
# Exits non-zero if any check fails. Each check is retried until TIMEOUT
# seconds have passed since the start of the script, so it can be run right
# after `docker compose up -d`.
#
# HTTP checks run inside the containers (docker compose exec), so they always
# test this compose project, even if another process (e.g. an SSH tunnel to a
# server) listens on the same localhost ports. Published host ports are
# checked separately via `docker compose port`.
#
# Usage: scripts/smoke-test.sh            (uses $COMPOSE_PROJECT_NAME if set)
#        TIMEOUT=300 scripts/smoke-test.sh
set -uo pipefail

TIMEOUT="${TIMEOUT:-240}"
GRAFANA_AUTH="${GRAFANA_AUTH:-admin:${GRAFANA_ADMIN_PASSWORD:-admin}}"
FLINK_JOB_REGEX="${FLINK_JOB_REGEX:-^Kafka Flink Job\$}"
PRINK_JOB_REGEX="${PRINK_JOB_REGEX:-^k[0-9]+_delta}"
NEWS2_DASHBOARD_UID="fe0tv8jblo4xsc"
DATASOURCE_UID="adyyzm74ahla8e"

cd "$(dirname "$0")/.."
START=$(date +%s)
FAILED=0

pass() { printf '  [PASS] %s\n' "$*"; }
fail() { printf '  [FAIL] %s\n' "$*"; FAILED=$((FAILED + 1)); }

# retry <description> <command...>: re-run the check until it succeeds or the
# global deadline is reached. The check prints a detail line.
retry() {
  local desc="$1"; shift
  local out
  while true; do
    if out="$("$@" 2>&1)"; then
      pass "${desc}${out:+ -- ${out}}"
      return 0
    fi
    if [ $(( $(date +%s) - START )) -ge "${TIMEOUT}" ]; then
      fail "${desc}${out:+ -- ${out}}"
      return 1
    fi
    sleep 5
  done
}

kafka() { docker compose exec -T kafka "$@"; }

# HTTP GET from inside a service container: http_get <service> <url>
http_get() {
  case "$1" in
    flink-jobmanager) docker compose exec -T "$1" curl -fsS "$2" ;;
    kafka-consumer)   docker compose exec -T "$1" python -c \
                        "import sys, urllib.request; sys.stdout.write(urllib.request.urlopen(sys.argv[1], timeout=10).read().decode())" "$2" ;;
    *)                docker compose exec -T "$1" wget -q -O - "$2" ;;
  esac
}

topic_offset() {
  kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:29092 --topic "$1" 2>/dev/null \
    | awk -F: '{s += $NF} END {print s + 0}'
}

check_kafka_healthy() {
  local id state
  id="$(docker compose ps -q kafka)"
  [ -n "${id}" ] || { echo "kafka container not running"; return 1; }
  state="$(docker inspect -f '{{.State.Health.Status}}' "${id}")"
  echo "health=${state}"
  [ "${state}" = "healthy" ]
}

check_topics() {
  local list missing=""
  list="$(kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --list 2>/dev/null)"
  for t in raw-topic processed-topic prink-topic; do
    echo "${list}" | grep -qx "${t}" || missing="${missing} ${t}"
  done
  [ -z "${missing}" ] && echo "raw-topic, processed-topic, prink-topic" && return 0
  echo "missing:${missing}"; return 1
}

check_topic_growing() {
  local topic="$1" a b
  a="$(topic_offset "${topic}")"; sleep 5; b="$(topic_offset "${topic}")"
  echo "${topic} end offset ${a} -> ${b}"
  [ "${b}" -gt "${a}" ]
}

check_topic_sample() {
  local topic="$1" msg
  # Skip empty "{}" records, show one real record (synthetic data).
  msg="$(kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:29092 \
          --topic "${topic}" --from-beginning --max-messages 20 --timeout-ms 15000 2>/dev/null \
          | grep -v '^{}$' | head -n 1)"
  [ -n "${msg}" ] || { echo "no records"; return 1; }
  echo "${msg}" | cut -c1-220
}

# Exactly one RUNNING job whose name matches the regex.
check_job_running() {
  local regex="$1" overview names n
  overview="$(http_get flink-jobmanager http://localhost:8081/jobs/overview)" || { echo "Flink REST not reachable"; return 1; }
  # one line per job: <name>\t<state>
  names="$(echo "${overview}" | grep -o '"name":"[^"]*","start-time":[-0-9]*,"end-time":[-0-9]*,"duration":[-0-9]*,"state":"[A-Z]*"' \
           | sed -E 's/^"name":"([^"]*)".*"state":"([A-Z]*)"$/\1	\2/')"
  n="$(echo "${names}" | awk -F'\t' -v re="${regex}" '$1 ~ re && $2 == "RUNNING"' | wc -l | tr -d ' ')"
  echo "${n} RUNNING job(s) matching /${regex}/ (all jobs: $(echo "${names}" | tr '\t\n' ' ;'))"
  [ "${n}" -eq 1 ]
}

check_consumer_metrics() {
  local v
  v="$(http_get kafka-consumer http://localhost:8000/metrics | awk '/^kafka_consumer_messages_total /{print $2}')"
  [ -n "${v}" ] || { echo "kafka_consumer_messages_total not exposed"; return 1; }
  echo "kafka_consumer_messages_total=${v}"
  awk -v v="${v}" 'BEGIN {exit !(v > 0)}'
}

check_prometheus_targets() {
  local json up down
  json="$(http_get prometheus 'http://localhost:9090/api/v1/targets?state=active')" || { echo "Prometheus not reachable"; return 1; }
  up="$(echo "${json}" | grep -o '"health":"up"' | wc -l | tr -d ' ')"
  down="$(echo "${json}" | grep -o '"health":"\(down\|unknown\)"' | wc -l | tr -d ' ')"
  echo "${up} up, ${down} down/unknown"
  [ "${up}" -gt 0 ] && [ "${down}" -eq 0 ]
}

check_prink_metrics() {
  http_get prometheus 'http://localhost:9090/api/v1/query?query=count(flink_taskmanager_job_task_operator_Prink_k_Value)' \
    | grep -q '"result":\[{' && echo "flink_taskmanager_job_task_operator_Prink_k_Value present"
}

check_grafana() {
  local g="http://${GRAFANA_AUTH}@localhost:3000"
  http_get grafana http://localhost:3000/api/health | grep -q '"database": *"ok"' || { echo "Grafana /api/health not ok"; return 1; }
  http_get grafana "${g}/api/dashboards/uid/${NEWS2_DASHBOARD_UID}" >/dev/null \
    || { echo "NEWS2 dashboard (${NEWS2_DASHBOARD_UID}) not found"; return 1; }
  http_get grafana "${g}/api/datasources/uid/${DATASOURCE_UID}/health" | grep -q '"status":"OK"' \
    || { echo "Prometheus datasource health check failed"; return 1; }
  echo "healthy, NEWS2 dashboard + Prometheus datasource OK"
}

check_host_ports() {
  local spec svc port missing=""
  for spec in kafka:9092 flink-jobmanager:8081 kafka-consumer:8000 prometheus:9090 grafana:3000; do
    svc="${spec%%:*}"; port="${spec##*:}"
    [ -n "$(docker compose port "${svc}" "${port}" 2>/dev/null)" ] || missing="${missing} ${spec}"
  done
  [ -z "${missing}" ] && echo "9092 8081 8000 9090 3000" && return 0
  echo "not published:${missing}"; return 1
}

echo "Smoke test (timeout ${TIMEOUT}s, project ${COMPOSE_PROJECT_NAME:-<default>})"
echo "Kafka"
retry "kafka healthy"                         check_kafka_healthy
retry "topics exist"                          check_topics
retry "producer writes to raw-topic"          check_topic_growing raw-topic
echo "Flink"
retry "flink-job RUNNING"                     check_job_running "${FLINK_JOB_REGEX}"
retry "Prink job RUNNING"                     check_job_running "${PRINK_JOB_REGEX}"
retry "records flow into processed-topic"     check_topic_growing processed-topic
retry "records flow into prink-topic"         check_topic_growing prink-topic
retry "processed-topic sample"                check_topic_sample processed-topic
retry "prink-topic sample"                    check_topic_sample prink-topic
echo "Monitoring"
retry "consumer metrics on :8000"             check_consumer_metrics
retry "Prometheus targets up"                 check_prometheus_targets
retry "Prink operator metrics in Prometheus"  check_prink_metrics
retry "Grafana"                               check_grafana
retry "host ports published"                  check_host_ports

if [ "${FAILED}" -gt 0 ]; then
  echo "SMOKE TEST FAILED (${FAILED} check(s))"
  exit 1
fi
echo "SMOKE TEST PASSED ($(( $(date +%s) - START ))s)"
