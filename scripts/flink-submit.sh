#!/usr/bin/env bash
# Submit a Flink job once the JobManager is reachable and has a free task slot.
#
# Used as the entrypoint of the job-submitting containers (flink-job,
# og-prink-job). Idempotent: if a job whose name matches JOB_NAME_REGEX is
# already running/scheduled, nothing is submitted, so `docker compose up`
# can be re-run without creating duplicate jobs (duplicate jobs write every
# record twice to the output topic).
#
# With WATCH_INTERVAL > 0 (default 30) the script keeps running and re-submits
# the job whenever it disappears, e.g. after a JobManager restart or a host
# reboot (the standalone cluster has no HA, so it forgets jobs on restart).
# Set WATCH_INTERVAL=0 to submit once and exit.
#
# Environment:
#   JAR              path to the job jar (required)
#   JOB_NAME_REGEX   extended regex matching the job name (required)
#   JOB_ARGS         extra program arguments (optional, e.g. "--k 5 --l 3")
#   FLINK_JM         JobManager REST endpoint (default flink-jobmanager:8081)
#   WAIT_TIMEOUT     seconds to wait for the cluster (default 300)
#   WATCH_INTERVAL   seconds between checks in watch mode (default 30, 0 = off)
set -euo pipefail

: "${JAR:?JAR must be set}"
: "${JOB_NAME_REGEX:?JOB_NAME_REGEX must be set}"
FLINK_JM="${FLINK_JM:-flink-jobmanager:8081}"
WAIT_TIMEOUT="${WAIT_TIMEOUT:-300}"
WATCH_INTERVAL="${WATCH_INTERVAL:-30}"

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') [submit] $*"; }

wait_for_cluster() {
  local deadline overview slots
  log "waiting for JobManager at ${FLINK_JM} with a free task slot (timeout ${WAIT_TIMEOUT}s)"
  deadline=$(( $(date +%s) + WAIT_TIMEOUT ))
  while true; do
    overview="$(curl -fsS "http://${FLINK_JM}/overview" 2>/dev/null || true)"
    slots="$(echo "${overview}" | sed -n 's/.*"slots-available":\([0-9]*\).*/\1/p')"
    if [ -n "${slots}" ] && [ "${slots}" -gt 0 ]; then
      log "cluster ready (${slots} free slots)"
      return 0
    fi
    if [ "$(date +%s)" -ge "${deadline}" ]; then
      log "ERROR: JobManager not ready after ${WAIT_TIMEOUT}s (last overview: ${overview:-none})"
      return 1
    fi
    sleep 3
  done
}

# Prints matching running/scheduled jobs; returns 0 if there is at least one,
# 1 if there is none, 2 if the JobManager could not be queried.
job_present() {
  local overview
  # Uses the REST API (cheap, no JVM start). Jobs in a terminal state
  # (FINISHED, FAILED, CANCELED) do not count.
  overview="$(curl -fsS "http://${FLINK_JM}/jobs/overview" 2>/dev/null)" || return 2
  echo "${overview}" \
    | grep -o '"jid":"[0-9a-f]*","name":"[^"]*","start-time":[-0-9]*,"end-time":[-0-9]*,"duration":[-0-9]*,"state":"[A-Z_]*"' \
    | sed -E 's/^"jid":"([0-9a-f]*)","name":"([^"]*)".*"state":"([A-Z_]*)"$/\1	\2	\3/' \
    | awk -F'\t' -v re="${JOB_NAME_REGEX}" '$2 ~ re && $3 !~ /^(FINISHED|FAILED|CANCELED)$/ { print "  " $1 " " $2 " (" $3 ")"; found = 1 } END { exit !found }'
}

submit_if_missing() {
  local rc=0
  job_present >/dev/null || rc=$?
  case "${rc}" in
    0) return 0 ;;
    2) log "WARN: could not list jobs on ${FLINK_JM}"; return 1 ;;
  esac
  wait_for_cluster || return 1
  log "no job matching /${JOB_NAME_REGEX}/ - submitting ${JAR} ${JOB_ARGS:-}"
  # shellcheck disable=SC2086
  flink run -d -m "${FLINK_JM}" "${JAR}" ${JOB_ARGS:-}
}

wait_for_cluster
if job_present; then
  log "job matching /${JOB_NAME_REGEX}/ is already running, not submitting again"
else
  submit_if_missing
fi

[ "${WATCH_INTERVAL}" -gt 0 ] || exit 0
log "watching every ${WATCH_INTERVAL}s; the job is re-submitted if it disappears"
while true; do
  sleep "${WATCH_INTERVAL}"
  submit_if_missing || log "WARN: check/submit failed, retrying in ${WATCH_INTERVAL}s"
done
