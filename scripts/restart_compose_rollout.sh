#!/usr/bin/env bash
set -euo pipefail

# Full rollout with service-aware sequencing.
# - CIM API: rolling restart across both instances (delegated to restart_cim_api.sh)
# - KPI API: recreate + endpoint readiness checks (/ci, /pue, /cfp)
# - MetricsDB RS: restart members one by one, then optionally step up metrics-db as primary
# - Remaining long-running services: sequential recreate (best-effort readiness checks)

COMPOSE_BIN=(docker compose)
KPI_SERVICE="${KPI_SERVICE:-kpi-service}"
KPI_PORT="${KPI_PORT:-8011}"
KPI_SERVICE_A="${KPI_SERVICE_A:-kpi-service-a}"
KPI_SERVICE_B="${KPI_SERVICE_B:-kpi-service-b}"
KPI_PORT_A="${KPI_PORT_A:-8011}"
KPI_PORT_B="${KPI_PORT_B:-8013}"
KPI_TIMEOUT_SECONDS="${KPI_TIMEOUT_SECONDS:-240}"
HTTP_INTERVAL_SECONDS="${HTTP_INTERVAL_SECONDS:-2}"

MONGO_RS_URI="${MONGO_RS_URI:-mongodb://metrics-db:27017,metrics-db-2:27017,metrics-db-3:27017/?replicaSet=rs0}"
MONGO_TIMEOUT_SECONDS="${MONGO_TIMEOUT_SECONDS:-240}"
MONGO_INTERVAL_SECONDS="${MONGO_INTERVAL_SECONDS:-2}"
PROMOTE_MAIN_METRICS_DB="${PROMOTE_MAIN_METRICS_DB:-1}"

log() {
  printf '[%s] %s\n' "$1" "$2"
}

compose_has_service() {
  local service="$1"
  "${COMPOSE_BIN[@]}" config --services | grep -Fx "$service" >/dev/null
}

wait_for_http_status() {
  local name="$1"
  local url="$2"
  local allowed_csv="$3"
  local timeout_s="$4"
  local method="${5:-GET}"
  local body="${6:-}"
  local content_type="${7:-application/json}"

  local deadline=$((SECONDS + timeout_s))
  local next_wait_log=$SECONDS
  while (( SECONDS < deadline )); do
    local code
    if [[ -n "$body" ]]; then
      code="$(curl -s -o /dev/null -w '%{http_code}' -m 4 -X "$method" -H "Content-Type: ${content_type}" --data "$body" "$url" 2>/dev/null || true)"
    else
      code="$(curl -s -o /dev/null -w '%{http_code}' -m 4 -X "$method" "$url" 2>/dev/null || true)"
    fi

    if [[ ",${allowed_csv}," == *",${code},"* ]]; then
      log ok "${name} reachable (${url}) -> HTTP ${code}"
      return 0
    fi

    if (( SECONDS >= next_wait_log )); then
      log info "${name} waiting (${url}) current=${code:-none} allowed=${allowed_csv}"
      next_wait_log=$((SECONDS + 15))
    fi

    sleep "${HTTP_INTERVAL_SECONDS}"
  done

  log error "${name} did not reach expected status in ${timeout_s}s (${url}, allowed: ${allowed_csv})"
  return 1
}

restart_service_no_deps() {
  local service="$1"
  log info "recreating ${service}"
  "${COMPOSE_BIN[@]}" up -d --build --force-recreate --no-deps "$service"
}

wait_kpi_endpoints() {
  local port="$1"
  local base="http://127.0.0.1:${port}/v1"

  # Readiness + route-availability checks.
  wait_for_http_status "kpi-openapi" "${base}/openapi.json" "200" "$KPI_TIMEOUT_SECONDS"
  wait_for_http_status "kpi-ci" "${base}/ci" "200,401,422" "$KPI_TIMEOUT_SECONDS" "POST" '{"lat":46.0,"lon":16.0}'
  wait_for_http_status "kpi-pue" "${base}/pue" "200,404,422" "$KPI_TIMEOUT_SECONDS" "POST" '{"site_name":"DUMMY-SITE"}'
  wait_for_http_status "kpi-cfp" "${base}/cfp?ci_g=100&pue=1.2&energy_wh=1000" "200,401,422" "$KPI_TIMEOUT_SECONDS"
}

mongo_eval_from_main() {
  local js="$1"
  "${COMPOSE_BIN[@]}" exec -T metrics-db mongosh "$MONGO_RS_URI" --quiet --eval "$js"
}

mongo_eval_on_service() {
  local service="$1"
  local js="$2"
  "${COMPOSE_BIN[@]}" exec -T "$service" mongosh --host "${service}:27017" --quiet --eval "$js"
}

wait_mongo_node_ping() {
  local service="$1"
  local deadline=$((SECONDS + MONGO_TIMEOUT_SECONDS))
  local next_wait_log=$SECONDS

  while (( SECONDS < deadline )); do
    if "${COMPOSE_BIN[@]}" exec -T "$service" mongosh --quiet --eval "db.adminCommand({ ping: 1 }).ok" >/dev/null 2>&1; then
      log ok "${service} ping OK"
      return 0
    fi
    if (( SECONDS >= next_wait_log )); then
      log info "${service} waiting for mongosh ping"
      next_wait_log=$((SECONDS + 15))
    fi
    sleep "$MONGO_INTERVAL_SECONDS"
  done

  log error "${service} did not become reachable within ${MONGO_TIMEOUT_SECONDS}s"
  return 1
}

wait_mongo_member_healthy() {
  local service="$1"
  local deadline=$((SECONDS + MONGO_TIMEOUT_SECONDS))
  local next_wait_log=$SECONDS

  while (( SECONDS < deadline )); do
    local state
    state="$(mongo_eval_from_main "const m=rs.status().members.find(x => x.name.startsWith('${service}:')); print(m ? m.stateStr : 'MISSING');" 2>/dev/null || true)"

    case "$state" in
      PRIMARY|SECONDARY)
        log ok "${service} in replica set state ${state}"
        return 0
        ;;
      MISSING)
        if (( SECONDS >= next_wait_log )); then
          log info "${service} not yet visible in rs.status(); waiting"
          next_wait_log=$((SECONDS + 15))
        fi
        ;;
      *)
        if [[ -n "$state" ]] && (( SECONDS >= next_wait_log )); then
          log info "${service} current state: ${state}"
          next_wait_log=$((SECONDS + 15))
        fi
        ;;
    esac

    sleep "$MONGO_INTERVAL_SECONDS"
  done

  log error "${service} did not reach PRIMARY/SECONDARY within ${MONGO_TIMEOUT_SECONDS}s"
  return 1
}

restart_mongo_member() {
  local service="$1"
  restart_service_no_deps "$service"
  wait_mongo_node_ping "$service"
  wait_mongo_member_healthy "$service"
}

mongo_current_primary() {
  mongo_eval_from_main "const p=(db.hello().primary||''); print(p);" 2>/dev/null || true
}

ensure_primary_is_not_main() {
  local current
  current="$(mongo_current_primary)"
  if [[ "$current" != "metrics-db:27017" ]]; then
    log ok "current PRIMARY is already ${current:-unknown}, not metrics-db"
    return 0
  fi

  log info "metrics-db is PRIMARY; moving PRIMARY before restarting metrics-db"
  local candidates=(metrics-db-2 metrics-db-3)
  for candidate in "${candidates[@]}"; do
    if ! compose_has_service "$candidate"; then
      continue
    fi
    log info "requesting step-up on ${candidate}"
    mongo_eval_on_service "$candidate" "try { db.adminCommand({ replSetStepUp: 1 }); print('STEPUP_REQUESTED'); } catch (e) { print(e.codeName || e.message); }" >/dev/null || true

    local deadline=$((SECONDS + MONGO_TIMEOUT_SECONDS))
    while (( SECONDS < deadline )); do
      local primary
      primary="$(mongo_current_primary)"
      if [[ "$primary" == "${candidate}:27017" ]]; then
        log ok "${candidate} is PRIMARY"
        return 0
      fi
      sleep "$MONGO_INTERVAL_SECONDS"
    done
  done

  log error "could not move PRIMARY away from metrics-db before restart"
  return 1
}

rollout_cim() {
  if [[ -x ./scripts/restart_cim_api.sh ]]; then
    log info "rolling CIM API (cim-fastapi-a/b)"
    ./scripts/restart_cim_api.sh
  else
    log error "scripts/restart_cim_api.sh not found or not executable"
    return 1
  fi
}

rollout_kpi() {
  if compose_has_service "$KPI_SERVICE_A" && compose_has_service "$KPI_SERVICE_B"; then
    log info "rolling KPI API (${KPI_SERVICE_A}/${KPI_SERVICE_B}) with peer readiness checks"

    wait_kpi_endpoints "$KPI_PORT_B"
    restart_service_no_deps "$KPI_SERVICE_A"
    wait_kpi_endpoints "$KPI_PORT_A"

    wait_kpi_endpoints "$KPI_PORT_A"
    restart_service_no_deps "$KPI_SERVICE_B"
    wait_kpi_endpoints "$KPI_PORT_B"
    return 0
  fi

  if compose_has_service "$KPI_SERVICE"; then
    log info "single KPI service mode (${KPI_SERVICE}); rollout is best-effort only"
    restart_service_no_deps "$KPI_SERVICE"
    wait_kpi_endpoints "$KPI_PORT"
    return 0
  fi

  log info "no KPI service found (${KPI_SERVICE_A}/${KPI_SERVICE_B}/${KPI_SERVICE}); skipping KPI rollout"
}

rollout_metrics_rs() {
  local members=(metrics-db-2 metrics-db-3)

  for svc in "${members[@]}"; do
    if ! compose_has_service "$svc"; then
      log info "${svc} not defined in compose, skipping"
      continue
    fi
    log info "rolling Mongo member ${svc}"
    restart_mongo_member "$svc"
  done

  if compose_has_service metrics-db; then
    ensure_primary_is_not_main
    log info "rolling Mongo member metrics-db"
    restart_mongo_member "metrics-db"
  fi

  if [[ "$PROMOTE_MAIN_METRICS_DB" == "1" ]] && compose_has_service metrics-db; then
    log info "attempting to promote metrics-db as PRIMARY (best effort)"
    mongo_eval_on_service "metrics-db" "try { db.adminCommand({ replSetStepUp: 1 }); print('STEPUP_REQUESTED'); } catch (e) { print(e.codeName || e.message); }" >/dev/null || true

    local deadline=$((SECONDS + MONGO_TIMEOUT_SECONDS))
    while (( SECONDS < deadline )); do
      local primary
      primary="$(mongo_current_primary)"
      if [[ "$primary" == "metrics-db:27017" ]]; then
        log ok "metrics-db is PRIMARY"
        return 0
      fi
      sleep "$MONGO_INTERVAL_SECONDS"
    done

    log info "metrics-db promotion did not complete in time; continuing"
  fi
}

rollout_remaining_services() {
  mapfile -t services < <("${COMPOSE_BIN[@]}" config --services)

  for svc in "${services[@]}"; do
    case "$svc" in
      cim-fastapi-a|cim-fastapi-b|"$KPI_SERVICE"|"$KPI_SERVICE_A"|"$KPI_SERVICE_B"|metrics-db|metrics-db-2|metrics-db-3|mongo-rs-init)
        continue
        ;;
    esac

    log info "recreating remaining service ${svc}"
    restart_service_no_deps "$svc"

    case "$svc" in
      cim-service)
        # cim-service handles POST only and returns 501 for GET / while healthy.
        wait_for_http_status "cim-service" "http://127.0.0.1:8012/" "200,307,404,501" 180 || true
        ;;
      sql-adapter)
        wait_for_http_status "sql-adapter" "http://127.0.0.1:8033/docs" "200,307" 180 || true
        ;;
      grafana)
        # Grafana in this stack serves behind /metricsdb-dashboard/v1/charts and commonly
        # returns redirect statuses during startup; accept redirects as healthy.
        wait_for_http_status "grafana" "http://127.0.0.1:8044/metricsdb-dashboard/v1/charts/login" "200,301,302,307,308" 180 || true
        ;;
    esac
  done
}

main() {
  rollout_cim
  rollout_kpi
  rollout_metrics_rs
  rollout_remaining_services
  log ok "compose rollout finished"
}

main "$@"
