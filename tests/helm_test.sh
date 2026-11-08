#!/usr/bin/env bash
set -euo pipefail

CHART="$(cd "$(dirname "$0")/.." && pwd)/helm/clickhouse-connector"
AUTH_KEY="0123456789abcdefSENTINEL"
FAILED=0

render() {
  helm template "$@" --skip-tests "$CHART" --set-string "config.authKey=${AUTH_KEY}"
}

check() {
  local description="$1"
  shift
  if "$@"; then
    echo "ok   - ${description}"
  else
    echo "FAIL - ${description}"
    FAILED=1
  fi
}

count() {
  grep -c -E "$1" <<<"$2" || true
}

command -v helm >/dev/null || { echo "helm is required"; exit 2; }

DEFAULT="$(render t)"

check "chart lints" bash -c "helm lint '$CHART' --set-string config.authKey=${AUTH_KEY} >/dev/null"

check "render fails without config.authKey" bash -c "! helm template t '$CHART' >/dev/null 2>&1"

check "server, consumer and valkey deployments are rendered" \
  test "$(count '^kind: Deployment$' "$DEFAULT")" -eq 3

check "connector points at the valkey service" \
  grep -q '"-redisServer", "t-valkey:6379"' <<<"$DEFAULT"

check "valkey service exists under the same name" \
  grep -q -E '^  name: t-valkey$' <<<"$DEFAULT"

check "release name containing valkey is not doubled" \
  bash -c "helm template valkey-q '$CHART' --set-string config.authKey=${AUTH_KEY} | grep -q '\"-redisServer\", \"valkey-q:6379\"'"

check "no image uses the latest tag" \
  test "$(count 'image: .*:latest($|")' "$DEFAULT")" -eq 0

check "third-party images are pinned by digest" \
  test "$(count 'image: .*(valkey/valkey|redis_exporter).*@sha256:[0-9a-f]{64}' "$DEFAULT")" -ge 3

IMAGES="$(count '^\s+image: ' "$DEFAULT")"
check "every container drops privileges and has a read-only root filesystem" \
  test "$(count 'readOnlyRootFilesystem: true' "$DEFAULT")" -ge "$IMAGES"
check "no container allows privilege escalation" \
  test "$(count 'allowPrivilegeEscalation: true' "$DEFAULT")" -eq 0
check "no container is privileged" \
  test "$(count 'privileged: true' "$DEFAULT")" -eq 0

check "service account tokens are not mounted" \
  test "$(count 'automountServiceAccountToken: true' "$DEFAULT")" -eq 0

check "auth key is only stored in the secret" \
  test "$(count "${AUTH_KEY}" "$DEFAULT")" -eq 0

check "valkey requires authentication" \
  grep -q 'usersExistingSecret\|valkey-users-secret' <<<"$DEFAULT"

check "valkey ACL blocks scripting and flush commands" \
  grep -q -E 'user default on .*-flushall.*-eval' <<<"$DEFAULT"

check "network policies are rendered for the connector and valkey" \
  test "$(count '^kind: NetworkPolicy$' "$DEFAULT")" -eq 2

check "valkey network policy allows no egress" \
  grep -q 'egress: \[\]' <<<"$DEFAULT"

check "http route is not rendered by default" \
  test "$(count '^kind: HTTPRoute$' "$DEFAULT")" -eq 0

ROUTE="$(render t --set httpRoute.enabled=true --set 'httpRoute.hostnames={ingest.example.com}')"
check "http route renders the configured hostname" \
  grep -q 'ingest.example.com' <<<"$ROUTE"

NO_VALKEY="$(render t --set valkey.enabled=false)"
check "valkey disabled removes its workloads and policy" \
  test "$(count 'name: t-valkey' "$NO_VALKEY")" -eq 0
check "valkey disabled keeps server and consumer" \
  test "$(count '^kind: Deployment$' "$NO_VALKEY")" -eq 2

MONITORED="$(render t --set serviceMonitor.enabled=true --set valkey.metrics.serviceMonitor.enabled=true)"
check "service monitors are rendered when enabled" \
  test "$(count '^kind: ServiceMonitor$' "$MONITORED")" -eq 2

AUTOSCALED="$(render t --set autoscaling.enabled=true)"
check "autoscaling leaves replica control to the HPA" \
  test "$(count '^  replicas:' "$AUTOSCALED")" -eq 1
check "autoscaling renders HorizontalPodAutoscaler" \
  grep -q '^kind: HorizontalPodAutoscaler$' <<<"$AUTOSCALED"

exit "$FAILED"
