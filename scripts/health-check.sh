#!/usr/bin/env bash
# health-check.sh — verify all infra components are up and accepting connections.
#
# Exits 0 if everything is healthy, 1 if any check fails.
# Designed to be run after setup-local.sh or as a standalone smoke test.
#
# Usage:
#   ./scripts/health-check.sh
#   ./scripts/health-check.sh --namespace my-ns

set -euo pipefail

NAMESPACE="${RAG_NAMESPACE:-rag}"
for arg in "$@"; do
  case $arg in
    --namespace) NAMESPACE="${2}"; shift 2 ;;
    --namespace=*) NAMESPACE="${arg#*=}"; shift ;;
  esac
done

GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; NC='\033[0m'
PASS=0; FAIL=0

pass() { echo -e "  ${GREEN}✓${NC} $*"; (( PASS++ )); }
fail() { echo -e "  ${RED}✗${NC} $*"; (( FAIL++ )); }
warn() { echo -e "  ${YELLOW}~${NC} $*"; }
section() { echo -e "\n${YELLOW}▸ $*${NC}"; }

# ── Kafka ────────────────────────────────────────────────────────────────────
section "Kafka (Strimzi)"

KAFKA_READY=$(kubectl get kafka kafka-cluster -n "$NAMESPACE" \
  -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || echo "NotFound")

if [[ "$KAFKA_READY" == "True" ]]; then
  pass "Kafka cluster 'kafka-cluster' is Ready"
else
  fail "Kafka cluster not ready (status: ${KAFKA_READY})"
  warn "Check: kubectl get kafka -n $NAMESPACE"
  warn "Logs:  kubectl logs -n strimzi -l name=strimzi-cluster-operator"
fi

for TOPIC in "page.crawled" "chunk.ready"; do
  TOPIC_SLUG="${TOPIC//./-}"
  TOPIC_READY=$(kubectl get kafkatopic "$TOPIC_SLUG" -n "$NAMESPACE" \
    -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || echo "NotFound")
  if [[ "$TOPIC_READY" == "True" ]]; then
    pass "KafkaTopic '$TOPIC' exists and is Ready"
  else
    fail "KafkaTopic '$TOPIC' not ready (status: ${TOPIC_READY})"
  fi
done

# ── Redis ─────────────────────────────────────────────────────────────────────
section "Redis"

REDIS_SVC=$(kubectl get svc rag-redis-master -n "$NAMESPACE" \
  -o jsonpath='{.spec.clusterIP}' 2>/dev/null || echo "")
if [[ -n "$REDIS_SVC" ]]; then
  pass "Redis service 'rag-redis-master' exists (ClusterIP: $REDIS_SVC)"
else
  fail "Redis service 'rag-redis-master' not found"
  warn "Check: kubectl get svc -n $NAMESPACE"
fi

REDIS_READY=$(kubectl get statefulset rag-redis-master -n "$NAMESPACE" \
  -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0")
if [[ "${REDIS_READY}" -ge 1 ]]; then
  pass "Redis StatefulSet has ${REDIS_READY} ready replica(s)"
else
  fail "Redis StatefulSet not ready (readyReplicas=${REDIS_READY})"
  warn "Check: kubectl describe statefulset rag-redis-master -n $NAMESPACE"
fi

# Ping Redis via a one-shot pod
REDIS_PING=$(kubectl run redis-ping-test \
  --image=redis:7-alpine \
  --restart=Never \
  --rm \
  --quiet \
  --namespace "$NAMESPACE" \
  -it \
  -- redis-cli -h rag-redis-master PING 2>/dev/null | tr -d '[:space:]' || echo "FAIL")
if [[ "$REDIS_PING" == "PONG" ]]; then
  pass "Redis PING → PONG (connection OK)"
else
  fail "Redis PING failed (got: '${REDIS_PING}')"
fi

# ── Milvus ───────────────────────────────────────────────────────────────────
section "Milvus"

MILVUS_READY=$(kubectl get deployment milvus-milvus -n "$NAMESPACE" \
  -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0")
if [[ "${MILVUS_READY}" -ge 1 ]]; then
  pass "Milvus deployment has ${MILVUS_READY} ready replica(s)"
else
  fail "Milvus deployment not ready (readyReplicas=${MILVUS_READY})"
  warn "Check: kubectl describe deployment milvus-milvus -n $NAMESPACE"
  warn "Logs:  kubectl logs -n $NAMESPACE -l app.kubernetes.io/name=milvus"
fi

MILVUS_SVC=$(kubectl get svc milvus-milvus -n "$NAMESPACE" \
  -o jsonpath='{.spec.clusterIP}' 2>/dev/null || echo "")
if [[ -n "$MILVUS_SVC" ]]; then
  pass "Milvus service exists (ClusterIP: $MILVUS_SVC)"
else
  fail "Milvus service not found"
fi

# Check collection exists (created by milvus-init job)
INIT_JOB_STATUS=$(kubectl get job milvus-init -n "$NAMESPACE" \
  -o jsonpath='{.status.succeeded}' 2>/dev/null || echo "0")
if [[ "${INIT_JOB_STATUS}" -ge 1 ]]; then
  pass "milvus-init job completed successfully (collection schema applied)"
else
  warn "milvus-init job not yet succeeded (status: ${INIT_JOB_STATUS}) — may still be running"
  warn "Check: kubectl logs job/milvus-init -n $NAMESPACE"
fi

# ── Query API ─────────────────────────────────────────────────────────────────
section "Query API"

QUERY_READY=$(kubectl get deployment query-api -n "$NAMESPACE" \
  -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0")
if [[ "${QUERY_READY}" == "0" ]] || [[ -z "${QUERY_READY}" ]]; then
  warn "query-api deployment not found or not ready — skipping (may not be deployed yet)"
else
  pass "query-api deployment has ${QUERY_READY} ready replica(s)"
fi

# ── KEDA ─────────────────────────────────────────────────────────────────────
section "KEDA operator"

KEDA_READY=$(kubectl get deployment keda-operator -n keda \
  -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0")
if [[ "${KEDA_READY}" -ge 1 ]]; then
  pass "KEDA operator is running (${KEDA_READY} ready)"
else
  fail "KEDA operator not ready"
  warn "Check: kubectl get pods -n keda"
fi

# ── Prometheus / Grafana ──────────────────────────────────────────────────────
section "kube-prometheus-stack"

PROM_READY=$(kubectl get deployment kube-prometheus-stack-prometheus -n monitoring \
  -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "")
if [[ -n "$PROM_READY" ]] && [[ "${PROM_READY}" -ge 1 ]]; then
  pass "Prometheus is running (${PROM_READY} ready)"
else
  # Use StatefulSet path — kube-prometheus-stack deploys Prometheus as a StatefulSet
  PROM_SS=$(kubectl get statefulset prometheus-kube-prometheus-stack-prometheus -n monitoring \
    -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0")
  if [[ "${PROM_SS}" -ge 1 ]]; then
    pass "Prometheus StatefulSet is running (${PROM_SS} ready)"
  else
    warn "Prometheus not found in 'monitoring' namespace — run 'make setup' to install kube-prometheus-stack"
  fi
fi

SM_COUNT=$(kubectl get servicemonitor -n "$NAMESPACE" --no-headers 2>/dev/null | wc -l | tr -d ' ')
PM_COUNT=$(kubectl get podmonitor -n "$NAMESPACE" --no-headers 2>/dev/null | wc -l | tr -d ' ')
RULE_COUNT=$(kubectl get prometheusrule -n "$NAMESPACE" --no-headers 2>/dev/null | wc -l | tr -d ' ')
if [[ "${SM_COUNT}" -ge 1 ]] || [[ "${PM_COUNT}" -ge 1 ]]; then
  pass "ServiceMonitors: ${SM_COUNT}, PodMonitors: ${PM_COUNT}, PrometheusRules: ${RULE_COUNT}"
else
  warn "No ServiceMonitors found in '$NAMESPACE' — deploy the Helm chart with monitoring.enabled=true"
fi

# ── Summary ──────────────────────────────────────────────────────────────────
echo ""
echo "──────────────────────────────────────────"
TOTAL=$(( PASS + FAIL ))
if [[ $FAIL -eq 0 ]]; then
  echo -e "${GREEN}All ${TOTAL} checks passed.${NC}"
  exit 0
else
  echo -e "${RED}${FAIL} of ${TOTAL} checks failed.${NC}"
  exit 1
fi
