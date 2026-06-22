#!/usr/bin/env bash
# setup-local.sh — bootstrap the full rag-pipeline stack on a local kind cluster.
#
# Idempotent: safe to re-run. Each step checks whether its resource already
# exists before creating it.
#
# Prerequisites (must be installed on the host):
#   kind       https://kind.sigs.k8s.io/docs/user/quick-start/#installation
#   kubectl    https://kubernetes.io/docs/tasks/tools/
#   helm       https://helm.sh/docs/intro/install/
#
# Usage:
#   ./scripts/setup-local.sh
#   ./scripts/setup-local.sh --skip-cluster   # reuse an existing kind cluster

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLUSTER_NAME="rag-pipeline"
NAMESPACE="rag"
RELEASE_NAME="rag"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()    { echo -e "${GREEN}[setup]${NC} $*"; }
warn()    { echo -e "${YELLOW}[warn]${NC}  $*"; }
error()   { echo -e "${RED}[error]${NC} $*" >&2; exit 1; }
step()    { echo -e "\n${GREEN}━━━ $* ━━━${NC}"; }

SKIP_CLUSTER=false
for arg in "$@"; do
  [[ "$arg" == "--skip-cluster" ]] && SKIP_CLUSTER=true
done

# ── 0. Preflight checks ──────────────────────────────────────────────────────
step "Preflight checks"
for cmd in kind kubectl helm; do
  command -v "$cmd" &>/dev/null || error "'$cmd' not found. Install it before running this script."
  info "$cmd: $(${cmd} version --short 2>/dev/null | head -1 || ${cmd} version | head -1)"
done

# ── 1. Create kind cluster ───────────────────────────────────────────────────
step "Kind cluster"
if $SKIP_CLUSTER; then
  warn "--skip-cluster set; assuming cluster '$CLUSTER_NAME' is already running"
elif kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
  warn "Cluster '$CLUSTER_NAME' already exists — skipping create (use 'make cluster-delete' to reset)"
else
  info "Creating kind cluster from kind-config.yaml ..."
  kind create cluster --config "$REPO_ROOT/kind-config.yaml"
fi

kubectl config use-context "kind-${CLUSTER_NAME}"
info "kubectl context: $(kubectl config current-context)"

# Create the app namespace immediately — Strimzi references it during install
# (it creates RoleBindings in the watched namespace). Must exist before step 4.
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
info "namespace '$NAMESPACE' ready"

# ── 2. Install nginx-ingress (for query-api) ─────────────────────────────────
step "nginx Ingress controller"
if kubectl get namespace ingress-nginx &>/dev/null; then
  warn "ingress-nginx already installed — skipping"
else
  info "Installing nginx ingress controller for kind ..."
  kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/main/deploy/static/provider/kind/deploy.yaml
  info "Waiting for ingress controller to be ready ..."
  kubectl wait --namespace ingress-nginx \
    --for=condition=ready pod \
    --selector=app.kubernetes.io/component=controller \
    --timeout=120s
fi

# ── 3. Install KEDA ──────────────────────────────────────────────────────────
step "KEDA"
helm repo add kedacore https://kedacore.github.io/charts 2>/dev/null || true
helm repo update kedacore
if helm status keda -n keda &>/dev/null; then
  warn "KEDA already installed — skipping"
else
  info "Installing KEDA ..."
  helm install keda kedacore/keda \
    --namespace keda \
    --create-namespace \
    --wait \
    --timeout 120s
fi

# ── 4. Install Strimzi Kafka operator ────────────────────────────────────────
step "Strimzi Kafka operator"
helm repo add strimzi https://strimzi.io/charts/ 2>/dev/null || true
helm repo update strimzi
STRIMZI_STATUS=$(helm status strimzi-kafka-operator -n strimzi -o json 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['info']['status'])" 2>/dev/null || echo "not-installed")
if [[ "$STRIMZI_STATUS" == "deployed" ]]; then
  warn "Strimzi already installed and deployed — skipping"
else
  if [[ "$STRIMZI_STATUS" != "not-installed" ]]; then
    warn "Strimzi release is in '$STRIMZI_STATUS' state — uninstalling before reinstall"
    helm uninstall strimzi-kafka-operator -n strimzi || true
    sleep 5
  fi
  info "Installing Strimzi operator ..."
  helm install strimzi-kafka-operator strimzi/strimzi-kafka-operator \
    --namespace strimzi \
    --create-namespace \
    --set watchNamespaces="{${NAMESPACE}}" \
    --wait \
    --timeout 180s
fi

# Wait for Strimzi CRDs to be established — the operator pod being ready does
# not guarantee the CRDs are registered yet; kubectl wait errors immediately if
# the CRD object doesn't exist, so we poll until it appears first.
info "Waiting for Strimzi CRDs to be established ..."
for crd in kafkas.kafka.strimzi.io kafkatopics.kafka.strimzi.io; do
  deadline=$((SECONDS + 120))
  until kubectl get crd "$crd" &>/dev/null; do
    [[ $SECONDS -ge $deadline ]] && error "Timed out waiting for CRD $crd"
    sleep 3
  done
  kubectl wait --for=condition=Established crd/"$crd" --timeout=60s
  info "CRD $crd established"
done

# ── 5. Install kube-prometheus-stack ─────────────────────────────────────────
step "kube-prometheus-stack (Prometheus + Grafana + Alertmanager)"
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts 2>/dev/null || true
helm repo update prometheus-community
if helm status kube-prometheus-stack -n monitoring &>/dev/null; then
  warn "kube-prometheus-stack already installed — skipping"
else
  info "Installing kube-prometheus-stack ..."
  helm install kube-prometheus-stack prometheus-community/kube-prometheus-stack \
    --namespace monitoring \
    --create-namespace \
    --set prometheus.prometheusSpec.podMonitorSelectorNilUsesHelmValues=false \
    --set prometheus.prometheusSpec.serviceMonitorSelectorNilUsesHelmValues=false \
    --set prometheus.prometheusSpec.ruleSelectorNilUsesHelmValues=false \
    --set grafana.adminPassword=admin \
    --set grafana.persistence.enabled=false \
    --wait \
    --timeout 300s
  # The three *SelectorNilUsesHelmValues=false flags tell Prometheus to discover
  # ALL ServiceMonitors/PodMonitors/Rules in the cluster, not just ones with
  # the Helm release label. Required when app monitors are in a different namespace.
fi

# ── 6. Local registry + app images ───────────────────────────────────────────
# Images must exist in the registry before Helm deploys (pods fail ImagePullBackOff
# if the registry is empty when the Deployment rolls out).
step "Local image registry"
chmod +x "$REPO_ROOT/scripts/setup-registry.sh"
CLUSTER_NAME="$CLUSTER_NAME" "$REPO_ROOT/scripts/setup-registry.sh"

step "Build + push app images"
REGISTRY="localhost:5001"
for SERVICE in crawler chunker embedding-service query-api; do
  SRC_DIR="$REPO_ROOT/$SERVICE"
  IMAGE="$REGISTRY/$SERVICE:latest"
  # Only build if no image exists yet; re-run 'make build-and-push' to force rebuild.
  if docker image inspect "$IMAGE" &>/dev/null; then
    warn "$IMAGE already built — skipping (run 'make build-and-push' to rebuild)"
  else
    info "Building $IMAGE ..."
    docker build -t "$IMAGE" "$SRC_DIR"
  fi
  info "Pushing $IMAGE to local registry ..."
  docker push "$IMAGE"
done

# ── 6a. Add Helm repos for app dependencies ───────────────────────────────────
step "Helm repos"
helm repo add bitnami https://charts.bitnami.com/bitnami 2>/dev/null || true
helm repo add milvus  https://zilliztech.github.io/milvus-helm 2>/dev/null || true
helm repo update

# ── 6b. Update umbrella chart dependencies ───────────────────────────────────
step "Helm dependency update"
helm dependency update "$REPO_ROOT/helm/rag-pipeline"

# ── 8. Deploy the umbrella chart ─────────────────────────────────────────────
step "Helm install/upgrade"
info "Deploying release '$RELEASE_NAME' into namespace '$NAMESPACE' ..."

# Full stack — all components enabled from milestone 6 onward.
helm upgrade --install "$RELEASE_NAME" "$REPO_ROOT/helm/rag-pipeline" \
  --namespace "$NAMESPACE" \
  --values "$REPO_ROOT/helm/rag-pipeline/values.yaml" \
  --wait \
  --timeout 600s

# ── 9. Apply KEDA ScaledObjects ──────────────────────────────────────────────
step "KEDA ScaledObjects"
kubectl apply -f "$REPO_ROOT/k8s/keda/chunker-scaledobject.yaml" -n "$NAMESPACE"
kubectl apply -f "$REPO_ROOT/k8s/keda/embedding-scaledobject.yaml" -n "$NAMESPACE"

# ── 10. Health check ─────────────────────────────────────────────────────────
step "Health check"
"$REPO_ROOT/scripts/health-check.sh"

info ""
info "Setup complete. Run 'make status' to see pod states."
info ""
info "Service endpoints (in-cluster):"
info "  Kafka:   kafka-cluster-kafka-bootstrap.${NAMESPACE}.svc.cluster.local:9092"
info "  Milvus:  milvus-milvus.${NAMESPACE}.svc.cluster.local:19530"
info "  Redis:   rag-redis-master.${NAMESPACE}.svc.cluster.local:6379"
info ""
info "Observability (run in separate terminals):"
info "  make port-prometheus  →  http://localhost:9090"
info "  make port-grafana     →  http://localhost:3000  (admin/admin)"
info "  Dashboard auto-loaded: Dashboards → RAG Pipeline"
info ""
info "Trigger a crawl: make crawl"
info "Run a query:     make port-query (then) make query"
