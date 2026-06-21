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
if helm status strimzi-kafka-operator -n strimzi &>/dev/null; then
  warn "Strimzi already installed — skipping"
else
  info "Installing Strimzi operator ..."
  helm install strimzi-kafka-operator strimzi/strimzi-kafka-operator \
    --namespace strimzi \
    --create-namespace \
    --set watchNamespaces="{${NAMESPACE}}" \
    --wait \
    --timeout 180s
fi

# ── 5. Add Helm repos for app dependencies ───────────────────────────────────
step "Helm repos"
helm repo add bitnami https://charts.bitnami.com/bitnami 2>/dev/null || true
helm repo add milvus  https://zilliztech.github.io/milvus-helm 2>/dev/null || true
helm repo update

# ── 6. Update umbrella chart dependencies ───────────────────────────────────
step "Helm dependency update"
helm dependency update "$REPO_ROOT/helm/rag-pipeline"

# ── 7. Create namespace ──────────────────────────────────────────────────────
step "Namespace"
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

# ── 8. Deploy the umbrella chart ─────────────────────────────────────────────
step "Helm install/upgrade"
info "Deploying release '$RELEASE_NAME' into namespace '$NAMESPACE' ..."

# Milestone 4: crawler + chunker enabled. Embedding/query enabled in later milestones.
helm upgrade --install "$RELEASE_NAME" "$REPO_ROOT/helm/rag-pipeline" \
  --namespace "$NAMESPACE" \
  --values "$REPO_ROOT/helm/rag-pipeline/values.yaml" \
  --set embeddingService.enabled=false \
  --set queryApi.enabled=false \
  --wait \
  --timeout 300s

# ── 9. Apply KEDA ScaledObjects ──────────────────────────────────────────────
step "KEDA ScaledObjects"
# Apply chunker ScaledObject. Embedding ScaledObject activates in milestone 5.
kubectl apply -f "$REPO_ROOT/k8s/keda/chunker-scaledobject.yaml" -n "$NAMESPACE"

# ── 10. Health check ─────────────────────────────────────────────────────────
step "Health check"
"$REPO_ROOT/scripts/health-check.sh"

info ""
info "Setup complete. Run 'make status' to see pod states."
info "Kafka bootstrap (in-cluster): kafka-cluster-kafka-bootstrap.${NAMESPACE}.svc.cluster.local:9092"
info "Milvus gRPC    (in-cluster): milvus-milvus.${NAMESPACE}.svc.cluster.local:19530"
info "Redis          (in-cluster): rag-redis-master.${NAMESPACE}.svc.cluster.local:6379"
