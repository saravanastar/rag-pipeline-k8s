#!/usr/bin/env bash
# setup-registry.sh — run a local Docker registry and wire it into the kind cluster.
#
# After this runs, push images to localhost:5001/name:tag and kind nodes pull
# them directly over the Docker bridge — much faster than kind load docker-image.
#
# Safe to re-run; the registry container and configmap are idempotent.

set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-rag-pipeline}"
REGISTRY_NAME="kind-registry"
REGISTRY_PORT="5001"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info() { echo -e "${GREEN}[registry]${NC} $*"; }
warn() { echo -e "${YELLOW}[warn]${NC}  $*"; }

# ── 1. Start the registry container ──────────────────────────────────────────
if docker inspect "$REGISTRY_NAME" &>/dev/null; then
  warn "Registry container '$REGISTRY_NAME' already exists — skipping create"
else
  info "Starting local registry on port $REGISTRY_PORT ..."
  docker run -d \
    --restart=always \
    --name "$REGISTRY_NAME" \
    -p "127.0.0.1:${REGISTRY_PORT}:5000" \
    registry:2
fi

# ── 2. Connect registry to kind's Docker network ──────────────────────────────
if docker network inspect kind &>/dev/null; then
  if docker network inspect kind | grep -q "$REGISTRY_NAME"; then
    warn "Registry already connected to kind network"
  else
    info "Connecting registry to kind Docker network ..."
    docker network connect kind "$REGISTRY_NAME"
  fi
else
  warn "kind network not found — create the cluster first with 'make cluster-create'"
  exit 1
fi

# ── 3. Patch containerd on each node to trust the local registry ──────────────
# Nodes resolve 'kind-registry' via the Docker bridge (same network).
# We add a mirror so image references like 'localhost:5001/foo' work from nodes.
info "Patching containerd config on cluster nodes ..."
for NODE in $(kind get nodes --name "$CLUSTER_NAME" 2>/dev/null); do
  docker exec "$NODE" bash -c "
    mkdir -p /etc/containerd/certs.d/localhost:${REGISTRY_PORT}
    cat > /etc/containerd/certs.d/localhost:${REGISTRY_PORT}/hosts.toml <<'EOF'
[host.\"http://${REGISTRY_NAME}:5000\"]
  capabilities = [\"pull\", \"resolve\"]
EOF
  "
  info "Patched node: $NODE"
done

# ── 4. Annotate the cluster with registry info (optional, for tooling) ────────
cat <<EOF | kubectl apply -f - &>/dev/null
apiVersion: v1
kind: ConfigMap
metadata:
  name: local-registry-hosting
  namespace: kube-public
data:
  localRegistryHosting.v1: |
    host: "localhost:${REGISTRY_PORT}"
    help: "https://kind.sigs.k8s.io/docs/user/local-registry/"
EOF

info ""
info "Local registry ready: localhost:${REGISTRY_PORT}"
info ""
info "Push images with:"
info "  docker tag  rag-pipeline/chunker:latest localhost:${REGISTRY_PORT}/chunker:latest"
info "  docker push localhost:${REGISTRY_PORT}/chunker:latest"
info ""
info "Or just run: make push-images"
