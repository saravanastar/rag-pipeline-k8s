#!/usr/bin/env bash
# install-metallb.sh — add a real LoadBalancer to the local kind cluster.
#
# After this runs, Services with type: LoadBalancer get an actual IP address
# in the 172.18.255.x range (kind's Docker bridge subnet) that you can curl
# directly from your laptop without any port-forward.
#
# Usage:
#   ./scripts/install-metallb.sh

set -euo pipefail

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info() { echo -e "${GREEN}[metallb]${NC} $*"; }
warn() { echo -e "${YELLOW}[warn]${NC}  $*"; }

# ── 1. Install MetalLB ────────────────────────────────────────────────────────
info "Installing MetalLB..."
kubectl apply -f https://raw.githubusercontent.com/metallb/metallb/v0.14.5/config/manifests/metallb-native.yaml

info "Waiting for MetalLB controller to be ready..."
kubectl wait --namespace metallb-system \
  --for=condition=ready pod \
  --selector=app=metallb,component=controller \
  --timeout=120s

# ── 2. Find kind's Docker bridge subnet ───────────────────────────────────────
# kind uses the Docker bridge network (usually 172.18.0.0/16).
# We allocate the top of that range for MetalLB IPs so they don't
# conflict with kind node IPs (which start from .2).
DOCKER_SUBNET=$(docker network inspect kind \
  --format '{{(index .IPAM.Config 0).Subnet}}' 2>/dev/null || echo "172.18.0.0/16")

# Take the last /28 of the subnet (x.x.255.200–x.x.255.250) as the IP pool.
SUBNET_PREFIX=$(echo "$DOCKER_SUBNET" | cut -d'.' -f1-2)
POOL_START="${SUBNET_PREFIX}.255.200"
POOL_END="${SUBNET_PREFIX}.255.250"

info "Docker bridge subnet: $DOCKER_SUBNET"
info "MetalLB IP pool: $POOL_START – $POOL_END"

# ── 3. Configure MetalLB IPAddressPool + L2Advertisement ─────────────────────
kubectl apply -f - <<EOF
apiVersion: metallb.io/v1beta1
kind: IPAddressPool
metadata:
  name: kind-pool
  namespace: metallb-system
spec:
  addresses:
    - ${POOL_START}-${POOL_END}
---
# L2Advertisement makes MetalLB announce IPs via ARP on the Docker bridge.
# This is what lets your laptop reach those IPs without routing changes.
apiVersion: metallb.io/v1beta1
kind: L2Advertisement
metadata:
  name: kind-l2
  namespace: metallb-system
spec:
  ipAddressPools:
    - kind-pool
EOF

info ""
info "MetalLB installed. Services with type: LoadBalancer now get real IPs."
info "Check assigned IPs with: kubectl get svc -A | grep LoadBalancer"
