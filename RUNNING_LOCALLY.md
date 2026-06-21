# Running Locally

This guide gets the full RAG pipeline running on your laptop using a local
[kind](https://kind.sigs.k8s.io/) Kubernetes cluster. No cloud account needed.

---

## Prerequisites

Install these tools before starting:

```bash
# macOS (Homebrew)
brew install kind kubectl helm docker

# Verify
kind version        # v0.23+
kubectl version     # v1.29+
helm version        # v3.14+
docker info         # Docker must be running
```

> **Docker Desktop users:** allocate at least **6 GB RAM** and **4 CPUs** in
> Docker Desktop → Settings → Resources. Milvus + Kafka + Prometheus together
> need it.

---

## Step 1 — Clone and bootstrap

```bash
git clone https://github.com/your-org/rag-pipeline-k8s
cd rag-pipeline-k8s

make setup
```

`make setup` is fully idempotent (safe to re-run). It does, in order:

| Step | What happens |
|---|---|
| Preflight | Checks kind / kubectl / helm are installed |
| kind cluster | Creates a 3-node cluster (`rag-pipeline`) from `kind-config.yaml` |
| nginx Ingress | Installs the ingress controller; maps host ports 8080/8443 |
| KEDA | Installs the KEDA operator (Kafka-lag autoscaler) |
| Strimzi | Installs the Strimzi Kafka operator |
| kube-prometheus-stack | Installs Prometheus + Grafana + Alertmanager |
| Helm deps | Pulls Redis and Milvus subchart tarballs |
| Helm deploy | Installs the `rag` release (Kafka, Redis, Milvus, all app pods) |
| KEDA ScaledObjects | Applies chunker + embedding-service autoscaling configs |
| Health check | Smoke-tests every component |

**Total time: ~10–15 minutes** on first run (operator images are large).

---

## Step 2 — Verify everything is up

```bash
make health     # smoke tests: Kafka Ready, topics exist, Redis PING, Milvus ready, KEDA up
make status     # pod overview across rag / keda / strimzi / monitoring namespaces
```

Expected `make status` output (abbreviated):

```
── rag namespace ──
NAME                          READY   STATUS    RESTARTS
pod/embedding-service-xxx     1/1     Running   0
pod/query-api-xxx             1/1     Running   0
pod/milvus-milvus-xxx         1/1     Running   0
pod/rag-redis-master-0        1/1     Running   0

── KEDA ScaledObjects ──
NAME                SCALETARGETKIND   MIN   MAX   READY
chunker             Deployment        0     4     True
embedding-service   Deployment        0     8     True
```

> The **chunker** and **embedding-service** show 0 ready replicas — that is
> correct. KEDA scales them to zero when there is nothing to process.

---

## Step 3 — Trigger a crawl

```bash
make crawl
```

This creates a one-off Job from the CronJob spec. The crawler fetches
`kubernetes.io/docs/concepts/`, computes content hashes, and emits
`page.crawled` events to Kafka.

Watch the pipeline process them in real time (open separate terminals):

```bash
# Terminal 1 — crawler progress
kubectl logs -n rag -l app.kubernetes.io/name=crawler -f

# Terminal 2 — KEDA scaling chunker up as lag grows
kubectl get pods -n rag -l app.kubernetes.io/name=chunker -w

# Terminal 3 — embedding service processing chunks
kubectl logs -n rag -l app.kubernetes.io/name=embedding-service -f
```

A full crawl of `docs/concepts/` (~200 pages) takes about 5–10 minutes.
On subsequent runs most pages are skipped (content hash unchanged) — you'll
see `skipped=190 emitted=3` style output.

---

## Step 4 — Query the pipeline

### Option A — via Ingress (recommended, no port-forward)

Add one line to `/etc/hosts`:

```bash
echo "127.0.0.1 rag.local" | sudo tee -a /etc/hosts
```

Then query directly on port 8080:

```bash
curl -s http://rag.local:8080/api/query \
  -H "Content-Type: application/json" \
  -d '{"text": "how does a Kubernetes pod restart policy work", "top_k": 5}' \
  | python3 -m json.tool
```

### Option B — via port-forward

```bash
make port-query        # runs in foreground; keep this terminal open
# in another terminal:
make query             # fires a sample question and pretty-prints the response
```

---

## Step 5 — Observe in Grafana

### Option A — MetalLB (recommended, gives a real IP)

```bash
make metallb           # installs MetalLB + assigns IPs to Grafana and Prometheus
make ips               # prints all LoadBalancer IPs
```

Then open the printed IP for Grafana in your browser (e.g. `http://172.18.255.200`).

### Option B — port-forward (fallback)

```bash
make port-grafana      # forwards Grafana to http://localhost:3000
```

**Login:** `admin` / `admin`

The RAG Pipeline dashboard is auto-loaded under **Dashboards → RAG Pipeline**.
It shows:
- Kafka consumer lag + KEDA replica scaling
- Crawler skip ratio (incremental diff efficiency)
- Embedding throughput and inference latency
- Query API p99 latency breakdown
- Active alerts

To view Prometheus directly:

```bash
make port-prometheus   # http://localhost:9090
# or after `make metallb`:
make ips               # use the Prometheus IP
```

---

## All make commands

Run `make help` at any time to see this list.

### Cluster lifecycle

| Command | What it does |
|---|---|
| `make setup` | **Start here.** Full idempotent bootstrap (cluster → operators → chart). |
| `make setup-skip-cluster` | Re-run setup but reuse an existing kind cluster. |
| `make cluster-create` | Create the kind cluster only. |
| `make cluster-delete` | Delete the kind cluster (destroys all data). |
| `make cluster-reset` | Delete + recreate the cluster from scratch. |

### Deploy

| Command | What it does |
|---|---|
| `make deploy` | Helm upgrade + apply KEDA ScaledObjects. |
| `make deploy-infra` | Same as deploy (full stack). |
| `make uninstall` | Uninstall the Helm release (keeps PVCs and namespace). |

### Helm

| Command | What it does |
|---|---|
| `make helm-deps` | Pull Redis + Milvus subchart tarballs. |
| `make helm-lint` | Lint the umbrella chart. |
| `make helm-template` | Render all templates to stdout (dry-run, no cluster needed). |
| `make helm-diff` | Diff running release vs local chart (requires `helm-diff` plugin). |

### Health and status

| Command | What it does |
|---|---|
| `make health` | Smoke tests: Kafka Ready, topics, Redis PING, Milvus, KEDA, monitors. |
| `make status` | Pod/svc/pvc overview across rag, keda, strimzi namespaces. |

### Pipeline operations

| Command | What it does |
|---|---|
| `make crawl` | Trigger a one-off crawl Job from the CronJob spec. |
| `make query` | Fire a sample query (requires `make port-query` running). |
| `make smoke-test` | End-to-end: trigger crawl → wait 60s → run query. |

### Networking

| Command | What it does |
|---|---|
| `make metallb` | Install MetalLB + assign real IPs to Grafana and Prometheus. |
| `make ips` | List all LoadBalancer IPs in the cluster. |
| `make port-query` | Forward query-api to `localhost:8000`. |
| `make port-prometheus` | Forward Prometheus to `localhost:9090` (fallback). |
| `make port-grafana` | Forward Grafana to `localhost:3000` (fallback). |
| `make port-milvus` | Forward Milvus gRPC to `localhost:19530`. |
| `make port-redis` | Forward Redis to `localhost:6379`. |
| `make import-dashboard` | Import Grafana dashboard via API (`GRAFANA_URL=http://...`). |

### Logs

| Command | What it does |
|---|---|
| `make logs-kafka` | Tail Strimzi operator logs. |
| `make logs-milvus` | Tail Milvus standalone logs. |
| `make logs-redis` | Tail Redis logs. |

---

## Teardown

```bash
make cluster-delete     # removes the kind cluster and all data
```

To remove only the app (keep the cluster and operators):

```bash
make uninstall
```

---

## Common issues

**`<pending>` external IP on a Service**  
kind has no cloud provider. Either use the Ingress (port 8080) or run `make metallb`.

**Chunker/embedding pods not starting**  
That's expected — KEDA scales them to zero when Kafka lag is zero. Run `make crawl` to trigger work and watch them scale up.

**Milvus init job still running**  
Milvus takes 60–90s to become ready on first install. The init job retries automatically. Check with:
```bash
kubectl logs job/milvus-init -n rag
```

**`ERROR: no endpoints available for service "kafka-cluster-kafka-bootstrap"`**  
Strimzi is still reconciling the Kafka cluster. Wait 2–3 minutes and retry:
```bash
kubectl get kafka -n rag    # wait for READY=True
```

**`query returned 0 results`**  
The embedding pipeline may not have finished yet. Check:
```bash
kubectl logs -n rag -l app.kubernetes.io/name=embedding-service | tail -20
# look for "embedded + inserted batch of N chunks"
```

**Docker Desktop OOM**  
Increase Docker memory to 8 GB. Milvus standalone + etcd + minio together use ~2 GB.
