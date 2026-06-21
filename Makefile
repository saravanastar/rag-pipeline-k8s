# rag-pipeline-k8s Makefile
# All commands target the local kind cluster unless otherwise noted.

CLUSTER_NAME   := rag-pipeline
NAMESPACE      := rag
RELEASE        := rag
HELM_CHART     := helm/rag-pipeline
KEDA_MANIFESTS := k8s/keda

.DEFAULT_GOAL := help

# ── Cluster lifecycle ─────────────────────────────────────────────────────────

.PHONY: cluster-create
cluster-create: ## Create the local kind cluster
	kind create cluster --config kind-config.yaml

.PHONY: cluster-delete
cluster-delete: ## Delete the local kind cluster (destroys all data)
	kind delete cluster --name $(CLUSTER_NAME)

.PHONY: cluster-reset
cluster-reset: cluster-delete cluster-create ## Delete and recreate the cluster

# ── Full local setup ──────────────────────────────────────────────────────────

.PHONY: setup
setup: ## Bootstrap everything: cluster + operators + infra chart (idempotent)
	@chmod +x scripts/setup-local.sh
	./scripts/setup-local.sh

.PHONY: setup-skip-cluster
setup-skip-cluster: ## Re-run setup but reuse existing kind cluster
	@chmod +x scripts/setup-local.sh
	./scripts/setup-local.sh --skip-cluster

# ── Helm operations ───────────────────────────────────────────────────────────

.PHONY: helm-deps
helm-deps: ## Update umbrella chart dependencies (pulls Redis + Milvus subcharts)
	helm dependency update $(HELM_CHART)

.PHONY: helm-lint
helm-lint: ## Lint the umbrella chart
	helm lint $(HELM_CHART)

.PHONY: helm-template
helm-template: ## Render chart templates to stdout (dry-run, no cluster needed)
	helm template $(RELEASE) $(HELM_CHART) --namespace $(NAMESPACE)

.PHONY: helm-diff
helm-diff: ## Show diff between running release and local chart (requires helm-diff plugin)
	helm diff upgrade $(RELEASE) $(HELM_CHART) --namespace $(NAMESPACE) \
	  --values $(HELM_CHART)/values.yaml

.PHONY: deploy-infra
deploy-infra: helm-deps ## Deploy/upgrade the full stack (all milestones)
	helm upgrade --install $(RELEASE) $(HELM_CHART) \
	  --namespace $(NAMESPACE) --create-namespace \
	  --values $(HELM_CHART)/values.yaml \
	  --wait --timeout 300s
	kubectl apply -f $(KEDA_MANIFESTS)/ -n $(NAMESPACE)

.PHONY: deploy
deploy: helm-deps ## Deploy/upgrade full stack (all components)
	helm upgrade --install $(RELEASE) $(HELM_CHART) \
	  --namespace $(NAMESPACE) --create-namespace \
	  --values $(HELM_CHART)/values.yaml \
	  --wait --timeout 300s
	kubectl apply -f $(KEDA_MANIFESTS)/ -n $(NAMESPACE)

.PHONY: uninstall
uninstall: ## Uninstall the Helm release (keeps namespace and PVCs)
	helm uninstall $(RELEASE) -n $(NAMESPACE)

# ── Observability ─────────────────────────────────────────────────────────────

.PHONY: health
health: ## Run the infra health-check script
	@chmod +x scripts/health-check.sh
	./scripts/health-check.sh

.PHONY: status
status: ## Show pod status across all relevant namespaces
	@echo "\n── rag namespace ──"
	kubectl get pods,svc,pvc -n $(NAMESPACE)
	@echo "\n── keda namespace ──"
	kubectl get pods -n keda
	@echo "\n── strimzi namespace ──"
	kubectl get pods -n strimzi
	@echo "\n── Kafka cluster ──"
	kubectl get kafka,kafkatopic -n $(NAMESPACE)
	@echo "\n── KEDA ScaledObjects ──"
	kubectl get scaledobjects -n $(NAMESPACE) 2>/dev/null || echo "(none deployed yet)"

.PHONY: logs-kafka
logs-kafka: ## Tail Strimzi operator logs
	kubectl logs -n strimzi -l name=strimzi-cluster-operator -f

.PHONY: logs-milvus
logs-milvus: ## Tail Milvus standalone logs
	kubectl logs -n $(NAMESPACE) -l app.kubernetes.io/name=milvus -f

.PHONY: logs-redis
logs-redis: ## Tail Redis logs
	kubectl logs -n $(NAMESPACE) -l app.kubernetes.io/name=redis -f

# ── Manual crawl trigger ──────────────────────────────────────────────────────

.PHONY: crawl
crawl: ## Trigger a one-off crawl job from the CronJob spec
	kubectl create job --from=cronjob/crawler manual-crawl-$(shell date +%s) -n $(NAMESPACE)

.PHONY: query
query: ## Run a test query against the local query-api (requires port-query to be running)
	@curl -s -X POST http://localhost:8000/query \
	  -H "Content-Type: application/json" \
	  -d '{"text": "how does a Kubernetes pod restart policy work", "top_k": 3}' | python3 -m json.tool

.PHONY: smoke-test
smoke-test: ## End-to-end smoke test: trigger crawl, wait, run query
	@echo "Triggering crawl..."
	kubectl create job --from=cronjob/crawler smoke-test-$(shell date +%s) -n $(NAMESPACE)
	@echo "Waiting 60s for crawl + chunk + embed pipeline to run..."
	@sleep 60
	@echo "Running test query via port-forward..."
	@kubectl port-forward svc/query-api 8000:8000 -n $(NAMESPACE) &
	@sleep 2
	@curl -s -X POST http://localhost:8000/query \
	  -H "Content-Type: application/json" \
	  -d '{"text": "what is a Kubernetes deployment", "top_k": 3}' | python3 -m json.tool
	@pkill -f "kubectl port-forward svc/query-api" || true

# ── Port-forwarding shortcuts ─────────────────────────────────────────────────

.PHONY: port-milvus
port-milvus: ## Forward Milvus gRPC to localhost:19530
	kubectl port-forward svc/milvus-milvus 19530:19530 -n $(NAMESPACE)

.PHONY: port-redis
port-redis: ## Forward Redis to localhost:6379
	kubectl port-forward svc/rag-redis-master 6379:6379 -n $(NAMESPACE)

.PHONY: port-query
port-query: ## Forward query-api to localhost:8000
	kubectl port-forward svc/query-api 8000:8000 -n $(NAMESPACE)

.PHONY: metallb
metallb: ## Install MetalLB so LoadBalancer Services get real IPs (no port-forward needed)
	@chmod +x scripts/install-metallb.sh
	./scripts/install-metallb.sh
	@echo ""
	@echo "Patch Grafana + Prometheus to LoadBalancer so you can hit them directly:"
	kubectl patch svc kube-prometheus-stack-grafana \
	  -n monitoring -p '{"spec":{"type":"LoadBalancer"}}'
	kubectl patch svc kube-prometheus-stack-prometheus \
	  -n monitoring -p '{"spec":{"type":"LoadBalancer"}}'
	@echo ""
	@echo "IPs assigned:"
	@kubectl get svc kube-prometheus-stack-grafana kube-prometheus-stack-prometheus \
	  -n monitoring --no-headers | awk '{print $$1, $$4, $$5}'

.PHONY: ips
ips: ## Show all LoadBalancer IPs in the cluster
	@echo "\n── LoadBalancer services ──"
	@kubectl get svc -A --field-selector spec.type=LoadBalancer \
	  -o custom-columns='NAMESPACE:.metadata.namespace,NAME:.metadata.name,EXTERNAL-IP:.status.loadBalancer.ingress[0].ip,PORT:.spec.ports[*].port'

.PHONY: port-prometheus
port-prometheus: ## Forward Prometheus UI to localhost:9090 (fallback if MetalLB not installed)
	kubectl port-forward svc/kube-prometheus-stack-prometheus 9090:9090 -n monitoring

.PHONY: port-grafana
port-grafana: ## Forward Grafana to localhost:3000 (fallback if MetalLB not installed)
	kubectl port-forward svc/kube-prometheus-stack-grafana 3000:80 -n monitoring

.PHONY: import-dashboard
import-dashboard: ## Import the RAG pipeline Grafana dashboard (set GRAFANA_URL if using LB IP)
	$(eval GRAFANA_URL ?= http://localhost:3000)
	@echo "Importing dashboard to $(GRAFANA_URL) ..."
	@curl -s -X POST admin:admin@$(GRAFANA_URL)/api/dashboards/import \
	  -H "Content-Type: application/json" \
	  -d "{\"dashboard\": $$(cat dashboards/rag-pipeline-grafana.json), \"overwrite\": true, \"folderId\": 0}" \
	  | python3 -m json.tool

# ── Help ──────────────────────────────────────────────────────────────────────

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'
