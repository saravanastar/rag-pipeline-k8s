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
deploy-infra: helm-deps ## Deploy/upgrade infra only (Kafka, Redis, Milvus) — no app pods
	helm upgrade --install $(RELEASE) $(HELM_CHART) \
	  --namespace $(NAMESPACE) --create-namespace \
	  --values $(HELM_CHART)/values.yaml \
	  --set crawler.enabled=false \
	  --set chunker.enabled=false \
	  --set embeddingService.enabled=false \
	  --set queryApi.enabled=false \
	  --wait --timeout 300s

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

# ── Help ──────────────────────────────────────────────────────────────────────

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'
