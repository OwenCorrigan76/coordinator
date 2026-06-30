# Kind Cluster Deployment for Coordinator Testing

This directory contains the deployment files for testing the coordinator with multimodal and disaggregated LLM inference in a local kind cluster.

## Prerequisites

**1. Build llm-d-router images:**

From llm-d-router repository root:
```bash
make docker-build-epp
make docker-build-sidecar
```

**2. Deploy llm-d-router infrastructure:**

```bash
cd /path/to/llm-d-router/deploy/kind
./setup-kind-cluster.sh
```

This script will:
- Create kind cluster `llm-d-inference-scheduler-dev`
- Load Docker images (EPP, sidecar, vllm-sim)
- Deploy CRDs, EPP, Envoy Gateway
- Deploy backend pods (TTS, STT, Image, LLM)
- Wait for all components to be ready

Verify before proceeding:
```bash
kubectl get pods
# All pods should show Running/Ready
```

## Files

- `coordinator-deployment.yaml` - Coordinator deployment with ConfigMap
- `disaggregated-llm-pods.yaml` - Phase-labeled pods (encode, prefill, decode)

## Deployment Options

### Option 1: Multimodal Gateway-Proxy (Simple)

Deploy coordinator with simple pass-through for multimodal endpoints:

```bash
kubectl apply -f coordinator-deployment.yaml
```

The default config uses `gateway-proxy` step:
```yaml
pipeline:
  steps:
    - type: gateway-proxy
```

### Option 2: Disaggregated LLM Pipeline (Advanced)

**Step 1: Deploy disaggregated backend pods**

```bash
kubectl apply -f disaggregated-llm-pods.yaml

# Wait for pods
kubectl wait --for=condition=ready pod -l llm-d.ai/role --timeout=120s
```

**Step 2: Update coordinator config to use disaggregated pipeline**

Edit `coordinator-deployment.yaml` ConfigMap:

```yaml
pipeline:
  steps:
    - type: encode
      params: {}
    - type: prefill
      params: {}
    - type: decode
      params: {}
```

**Step 3: Deploy coordinator**

```bash
kubectl apply -f coordinator-deployment.yaml
```

## Testing

### Test Multimodal Routing

```bash
# Port-forward
kubectl port-forward svc/coordinator 8080:80 &

# Test TTS
curl -X POST http://localhost:8080/v1/audio/speech \
  -H 'Content-Type: application/json' \
  -d '{"model":"tts-1","input":"hello"}'
```

**Expected:**
- Coordinator receives request
- Gateway-proxy forwards to Envoy
- EPP routes to TTS pod
- Response returned

### Test Disaggregated LLM

```bash
# Test chat completions
curl -X POST http://localhost:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"llama-3","messages":[{"role":"user","content":"hi"}]}'
```

**Expected:**
- Coordinator executes encode → prefill → decode pipeline
- Each step sets EPP-Phase header
- Requests route through Envoy → EPP → respective pods

### Verify Coordinator Logs

```bash
kubectl logs deployment/coordinator --tail=30

# Look for:
# - "received request"
# - "proxying request" (gateway-proxy mode)
# - "sending request" (disaggregated mode)
```

### Verify EPP Discovery

```bash
kubectl logs deployment/food-review-endpoint-picker -c epp --tail=200 | \
  grep "Before running filter plugins" -A1

# Should show all pods including:
# - llm-encode-vllm-sim (llm-d.ai/role: encode)
# - llm-prefill-vllm-sim (llm-d.ai/role: prefill)
# - llm-decode-vllm-sim (llm-d.ai/role: decode)
```

## Architecture

### Multimodal Flow
```
Client
  ↓ /v1/audio/speech
Coordinator (gateway-proxy)
  ↓
Envoy → EPP → TTS Pod
```

### Disaggregated LLM Flow
```
Client
  ↓ /v1/chat/completions
Coordinator
  ↓ encode step (EPP-Phase: encode)
Envoy → EPP → Encode Pod
  ↓ prefill step (EPP-Phase: prefill)
Envoy → EPP → Prefill Pod
  ↓ decode step (EPP-Phase: decode)
Envoy → EPP → Decode Pod
  ↓
Client (response)
```

## Disaggregated Pods

| Pod Name | Label | Model | Containers |
|----------|-------|-------|------------|
| llm-encode-vllm-sim | llm-d.ai/role: encode | llama-3-encode | sidecar + vllm |
| llm-prefill-vllm-sim | llm-d.ai/role: prefill | llama-3-prefill | sidecar + vllm |
| llm-decode-vllm-sim | llm-d.ai/role: decode | llama-3-decode | sidecar + vllm |

## Troubleshooting

### Coordinator Pod Not Starting

```bash
# Check image
kubectl get deployment coordinator -o yaml | grep image

# Check logs
kubectl logs deployment/coordinator --tail=50
```

### Config Not Loading

```bash
# Verify ConfigMap
kubectl get configmap coordinator-config -o yaml

# Check pod args
kubectl get deployment coordinator -o yaml | grep -A3 args
```

### 404 from Backend

Expected - vllm-sim is a mock. The routing logic is what we're testing.

## Cleanup

```bash
# Remove coordinator
kubectl delete -f coordinator-deployment.yaml

# Remove disaggregated pods
kubectl delete -f disaggregated-llm-pods.yaml

# Full cleanup (from llm-d-router repo)
kind delete cluster --name llm-d-inference-scheduler-dev
```

## Documentation

See [MULTIMODAL-INTEGRATION.md](../../MULTIMODAL-INTEGRATION.md) in the repo root for detailed documentation.
