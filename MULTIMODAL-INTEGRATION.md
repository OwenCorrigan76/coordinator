# Multimodal & Disaggregated LLM Integration Guide

This document describes the coordinator integration with llm-d's EPP (Endpoint Picker) for both multimodal routing and disaggregated LLM inference.

## Overview

The Coordinator supports two types of inference workflows:

1. **Multimodal endpoints** (`/v1/audio/speech`, `/v1/audio/transcriptions`, `/v1/images/generations`) - Simple pass-through via `gateway-proxy` step
2. **Disaggregated LLM** (`/v1/chat/completions`, `/v1/completions`) - Full pipeline orchestration via `encode → prefill → decode` steps

Both workflows route through Envoy Gateway and EPP for intelligent pod selection.

## Architecture

### Multimodal Flow (Simple)
```
Client
  ↓ /v1/audio/speech
Coordinator (port 8080)
  ↓ gateway-proxy step
Envoy Gateway (inference-gateway-istio)
  ↓ ext-proc gRPC
EPP (modality filter)
  ↓ routes based on path + llm-d.ai/model-arch label
TTS Backend Pod (llm-d.ai/model-arch: autoregressive-tts)
```

### Disaggregated LLM Flow (Complex)
```
Client
  ↓ /v1/chat/completions
Coordinator (port 8080)
  ↓ encode step (sets EPP-Phase: encode)
Envoy → EPP → Encode Pod (llm-d.ai/role: encode)
  ↓
  ↓ prefill step (sets EPP-Phase: prefill)
Envoy → EPP → Prefill Pod (llm-d.ai/role: prefill)
  ↓
  ↓ decode step (sets EPP-Phase: decode)
Envoy → EPP → Decode Pod (llm-d.ai/role: decode)
  ↓
Client (streaming response)
```

## Changes Made to Coordinator

### 1. Added Gateway-Proxy Step

**File:** `pkg/steps/gateway_proxy.go`

A new pipeline step that forwards requests to Envoy Gateway without modification. Used for multimodal endpoints that don't need disaggregation.

```go
package steps

import (
	"context"
	"errors"

	"sigs.k8s.io/controller-runtime/pkg/log"
	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"

	"github.com/llm-d/coordinator/pkg/gateway"
	"github.com/llm-d/coordinator/pkg/pipeline"
)

const GatewayProxyStepName = "gateway-proxy"

func init() {
	pipeline.Register(GatewayProxyStepName, NewGatewayProxyStep)
}

type GatewayProxyStep struct {
	gwClient   *gateway.Client
	targetPath string
}

func NewGatewayProxyStep(gwClient *gateway.Client, params map[string]any) (pipeline.Step, error) {
	if gwClient == nil {
		return nil, errors.New("gateway-proxy: gateway client is required")
	}

	step := &GatewayProxyStep{
		gwClient: gwClient,
	}

	if targetPath, ok := params["target_path"].(string); ok {
		step.targetPath = targetPath
	}

	return step, nil
}

func (s *GatewayProxyStep) Name() string {
	return GatewayProxyStepName
}

func (s *GatewayProxyStep) Execute(ctx context.Context, reqCtx *pipeline.RequestContext) error {
	logger := log.FromContext(ctx).WithName(GatewayProxyStepName)

	targetPath := reqCtx.OriginalPath
	if s.targetPath != "" {
		targetPath = s.targetPath
		reqCtx.OriginalPath = s.targetPath
	}

	logger.V(logutil.DEFAULT).Info("proxying request", "path", targetPath, "stream", reqCtx.Stream)

	proxyReq, err := newDecodeProxyRequest(ctx, logger, GatewayProxyStepName, reqCtx, s.gwClient, reqCtx.Body, nil)
	if err != nil {
		return err
	}

	proxy := newDecodeProxy(logger, s.gwClient.Transport(), nil)
	proxy.ServeHTTP(reqCtx.ResponseWriter, proxyReq)

	return nil
}
```

### 2. Added Multimodal Path Constants

**File:** `pkg/gateway/paths.go`

```go
const (
	PathChatCompletions      = "/v1/chat/completions"
	PathCompletions          = "/v1/completions"
	PathAudioSpeech          = "/v1/audio/speech"
	PathAudioTranscriptions  = "/v1/audio/transcriptions"
	PathImagesGenerations    = "/v1/images/generations"
	DefaultGeneratePath      = "/inference/v1/generate"
	
	EPPPhaseHeader    = "EPP-Phase"
	ContentTypeHeader = "Content-Type"
	ContentTypeJSON   = "application/json"

	PhaseEncode  = "encode"
	PhasePrefill = "prefill"
	PhaseDecode  = "decode"
)
```

### 3. Registered Multimodal Routes

**File:** `pkg/server/server.go`

```go
r.Post(gateway.PathChatCompletions, s.handleInference)
r.Post(gateway.PathCompletions, s.handleInference)
r.Post(gateway.PathAudioSpeech, s.handleInference)
r.Post(gateway.PathAudioTranscriptions, s.handleInference)
r.Post(gateway.PathImagesGenerations, s.handleInference)
r.Get("/healthz", s.handleHealth)
r.Get("/readyz", s.handleHealth)
```

### 4. Existing Pipeline Steps

The coordinator already had disaggregation steps that set EPP-Phase headers:

- **encode.go** (line 115): `headers[gateway.EPPPhaseHeader] = gateway.PhaseEncode`
- **prefill.go** (line 92): `headers[gateway.EPPPhaseHeader] = gateway.PhasePrefill`
- **decode.go**: `headers[gateway.EPPPhaseHeader] = gateway.PhaseDecode`

## Backend Pod Configuration

### Disaggregated LLM Pods

Created three separate pod deployments with `llm-d.ai/role` labels for phase-based routing:

**File:** `deploy/kind/disaggregated-llm-pods.yaml`

```yaml
---
# Encode Pod
apiVersion: apps/v1
kind: Deployment
metadata:
  name: llm-encode-vllm-sim
spec:
  template:
    metadata:
      labels:
        app: food-review-inference-pool
        llm-d.ai/inferenceServing: "true"
        llm-d.ai/model-arch: autoregressive-llm
        llm-d.ai/role: encode  # Phase label
    spec:
      containers:
      - name: routing-sidecar
        image: ghcr.io/llm-d/llm-d-router-disagg-sidecar:dev
        args: ["--port=8000", "--vllm-port=8200", ...]
        ports:
        - containerPort: 8000
      - name: vllm
        image: ghcr.io/llm-d/llm-d-inference-sim:latest
        args: ["--port=8200", "--model=llama-3-encode", ...]
        ports:
        - containerPort: 8200

---
# Prefill Pod (similar structure with llm-d.ai/role: prefill)
---
# Decode Pod (similar structure with llm-d.ai/role: decode)
```

**Key points:**
- Each pod has TWO containers: routing-sidecar (port 8000) + vllm sim (port 8200)
- `app: food-review-inference-pool` label for EPP discovery
- `llm-d.ai/role` label: `encode`, `prefill`, or `decode`
- `llm-d.ai/model-arch: autoregressive-llm` for modality filtering

## Deployment

### Prerequisites

Before deploying the coordinator, ensure the llm-d-router infrastructure is running:

```bash
# From llm-d-router repository
cd /path/to/llm-d-router/deploy/kind
./setup-kind-cluster.sh
```

This sets up the kind cluster with EPP and backend pods.

## Pipeline Configurations

### Multimodal Pipeline (gateway-proxy)

**File:** `deploy/kind/coordinator-deployment.yaml` (ConfigMap section)

```yaml
# Default configuration in coordinator-deployment.yaml
pipeline:
  kv_connector: kv-shared-storage
  ec_connector: ec-shared-storage
  use_openai_format: true
  
  steps:
    - type: gateway-proxy
      params: {}
```

### Disaggregated LLM Pipeline (encode → prefill → decode)

**Configuration:** Edit the ConfigMap in `deploy/kind/coordinator-deployment.yaml`

```yaml
# Modified configuration for disaggregated mode
pipeline:
  kv_connector: kv-shared-storage
  ec_connector: ec-shared-storage
  use_openai_format: true
  
  steps:
    - type: encode
      params: {}
    - type: prefill
      params: {}
    - type: decode
      params: {}
```

## Building Coordinator Image

Before deploying, build the coordinator image:

```bash
# From coordinator repository root
# Build for arm64 (Mac M1/M2)
TARGETARCH=arm64 make image-build-coordinator

# For amd64 (Intel)
TARGETARCH=amd64 make image-build-coordinator

# Verify image was built
docker images | grep coordinator
```

### Load Image into Kind Cluster

```bash
kind --name llm-d-inference-scheduler-dev load docker-image ghcr.io/llm-d/llm-d-coordinator:dev
```

## Deploying Coordinator

### Option 1: Multimodal Gateway-Proxy (Default)

```bash
cd deploy/kind
kubectl apply -f coordinator-deployment.yaml
kubectl wait --for=condition=available deployment/coordinator --timeout=60s
```

### Option 2: Disaggregated LLM Pipeline

```bash
cd deploy/kind

# Deploy disaggregated pods
kubectl apply -f disaggregated-llm-pods.yaml
kubectl wait --for=condition=ready pod -l llm-d.ai/role --timeout=120s

# Edit coordinator-deployment.yaml ConfigMap to use:
#   steps:
#     - type: encode
#     - type: prefill  
#     - type: decode

# Deploy coordinator
kubectl apply -f coordinator-deployment.yaml
kubectl wait --for=condition=available deployment/coordinator --timeout=60s
```

## Testing

### Test Multimodal Routing

```bash
# Port-forward to coordinator
kubectl port-forward svc/coordinator 8080:80 &

# Test TTS endpoint
curl -X POST http://localhost:8080/v1/audio/speech \
  -H 'Content-Type: application/json' \
  -d '{"model":"tts-1","input":"hello world"}'
```

**Expected flow:**
1. Coordinator receives request on `/v1/audio/speech`
2. Gateway-proxy step forwards to Envoy
3. EPP modality-filter sees path `/v1/audio/speech`
4. Filters to pod with `llm-d.ai/model-arch: autoregressive-tts`
5. Routes to TTS backend pod

**Check coordinator logs:**
```bash
kubectl logs deployment/coordinator --tail=20
```
Expected: `{"msg":"proxying request","path":"/v1/audio/speech"}`

**Check EPP logs:**
```bash
kubectl logs deployment/<epp-deployment> -c epp --tail=50 | grep "modality-filter"
```
Expected: Pod filtered to `autoregressive-tts` architecture

### Test Disaggregated LLM

```bash
# Test LLM endpoint with disaggregated pipeline
curl -X POST http://localhost:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"llama-3-prefill","messages":[{"role":"user","content":"Hello"}]}'
```

**Expected flow:**
1. Coordinator receives `/v1/chat/completions`
2. Encode step skipped (no multimodal content)
3. Prefill step sets `EPP-Phase: prefill` header
4. EPP routes to pod with `llm-d.ai/role: prefill`
5. Decode step sets `EPP-Phase: decode` header  
6. EPP routes to pod with `llm-d.ai/role: decode`

**Check coordinator logs:**
```bash
kubectl logs deployment/coordinator --tail=30
```
Expected:
```json
{"msg":"received request","path":"/v1/chat/completions"}
{"msg":"sending request","x-request-id":"..."}  # prefill step
```

**Check EPP discovered disaggregated pods:**
```bash
kubectl logs deployment/<epp-deployment> -c epp --tail=200 | grep "Before running filter"
```
Expected: Endpoints list includes pods with `llm-d.ai/role: encode`, `prefill`, `decode`

### Verify Pod Discovery

```bash
# Check all inference pods
kubectl get pods -l llm-d.ai/inferenceServing=true -o wide

# Check disaggregated pods specifically
kubectl get pods -l llm-d.ai/role --show-labels
```

Expected output:
```
NAME                                    READY   STATUS    LABELS
llm-encode-vllm-sim-...                 2/2     Running   llm-d.ai/role=encode,...
llm-prefill-vllm-sim-...                2/2     Running   llm-d.ai/role=prefill,...
llm-decode-vllm-sim-...                 2/2     Running   llm-d.ai/role=decode,...
```

## Configuration Details

### Gateway Address

**Critical:** The coordinator must point to Envoy Gateway, not directly to EPP.

- ✅ Correct: `http://inference-gateway-istio.default.svc.cluster.local`
- ❌ Wrong: `http://epp-service:9002` (EPP expects gRPC ext-proc from Envoy)

### EPP Phase Filtering

**Current Status:** EPP's `decode-filter` plugin does NOT filter by `EPP-Phase` header or `llm-d.ai/role` label yet.

**What works:**
- ✅ Coordinator sets correct `EPP-Phase` headers
- ✅ EPP discovers all pods with `llm-d.ai/role` labels
- ✅ Full request chain: Coordinator → Envoy → EPP → Backend

**What needs configuration:**
- ⚠️ EPP needs phase-aware filtering (use `disagg-profile-handler` or configure `decode-filter`)
- Currently EPP returns all autoregressive-llm pods regardless of role

**To enable phase-based routing in EPP:**

Option 1 - Use disagg-profile-handler:
```yaml
# epp-config.yaml
plugins:
  - type: disagg-profile-handler  # Instead of single-profile-handler
    parameters:
      # Configure prefill-profile and decode-profile
```

Option 2 - Configure decode-filter to check EPP-Phase header + llm-d.ai/role label (requires EPP code changes or plugin configuration)

## How EPP Routing Works

### Modality Filter

Routes based on endpoint path → compatible model architectures:

| Endpoint Path | Compatible Architectures |
|---------------|-------------------------|
| `/v1/audio/speech` | `autoregressive-tts`, `omni-llm` |
| `/v1/audio/transcriptions` | `encoder-decoder-stt` |
| `/v1/images/generations` | `diffusion` |
| `/v1/chat/completions` | `autoregressive-llm`, `omni-llm` |

### Phase Filter (Future)

When configured with disagg-profile-handler, will route based on:
- `EPP-Phase` header (`encode`, `prefill`, `decode`)
- `llm-d.ai/role` label on pods

## Troubleshooting

### 404 Not Found from Backend

**Symptom:** Request reaches backend but returns 404

**Cause:** vllm-sim is a mock that doesn't implement all endpoints

**Solution:** This is expected behavior for testing. Real vLLM backends will handle these endpoints.

### Coordinator Returns 404

**Symptom:** Coordinator itself returns 404

**Cause:** Routes not registered in server.go

**Fix:** Verify multimodal routes are registered (see section 3 above)

### EPP Not Discovering Pods

**Symptom:** EPP logs show "Pod removed or not added"

**Causes:**
1. Missing vllm container (only routing-sidecar deployed)
2. Wrong app label (must be `food-review-inference-pool`)
3. vllm container returning 503 on health checks

**Fix:** Ensure both containers are running and healthy:
```bash
kubectl get pods -l llm-d.ai/role
# Should show 2/2 READY

kubectl logs <pod-name> -c vllm --tail=20
# Should show "Server starting"
```

### Timeout Errors

**Symptom:** Requests hang or timeout

**Cause:** Gateway address incorrect

**Fix:** Verify coordinator config:
```yaml
gateway:
  address: "http://inference-gateway-istio.default.svc.cluster.local"
```

## Repository Structure

```
coordinator/
├── pkg/                           # Source code
│   ├── steps/                     # Pipeline steps
│   │   ├── gateway_proxy.go       # NEW: Simple proxy step
│   │   ├── encode.go              # Sets EPP-Phase: encode
│   │   ├── prefill.go             # Sets EPP-Phase: prefill
│   │   └── decode.go              # Sets EPP-Phase: decode
│   ├── gateway/                   
│   │   └── paths.go               # NEW: Multimodal path constants
│   └── server/
│       └── server.go              # NEW: Multimodal route registration
├── deploy/
│   └── kind/                      # Kind deployment files
│       ├── README.md              # Quick start guide
│       ├── coordinator-deployment.yaml
│       └── disaggregated-llm-pods.yaml
└── MULTIMODAL-INTEGRATION.md     # This file
```

## Complete Deployment Workflow

### 1. Deploy llm-d-router Infrastructure (Required First)

From the [llm-d-router repository](https://github.com/rh-waterford-et/llm-d-router):

```bash
cd /path/to/llm-d-router/deploy/kind
./setup-kind-cluster.sh
```

This creates the kind cluster with EPP and backend pods.

### 2. Build and Deploy Coordinator

From this repository:

```bash
# Build image
TARGETARCH=arm64 make image-build-coordinator

# Load into kind
kind --name llm-d-inference-scheduler-dev load docker-image ghcr.io/llm-d/llm-d-coordinator:dev

# Deploy
cd deploy/kind
kubectl apply -f coordinator-deployment.yaml

# Optional: Deploy disaggregated pods
kubectl apply -f disaggregated-llm-pods.yaml
```

### 3. Test

```bash
kubectl port-forward svc/coordinator 8080:80 &
curl -X POST http://localhost:8080/v1/audio/speech \
  -H 'Content-Type: application/json' \
  -d '{"model":"tts-1","input":"test"}'
```

## Summary

**Multimodal Integration:**
- ✅ 1 new pipeline step (`gateway-proxy`)
- ✅ 3 new path constants
- ✅ 3 new route registrations
- ✅ EPP modality-filter working

**Disaggregated LLM Integration:**
- ✅ Using existing encode/prefill/decode steps
- ✅ EPP-Phase headers set correctly
- ✅ 3 separate backend pods with role labels deployed
- ✅ EPP discovers all disaggregated pods
- ⚠️ EPP phase-based filtering needs configuration (disagg-profile-handler)

The coordinator provides a unified API entry point for all inference types while EPP handles intelligent routing based on:
- Request path analysis (modality filter)
- Pod label filtering (`llm-d.ai/model-arch`, `llm-d.ai/role`)
- Real-time scoring (queue depth, KV cache utilization)

No changes to existing LLM pipeline logic were needed!
