# Multimodal Endpoint Integration Guide

This document describes how we integrated multimodal endpoints (TTS, STT, image generation) with the llm-d Coordinator for routing through EPP (Endpoint Picker).

## Overview

The Coordinator now supports routing multimodal inference requests (`/v1/audio/speech`, `/v1/audio/transcriptions`, `/v1/images/generations`) through a simple gateway-proxy pipeline step. These requests are forwarded to an Envoy Gateway, which uses EPP's modality filter to route to the correct backend pods based on `llm-d.ai/model-arch` labels.

## Architecture

```
Client
  ↓
Coordinator (port 8080)
  ↓ gateway-proxy step
Envoy Gateway (inference-gateway-istio)
  ↓ ext-proc gRPC
EPP (modality filter)
  ↓ routes based on path + llm-d.ai/model-arch label
Backend Pods (TTS/STT/Image/LLM)
```

## Changes Made

### 1. Added Gateway-Proxy Step

**File:** `pkg/steps/gateway_proxy.go`

A new pipeline step that forwards requests to the Envoy Gateway without modification. This is simpler than the full LLM disaggregation pipeline and is suitable for endpoints that don't need encode/prefill/decode orchestration.

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

## Building and Deploying

### Prerequisites

- Docker Desktop running
- Access to a Kubernetes cluster with:
  - Envoy Gateway (e.g., `inference-gateway-istio`)
  - EPP deployment with modality filter enabled
  - Backend pods labeled with `llm-d.ai/model-arch`

### Build Coordinator Image

```bash
# Build for arm64 (Mac M1/M2)
TARGETARCH=arm64 make image-build-coordinator

# For amd64 (Intel)
TARGETARCH=amd64 make image-build-coordinator

# Verify image was built
docker images | grep coordinator
```

### Load into Kind Cluster (if using kind)

```bash
kind --name <cluster-name> load docker-image ghcr.io/llm-d/llm-d-coordinator:dev
```

### Deploy to Kubernetes

**Configuration:** `configs/coordinator-multimodal.yaml`

```yaml
log_level: 2

server:
  listen_addr: ":8080"
  read_timeout: 30s
  write_timeout: 120s
  shutdown_timeout: 25s

gateway:
  # IMPORTANT: Point to Envoy Gateway, not EPP directly
  address: "http://inference-gateway-istio.default.svc.cluster.local"
  max_idle_conns_per_host: 100
  idle_conn_timeout: 90s
  timeout: 60s

pipeline:
  kv_connector: kv-shared-storage
  ec_connector: ec-shared-storage
  use_openai_format: true

  steps:
    - type: gateway-proxy
      params: {}
```

**Kubernetes Deployment:**

```yaml
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: coordinator-config
  namespace: default
data:
  coordinator.yaml: |
    log_level: 2
    server:
      listen_addr: ":8080"
      read_timeout: 30s
      write_timeout: 120s
      shutdown_timeout: 25s
    gateway:
      address: "http://inference-gateway-istio.default.svc.cluster.local"
      max_idle_conns_per_host: 100
      idle_conn_timeout: 90s
      timeout: 60s
    pipeline:
      kv_connector: kv-shared-storage
      ec_connector: ec-shared-storage
      use_openai_format: true
      steps:
        - type: gateway-proxy
          params: {}

---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: coordinator
  namespace: default
  labels:
    app: coordinator
spec:
  replicas: 1
  selector:
    matchLabels:
      app: coordinator
  template:
    metadata:
      labels:
        app: coordinator
    spec:
      containers:
      - name: coordinator
        image: ghcr.io/llm-d/llm-d-coordinator:dev
        imagePullPolicy: IfNotPresent
        args:
        - --config=/etc/coordinator/coordinator.yaml
        ports:
        - containerPort: 8080
          name: http
          protocol: TCP
        volumeMounts:
        - name: config
          mountPath: /etc/coordinator
          readOnly: true
      volumes:
      - name: config
        configMap:
          name: coordinator-config

---
apiVersion: v1
kind: Service
metadata:
  name: coordinator
  namespace: default
spec:
  selector:
    app: coordinator
  ports:
  - port: 80
    targetPort: 8080
    protocol: TCP
    name: http
  type: ClusterIP
```

**Deploy:**

```bash
kubectl apply -f deploy/coordinator-multimodal.yaml
kubectl rollout status deployment/coordinator --timeout=60s
kubectl get pods -l app=coordinator
```

## Testing

### Test TTS Endpoint

```bash
# Port-forward to coordinator
kubectl port-forward svc/coordinator 8080:80 &

# Test TTS endpoint
curl -X POST http://localhost:8080/v1/audio/speech \
  -H 'Content-Type: application/json' \
  -d '{"model":"tts-1","input":"hello world"}'
```

### Test STT Endpoint

```bash
curl -X POST http://localhost:8080/v1/audio/transcriptions \
  -H 'Content-Type: application/json' \
  -d '{"model":"whisper-1","file":"test.wav"}'
```

### Test Image Generation Endpoint

```bash
curl -X POST http://localhost:8080/v1/images/generations \
  -H 'Content-Type: application/json' \
  -d '{"model":"dall-e-3","prompt":"a test image"}'
```

### Verify Routing

Check that requests flow through the full stack:

```bash
# Check coordinator logs
kubectl logs deployment/coordinator --tail=20

# Check EPP logs (if accessible)
kubectl logs deployment/<epp-deployment> -c epp --tail=50 | grep "modality-filter"
```

**Expected coordinator logs:**
```json
{"msg":"received request","path":"/v1/audio/speech","model":"tts-1"}
{"msg":"proxying request","path":"/v1/audio/speech","stream":false}
```

**Expected EPP logs:**
```json
{"msg":"Running filter plugin","plugin":"modality-filter/modality-filter"}
{"msg":"Completed running filter plugin successfully","endpoints":[{"PodName":"tts-vllm-sim-...","llm-d.ai/model-arch":"autoregressive-tts"}]}
```

## Configuration Details

### Gateway Address

**Critical:** The coordinator must point to the Envoy Gateway, not directly to EPP.

- ✅ Correct: `http://inference-gateway-istio.default.svc.cluster.local`
- ❌ Wrong: `http://epp-service:9002` (EPP expects gRPC ext-proc from Envoy, not HTTP)

### Pipeline Step

The `gateway-proxy` step:
- Accepts the request
- Forwards to Envoy Gateway via HTTP
- Envoy calls EPP via gRPC ext-proc
- EPP's modality filter selects the correct backend pod
- Response streams back through the chain

This is simpler than the full disaggregation pipeline:
- Full LLM: `replace-media-urls → render → encode → prefill → decode`
- Multimodal: `gateway-proxy` (single step)

## Troubleshooting

### 404 Not Found

If the coordinator returns 404 for multimodal endpoints:

1. Verify routes are registered in `pkg/server/server.go`
2. Check coordinator logs for route registration
3. Rebuild and reload the image

### Timeout Errors

If you see `dial tcp: i/o timeout`:

1. Verify Envoy Gateway service exists:
   ```bash
   kubectl get svc inference-gateway-istio
   ```

2. Check coordinator config points to correct gateway address

3. Verify network policies allow coordinator → gateway traffic

### Configuration Not Loading

If the coordinator uses default config instead of your ConfigMap:

1. Verify the pod has `--config` argument:
   ```bash
   kubectl get deployment coordinator -o yaml | grep -A3 args
   ```

2. Expected: `args: ["--config=/etc/coordinator/coordinator.yaml"]`

3. If missing, add to deployment spec and reapply

## Updating After Code Changes

```bash
# 1. Rebuild the image
TARGETARCH=arm64 make image-build-coordinator

# 2. Reload into kind (if using kind)
kind --name <cluster-name> load docker-image ghcr.io/llm-d/llm-d-coordinator:dev

# 3. Restart the deployment
kubectl rollout restart deployment/coordinator
kubectl rollout status deployment/coordinator
```

## How EPP Routing Works

The modality filter in EPP routes based on:

1. **Request path** → maps to compatible model architectures
2. **Pod labels** → filters pods by `llm-d.ai/model-arch`

**Mapping:**

| Endpoint Path | Compatible Architectures |
|---------------|-------------------------|
| `/v1/audio/speech` | `autoregressive-tts`, `omni-llm` |
| `/v1/audio/transcriptions` | `encoder-decoder-stt` |
| `/v1/images/generations` | `diffusion` |
| `/v1/chat/completions` | `autoregressive-llm`, `omni-llm` |

**Example flow for `/v1/audio/speech`:**

1. Coordinator receives request
2. Gateway-proxy forwards to Envoy
3. Envoy calls EPP via ext-proc
4. EPP modality filter:
   - Sees path `/v1/audio/speech`
   - Filters to pods with label `llm-d.ai/model-arch: autoregressive-tts`
5. EPP returns selected pod to Envoy
6. Envoy forwards request to TTS backend pod

## Future Enhancements

### Disaggregated Multimodal

For more complex multimodal workloads, you could create full disaggregation pipelines:

```yaml
# Future: TTS disaggregation
tts:
  endpoints: ["/v1/audio/speech"]
  steps:
    - type: text-encode      # Separate text encoder pod
    - type: acoustic-model   # Separate acoustic model pod
    - type: vocoder          # Separate vocoder pod (streaming)
```

Backend pods would be split by phase:
- Text encoder pods: `llm-d.ai/phase: text-encode`
- Acoustic model pods: `llm-d.ai/phase: acoustic`
- Vocoder pods: `llm-d.ai/phase: vocoder`

### Multiple Pipelines

The coordinator can support different pipelines per endpoint:

```yaml
pipeline:
  steps:
    # Default for all endpoints
    - type: gateway-proxy

# Or endpoint-specific pipelines (future enhancement):
pipelines:
  llm:
    endpoints: ["/v1/chat/completions"]
    steps: [replace-media-urls, render, encode, prefill, decode]
  
  tts:
    endpoints: ["/v1/audio/speech"]
    steps: [gateway-proxy]
```

## Summary

The coordinator now provides a unified API entry point for:
- ✅ LLM requests (chat/completions)
- ✅ TTS requests (audio/speech)
- ✅ STT requests (audio/transcriptions)
- ✅ Image generation requests (images/generations)

All requests benefit from EPP's intelligent routing based on:
- Request path analysis
- Pod label filtering (`llm-d.ai/model-arch`)
- Real-time scoring (queue depth, KV cache utilization)

The integration is minimal and non-invasive:
- 1 new pipeline step (`gateway-proxy`)
- 3 new path constants
- 3 new route registrations

No changes to existing LLM pipeline logic!
