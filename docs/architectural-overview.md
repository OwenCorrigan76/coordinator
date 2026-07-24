# Coordinator: Architectural Overview

The coordinator is a stateless Go service that accepts OpenAI-compatible inference
requests and drives them through a configurable plugin pipeline to disaggregated
vLLM worker pools. It is an alternative to the sidecar-based orchestration in
[llm-d-router](https://github.com/llm-d/llm-d-router): where llm-d-router embeds
orchestration in a sidecar on the decode pod, the coordinator pulls that logic
out to a standalone service in front of the Inference Gateway.

**The central tension:** tokenizing once in the coordinator eliminates redundant
per-worker tokenization, but the tokens-in wire format (`/inference/v1/generate`)
ships large pre-processed pixel tensors that dwarf the raw image sizes. The
OpenAI-format path avoids that bulk at the cost of re-preprocessing on each worker.
This tradeoff governs the `use_openai_format` knob and pervades the performance
section of this document.

---

## Table of Contents

- [Functional Requirements](#functional-requirements)
- [Non-Functional Requirements](#non-functional-requirements)
- [System API and Request Lifecycle](#system-api-and-request-lifecycle)
- [Functional Design](#functional-design)
- [Non-Functional Optimisation](#non-functional-optimisation)
- [Coordinator vs. llm-d-router Sidecar Model](#coordinator-vs-llm-d-router-sidecar-model)

---

## Functional Requirements

| # | Requirement |
|---|---|
| FR-1 | Accept OpenAI-compatible inference requests on `/v1/chat/completions` and `/v1/completions`. |
| FR-2 | Drive each request through an operator-configured, ordered pipeline of processing steps. |
| FR-3 | Download and inline external media URLs before inference (replace-media-urls step). |
| FR-4 | Tokenize prompts once via the render service and propagate token IDs to all downstream steps. |
| FR-5 | Fan out one encode request per multimodal entry to encoder worker pools, in parallel. |
| FR-6 | Issue a single prefill request carrying token IDs, EC transfer descriptors, and KV hints. |
| FR-7 | Issue a final decode request and stream (SSE) or buffer the response back to the client. |
| FR-8 | Support a conditional fast-path: attempt decode immediately after render; if the decode pod already holds the KV cache, serve without encode or prefill. |
| FR-9 | Support pluggable KV-cache transfer protocols (NIXL, SGLang, shared storage) chosen per deployment. |
| FR-10 | Support pluggable EC (Embedding Cache) transfer protocols (NIXL, shared storage) chosen per deployment. |
| FR-11 | Allow new pipeline stages to be added as self-contained plugin steps without modifying the pipeline or existing steps. |
| FR-12 | Support two upstream wire formats per step: the standard OpenAI path and the internal tokens-in path (`/inference/v1/generate`). |

What it does **not** do: select pods (deferred to the per-phase Endpoint Picker),
execute model inference, persist any state between requests, or run sidecar logic
on worker pods.

---

## Non-Functional Requirements

| Attribute | Concern | Current Trade-off |
|---|---|---|
| Performance / Latency | Each disaggregated request crosses at least three sequential gateway round-trips (encode fan-out + prefill + decode). The conditional-decode fast path collapses this to one when KV is already cached. | Single-tokenization saves per-step re-tokenization, but the tokens-in format can send ~1 MB/image to encode and ~20 MB/image to prefill for HD content. The OpenAI format defaults are smaller on the wire but force workers to re-preprocess images. |
| Scalability | The coordinator is stateless: every instance can serve every request with no shared state. Horizontal scaling is a plain replica count increase behind a load balancer. | Encode fan-out multiplies in-flight gateway connections per request. `max_parallel` and `max_idle_conns_per_host` are the knobs; a burst of high-image-count requests can saturate the connection pool before the gateway saturates. |
| Availability | No single coordinator instance holds persistent state. A failed instance does not affect in-flight requests on other instances. | EPP-side availability depends on the gateway and per-phase EPP configuration, which are outside the coordinator's scope. A decode worker returning a non-412 error terminates the request; there is no coordinator-side retry. |
| Extensibility | The step plugin model is the only extension surface. Steps register in `init()` against the pipeline registry; new behavior requires no changes to the pipeline or other steps. Connectors are selected by name at config load time. | The `StepFactory` signature (`func(gwClient, params) (Step, error)`) is a stable interface contract. Breaking it would require updating every registered step. |
| Observability | Request IDs are propagated through every upstream call. Per-step wall-clock timings are emitted at INFO level. Log level 5 (trace) logs request and response bodies through the gateway client. | Trace logs include `kv_transfer_params` and `ec_transfer_params` fields (bootstrap host/room, remote host, remote port). Trace must not be enabled in production; a compromised log store would expose those coordination fields. |

---

## System API and Request Lifecycle

The coordinator exposes an OpenAI-compatible HTTP API. All inference paths flow through
the pipeline; health endpoints short-circuit before it.

### Endpoints

```
POST /v1/chat/completions   → OpenAI chat completion object (or SSE stream)
POST /v1/completions        → OpenAI completion object (or SSE stream)
GET  /healthz               → 200 OK (liveness)
GET  /readyz                → 200 OK (readiness)
```

The coordinator is an active client of the Inference Gateway, not a one-shot proxy.
It issues one HTTP request per phase (plus one per image for encode fan-out) and
sequences those round-trips, threading state from each response into the next request.

### Request Lifecycle

```
Client
  |
  | POST /v1/chat/completions (or /v1/completions)
  v
[Entry server]                         (pkg/server/)
  | - read + size-limit body (400/413 on failure)
  | - parse JSON into map[string]any
  | - extract model, stream, request_id
  | - construct RequestContext
  | - call pipeline.Execute()
  v
[Pipeline]                             (pkg/pipeline/)
  |-- [replace-media-urls]  download image_url refs, inline as data: URIs,
  |                         seed RequestContext.MultimodalEntries
  |
  |-- [render]              POST to render service, get token_ids + per-image
  |                         hash/placeholder/kwargs; populate TokenIDs,
  |                         enrich MultimodalEntries
  |
  |-- [conditional-decode]  POST EPP-Phase:decode + Prefer:if-available to gateway
  |       |                  → 2xx: stream to client, return ErrPipelineDone (stop)
  |       |                  → 412: cache miss, continue pipeline
  |       v
  |-- [encode]              fan-out: one POST per MultimodalEntry (EPP-Phase:encode)
  |                         merge ec_transfer_params into RequestContext.ECTransferParams
  |
  |-- [prefill]             POST EPP-Phase:prefill with token_ids + ec_transfer_params
  |                         + kv_transfer_params hint; capture KVTransferParams from response
  |
  |-- [decode]              POST EPP-Phase:decode with token_ids + kv_transfer_params;
  |                         proxy response (SSE or buffered JSON) to client via ResponseWriter
  v
[Client receives response]
```

Steps are skipped at runtime when they do not apply: `replace-media-urls` is a no-op
for `/v1/completions`; `render` is skipped when the prompt is already a token array;
`encode` is skipped when `MultimodalEntries` is empty; `conditional-decode` is optional
and must be configured explicitly.

### EPP-Phase Routing

Every coordinator-to-gateway call carries an `EPP-Phase` header that the Inference
Gateway uses to route to the correct per-phase EPP and worker pool.

| Step | EPP-Phase value | Path |
|---|---|---|
| encode | `encode` | `/v1/chat/completions` or `/inference/v1/generate` |
| prefill | `prefill` | `/v1/chat/completions`, `/v1/completions`, or `/inference/v1/generate` |
| decode / conditional-decode | `decode` | `/v1/chat/completions` or `/v1/completions` |

### Background flows

None. The coordinator is fully synchronous per-request; there are no background
goroutines, polling loops, or shared mutable state.

### Cross-phase state (RequestContext fields)

| Field | Produced by | Consumed by |
|---|---|---|
| `TokenIDs` | render | conditional-decode, encode, prefill, decode |
| `MultimodalEntries` (hash, placeholder, kwargs) | replace-media-urls (base64), render (hash/placeholder/kwargs) | encode, prefill, decode |
| `ECTransferParams` | encode (via EC connector) | prefill |
| `KVTransferParams` | prefill (from response body, via KV connector) | decode |

---

## Functional Design

**Architectural style:** plugin pipeline inside stateless replicas. Each request is
an independent execution through an ordered list of self-contained steps. No shared
mutable state; no coordination between instances.

### Layers

```
+--------------------------------------------------+
|  INGRESS LAYER                                   |
|  chi HTTP server  (pkg/server/)                  |
|  - /v1/chat/completions  /v1/completions         |
|  - /healthz  /readyz                             |
+------------------------+-------------------------+
                         | RequestContext
+------------------------v-------------------------+
|  PIPELINE LAYER                                  |
|  Pipeline executor  (pkg/pipeline/)              |
|  - ordered step loop, cancellation, timings      |
|  - Step interface + StepFactory + Registry       |
|  - RequestContext  (per-request state)           |
|                                                  |
|  Built-in steps  (pkg/steps/)                    |
|  replace-media-urls | render | conditional-decode|
|  encode | prefill | decode                       |
+-------+-----------------------+-----------------+
        | HTTP (keep-alive)     | HTTP
+-------v-------+   +-----------v-----------+
|  GATEWAY      |   |  SIDE SERVICES        |
|  CLIENT LAYER |   |  (render service,     |
|  (pkg/gateway)|   |   media origins)      |
|  keep-alive   |   +-----------------------+
|  conn pool    |
+-------+-------+
        |
+-------v------------------------------------------+
|  INFRASTRUCTURE LAYER                            |
|  Inference Gateway  (external)                   |
|  +--------+   +----------+   +----------+        |
|  | EPP-E  |   |  EPP-P   |   |  EPP-D   |        |
|  +---+----+   +----+-----+   +----+-----+        |
|      |             |              |               |
|  Encode pool   Prefill pool   Decode pool         |
|  (vLLM)        (vLLM)         (vLLM)             |
|                                                  |
|  KV/EC transfer (NIXL | SGLang | shared storage) |
+--------------------------------------------------+
```

### Key Design Patterns

| Pattern | Where used | Why |
|---|---|---|
| Plugin pipeline | `pkg/pipeline/pipeline.go`, `pkg/pipeline/registry.go` | New stages (steps) are added without touching the executor or existing steps. Steps self-register via `init()`. |
| Dependency injection via setter interfaces | `pkg/gateway/client.go` (`ClientAware`), `cmd/coordinator/main.go` | Steps are constructed from config params only; runtime dependencies (gateway client) are injected post-construction so the registry stays type-agnostic. |
| Connector abstraction | `pkg/connectors/kv/`, `pkg/connectors/ec/` | KV and EC wire shapes vary by deployment (NIXL P2P RDMA, SGLang bootstrap, shared filesystem). Steps call the connector interface; the concrete protocol is selected by name at config load time. |
| RequestContext as message bus | `pkg/pipeline/context.go` | Each step reads and mutates a single per-request struct rather than passing values through function arguments. This keeps the `Step` interface stable (`Execute(ctx, reqCtx) error`) as new fields are added. |
| ErrPipelineDone sentinel | `pkg/pipeline/pipeline.go` | A step that has already written the response (e.g., `conditional-decode` on a cache hit) signals the pipeline to stop without treating the early exit as an error. |

### Data Types in Flight

- **`RequestContext.Body`** (`map[string]any`): the parsed JSON request, mutated in place. `replace-media-urls` rewrites `image_url` values to `data:` URIs. `render` populates `tokens` / rewrites `prompt` to a token array for downstream steps.
- **`MultimodalEntry`** (`pkg/pipeline/context.go`): one per image. Fields: `Index`, `Hash` (hex, routing key for EC handoff), `Base64Data`, `ContentType`, `KwargsData` (base64 msgpack pixel tensor ~1 MB per HD image), `Placeholder` (offset + length in token array).
- **`ECTransferParams`** (`[]map[string]any`): ordered list of single-key maps `mm_hash → descriptor`. Descriptor shape is connector-specific: for `ec-nixl` it contains `peer_host`, `peer_port`, `size_bytes`, `nixl_agent_metadata_b64`. Empty for `ec-shared-storage`.
- **`KVTransferParams`** (`map[string]any`): KV handoff from prefill response. For `kv-nixl`: `remote_engine_id`, `remote_block_ids`, `remote_host`, `remote_port`, `tp_size`. For `kv-shared-storage`: no descriptor on the wire.

### The Built-in Steps

| Step type | Package | Role |
|---|---|---|
| `replace-media-urls` | `pkg/steps/` | Fan-out concurrent image downloads; inlines as data: URIs; seeds MultimodalEntries. |
| `render` | `pkg/steps/` | Single HTTP call to the render service; produces TokenIDs and per-image metadata. Short-circuits when prompt is already a token array. |
| `conditional-decode` | `pkg/steps/` | Attempt decode with `Prefer: if-available`; on 412 continue; otherwise stream response and stop pipeline. |
| `encode` | `pkg/steps/` | Parallel fan-out (one gateway call per MultimodalEntry, EPP-Phase:encode); merges EC descriptors. |
| `prefill` | `pkg/steps/` | Single gateway call (EPP-Phase:prefill) with full token sequence + EC/KV hints; captures KVTransferParams from response. |
| `decode` | `pkg/steps/` | Single gateway call (EPP-Phase:decode); proxies SSE or buffered JSON response to client via `ResponseWriter`. |

### Connector Selection

KV and EC connectors are selected deployment-wide in `pipeline.kv_connector` /
`pipeline.ec_connector` and injected into every step's `params` before the factory
runs. A step can override per its own `params`. KV and EC are independent: any
`kv-*` / `ec-*` pairing is valid.

| Key | Values |
|---|---|
| `kv_connector` | `kv-nixl` (NIXL P2P RDMA), `kv-sglang` (SGLang bootstrap), `kv-shared-storage` (default) |
| `ec_connector` | `ec-nixl` (NIXL P2P RDMA), `ec-shared-storage` (default) |

---

## Non-Functional Optimisation

### Eliminate Single Points of Failure

| Risk | Mitigation | Where |
|---|---|---|
| Coordinator instance failure | Stateless replicas; any instance serves any request; no coordination. Load balancer routes around failed pods. | Deployment topology |
| Decode worker unavailable | EPP-D selects a different pod in its pool. The coordinator does not address pods directly. | EPP configuration |
| Render service unavailable | The `render` step returns an `UpstreamError`; the server maps it to a client-visible error. There is no coordinator-side retry. | `pkg/steps/render.go` |

### Eliminate Bottlenecks

The fundamental hot-path bottleneck is sequential gateway round-trips. The coordinator
attacks it on three levels:

1. **Reuse it:** tokenize once in the render step; token IDs flow to encode, prefill,
   and decode without re-tokenization on workers.
2. **Specialise for it:** encode fan-out is parallelised (bounded by `max_parallel`).
   The conditional-decode fast path collapses encode + prefill + decode into a single
   round-trip for cache-warm requests.
3. **Protect it:** the gateway client uses a keep-alive connection pool
   (`max_idle_conns_per_host`, `idle_conn_timeout`) to avoid per-request TCP setup
   overhead. The render step also keeps a dedicated pool to the render service.

### Optimise Critical Paths

The SLA-critical metric is end-to-end latency from client request receipt to first
streamed token (TTFT).

```
Client → [parse: ~0ms] → [replace-media-urls: O(images), parallel] →
[render: 1 round-trip to render service] →
[conditional-decode: 1 round-trip]
  └─ cache hit: stream response → done          ← fast path
  └─ cache miss:
      [encode: N parallel round-trips, N=image count] →
      [prefill: 1 round-trip] →
      [decode: 1 round-trip + stream] → done    ← full path
```

Optimisation fires by step:

| Step | Optimisation |
|---|---|
| `replace-media-urls` | Concurrent downloads bounded by `max_concurrent_downloads`. External downloads bypass the fast path entirely for subsequent requests when combined with `conditional-decode`. |
| `render` | Single HTTP call; keep-alive pool. Short-circuits when prompt is already tokenized. |
| `conditional-decode` | Eliminates encode + prefill for cache-warm requests (returns `ErrPipelineDone` on 2xx). |
| `encode` | `errgroup` with `max_parallel` bound; encodes run in parallel, not serially. |
| `gateway client` | Keep-alive pool avoids TCP handshake per phase call. |

### Wire Format Trade-off (Tokens-In vs. OpenAI Format)

`use_openai_format` controls whether the coordinator sends the raw image (small body,
worker re-preprocesses) or the pre-preprocessed tensor (large body, worker skips
preprocessing). Only relevant for multimodal requests.

| Format | Encode body per HD image | Worker preprocessing cost |
|---|---|---|
| `true` (OpenAI, default) | ~110–270 KB | Worker re-runs vision preprocessor |
| `false` (tokens-in) | ~20 MB (clamped by `max_pixels`) | None; worker consumes tensor directly |

The OpenAI format (`true`) is the tested default. The tokens-in format (`false`) is
experimental; the payload size at high resolution makes it unsuitable for most
deployments until the vLLM prefill worker can accept `image_grid_thw` separately
(currently the full `kwargs_data` tensor must be sent to prefill for mRoPE, even
though prefill only needs the shape metadata, not the pixel values).

### Algorithms and Data Structures Matched to Access Patterns

| Access pattern | Structure / Algorithm | Package |
|---|---|---|
| Ordered step execution with early exit | Slice of `Step` interfaces, linear scan; `errors.Is(ErrPipelineDone)` for early exit | `pkg/pipeline/pipeline.go` |
| Step lookup by type name at config load | `map[string]StepFactory` registry | `pkg/pipeline/registry.go` |
| Parallel image downloads | `golang.org/x/sync/errgroup` with semaphore (`max_concurrent_downloads`) | `pkg/steps/` (`replace-media-urls`) |
| Parallel encode fan-out | `golang.org/x/sync/errgroup` with semaphore (`max_parallel`) | `pkg/steps/encode.go` |
| KV/EC wire-shape selection | Named switch in `Build()` factory functions | `pkg/connectors/kv/kv.go`, `pkg/connectors/ec/ec.go` |
| Per-request state threading | Single mutable struct on the stack of `Execute` | `pkg/pipeline/context.go` |
| Header forwarding without hop-by-hop leakage | Allowlist + blocklist over normalized lowercase keys | `pkg/pipeline/context.go` (`ForwardedHeaders`) |

---

## Coordinator vs. llm-d-router Sidecar Model

Both models route inference through the same Inference Gateway and EPP scheduling
machinery. The difference is where disaggregation is orchestrated and where
tokenization happens.

| Dimension | llm-d-router (sidecar) | Coordinator |
|---|---|---|
| Orchestration location | vLLM sidecar on the decode pod | Standalone service in front of the Inference Gateway |
| Sidecar required | Yes (decode pod only) | No |
| Pipeline versatility | Fixed E/P/D orchestration in the sidecar | Configurable plugin pipeline; new stages added without touching existing code |
| EPP scheduling | One cycle selects all phases (`disagg-profile-handler`) | One EPP call per phase; coordinator drives the cascade |
| Pod selection timing | All phase pods chosen at request entry | Deferred per phase: each pod is selected when that phase's call is made |
| Tokenization | On the workers (sidecar forwards prompts) | Once, in the coordinator render step; token IDs reused downstream |
| Cross-phase state | Held in sidecar memory | Held on `RequestContext` in the coordinator |

The deferred scheduling model means the coordinator can incorporate state from earlier
phases when scheduling later ones (e.g., the decode pod that was selected and returned
412 may surface cache-locality hints for the prefill step in a future version).

---

## References

- [communication.md](communication.md): per-stage wire-format reference with exact
  request and response JSON for every step and both wire formats.
- [llm-d](https://github.com/llm-d/llm-d): umbrella project covering overall goals,
  components, and deployment.
- [llm-d-router architecture](https://github.com/llm-d/llm-d-router/blob/main/docs/architecture.md):
  the Inference Gateway, the EPP, and the sidecar-based disaggregation model the
  coordinator replaces.
- [llm-d-inference-scheduler](https://github.com/llm-d/llm-d-inference-scheduler):
  the EPP scheduling implementation (profile handlers, filters, scorers, deciders).
- [Gateway API Inference Extension endpoint picker protocol](https://github.com/kubernetes-sigs/gateway-api-inference-extension/tree/main/docs/proposals/004-endpoint-picker-protocol):
  the protocol the gateway uses to consult the per-phase EPP.
