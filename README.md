# ai-gateway

An OpenAI-compatible HTTP gateway in Go that routes `POST /v1/chat/completions` across Ollama (local), OpenRouter and Gemini by tenant tier. It authenticates tenant API keys, reserves a token budget before making the call, retries and falls back across providers (including mid-stream), records an end-to-end request trace, publishes a usage event per request, and exports OpenTelemetry traces/metrics.

Repo 1 of a small AI-infra portfolio. Design spec: `docs/superpowers/specs/2026-09-19-ai-gateway-design.md`.

## Architecture

Request pipeline (`internal/api/pipeline.go`):

```
Client
  │  POST /v1/chat/completions  (Authorization: Bearer <api-key>)
  ▼
api            → parses/validates the OpenAI-shaped request, generates X-Request-Id
  ▼
auth           → resolves the API key hash to a Tenant{id, tier, rpm_limit, monthly_token_budget}
  ▼
ratelimit      → per-tenant token bucket; 429 + Retry-After if exhausted
  ▼
budget.reserve → atomic DynamoDB UpdateItem, pessimistic token estimate; 402 if it would exceed the monthly budget
  ▼
router         → alias + tier → ordered list of backend targets (config/routing.yaml)
  ▼
resilience     → calls the cascade with retry/fallback and Transient/Permanent error classification
  ▼
backend        → adapter for ollama / openrouter / gemini, non-streaming or SSE
  ▼
budget.settle  → adjusts `used` by the real token delta from the backend's usage
  ▼
usage          → publishes usage_event v1 to SQS
  ▼
trace          → one trace_event per stage above, keyed by request_id
  ▼
Response (JSON or SSE) + X-Request-Id
```

Module responsibilities (one line each):

| Module | Responsibility |
|---|---|
| `internal/api` | HTTP handlers, request/response shaping, SSE framing, error-to-status mapping |
| `internal/auth` | API key hashing and tenant lookup |
| `internal/ratelimit` | Per-tenant request-rate limiting |
| `internal/budget` | Atomic reserve/settle of a tenant's monthly token budget |
| `internal/router` | Resolves `alias + tier` to an ordered backend cascade from `config/routing.yaml` |
| `internal/backend/{ollama,openrouter,gemini}` | Provider adapters (chat + streaming) |
| `internal/resilience` | Retry, backoff and cross-backend fallback |
| `internal/usage` | Builds and publishes `usage_event` v1 to SQS |
| `internal/trace` | Trace event persistence, forbidden-key-checked structured logger, OTel wiring |
| `internal/stats` | In-memory rolling window backing `GET /stats` |
| `internal/core` | Shared types (`BackendError`, `Reservation`, chat contracts) |

Storage:
- DynamoDB `tenants` — pk `api_key_hash`, holds tier/rpm/budget.
- DynamoDB `budgets` — pk `tenant_id`, sk `period`; `used`/`limit` in tokens.
- DynamoDB `reservations` — one row per in-flight reservation, TTL ~15 min for orphan cleanup.
- DynamoDB `requests` / `trace_events` — pk `request_id` (+ sk `seq` for events).
- SQS `usage-events` queue, with a dead-letter queue for failed deliveries.

Five-block reference model (from the design spec):
- **Gateway** — this repo.
- **Deterministic orchestrator** — the pipeline itself (auth → ratelimit → budget → router → resilience → usage → trace), all finite rules, no LLM in the control path.
- **Model + tools** — the backend providers (Ollama/OpenRouter/Gemini), external to this repo.
- **Approval gate** — not applicable: no subtask here is an autonomous agent and no action is irreversible (routing, budget and rate-limit decisions are all finite rules), so there is nothing to gate on human approval.
- **Observability** — `X-Request-Id` + `trace_events` + OTel spans/metrics, all present.

## Design rules that are enforced in code

- Budget is reserved before the backend call, via an atomic `UpdateItem` `ConditionExpression` (never check-then-act) — `internal/budget/budget.go` (`Reserve`).
- In streaming, fallback to the next backend only happens before the first chunk reaches the client; after the first byte, a backend error ends the stream instead of mixing two models' output — `internal/resilience/resilience.go` (`ErrStreamFailedAfterFirstByte`, `CallStream`).
- Error classification (`Transient`/`Permanent`) drives whether resilience retries or gives up — `internal/core` (`BackendError`), consumed in `internal/resilience/resilience.go`.
- Tool access is a permission, not a prompt instruction: each backend adapter is wired explicitly into the pipeline's backend map, nothing is gated by asking the model nicely — `cmd/gateway/main.go`, `internal/api/status.go` (`toBackends`).
- The structured logger has a tested forbidden-key list so message/prompt/content never reach stdout — `internal/trace/logger.go` (`ForbiddenKeys`), `internal/trace/logger_test.go`.
- API keys never appear in URLs; they arrive only via the `Authorization: Bearer` header and are looked up by SHA-256 hash — `internal/auth`.
- Rate limiting is in-memory, per instance, by design (see Mini-ADR below) — `internal/ratelimit/limiter.go`.
- OpenTelemetry is optional: the gateway runs without a collector; export is only attempted when `OTEL_EXPORTER_OTLP_ENDPOINT` is set — `cmd/gateway/main.go`.

## Item from the original 15-project list → where in the code

Covers items 6 (multi-model gateway) and 13 (observability spine) of the source backlog.

| Sub-item | Where |
|---|---|
| Routing (alias + tier cascade) | `internal/router/router.go`, `config/routing.yaml` |
| Retries | `internal/resilience/resilience.go` (`Call`, backoff, cap of 2 attempts per backend) |
| Fallback (incl. streaming first-byte rule) | `internal/resilience/resilience.go` (`CallStream`) |
| Rate limit | `internal/ratelimit/limiter.go` |
| Budget | `internal/budget/budget.go` (`Reserve`/`Settle`) |
| Traces | `internal/trace/otel.go`, root span `POST /v1/chat/completions` with children `auth`, `budget.reserve`, `route`, `backend.call`, `budget.settle`, `usage.publish` |
| `X-Request-Id` | generated in `internal/api/pipeline.go`, returned on the header and in every error body, persisted on every `trace_event` |
| Metrics | `internal/trace/otel.go` (`gateway_requests_total`, `gateway_latency_ms`, `gateway_tokens_total`, `gateway_ttft_ms`) exported to Prometheus via the OTel Collector |
| Alerts | **not yet built** — no alerting rules exist in `deploy/prometheus.yml`; see Known limitations |

## Course note (UniPDS Pós IA Aplicada) → decision in this repo

| Course note | Decision here |
|---|---|
| Módulo 02/01 — Gateway Roteador de Modelos (OpenRouter) | Static alias + tier cascade in `config/routing.yaml`, resolved by `internal/router` (Mini-ADR "alias + cascata por tier, com regra fixa para o forte" in the spec) |
| Módulo 03/07 — Segurança de API: Auth e Rate Limiting no MCP | API key via `Authorization: Bearer`, SHA-256 hash never stored/logged in plain text (`internal/auth`); token-bucket rate limit in `internal/ratelimit` |
| Módulo 08/05 — Observabilidade, Implantação Híbrida e Model Tiering | `free`/`standard`/`premium` tiers in `config/routing.yaml`; token budget reserved before the call, not after (`internal/budget`) |
| Módulo 04b/07 — Observabilidade e Limites de Autonomia | `X-Request-Id` + `trace_events` end to end (`internal/trace`); no subtask here is an autonomous agent (routing/budget/rate-limit are finite rules, see spec "Agente ou regra") |
| Proposta — Padrões extraídos da Pós IA Aplicada | Tested logger forbidden-key list (`internal/trace/logger.go`), typed `Transient`/`Permanent` error (never a `startsWith` check), atomic `ConditionExpression` on budget (never a two-step check-then-act) |

## Run locally

Prerequisites: Go 1.27, Docker + Docker Compose, Ollama running locally with `qwen2.5-coder:1.5b` pulled. `awslocal`/`tflocal` are optional — the `infra/` Terraform can also be applied with plain `terraform` pointed at LocalStack's endpoint.

```bash
# 1. Local AWS (DynamoDB + SQS) via LocalStack
docker compose up -d localstack

# 2. Provision tables/queues
cd infra && tflocal init && tflocal apply -auto-approve && cd ..

# 3. Seed dev tenants (fake keys, local-only — never real credentials)
go run ./scripts/seed_tenants.go

# 4. Environment (defaults shown; only override what you need)
export AWS_ENDPOINT_URL=http://localhost:4566
export AWS_ACCESS_KEY_ID=test     # LocalStack accepts any value
export AWS_SECRET_ACCESS_KEY=test # LocalStack accepts any value
export AWS_REGION=us-east-1
export ROUTING_CONFIG=config/routing.yaml     # default
export OLLAMA_BASE_URL=http://localhost:11434 # default from routing.yaml
export GATEWAY_ADDR=:8080                     # default
# export OPENROUTER_API_KEY=...   # only needed to exercise the openrouter backend
# export GEMINI_API_KEY=...       # only needed to exercise the gemini backend
# export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318  # optional (OTLP/HTTP, not the gRPC 4317 port)

# 5. Run
go run ./cmd/gateway
```

Non-streaming call:

```bash
curl http://localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer dev-free-key" \
  -H "Content-Type: application/json" \
  -d '{"model":"nuva/fast","messages":[{"role":"user","content":"hi"}]}'
```

Streaming call:

```bash
curl -N http://localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer dev-standard-key" \
  -H "Content-Type: application/json" \
  -d '{"model":"nuva/fast","messages":[{"role":"user","content":"hi"}],"stream":true}'
```

Missing/invalid key:

```bash
curl -i http://localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"nuva/fast","messages":[{"role":"user","content":"hi"}]}'
# HTTP/1.1 401, {"error":{"type":"missing_api_key",...}}
```

Other endpoints: `GET /v1/models`, `GET /readyz` (checks the store and at least one backend), `GET /stats` (needs a valid API key — same auth as chat completions).

Dev tenants seeded by `scripts/seed_tenants.go`: `dev-free-key` / `dev-standard-key` / `dev-premium-key`, one per tier — fake, local-only keys, never real credentials.

Observability stack (optional, gateway runs fine without it):

```bash
docker compose up -d otel-collector tempo prometheus grafana
```

Grafana at `http://localhost:3000`, dashboard "ai-gateway" provisioned from `deploy/grafana/dashboards/gateway.json`.

## API

| Endpoint | Auth | Notes |
|---|---|---|
| `POST /v1/chat/completions` | required | OpenAI-shaped request/response; `stream: true` for SSE; optional `max_tokens` (1-32768, rejected outside that range with 400 `invalid_request`) |
| `GET /v1/models` | none | lists aliases from `config/routing.yaml` |
| `GET /healthz` | none | liveness |
| `GET /readyz` | none | checks the store and at least one backend |
| `GET /stats` | required | latency/token percentiles from the in-memory window, scoped to the caller's own tenant plus a global `total` rollup (no other tenant's routes or ids) |

Error types (JSON body `{"error":{"type","message","request_id"}}`, plus HTTP status):

| Type | Status | When |
|---|---|---|
| `missing_api_key` | 401 | no `Authorization` header |
| `invalid_api_key` | 401 | key doesn't resolve to a tenant |
| `rate_limited` | 429 | token bucket exhausted; `Retry-After` header set |
| `budget_exceeded` | 402 | reservation would exceed the tenant's monthly budget |
| `unknown_model` | 400 | alias not found in `routing.yaml` |
| `tier_forbidden` | 403 | tenant's tier can't reach that target directly |
| `unknown_provider` | 400 | target names a provider not configured |
| `invalid_request` | 400 | malformed body, empty `messages`, empty `model` |
| `all_backends_failed` | 502 | every backend in the cascade failed; body includes an `attempts` array (`provider`, `model`, `status`, `error`) |
| `backend_stream_failed` | n/a (SSE event) | a backend fails after the first chunk was already sent to the client; delivered as an SSE `error` event followed by `data: [DONE]`, not an HTTP status |

`X-Request-Id` is generated per request, returned on the response header, and included in every error body and every `trace_event`.

`usage_event` v1 (published to SQS, one per finished or failed request, metadata only — never message content):

```json
{
  "event_version": 1,
  "request_id": "b3f1...",
  "tenant_id": "tenant-standard",
  "tier": "standard",
  "alias": "nuva/fast",
  "provider": "ollama",
  "model": "qwen2.5-coder:1.5b",
  "prompt_tokens": 12,
  "completion_tokens": 48,
  "ttft_ms": 210,
  "latency_ms": 890,
  "status": "ok",
  "error_class": "",
  "attempts": [{"provider": "ollama", "model": "qwen2.5-coder:1.5b", "status": "ok", "latency_ms": 890}],
  "ts": "2026-09-19T20:00:00Z"
}
```

## Testing

- Unit tests: `go test -race ./...` — no external dependency, run in CI.
- Integration tests: `go test -tags integration ./...` — needs LocalStack up (`docker compose up -d localstack`); exercises DynamoDB/SQS code paths.
- `-race` runs in CI, not locally on Windows (no `gcc` in the dev environment used to build this repo).
- Fixtures: Ollama's fixtures and OpenRouter's non-streaming success fixture (`internal/backend/{ollama,openrouter}/testdata/*.json`) were recorded against real calls. OpenRouter's streaming fixture and `chat_error.json` (429) are synthetic, as is Gemini's fixture (`internal/backend/gemini/testdata/generate_success.json`), pending a real `GEMINI_API_KEY`/streaming capture to record live ones.
- CI jobs (`.github/workflows/ci.yml`): `test` (`go vet`, `go test -race`, `golangci-lint`, `gitleaks`), `integration` (LocalStack + `-tags integration`), `terraform-plan` (`tflocal plan` with `enable_fargate=false`, DynamoDB + SQS only).

## Performance

Local benchmark against a stub Ollama backend, after moving trace writes and usage publishing off the request path (`bench/run.sh`, two modes):

- **`STORE_BACKEND=memory`** (gateway overhead alone, no AWS): **~15,200 req/s** at concurrency 50, p50 **2.6 ms**, p99 **11.5 ms**; **~7,500 req/s** streaming; the auth-reject path does **~62,700 req/s**. With a 200 ms backend the gateway adds ~1 ms on top — throughput is exactly `concurrency / backend latency`, i.e. the backend is the only limit left.
- **LocalStack (DynamoDB + SQS)**: **~18 req/s** at concurrency 50 (p50 2.7 s), up from ~7 req/s, and concurrency 200 no longer collapses into timeouts (~21 req/s vs. total failure before).

The old ~7 req/s ceiling was ~11 synchronous store round trips per request; it is now 2 (auth + budget reserve/settle), with traces and usage events batched onto bounded background queues. What is left in LocalStack mode is LocalStack's own single-process emulator, which the memory-mode numbers isolate away. Full method, before/after table and caveats: `docs/benchmarks/2026-09-20-local.md`. Reproduce with `bash bench/run.sh memory` and `bash bench/run.sh`.

## Deploy

See `docs/deploy-fargate.md` for a real ECS Fargate deploy (manual `apply` against a real AWS account, outside LocalStack). Fargate is behind the `enable_fargate` Terraform flag, off by default and off in CI — LocalStack Community doesn't emulate ECS/ALB well enough to validate it. Provider secrets (`OPENROUTER_API_KEY`, `GEMINI_API_KEY`) are passed by ARN via `provider_secret_arns`, resolved from Secrets Manager, never as plain task-definition environment variables.

The ALB always terminates TLS: HTTPS:443 forwards to the service, HTTP:80 redirects (301) to HTTPS. `acm_certificate_arn` (an ACM certificate for the gateway's domain, requested and validated beforehand) is required whenever `enable_fargate=true`.

Cost warning: Fargate (512 CPU / 1024 MB, 1 task) + an ALB running 24/7 costs on the order of tens of USD/month even at low traffic — don't leave it running without a reason.

## Known limitations & phase 2

- **Rate limit is per instance** (in-memory token bucket, `internal/ratelimit`): running two gateway replicas multiplies the effective per-tenant limit. Revisit when a second replica actually goes to production.
- **LocalStack Community lacks full DynamoDB IAM/TTL parity** with real AWS — validate against a real account before relying on it in production (see `docs/deploy-fargate.md`).
- **Direct-target metrics collapse to `direct`**: OTel labels are limited to the aliases/tiers in `config/routing.yaml` to bound cardinality, so a request routed straight at a backend (bypassing an alias) loses per-model granularity in metrics.
- **Gemini fixtures are synthetic**, recorded without a live API key; behavior may diverge from the real API until a recorded fixture replaces them.
- **No alerting rules yet** — Prometheus/Grafana are set up for dashboards, not alerts.

`max_tokens` (optional, 1-32768) is validated and forwarded to every backend's native field (Ollama `options.num_predict`, OpenRouter `max_tokens`, Gemini `generationConfig.maxOutputTokens`) and used in the pre-call budget estimate. `GET /stats` returns only the caller's own tenant entries plus a tenant-free global `total`. CI's integration job provisions LocalStack for real before testing: it waits for the health endpoint, then runs `tflocal apply` against `infra/` and seeds tenants, instead of testing against tables/queues that were never created.

Phase 2: a semantic cache (embeddings + vector store) for repeated prompts — out of scope today. It's added once `llm-loadgen` (a companion load generator) shows a repeated-prompt ratio that justifies the cost of maintaining a vector index.

Companion repos (planned, not part of this repo):
- `llm-loadgen` (Rust) — load generator, would surface the repeated-prompt signal for phase 2.
- `tenant-agent` (Python / LangGraph) — tenant-facing agent consuming this gateway.
- `finops-control-plane` (Java / Spring) — billing/cost dashboard consuming `usage_event`, multi-region failover.

## License

MIT — see `LICENSE`.
