# Security model

What the gateway actually defends, where, and what is deliberately left
open. For how to report a problem, see [`SECURITY.md`](../SECURITY.md).

## Trust boundaries

Three actors the gateway does not control, and therefore does not trust:

1. **The HTTP client** — may send anything, at any rate, and may stop
   reading at any moment.
2. **The LLM provider** — its response is untrusted input like any other:
   it can be huge, malformed, or report impossible token counts.
3. **The AWS store** — may throttle or fail, which must look like an
   outage, never like a credential problem.

## Controls

| Control | Where | Note |
|---|---|---|
| API key stored as SHA-256, never plaintext | `internal/auth/tenant.go` | The hash is the table's partition key, so lookup is hit/miss-symmetric and no comparison happens in Go (no timing oracle). |
| Explicit `Bearer` parsing, key length 8..256 | `internal/api/auth_header.go` | The provided key is never echoed or logged. |
| Pre-auth per-IP limiter | `internal/ratelimit/iplimiter.go` | Token bucket charged only by auth *failures*, 60/min per IP by default, TTL-evicted. Over-limit requests never reach the tenant store, so brute force costs the gateway nothing. Applies to `/v1/chat/completions` and `/stats`. `X-Forwarded-For` is honoured only when `TRUST_PROXY=true`. |
| Per-tenant rate limit | `internal/ratelimit/limiter.go` | Token bucket keyed by the *authenticated* tenant; `Retry-After` capped at 300s. Per instance: two replicas double the effective limit (known, documented). |
| Per-tenant monthly token budget | `internal/budget` | Pessimistic reservation before the call (atomic conditional update), settled with real usage after. |
| Provider usage sanity bounds | `internal/api/pipeline.go` (`sanitizeRealTokens`) | A reported total outside `[0, 4*estimate]` falls back to the estimate, so a provider cannot credit a tenant's budget or overcharge it. |
| Input validation | `internal/api/pipeline.go`, `internal/router/validate.go` | 1 MiB body, <=64 messages, <=256 KiB of prompt bytes, `max_tokens` 1..32768, roles restricted to `system`/`user`/`assistant`, model names restricted to `^[A-Za-z0-9._:/-]{1,128}$` with no `.`/`..` segments, trailing JSON garbage rejected. |
| Bounded upstream reads | `internal/httpx` | 8 MiB per buffered body, 1 MiB per stream line: a provider answering with gigabytes cannot OOM the process. |
| Timeouts everywhere | `internal/httpx`, `cmd/gateway/main.go` | Server: `ReadHeaderTimeout` 10s, `ReadTimeout` 30s, `IdleTimeout` 120s (no global `WriteTimeout` — it would kill SSE). Client: shared transport with `ResponseHeaderTimeout` 30s (the TTFT guard) and a 120s context budget per non-streaming attempt. |
| SSE slow-client cut | `internal/api/pipeline.go` (`StreamWriteTimeout`) | A write deadline per chunk, so a client that opens a stream and stops reading is dropped instead of pinning a goroutine and a paid upstream connection. |
| Security headers | `internal/api/middleware.go` | `X-Content-Type-Options: nosniff`, `Cache-Control: no-store`, `Referrer-Policy: no-referrer`, HSTS. No CORS headers at all — correct for a server-to-server API. |
| Request id never trusted from the client | `internal/api/requestid.go` | Always server-generated; a client-supplied `X-Request-Id` is sanitized, bounded to 128 chars, echoed as `X-Client-Request-Id` and recorded as `client_request_id`. |
| Secrets never in code or URLs | `config/routing.yaml` + Secrets Manager | The env var *name* comes from config; the value comes from the environment (ECS `secrets`/`valueFrom`). Provider keys travel in headers, never in a query string, and never reach the client. |
| Least-privilege IAM | `infra/modules/ecs_fargate` | Task role: six DynamoDB actions scoped to the five table ARNs, `sqs:SendMessage`/`GetQueueUrl` scoped to the queue ARN. Execution role: log actions scoped to the log group; `ecr:GetAuthorizationToken` is `*` only because AWS does not support a resource for it. |
| Segregated security groups | `infra/modules/ecs_fargate/main.tf` | The module creates both: the ALB SG takes 443/80 from the internet, the task SG takes 8080 **from the ALB SG only**. Supplying your own means supplying both, enforced by a precondition. |
| No content in logs, spans or traces | `internal/trace/logger.go`, `internal/trace/store.go`, `internal/memstore` | A tested forbidden-key list (`message`, `messages`, `content`, `payload`, `prompt`, `completion`, ...) is dropped by the logger and rejected by every trace store. Trace payloads are metadata only, capped at 64 KiB. |
| Non-root, distroless image | `Dockerfile` | Multi-stage build, static binary, `gcr.io/distroless/static-debian12:nonroot` with `USER nonroot:nonroot`. `.dockerignore` excludes `.git`, `.env*`, `*.tfstate*`, `.terraform`, `infra/`, `bench/`, `.superpowers/`. |
| CI least privilege | `.github/workflows/ci.yml` | `permissions: contents: read` at the top, every action pinned to a full commit SHA with its version in a comment, exact golangci-lint version, no job references `secrets.`. |

## Unauthenticated endpoints

- `GET /healthz` — static.
- `GET /readyz` — result cached for 5s, so hammering it cannot exhaust the
  DynamoDB control-plane quota or generate load on Ollama.
- `GET /v1/models` — configured aliases only. It no longer lists the backend
  inventory (which provider, which model tags are pulled), since that was
  free reconnaissance.

## Known gaps

Deliberate, not oversights:

- **No pepper on the API key hash.** The hash is the table's partition key,
  so it must stay deterministic; the right fix is `HMAC-SHA256` with a
  pepper from Secrets Manager, which requires re-hashing every existing
  tenant row. Until then, a leaked `tenants` table snapshot is brute-forcible
  offline for weak keys. Production keys should be 32 bytes from
  `crypto/rand`.
- **No revocation or expiry field.** Revoking a leaked key today means
  deleting the row. A `status`/`expires_at` pair (with DynamoDB TTL) plus a
  revoke script is the fix, and also needs a table migration.
- **Rate limit and budget reservation are per instance / eventually
  consistent across replicas.** A second replica doubles the effective RPM.
- **The 502 body lists the cascade attempts** (provider, model, 200 bytes of
  upstream error). Deliberate: it is what makes a failure debuggable. It
  does expose internal topology to an authenticated caller, and would move
  to "detailed in the trace, summarized in the response" if the gateway ever
  served untrusted third parties.
- **The pre-auth IP limiter's bucket is shared by every client behind the
  same IP.** Only auth failures count, so well-behaved tenants don't burn the
  budget themselves, but a NAT gateway or corporate egress puts many clients
  behind one IP — a brute-forcer among them exhausts the shared 60/min bucket
  and 429s the rest until it refills. Tune with
  `IP_AUTH_FAILURES_PER_MINUTE` if a deployment's client population makes 60
  too tight.
- **One transitive advisory stays open**: GO-2026-6443 in
  `google.golang.org/grpc`, which arrives via the OTel exporter. It is not
  reachable (the gateway exports OTLP over HTTP and runs no gRPC server) and
  the advisory's fixed version is currently an unreleased dev commit, so the
  dependency stays on the latest stable release.
