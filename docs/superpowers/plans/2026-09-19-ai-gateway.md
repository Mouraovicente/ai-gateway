# ai-gateway Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Construir o `ai-gateway`, um proxy HTTP OpenAI-compatible em Go que roteia `POST /v1/chat/completions` entre Ollama, OpenRouter e Gemini por alias/tier, com autenticação por API key, rate limit, reserva de orçamento de tokens, retry/fallback, tracing e publicação de `usage_event`.

**Architecture:** `cmd/gateway/main.go` monta um pipeline de middlewares HTTP (`net/http` stdlib, mux Go 1.22+) sobre módulos isolados em `internal/*`: `api` traduz HTTP↔`core.ChatRequest`, `auth`/`ratelimit`/`budget` decidem se a requisição prossegue, `router`/`resilience` escolhem e chamam o backend certo com fallback, `backend/{ollama,openrouter,gemini}` falam o protocolo nativo de cada provedor, `trace`/`usage`/`stats` registram o que aconteceu. Estado compartilhado (tenants, budget, requests, trace_events) vive em DynamoDB (LocalStack em dev); eventos de uso vão para SQS.

**Tech Stack:** Go 1.27, stdlib `net/http` (mux nativo), `github.com/aws/aws-sdk-go-v2` (+config, dynamodb, sqs, feature/dynamodb/attributevalue), `go.opentelemetry.io/otel` (+sdk, otlptrace/otlptracehttp, otlpmetric), `golang.org/x/time/rate`, `gopkg.in/yaml.v3`, `github.com/google/uuid`; testes com `testing`+`net/http/httptest`; LocalStack via docker compose; Terraform + `tflocal`.

**Spec:** `docs/superpowers/specs/2026-09-19-ai-gateway-design.md`

## Global Constraints

- Go 1.27 (instalar antes do primeiro commit); módulo `github.com/vicentemoura/ai-gateway` — placeholder confirmado/ajustado na Task 1 com `gh api user -q .login`.
- Só stdlib `net/http` com mux nativo do Go 1.22+ (`mux.HandleFunc("POST /v1/chat/completions", handler)`); proibido chi/gin/echo/qualquer router de terceiros.
- Dependências permitidas, nenhuma outra: `github.com/aws/aws-sdk-go-v2` + `config`, `service/dynamodb`, `service/sqs`, `feature/dynamodb/attributevalue`; `go.opentelemetry.io/otel` + `sdk`, `exporters/otlp/otlptrace/otlptracehttp`, `otlpmetric`; `golang.org/x/time/rate`; `gopkg.in/yaml.v3`; `github.com/google/uuid`.
- Testes só com `testing` + `net/http/httptest`. LocalStack via `docker compose` (nunca testcontainers). Testes de integração atrás de build tag `//go:build integration`.
- Layout de diretórios: `cmd/gateway/main.go`, `internal/{api,auth,ratelimit,budget,router,backend/{ollama,openrouter,gemini},resilience,trace,usage,stats,config,core}`.
- `internal/core` guarda os tipos compartilhados: `ChatRequest`, `Message`, `ChatChunk`, `ChatResponse`, `Usage`, `BackendTarget`, `Tenant`, `Reservation`, erro tipado `BackendError{Class ErrorClass (Transient|Permanent), Status int, Err error}`.
- Logger: `log/slog` com handler JSON e `ReplaceAttr` que descarta as chaves proibidas `message, messages, answer, trace, content, payload, prompt, completion`; lista exportada `trace.ForbiddenKeys`; teste garante que nenhuma delas sai no log.
- `X-Request-Id`: uuid v4 gerado em middleware, devolvido no header de resposta e no campo `request_id` de todo corpo de erro `{ "error": { "type": "...", "message": "...", "request_id": "..." } }`.
- Budget: DynamoDB `UpdateItem` com `ConditionExpression "attribute_not_exists(used) OR used + :est <= :limit"` e `ADD used :est`; `settle` faz `ADD used :delta` (delta = real - estimado, pode ser negativo). Reserva registrada em item com TTL de 15 min; `settle` apaga o item de reserva.
- Rate limit: `golang.org/x/time/rate`, um `*rate.Limiter` por tenant guardado em `sync.Map`; `Retry-After` inteiro em segundos.
- Retry: máximo 2 tentativas por backend, backoff 200ms×2^n com jitter, só para `ErrorClass == Transient`; depois passa para o próximo target da cascata; se todos falharem, `502` `all_backends_failed` com lista `attempts` no corpo.
- SSE: `Content-Type: text/event-stream`; cada chunk `data: {json}\n\n`; terminar com `data: [DONE]\n\n`; `http.Flusher` após cada chunk; respeitar `r.Context().Done()` — nesse caso `settle` com tokens parciais e `trace_event` tipo `error` com `reason: client_closed`.
- `usage_event` v1 exatamente: `{event_version, request_id, tenant_id, tier, alias, provider, model, prompt_tokens, completion_tokens, ttft_ms, latency_ms, status, error_class, attempts:[{provider, model, status, latency_ms}], ts}`. Nunca contém `messages`/conteúdo.
- `config/routing.yaml`: `aliases: {nuva/fast: {tiers: {free: [{provider, model}], standard: [...], premium: [...]}}, nuva/smart: {...}}` e `providers: {ollama: {base_url}, openrouter: {base_url, api_key_env: OPENROUTER_API_KEY}, gemini: {base_url, api_key_env: GEMINI_API_KEY}}`. Alias desconhecido → `400 unknown_model`. Nome direto de backend `ollama/<model>` só permitido no tier `premium`.
- Ollama: API nativa `POST /api/chat` (campos `prompt_eval_count`, `eval_count`, `done`), nunca o endpoint compatível OpenAI. OpenRouter: `POST /v1/chat/completions` com `stream_options: {include_usage: true}`. Gemini: `generateContent` / `streamGenerateContent?alt=sse`, usa `usageMetadata`.
- Cada adaptador de backend tem teste com `httptest.Server` e fixtures em `internal/backend/<provider>/testdata/*.json`.
- CI GitHub Actions: `go vet`, `go test -race ./...`, `golangci-lint`, `gitleaks`, job de integração que sobe LocalStack (`docker compose up -d localstack`) e roda `go test -tags integration ./...`. Terraform em `infra/` com módulos `dynamodb`, `sqs`, `ecs_fargate`; job `tflocal init && tflocal plan`.
- Docker compose com serviços `gateway`, `localstack` (`SERVICES=dynamodb,sqs`), `otel-collector`, `tempo`, `prometheus`, `grafana` (dashboard provisionado em `deploy/grafana/dashboards/gateway.json`). Ollama roda fora do compose, no host (`http://host.docker.internal:11434`).
- Commits em inglês, Conventional Commits, terminando com `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`.

---

### Task 0: Pré-requisitos

**Files:**
- Nenhum arquivo de código; só comandos de verificação no terminal.

**Interfaces:**
- Consumes: nada.
- Produces: confirmação de que o ambiente está pronto para a Task 1 (versões instaladas, Ollama respondendo, GitHub autenticado).

- [ ] **Step 1: Verificar Go >= 1.27**

Run: `go version`
Expected: `go version go1.27.x ...`. Se a versão for menor, instalar Go 1.27 (Windows: `winget install GoLang.Go` ou baixar de https://go.dev/dl/) antes de prosseguir — o spec exige atualizar Go antes do `go mod init` (Mini-ADR "Go 1.23.2 desatualizado").

- [ ] **Step 2: Verificar Docker Compose**

Run: `docker compose version`
Expected: `Docker Compose version v2.x.x`. Se ausente, instalar Docker Desktop.

- [ ] **Step 3: Verificar Terraform e tflocal**

Run: `terraform -version && tflocal --version`
Expected: `Terraform v1.x.x` e uma versão de `tflocal` (wrapper do `terraform-local`). Se `tflocal` faltar: `pip install terraform-local`.

- [ ] **Step 4: Verificar awslocal**

Run: `awslocal --version`
Expected: versão do `awscli-local`. Se faltar: `pip install awscli-local`.

- [ ] **Step 5: Verificar Ollama rodando com o modelo esperado**

Run: `curl http://localhost:11434/api/tags`
Expected: JSON com `models` incluindo `qwen2.5-coder:1.5b` (usado como backend `free` no `routing.yaml` da Task 1). Se o Ollama não estiver rodando, iniciar o serviço antes de continuar (as Tasks 3/4 dependem dele para gravar fixtures reais).

- [ ] **Step 6: Verificar autenticação `gh`**

Run: `gh auth status`
Expected: `Logged in to github.com as <usuário>`. Guardar o `<usuário>` — a Task 1 usa `gh api user -q .login` para fixar o path do módulo Go.

---

### Task 1: Bootstrap do módulo, layout, config de roteamento e CI

**Files:**
- Create: `go.mod`
- Create: `cmd/gateway/main.go`
- Create: `config/routing.yaml`
- Create: `internal/config/routing.go`
- Create: `internal/config/testdata/routing.yaml`
- Test: `internal/config/routing_test.go`
- Create: `docker-compose.yml`
- Create: `.github/workflows/ci.yml`
- Create: `.golangci.yml`
- Create: `.gitignore`

**Interfaces:**
- Consumes: nada (primeira task de código).
- Produces: `config.LoadRouting(path string) (*config.Routing, error)` retornando `*config.Routing{Aliases map[string]config.AliasTiers, Providers map[string]config.Provider}`, com `AliasTiers{Tiers map[string][]config.BackendTargetConfig}` e `BackendTargetConfig{Provider, Model string}`, `Provider{BaseURL, APIKeyEnv string}`. Usado pelo `router` a partir da Task 9. `main.go` expõe `GET /healthz` num `http.ServeMux` (rotas adicionais chegam na Task 5).

- [ ] **Step 1: Confirmar usuário GitHub e inicializar o módulo**

Run:
```bash
GH_USER=$(gh api user -q .login)
go mod init github.com/$GH_USER/ai-gateway
```
Expected: `go.mod` criado com `module github.com/<usuario>/ai-gateway`. Abrir `go.mod` e garantir que a diretiva seja `go 1.27` (editar manualmente se o `go mod init` gravar outra versão).

- [ ] **Step 2: Criar o layout de diretórios**

Run:
```bash
mkdir -p cmd/gateway internal/api internal/auth internal/ratelimit internal/budget internal/router internal/backend/ollama internal/backend/openrouter internal/backend/gemini internal/resilience internal/trace internal/usage internal/stats internal/config internal/core config infra/modules/dynamodb infra/modules/sqs infra/modules/ecs_fargate deploy/grafana/dashboards
```
Expected: diretórios criados sem erro, batendo com a lista de Global Constraints.

- [ ] **Step 3: Escrever o teste de `config.LoadRouting` (falhando)**

Create `internal/config/routing_test.go`:
```go
package config

import "testing"

func TestLoadRouting_ParsesAliasesAndProviders(t *testing.T) {
	r, err := LoadRouting("testdata/routing.yaml")
	if err != nil {
		t.Fatalf("LoadRouting returned error: %v", err)
	}

	fast, ok := r.Aliases["nuva/fast"]
	if !ok {
		t.Fatalf("expected alias nuva/fast to be present")
	}
	freeTier, ok := fast.Tiers["free"]
	if !ok || len(freeTier) != 1 {
		t.Fatalf("expected nuva/fast free tier with 1 target, got %+v", freeTier)
	}
	if freeTier[0].Provider != "ollama" || freeTier[0].Model != "qwen2.5-coder:1.5b" {
		t.Fatalf("unexpected free tier target: %+v", freeTier[0])
	}

	ollama, ok := r.Providers["ollama"]
	if !ok || ollama.BaseURL != "http://localhost:11434" {
		t.Fatalf("unexpected ollama provider config: %+v", ollama)
	}
}

func TestLoadRouting_MissingFile(t *testing.T) {
	if _, err := LoadRouting("testdata/does-not-exist.yaml"); err == nil {
		t.Fatalf("expected error for missing file")
	}
}
```

Create `internal/config/testdata/routing.yaml`:
```yaml
aliases:
  nuva/fast:
    tiers:
      free:
        - provider: ollama
          model: qwen2.5-coder:1.5b
      standard:
        - provider: ollama
          model: qwen2.5-coder:1.5b
        - provider: openrouter
          model: openai/gpt-4o-mini
      premium:
        - provider: openrouter
          model: openai/gpt-4o-mini
  nuva/smart:
    tiers:
      free:
        - provider: ollama
          model: phi4-mini
      standard:
        - provider: openrouter
          model: openai/gpt-4o
      premium:
        - provider: gemini
          model: gemini-1.5-pro
providers:
  ollama:
    base_url: http://localhost:11434
  openrouter:
    base_url: https://openrouter.ai/api
    api_key_env: OPENROUTER_API_KEY
  gemini:
    base_url: https://generativelanguage.googleapis.com
    api_key_env: GEMINI_API_KEY
```

- [ ] **Step 4: Rodar o teste e ver falhar**

Run: `go test ./internal/config/... -run TestLoadRouting -v`
Expected: FAIL — `undefined: LoadRouting` (o pacote `config` ainda não tem `routing.go`).

- [ ] **Step 5: Implementar `internal/config/routing.go`**

```go
package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// BackendTargetConfig is one entry in an alias/tier cascade, as read from YAML.
type BackendTargetConfig struct {
	Provider string `yaml:"provider"`
	Model    string `yaml:"model"`
}

// AliasTiers maps tier name ("free", "standard", "premium") to its ordered cascade.
type AliasTiers struct {
	Tiers map[string][]BackendTargetConfig `yaml:"tiers"`
}

// Provider holds connection info for one backend provider.
type Provider struct {
	BaseURL   string `yaml:"base_url"`
	APIKeyEnv string `yaml:"api_key_env"`
}

// Routing is the parsed content of config/routing.yaml.
type Routing struct {
	Aliases   map[string]AliasTiers `yaml:"aliases"`
	Providers map[string]Provider   `yaml:"providers"`
}

// LoadRouting reads and parses a routing YAML file from disk.
func LoadRouting(path string) (*Routing, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: reading routing file %s: %w", path, err)
	}
	var r Routing
	if err := yaml.Unmarshal(data, &r); err != nil {
		return nil, fmt.Errorf("config: parsing routing file %s: %w", path, err)
	}
	return &r, nil
}
```

- [ ] **Step 6: Adicionar a dependência yaml.v3 e rodar o teste**

Run:
```bash
go get gopkg.in/yaml.v3
go test ./internal/config/... -run TestLoadRouting -v
```
Expected: PASS nos dois casos.

- [ ] **Step 7: Criar o `config/routing.yaml` real**

Create `config/routing.yaml` com o mesmo conteúdo YAML do Step 3 (é a política real de roteamento que `router`/`resilience` carregam a partir da Task 9).

- [ ] **Step 8: Criar `cmd/gateway/main.go` mínimo**

```go
package main

import (
	"log/slog"
	"net/http"
	"os"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})

	addr := ":8080"
	logger.Info("starting ai-gateway", "addr", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		logger.Error("server stopped", "error", err)
		os.Exit(1)
	}
}
```

- [ ] **Step 9: Rodar `go build` para validar o bootstrap**

Run: `go build ./...`
Expected: build sem erro.

- [ ] **Step 10: Criar `docker-compose.yml`**

```yaml
services:
  gateway:
    build: .
    ports:
      - "8080:8080"
    environment:
      - OLLAMA_BASE_URL=http://host.docker.internal:11434
      - AWS_ENDPOINT_URL=http://localstack:4566
    extra_hosts:
      - "host.docker.internal:host-gateway"
    depends_on:
      - localstack

  localstack:
    image: localstack/localstack:3
    ports:
      - "4566:4566"
    environment:
      - SERVICES=dynamodb,sqs

  otel-collector:
    image: otel/opentelemetry-collector-contrib:0.108.0
    volumes:
      - ./deploy/otel-collector-config.yaml:/etc/otelcol-contrib/config.yaml
    ports:
      - "4318:4318"

  tempo:
    image: grafana/tempo:2.5.0
    command: ["-config.file=/etc/tempo.yaml"]
    volumes:
      - ./deploy/tempo.yaml:/etc/tempo.yaml
    ports:
      - "3200:3200"

  prometheus:
    image: prom/prometheus:v2.54.1
    volumes:
      - ./deploy/prometheus.yml:/etc/prometheus/prometheus.yml
    ports:
      - "9090:9090"

  grafana:
    image: grafana/grafana:11.1.0
    ports:
      - "3000:3000"
    volumes:
      - ./deploy/grafana/dashboards:/etc/grafana/provisioning/dashboards
```
Nota: `deploy/otel-collector-config.yaml`, `deploy/tempo.yaml`, `deploy/prometheus.yml` e `deploy/grafana/dashboards/gateway.json` são criados na Task 13; até lá, só `gateway` e `localstack` sobem — suficiente para as tasks anteriores.

- [ ] **Step 11: Criar `.golangci.yml` mínimo**

```yaml
run:
  timeout: 3m
linters:
  enable:
    - govet
    - staticcheck
    - unused
    - errcheck
```

- [ ] **Step 12: Criar `.gitignore`**

```
/ai-gateway
*.exe
.env
```

- [ ] **Step 13: Criar o workflow de CI**

Create `.github/workflows/ci.yml`:
```yaml
name: CI
on:
  push:
  pull_request:

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: "1.27"
      - run: go vet ./...
      - run: go test -race ./...
      - uses: golangci/golangci-lint-action@v6
        with:
          version: latest
      - uses: gitleaks/gitleaks-action@v2

  integration:
    runs-on: ubuntu-latest
    needs: test
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: "1.27"
      - run: docker compose up -d localstack
      - run: go test -tags integration ./...
        env:
          AWS_ENDPOINT_URL: http://localhost:4566

  terraform-plan:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: pip install terraform-local
      - uses: hashicorp/setup-terraform@v3
      - run: docker compose up -d localstack
      - working-directory: infra
        run: tflocal init && tflocal plan
```

- [ ] **Step 14: Commit**

```bash
git init
git add go.mod go.sum cmd config internal docker-compose.yml .github .golangci.yml .gitignore
git commit -m "chore: bootstrap ai-gateway module, layout, routing config and CI

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 2: Contratos internos (`internal/core`)

**Files:**
- Create: `internal/core/types.go`
- Create: `internal/core/errors.go`
- Test: `internal/core/errors_test.go`

**Interfaces:**
- Consumes: nada.
- Produces (usado por todas as tasks seguintes): `core.Message{Role, Content string}`; `core.ChatRequest{RequestID, TenantID, Alias, Model string, Messages []core.Message, MaxTokens int, Stream bool}`; `core.Usage{PromptTokens, CompletionTokens int}`; `core.ChatChunk{Delta string, FinishReason string, Usage *core.Usage}`; `core.ChatResponse{Message core.Message, Usage core.Usage, FinishReason string}`; `core.StreamBackend interface{ChatStream(ctx context.Context, model string, req core.ChatRequest) (<-chan core.ChatChunk, <-chan error)}`; `core.BackendTarget{Provider, Model string}`; `core.Tenant{ID, APIKeyHash, Tier string, RPMLimit int, MonthlyTokenBudget int}`; `core.Reservation{ID, TenantID, Period string, EstimatedTokens int}`; `core.ErrorClass` (`core.Transient`, `core.Permanent`); `core.BackendError{Class core.ErrorClass, Status int, Err error}` implementando `error`.

- [ ] **Step 1: Escrever o teste de `BackendError` (falhando)**

Create `internal/core/errors_test.go`:
```go
package core

import (
	"errors"
	"testing"
)

func TestBackendError_ErrorMessageWrapsUnderlying(t *testing.T) {
	underlying := errors.New("connection refused")
	be := &BackendError{Class: Transient, Status: 0, Err: underlying}

	if got := be.Error(); got != "backend error (transient): connection refused" {
		t.Fatalf("unexpected Error() output: %q", got)
	}
	if !errors.Is(be, underlying) {
		t.Fatalf("expected errors.Is to unwrap to underlying error")
	}
}

func TestBackendError_ClassifiesTransientVsPermanent(t *testing.T) {
	transient := &BackendError{Class: Transient, Status: 503}
	permanent := &BackendError{Class: Permanent, Status: 400}

	if !transient.IsTransient() {
		t.Fatalf("expected 503 BackendError to be transient")
	}
	if permanent.IsTransient() {
		t.Fatalf("expected 400 BackendError to be permanent")
	}
}
```

- [ ] **Step 2: Rodar o teste e ver falhar**

Run: `go test ./internal/core/... -v`
Expected: FAIL — `undefined: BackendError`, `undefined: Transient` (pacote `core` ainda não existe).

- [ ] **Step 3: Implementar `internal/core/types.go`**

```go
package core

import "context"

// Message is one turn in a chat conversation, OpenAI-compatible shape.
type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// ChatRequest is the gateway's internal representation of an incoming
// POST /v1/chat/completions request, after the api module parses HTTP.
type ChatRequest struct {
	RequestID string
	TenantID  string
	Alias     string // e.g. "nuva/fast", or a direct "ollama/qwen2.5-coder:1.5b"
	Tier      string
	Messages  []Message
	MaxTokens int
	Stream    bool
}

// Usage reports token counts for a completed (or partially completed) call.
type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
}

// ChatChunk is one Server-Sent Events delta emitted during streaming.
type ChatChunk struct {
	Delta        string
	FinishReason string
	Usage        *Usage // only set on the final chunk, when the backend reports it
}

// ChatResponse is the full, non-streaming completion result.
type ChatResponse struct {
	Message      Message
	Usage        Usage
	FinishReason string
}

// StreamBackend is implemented by every backend adapter (ollama, openrouter,
// gemini) for the streaming call path. resilience.CallStream (Task 9) uses
// this interface, resolved per BackendTarget, to try each target's stream in
// order without depending on any concrete backend package.
type StreamBackend interface {
	ChatStream(ctx context.Context, model string, req ChatRequest) (<-chan ChatChunk, <-chan error)
}

// BackendTarget names one entry in a routing cascade: a provider and its model.
type BackendTarget struct {
	Provider string
	Model    string
}

// Tenant is the authenticated caller's identity and limits.
type Tenant struct {
	ID                 string
	APIKeyHash         string
	Tier               string
	RPMLimit           int
	MonthlyTokenBudget int
}

// Reservation is a pending budget hold created by budget.Reserve.
type Reservation struct {
	ID              string
	TenantID        string
	Period          string // "YYYY-MM"
	EstimatedTokens int
}
```

- [ ] **Step 4: Implementar `internal/core/errors.go`**

```go
package core

import "fmt"

// ErrorClass tells resilience whether a BackendError is worth retrying.
type ErrorClass int

const (
	// Transient errors (timeout, 5xx, provider 429, connection refused) may
	// succeed on retry or on the next backend in the cascade.
	Transient ErrorClass = iota
	// Permanent errors (4xx validation, unknown model, invalid key) never
	// succeed on retry; resilience must move straight to the next backend
	// without retrying the same one.
	Permanent
)

func (c ErrorClass) String() string {
	if c == Transient {
		return "transient"
	}
	return "permanent"
}

// BackendError is the typed error every backend adapter must return so that
// resilience can decide whether to retry, fall back, or give up.
type BackendError struct {
	Class  ErrorClass
	Status int
	Err    error
}

func (e *BackendError) Error() string {
	return fmt.Sprintf("backend error (%s): %v", e.Class, e.Err)
}

func (e *BackendError) Unwrap() error {
	return e.Err
}

// IsTransient reports whether this error should trigger a retry/fallback.
func (e *BackendError) IsTransient() bool {
	return e.Class == Transient
}
```

- [ ] **Step 5: Rodar o teste e ver passar**

Run: `go test ./internal/core/... -v`
Expected: PASS nos dois casos.

- [ ] **Step 6: Commit**

```bash
git add internal/core
git commit -m "feat(core): add shared request/response and typed backend error types

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 3: Backend Ollama — chamada não-streaming

**Files:**
- Create: `internal/backend/ollama/client.go`
- Create: `internal/backend/ollama/testdata/chat_success.json`
- Create: `internal/backend/ollama/testdata/chat_error.json`
- Test: `internal/backend/ollama/client_test.go`

**Interfaces:**
- Consumes: `core.ChatRequest`, `core.Message`, `core.ChatResponse`, `core.Usage`, `core.BackendError`, `core.Transient`, `core.Permanent` (Task 2).
- Produces: `ollama.NewClient(baseURL string) *ollama.Client`; `(*ollama.Client).Chat(ctx context.Context, model string, req core.ChatRequest) (core.ChatResponse, error)` — usado pelo `router`/`resilience` a partir da Task 9, e diretamente pelo `api` na Task 4/5 como target fixo de bootstrap.

- [ ] **Step 1: Gravar a fixture de sucesso (sintética, plausível)**

Create `internal/backend/ollama/testdata/chat_success.json`:
```json
{
  "model": "qwen2.5-coder:1.5b",
  "created_at": "2026-09-19T12:00:00Z",
  "message": {
    "role": "assistant",
    "content": "Olá! Como posso ajudar?"
  },
  "done": true,
  "done_reason": "stop",
  "prompt_eval_count": 12,
  "eval_count": 8
}
```

Create `internal/backend/ollama/testdata/chat_error.json`:
```json
{
  "error": "model \"does-not-exist\" not found, try pulling it first"
}
```

- [ ] **Step 2: Escrever o teste de `Chat` bem-sucedido (falhando)**

Create `internal/backend/ollama/client_test.go`:
```go
package ollama

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/vicentemoura/ai-gateway/internal/core"
)

func serveFixture(t *testing.T, status int, fixturePath string) *httptest.Server {
	t.Helper()
	body, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatalf("reading fixture %s: %v", fixturePath, err)
	}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/chat" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		w.Write(body)
	}))
}

func TestChat_Success(t *testing.T) {
	srv := serveFixture(t, http.StatusOK, "testdata/chat_success.json")
	defer srv.Close()

	client := NewClient(srv.URL)
	req := core.ChatRequest{
		RequestID: "req-1",
		Messages:  []core.Message{{Role: "user", Content: "oi"}},
	}

	resp, err := client.Chat(context.Background(), "qwen2.5-coder:1.5b", req)
	if err != nil {
		t.Fatalf("Chat returned error: %v", err)
	}
	if resp.Message.Content != "Olá! Como posso ajudar?" {
		t.Fatalf("unexpected message content: %q", resp.Message.Content)
	}
	if resp.Usage.PromptTokens != 12 || resp.Usage.CompletionTokens != 8 {
		t.Fatalf("unexpected usage: %+v", resp.Usage)
	}
	if resp.FinishReason != "stop" {
		t.Fatalf("unexpected finish reason: %q", resp.FinishReason)
	}
}

func TestChat_ModelNotFound_IsPermanentError(t *testing.T) {
	srv := serveFixture(t, http.StatusNotFound, "testdata/chat_error.json")
	defer srv.Close()

	client := NewClient(srv.URL)
	req := core.ChatRequest{RequestID: "req-2", Messages: []core.Message{{Role: "user", Content: "oi"}}}

	_, err := client.Chat(context.Background(), "does-not-exist", req)
	if err == nil {
		t.Fatalf("expected error for 404 response")
	}
	var be *core.BackendError
	if !errorsAs(err, &be) {
		t.Fatalf("expected *core.BackendError, got %T", err)
	}
	if be.Class != core.Permanent {
		t.Fatalf("expected Permanent class for 404, got %v", be.Class)
	}
	if be.Status != http.StatusNotFound {
		t.Fatalf("expected status 404, got %d", be.Status)
	}
}
```

Add a small helper in the same test file to avoid importing `errors` twice awkwardly:
```go
func errorsAs(err error, target **core.BackendError) bool {
	be, ok := err.(*core.BackendError)
	if ok {
		*target = be
	}
	return ok
}
```

- [ ] **Step 3: Rodar o teste e ver falhar**

Run: `go test ./internal/backend/ollama/... -v`
Expected: FAIL — `undefined: NewClient` (arquivo `client.go` ainda não existe).

- [ ] **Step 4: Implementar `internal/backend/ollama/client.go`**

```go
package ollama

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/vicentemoura/ai-gateway/internal/core"
)

// Client talks to Ollama's native /api/chat endpoint (not the OpenAI-compatible one).
type Client struct {
	baseURL string
	http    *http.Client
}

func NewClient(baseURL string) *Client {
	return &Client{baseURL: baseURL, http: &http.Client{}}
}

type chatRequestBody struct {
	Model    string          `json:"model"`
	Messages []core.Message  `json:"messages"`
	Stream   bool            `json:"stream"`
}

type chatResponseBody struct {
	Message         core.Message `json:"message"`
	Done            bool         `json:"done"`
	DoneReason      string       `json:"done_reason"`
	PromptEvalCount int          `json:"prompt_eval_count"`
	EvalCount       int          `json:"eval_count"`
	Error           string       `json:"error"`
}

// Chat performs a single non-streaming call to Ollama's /api/chat.
func (c *Client) Chat(ctx context.Context, model string, req core.ChatRequest) (core.ChatResponse, error) {
	body, err := json.Marshal(chatRequestBody{Model: model, Messages: req.Messages, Stream: false})
	if err != nil {
		return core.ChatResponse{}, &core.BackendError{Class: core.Permanent, Err: fmt.Errorf("ollama: marshaling request: %w", err)}
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/chat", bytes.NewReader(body))
	if err != nil {
		return core.ChatResponse{}, &core.BackendError{Class: core.Permanent, Err: fmt.Errorf("ollama: building request: %w", err)}
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return core.ChatResponse{}, &core.BackendError{Class: core.Transient, Err: fmt.Errorf("ollama: request failed: %w", err)}
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return core.ChatResponse{}, &core.BackendError{Class: core.Transient, Err: fmt.Errorf("ollama: reading response: %w", err)}
	}

	if resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests {
		return core.ChatResponse{}, &core.BackendError{Class: core.Transient, Status: resp.StatusCode, Err: fmt.Errorf("ollama: %s", string(raw))}
	}
	if resp.StatusCode >= 400 {
		return core.ChatResponse{}, &core.BackendError{Class: core.Permanent, Status: resp.StatusCode, Err: fmt.Errorf("ollama: %s", string(raw))}
	}

	var parsed chatResponseBody
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return core.ChatResponse{}, &core.BackendError{Class: core.Transient, Err: fmt.Errorf("ollama: parsing response: %w", err)}
	}

	return core.ChatResponse{
		Message:      parsed.Message,
		FinishReason: parsed.DoneReason,
		Usage: core.Usage{
			PromptTokens:     parsed.PromptEvalCount,
			CompletionTokens: parsed.EvalCount,
		},
	}, nil
}
```

- [ ] **Step 5: Rodar o teste e ver passar**

Run: `go test ./internal/backend/ollama/... -v`
Expected: PASS nos dois casos.

- [ ] **Step 6: Gravar a fixture real com curl e substituir a sintética**

Run (com Ollama local rodando, conforme Task 0 Step 5):
```bash
curl -s http://localhost:11434/api/chat -d '{"model":"qwen2.5-coder:1.5b","messages":[{"role":"user","content":"oi"}],"stream":false}' | tee internal/backend/ollama/testdata/chat_success.json
```
Expected: o comando grava a resposta real do Ollama em `chat_success.json`, substituindo a fixture sintética. Conferir manualmente que os campos `message.content`, `done_reason`, `prompt_eval_count` e `eval_count` existem no JSON gravado — se `done_reason` não vier no payload do Ollama instalado, ajustar o teste do Step 2 para não fixar esse valor literal e sim checar apenas que não é vazio.

- [ ] **Step 7: Rodar o teste de novo contra a fixture real**

Run: `go test ./internal/backend/ollama/... -v`
Expected: PASS (a asserção de conteúdo exato pode precisar de ajuste manual se a fixture real tiver texto diferente de "Olá! Como posso ajudar?" — ajustar a asserção do Step 2 para checar `resp.Message.Content != ""` em vez do texto literal, já que o modelo real não é determinístico).

- [ ] **Step 8: Commit**

```bash
git add internal/backend/ollama
git commit -m "feat(backend/ollama): add non-streaming chat client with real fixture

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 4: Backend Ollama — streaming, `/v1/models` e `/healthz`

**Files:**
- Modify: `internal/backend/ollama/client.go`
- Create: `internal/backend/ollama/testdata/chat_stream.ndjson`
- Modify: `internal/backend/ollama/client_test.go`
- Modify: `cmd/gateway/main.go`
- Create: `internal/backend/ollama/tags.go`
- Test: `internal/backend/ollama/tags_test.go`
- Create: `internal/backend/ollama/testdata/tags_success.json`

**Interfaces:**
- Consumes: `core.ChatChunk` (Task 2), `ollama.Client` (Task 3).
- Produces: `(*ollama.Client).ChatStream(ctx context.Context, model string, req core.ChatRequest) (<-chan core.ChatChunk, <-chan error)` — canal de chunks fechado ao final, canal de erro recebe no máximo um valor; `(*ollama.Client).Tags(ctx context.Context) ([]string, error)` — usado por `/v1/models` e por `/healthz` (warm-up check, ver spec "Riscos").

- [ ] **Step 1: Gravar a fixture de streaming (sintética, formato NDJSON do Ollama)**

Create `internal/backend/ollama/testdata/chat_stream.ndjson`:
```
{"model":"qwen2.5-coder:1.5b","created_at":"2026-09-19T12:00:00Z","message":{"role":"assistant","content":"Ol"},"done":false}
{"model":"qwen2.5-coder:1.5b","created_at":"2026-09-19T12:00:00Z","message":{"role":"assistant","content":"á!"},"done":false}
{"model":"qwen2.5-coder:1.5b","created_at":"2026-09-19T12:00:01Z","message":{"role":"assistant","content":""},"done":true,"done_reason":"stop","prompt_eval_count":12,"eval_count":8}
```

- [ ] **Step 2: Escrever o teste de `ChatStream` (falhando)**

Append to `internal/backend/ollama/client_test.go`:
```go
func TestChatStream_EmitsDeltasThenFinalUsage(t *testing.T) {
	body, err := os.ReadFile("testdata/chat_stream.ndjson")
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.WriteHeader(http.StatusOK)
		w.Write(body)
	}))
	defer srv.Close()

	client := NewClient(srv.URL)
	req := core.ChatRequest{RequestID: "req-3", Messages: []core.Message{{Role: "user", Content: "oi"}}}

	chunks, errs := client.ChatStream(context.Background(), "qwen2.5-coder:1.5b", req)

	var deltas []string
	var final *core.ChatChunk
	for chunk := range chunks {
		chunk := chunk
		if chunk.Usage != nil {
			final = &chunk
		} else {
			deltas = append(deltas, chunk.Delta)
		}
	}
	if err := <-errs; err != nil {
		t.Fatalf("unexpected stream error: %v", err)
	}

	if len(deltas) != 2 || deltas[0] != "Ol" || deltas[1] != "á!" {
		t.Fatalf("unexpected deltas: %+v", deltas)
	}
	if final == nil {
		t.Fatalf("expected a final chunk with usage")
	}
	if final.Usage.PromptTokens != 12 || final.Usage.CompletionTokens != 8 {
		t.Fatalf("unexpected final usage: %+v", final.Usage)
	}
	if final.FinishReason != "stop" {
		t.Fatalf("unexpected finish reason: %q", final.FinishReason)
	}
}
```

- [ ] **Step 3: Rodar o teste e ver falhar**

Run: `go test ./internal/backend/ollama/... -run TestChatStream -v`
Expected: FAIL — `undefined: ChatStream`.

- [ ] **Step 4: Implementar `ChatStream` em `internal/backend/ollama/client.go`**

Add to the same file (after `Chat`):
```go
// ChatStream performs a streaming call to Ollama's /api/chat, decoding one
// NDJSON object per line. The chunk channel is closed when the stream ends;
// the error channel receives at most one value.
func (c *Client) ChatStream(ctx context.Context, model string, req core.ChatRequest) (<-chan core.ChatChunk, <-chan error) {
	chunks := make(chan core.ChatChunk)
	errs := make(chan error, 1)

	go func() {
		defer close(chunks)
		defer close(errs)

		body, err := json.Marshal(chatRequestBody{Model: model, Messages: req.Messages, Stream: true})
		if err != nil {
			errs <- &core.BackendError{Class: core.Permanent, Err: fmt.Errorf("ollama: marshaling request: %w", err)}
			return
		}

		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/chat", bytes.NewReader(body))
		if err != nil {
			errs <- &core.BackendError{Class: core.Permanent, Err: fmt.Errorf("ollama: building request: %w", err)}
			return
		}
		httpReq.Header.Set("Content-Type", "application/json")

		resp, err := c.http.Do(httpReq)
		if err != nil {
			errs <- &core.BackendError{Class: core.Transient, Err: fmt.Errorf("ollama: request failed: %w", err)}
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 400 {
			raw, _ := io.ReadAll(resp.Body)
			class := core.Permanent
			if resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests {
				class = core.Transient
			}
			errs <- &core.BackendError{Class: class, Status: resp.StatusCode, Err: fmt.Errorf("ollama: %s", string(raw))}
			return
		}

		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			line := scanner.Bytes()
			if len(line) == 0 {
				continue
			}
			var parsed chatResponseBody
			if err := json.Unmarshal(line, &parsed); err != nil {
				errs <- &core.BackendError{Class: core.Transient, Err: fmt.Errorf("ollama: parsing stream line: %w", err)}
				return
			}
			if parsed.Done {
				chunks <- core.ChatChunk{
					FinishReason: parsed.DoneReason,
					Usage: &core.Usage{
						PromptTokens:     parsed.PromptEvalCount,
						CompletionTokens: parsed.EvalCount,
					},
				}
				return
			}
			select {
			case chunks <- core.ChatChunk{Delta: parsed.Message.Content}:
			case <-ctx.Done():
				return
			}
		}
		if err := scanner.Err(); err != nil {
			errs <- &core.BackendError{Class: core.Transient, Err: fmt.Errorf("ollama: reading stream: %w", err)}
		}
	}()

	return chunks, errs
}
```

Add `"bufio"` to the import block at the top of `client.go`.

- [ ] **Step 5: Rodar o teste e ver passar**

Run: `go test ./internal/backend/ollama/... -v`
Expected: PASS em todos os casos (`Chat`, `ChatStream`, erro 404).

- [ ] **Step 6: Gravar a fixture de streaming real com curl**

Run (Ollama local rodando):
```bash
curl -s http://localhost:11434/api/chat -d '{"model":"qwen2.5-coder:1.5b","messages":[{"role":"user","content":"oi"}],"stream":true}' | tee internal/backend/ollama/testdata/chat_stream.ndjson
```
Expected: arquivo NDJSON real gravado, substituindo a fixture sintética. Ajustar a asserção do Step 2 para não fixar o texto exato dos deltas (modelo real não é determinístico) — checar apenas `len(deltas) > 0` e que a concatenação não é vazia.

- [ ] **Step 7: Fixture e teste de `/api/tags` (para `/v1/models`)**

Create `internal/backend/ollama/testdata/tags_success.json`:
```json
{
  "models": [
    {"name": "qwen2.5-coder:1.5b"},
    {"name": "phi4-mini"},
    {"name": "gemma4-coder"}
  ]
}
```

Create `internal/backend/ollama/tags_test.go`:
```go
package ollama

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func TestTags_ListsModelNames(t *testing.T) {
	body, err := os.ReadFile("testdata/tags_success.json")
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/tags" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(body)
	}))
	defer srv.Close()

	client := NewClient(srv.URL)
	names, err := client.Tags(context.Background())
	if err != nil {
		t.Fatalf("Tags returned error: %v", err)
	}
	if len(names) != 3 || names[0] != "qwen2.5-coder:1.5b" {
		t.Fatalf("unexpected names: %+v", names)
	}
}
```

- [ ] **Step 8: Rodar o teste e ver falhar**

Run: `go test ./internal/backend/ollama/... -run TestTags -v`
Expected: FAIL — `undefined: Tags`.

- [ ] **Step 9: Implementar `internal/backend/ollama/tags.go`**

```go
package ollama

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/vicentemoura/ai-gateway/internal/core"
)

type tagsResponseBody struct {
	Models []struct {
		Name string `json:"name"`
	} `json:"models"`
}

// Tags returns the list of model names Ollama currently has pulled, used by
// GET /v1/models and by the /healthz warm-up check.
func (c *Client) Tags(ctx context.Context) ([]string, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/api/tags", nil)
	if err != nil {
		return nil, &core.BackendError{Class: core.Permanent, Err: fmt.Errorf("ollama: building tags request: %w", err)}
	}

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return nil, &core.BackendError{Class: core.Transient, Err: fmt.Errorf("ollama: tags request failed: %w", err)}
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return nil, &core.BackendError{Class: core.Transient, Status: resp.StatusCode, Err: fmt.Errorf("ollama: tags returned %d", resp.StatusCode)}
	}

	var parsed tagsResponseBody
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, &core.BackendError{Class: core.Transient, Err: fmt.Errorf("ollama: parsing tags: %w", err)}
	}

	names := make([]string, 0, len(parsed.Models))
	for _, m := range parsed.Models {
		names = append(names, m.Name)
	}
	return names, nil
}
```

- [ ] **Step 10: Rodar o teste e ver passar**

Run: `go test ./internal/backend/ollama/... -v`
Expected: PASS em todos os casos.

- [ ] **Step 11: Gravar a fixture real de `/api/tags`**

Run: `curl -s http://localhost:11434/api/tags | tee internal/backend/ollama/testdata/tags_success.json`
Expected: lista real de modelos instalados (deve incluir `qwen2.5-coder:1.5b`, conforme Task 0 Step 5). Ajustar a asserção do Step 7 se a ordem/quantidade de modelos reais divergir do fixture sintético — checar `len(names) > 0` e `contains(names, "qwen2.5-coder:1.5b")` em vez de índices fixos.

- [ ] **Step 12: Commit**

```bash
git add internal/backend/ollama
git commit -m "feat(backend/ollama): add streaming chat and tags listing with real fixtures

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 5: `api` OpenAI-compatible contra target fixo, SSE e `X-Request-Id`

**Files:**
- Create: `internal/api/requestid.go`
- Test: `internal/api/requestid_test.go`
- Create: `internal/api/errors.go`
- Create: `internal/api/chat.go`
- Test: `internal/api/chat_test.go`
- Modify: `cmd/gateway/main.go`

**Interfaces:**
- Consumes: `ollama.Client.Chat`/`ChatStream` (Task 3/4), `core.ChatRequest`/`Message` (Task 2).
- Produces: `api.RequestIDMiddleware(next http.Handler) http.Handler` (gera uuid v4, seta header `X-Request-Id`, injeta no `context.Context` via `api.RequestIDFromContext(ctx) string`); `api.WriteError(w http.ResponseWriter, requestID string, status int, errType, message string)` escrevendo `{"error":{"type","message","request_id"}}`; `api.NewChatHandler(client *ollama.Client, model string) http.Handler` — handler fixo desta task, substituído pelo pipeline completo (auth→ratelimit→budget→router→resilience) na Task 9.

- [ ] **Step 1: Escrever o teste do middleware de request id (falhando)**

Create `internal/api/requestid_test.go`:
```go
package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRequestIDMiddleware_SetsHeaderAndContext(t *testing.T) {
	var gotFromContext string
	handler := RequestIDMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotFromContext = RequestIDFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	handler.ServeHTTP(rec, req)

	headerID := rec.Header().Get("X-Request-Id")
	if headerID == "" {
		t.Fatalf("expected X-Request-Id header to be set")
	}
	if headerID != gotFromContext {
		t.Fatalf("header id %q does not match context id %q", headerID, gotFromContext)
	}
	if len(headerID) != 36 {
		t.Fatalf("expected a uuid-shaped id, got %q", headerID)
	}
}
```

- [ ] **Step 2: Rodar o teste e ver falhar**

Run: `go test ./internal/api/... -run TestRequestIDMiddleware -v`
Expected: FAIL — `undefined: RequestIDMiddleware`.

- [ ] **Step 3: Implementar `internal/api/requestid.go`**

```go
package api

import (
	"context"
	"net/http"

	"github.com/google/uuid"
)

type contextKey string

const requestIDKey contextKey = "request_id"

// RequestIDMiddleware generates a uuid v4 for every request, sets it as the
// X-Request-Id response header, and makes it retrievable via context.
func RequestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := uuid.NewString()
		w.Header().Set("X-Request-Id", id)
		ctx := context.WithValue(r.Context(), requestIDKey, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// RequestIDFromContext retrieves the request id set by RequestIDMiddleware.
// Returns "" if none was set (e.g. in a test that skips the middleware).
func RequestIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey).(string)
	return id
}
```

- [ ] **Step 4: Adicionar `google/uuid` e rodar o teste**

Run:
```bash
go get github.com/google/uuid
go test ./internal/api/... -run TestRequestIDMiddleware -v
```
Expected: PASS.

- [ ] **Step 5: Implementar `internal/api/errors.go`**

```go
package api

import (
	"encoding/json"
	"net/http"
)

type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Type      string `json:"type"`
	Message   string `json:"message"`
	RequestID string `json:"request_id"`
}

// WriteError writes the gateway's standard error envelope, always including
// the request id so a caller can correlate with trace_events.
func WriteError(w http.ResponseWriter, requestID string, status int, errType, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(errorBody{Error: errorDetail{Type: errType, Message: message, RequestID: requestID}})
}
```

- [ ] **Step 6: Escrever o teste end-to-end do handler de chat (falhando)**

Create `internal/api/chat_test.go`:
```go
package api

import (
	"bufio"
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/vicentemoura/ai-gateway/internal/backend/ollama"
)

func TestChatHandler_NonStreaming_ReturnsOpenAIShapedResponse(t *testing.T) {
	fixture, err := os.ReadFile("../backend/ollama/testdata/chat_success.json")
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}
	ollamaSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(fixture)
	}))
	defer ollamaSrv.Close()

	client := ollama.NewClient(ollamaSrv.URL)
	handler := RequestIDMiddleware(NewChatHandler(client, "qwen2.5-coder:1.5b"))

	reqBody := `{"model":"nuva/fast","messages":[{"role":"user","content":"oi"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(reqBody))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("X-Request-Id") == "" {
		t.Fatalf("expected X-Request-Id header on response")
	}

	var parsed struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &parsed); err != nil {
		t.Fatalf("parsing response body: %v", err)
	}
	if len(parsed.Choices) != 1 || parsed.Choices[0].Message.Content == "" {
		t.Fatalf("unexpected choices: %+v", parsed.Choices)
	}
}

func TestChatHandler_Streaming_EmitsSSEWithDoneSentinel(t *testing.T) {
	fixture, err := os.ReadFile("../backend/ollama/testdata/chat_stream.ndjson")
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}
	ollamaSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(fixture)
	}))
	defer ollamaSrv.Close()

	client := ollama.NewClient(ollamaSrv.URL)
	handler := RequestIDMiddleware(NewChatHandler(client, "qwen2.5-coder:1.5b"))

	reqBody := `{"model":"nuva/fast","stream":true,"messages":[{"role":"user","content":"oi"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(reqBody))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("expected text/event-stream, got %q", ct)
	}

	scanner := bufio.NewScanner(bytes.NewReader(rec.Body.Bytes()))
	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	last := lines[len(lines)-1]
	if last != "data: [DONE]" {
		t.Fatalf("expected last SSE line to be the DONE sentinel, got %q", last)
	}
}
```

- [ ] **Step 7: Rodar o teste e ver falhar**

Run: `go test ./internal/api/... -run TestChatHandler -v`
Expected: FAIL — `undefined: NewChatHandler`.

- [ ] **Step 8: Implementar `internal/api/chat.go`**

```go
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/vicentemoura/ai-gateway/internal/backend/ollama"
	"github.com/vicentemoura/ai-gateway/internal/core"
)

type incomingMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatCompletionRequest struct {
	Model    string            `json:"model"`
	Messages []incomingMessage `json:"messages"`
	Stream   bool              `json:"stream"`
}

// NewChatHandler is the Task 5 bootstrap handler: it always calls the given
// fixed Ollama model, with no auth/ratelimit/budget/routing. The full
// pipeline (auth -> ratelimit -> budget -> router -> resilience) replaces
// the target selection in Task 9, reusing this same HTTP translation layer.
func NewChatHandler(client *ollama.Client, model string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := RequestIDFromContext(r.Context())

		var body chatCompletionRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			WriteError(w, requestID, http.StatusBadRequest, "invalid_request", "malformed JSON body")
			return
		}

		messages := make([]core.Message, 0, len(body.Messages))
		for _, m := range body.Messages {
			messages = append(messages, core.Message{Role: m.Role, Content: m.Content})
		}
		chatReq := core.ChatRequest{RequestID: requestID, Messages: messages, Stream: body.Stream}

		if body.Stream {
			serveStream(w, r, client, model, chatReq, requestID)
			return
		}
		serveNonStream(r.Context(), w, client, model, chatReq, requestID)
	})
}

func serveNonStream(ctx context.Context, w http.ResponseWriter, client *ollama.Client, model string, chatReq core.ChatRequest, requestID string) {
	resp, err := client.Chat(ctx, model, chatReq)
	if err != nil {
		WriteError(w, requestID, http.StatusBadGateway, "backend_error", err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"id":      requestID,
		"object":  "chat.completion",
		"choices": []map[string]any{{"index": 0, "message": map[string]string{"role": "assistant", "content": resp.Message.Content}, "finish_reason": resp.FinishReason}},
		"usage":   map[string]int{"prompt_tokens": resp.Usage.PromptTokens, "completion_tokens": resp.Usage.CompletionTokens},
	})
}

func serveStream(w http.ResponseWriter, r *http.Request, client *ollama.Client, model string, chatReq core.ChatRequest, requestID string) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		WriteError(w, requestID, http.StatusInternalServerError, "streaming_unsupported", "response writer does not support flushing")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)

	chunks, errs := client.ChatStream(r.Context(), model, chatReq)
	for chunk := range chunks {
		select {
		case <-r.Context().Done():
			return
		default:
		}
		payload, _ := json.Marshal(map[string]any{
			"id":      requestID,
			"object":  "chat.completion.chunk",
			"choices": []map[string]any{{"index": 0, "delta": map[string]string{"content": chunk.Delta}, "finish_reason": chunk.FinishReason}},
		})
		fmt.Fprintf(w, "data: %s\n\n", payload)
		flusher.Flush()
	}
	if err := <-errs; err != nil {
		payload, _ := json.Marshal(map[string]string{"error": err.Error()})
		fmt.Fprintf(w, "data: %s\n\n", payload)
		flusher.Flush()
	}
	fmt.Fprint(w, "data: [DONE]\n\n")
	flusher.Flush()
}
```

- [ ] **Step 9: Rodar o teste e ver passar**

Run: `go test ./internal/api/... -v`
Expected: PASS em todos os casos (request id, chat não-streaming, chat streaming com sentinela `[DONE]`).

- [ ] **Step 10: Ligar o handler em `cmd/gateway/main.go`**

Replace the body of `main.go` from Task 1 with:
```go
package main

import (
	"log/slog"
	"net/http"
	"os"

	"github.com/vicentemoura/ai-gateway/internal/api"
	"github.com/vicentemoura/ai-gateway/internal/backend/ollama"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	ollamaBaseURL := os.Getenv("OLLAMA_BASE_URL")
	if ollamaBaseURL == "" {
		ollamaBaseURL = "http://localhost:11434"
	}
	client := ollama.NewClient(ollamaBaseURL)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})
	mux.Handle("POST /v1/chat/completions", api.NewChatHandler(client, "qwen2.5-coder:1.5b"))

	handler := api.RequestIDMiddleware(mux)

	addr := ":8080"
	logger.Info("starting ai-gateway", "addr", addr)
	if err := http.ListenAndServe(addr, handler); err != nil {
		logger.Error("server stopped", "error", err)
		os.Exit(1)
	}
}
```

- [ ] **Step 11: Rodar `go build` e checar manualmente**

Run:
```bash
go build ./...
go vet ./...
```
Expected: build e vet sem erro.

- [ ] **Step 12: Commit**

```bash
git add internal/api cmd/gateway
git commit -m "feat(api): add OpenAI-compatible chat handler with SSE and request id middleware

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 6: Logger com chaves proibidas e `trace` (`requests`/`trace_events` em DynamoDB)

**Files:**
- Create: `internal/trace/logger.go`
- Test: `internal/trace/logger_test.go`
- Create: `internal/trace/store.go`
- Test: `internal/trace/store_test.go` (build tag `integration`)
- Create: `infra/modules/dynamodb/main.tf`
- Create: `infra/modules/dynamodb/variables.tf`
- Create: `infra/main.tf`

**Interfaces:**
- Consumes: `core.BackendError` (Task 2), AWS SDK v2 (`aws-sdk-go-v2/config`, `service/dynamodb`).
- Produces: `trace.ForbiddenKeys []string`; `trace.NewLogger(w io.Writer) *slog.Logger`; `trace.EventType` (`Auth, RateLimit, BudgetReserve, Route, BackendAttempt, BackendResult, BudgetSettle, UsagePublish, Error`); `trace.Store` interface `{RecordRequest(ctx, requestID, tenantID, alias string) error; RecordEvent(ctx context.Context, requestID string, seq int, eventType trace.EventType, node string, payload map[string]any) error}`; `trace.NewDynamoStore(client *dynamodb.Client, requestsTable, eventsTable string) trace.Store` — usado por `api`/`resilience`/`budget`/`usage` a partir da Task 9.

- [ ] **Step 1: Escrever o teste do logger com chaves proibidas (falhando)**

Create `internal/trace/logger_test.go`:
```go
package trace

import (
	"bytes"
	"strings"
	"testing"
)

func TestNewLogger_DropsForbiddenKeys(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(&buf)

	logger.Info("request handled",
		"request_id", "req-123",
		"prompt", "this must never be logged",
		"content", "neither must this",
		"messages", []string{"nope"},
		"answer", "nope",
		"payload", map[string]string{"x": "y"},
		"trace", "nope",
		"completion", "nope",
		"status", 200,
	)

	output := buf.String()
	for _, key := range ForbiddenKeys {
		if strings.Contains(output, `"`+key+`"`) {
			t.Fatalf("forbidden key %q leaked into log output: %s", key, output)
		}
	}
	if !strings.Contains(output, `"request_id":"req-123"`) {
		t.Fatalf("expected allowed key request_id to be present: %s", output)
	}
	if !strings.Contains(output, `"status":200`) {
		t.Fatalf("expected allowed key status to be present: %s", output)
	}
}
```

- [ ] **Step 2: Rodar o teste e ver falhar**

Run: `go test ./internal/trace/... -run TestNewLogger -v`
Expected: FAIL — `undefined: NewLogger`, `undefined: ForbiddenKeys`.

- [ ] **Step 3: Implementar `internal/trace/logger.go`**

```go
package trace

import (
	"io"
	"log/slog"
)

// ForbiddenKeys is the tested list of attribute keys the logger must never
// emit, because they could contain prompt/completion content or secrets.
var ForbiddenKeys = []string{"message", "messages", "answer", "trace", "content", "payload", "prompt", "completion"}

func isForbidden(key string) bool {
	for _, k := range ForbiddenKeys {
		if k == key {
			return true
		}
	}
	return false
}

// NewLogger returns a JSON slog.Logger that drops any attribute whose key is
// in ForbiddenKeys, so prompts/completions/secrets never reach stdout logs.
func NewLogger(w io.Writer) *slog.Logger {
	handler := slog.NewJSONHandler(w, &slog.HandlerOptions{
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if isForbidden(a.Key) {
				return slog.Attr{}
			}
			return a
		},
	})
	return slog.New(handler)
}
```

- [ ] **Step 4: Rodar o teste e ver passar**

Run: `go test ./internal/trace/... -run TestNewLogger -v`
Expected: PASS.

- [ ] **Step 5: Escrever o teste de `RecordEvent`/`RecordRequest` (integração, falhando)**

Create `internal/trace/store_test.go`:
```go
//go:build integration

package trace

import (
	"context"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

func newTestDynamoClient(t *testing.T) *dynamodb.Client {
	t.Helper()
	endpoint := os.Getenv("AWS_ENDPOINT_URL")
	if endpoint == "" {
		endpoint = "http://localhost:4566"
	}
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion("us-east-1"))
	if err != nil {
		t.Fatalf("loading AWS config: %v", err)
	}
	return dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) { o.BaseEndpoint = &endpoint })
}

func TestDynamoStore_RecordRequestAndEvent(t *testing.T) {
	client := newTestDynamoClient(t)
	store := NewDynamoStore(client, "requests", "trace_events")
	ctx := context.Background()

	if err := store.RecordRequest(ctx, "req-int-1", "tenant-1", "nuva/fast"); err != nil {
		t.Fatalf("RecordRequest: %v", err)
	}
	if err := store.RecordEvent(ctx, "req-int-1", 1, Auth, "auth", map[string]any{"tenant_id": "tenant-1"}); err != nil {
		t.Fatalf("RecordEvent: %v", err)
	}
}
```

- [ ] **Step 6: Rodar o teste de integração e ver falhar**

Run: `docker compose up -d localstack && go test -tags integration ./internal/trace/... -v`
Expected: FAIL — `undefined: NewDynamoStore`, `undefined: Auth` (as tabelas ainda nem existem no LocalStack).

- [ ] **Step 7: Implementar `internal/trace/store.go`**

```go
package trace

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// EventType classifies a trace_event, matching the pipeline stage that emitted it.
type EventType string

const (
	Auth           EventType = "auth"
	RateLimit      EventType = "ratelimit"
	BudgetReserve  EventType = "budget_reserve"
	Route          EventType = "route"
	BackendAttempt EventType = "backend_attempt"
	BackendResult  EventType = "backend_result"
	BudgetSettle   EventType = "budget_settle"
	UsagePublish   EventType = "usage_publish"
	Error          EventType = "error"
)

// Store persists request metadata and the trace_events sequence for a request.
type Store interface {
	RecordRequest(ctx context.Context, requestID, tenantID, alias string) error
	RecordEvent(ctx context.Context, requestID string, seq int, eventType EventType, node string, payload map[string]any) error
}

type dynamoStore struct {
	client        *dynamodb.Client
	requestsTable string
	eventsTable   string
}

// NewDynamoStore builds a Store backed by the requests and trace_events DynamoDB tables.
func NewDynamoStore(client *dynamodb.Client, requestsTable, eventsTable string) Store {
	return &dynamoStore{client: client, requestsTable: requestsTable, eventsTable: eventsTable}
}

type requestItem struct {
	RequestID string `dynamodbav:"request_id"`
	TenantID  string `dynamodbav:"tenant_id"`
	Alias     string `dynamodbav:"alias"`
	CreatedAt string `dynamodbav:"created_at"`
}

func (s *dynamoStore) RecordRequest(ctx context.Context, requestID, tenantID, alias string) error {
	item, err := attributevalue.MarshalMap(requestItem{
		RequestID: requestID,
		TenantID:  tenantID,
		Alias:     alias,
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		return fmt.Errorf("trace: marshaling request item: %w", err)
	}
	_, err = s.client.PutItem(ctx, &dynamodb.PutItemInput{TableName: &s.requestsTable, Item: item})
	if err != nil {
		return fmt.Errorf("trace: writing request item: %w", err)
	}
	return nil
}

type eventItem struct {
	RequestID string `dynamodbav:"request_id"`
	Seq       int    `dynamodbav:"seq"`
	Type      string `dynamodbav:"type"`
	Node      string `dynamodbav:"node"`
	Payload   string `dynamodbav:"payload_json"`
}

func (s *dynamoStore) RecordEvent(ctx context.Context, requestID string, seq int, eventType EventType, node string, payload map[string]any) error {
	for _, forbidden := range ForbiddenKeys {
		if _, ok := payload[forbidden]; ok {
			return fmt.Errorf("trace: payload contains forbidden key %q", forbidden)
		}
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("trace: marshaling event payload: %w", err)
	}
	item, err := attributevalue.MarshalMap(eventItem{
		RequestID: requestID,
		Seq:       seq,
		Type:      string(eventType),
		Node:      node,
		Payload:   string(payloadJSON),
	})
	if err != nil {
		return fmt.Errorf("trace: marshaling event item: %w", err)
	}
	_, err = s.client.PutItem(ctx, &dynamodb.PutItemInput{TableName: &s.eventsTable, Item: item})
	if err != nil {
		return fmt.Errorf("trace: writing event item: %w", err)
	}
	return nil
}
```

- [ ] **Step 8: Adicionar as dependências AWS SDK**

Run: `go get github.com/aws/aws-sdk-go-v2/config github.com/aws/aws-sdk-go-v2/service/dynamodb github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue`

- [ ] **Step 9: Criar as tabelas no LocalStack via Terraform**

Create `infra/modules/dynamodb/variables.tf`:
```hcl
variable "table_name" {
  type = string
}

variable "hash_key" {
  type = string
}

variable "range_key" {
  type    = string
  default = null
}

variable "ttl_attribute" {
  type    = string
  default = null
}
```

Create `infra/modules/dynamodb/main.tf`:
```hcl
resource "aws_dynamodb_table" "this" {
  name         = var.table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = var.hash_key
  range_key    = var.range_key

  attribute {
    name = var.hash_key
    type = "S"
  }

  dynamic "attribute" {
    for_each = var.range_key == null ? [] : [var.range_key]
    content {
      name = attribute.value
      type = attribute.value == "seq" ? "N" : "S"
    }
  }

  dynamic "ttl" {
    for_each = var.ttl_attribute == null ? [] : [var.ttl_attribute]
    content {
      attribute_name = ttl.value
      enabled        = true
    }
  }
}

output "table_name" {
  value = aws_dynamodb_table.this.name
}
```

Create `infra/main.tf`:
```hcl
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region                      = "us-east-1"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    dynamodb = "http://localhost:4566"
    sqs      = "http://localhost:4566"
  }
}

module "tenants_table" {
  source   = "./modules/dynamodb"
  table_name = "tenants"
  hash_key   = "api_key_hash"
}

module "budgets_table" {
  source     = "./modules/dynamodb"
  table_name = "budgets"
  hash_key   = "tenant_id"
  range_key  = "period"
  ttl_attribute = "ttl"
}

module "requests_table" {
  source     = "./modules/dynamodb"
  table_name = "requests"
  hash_key   = "request_id"
}

module "trace_events_table" {
  source     = "./modules/dynamodb"
  table_name = "trace_events"
  hash_key   = "request_id"
  range_key  = "seq"
}

module "reservations_table" {
  source        = "./modules/dynamodb"
  table_name    = "reservations"
  hash_key      = "reservation_id"
  ttl_attribute = "ttl"
}
```

Nota: `reservations_table` já entra aqui, na Task 6, mesmo só sendo consumida pelo `budget.Store` da Task 8 — assim a tabela existe desde já e nenhuma task posterior precisa voltar a este arquivo para criá-la.

- [ ] **Step 10: Aplicar via `tflocal` e rodar o teste de integração**

Run:
```bash
docker compose up -d localstack
cd infra && tflocal init && tflocal apply -auto-approve && cd ..
go test -tags integration ./internal/trace/... -v
```
Expected: `tflocal apply` cria as 5 tabelas no LocalStack (`tenants`, `budgets`, `requests`, `trace_events`, `reservations`); o teste passa (`RecordRequest` e `RecordEvent` sem erro).

- [ ] **Step 11: Commit**

```bash
git add internal/trace infra
git commit -m "feat(trace): add forbidden-key logger and DynamoDB requests/trace_events store

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 7: `auth` (tenants em DynamoDB) e `ratelimit`

**Files:**
- Create: `internal/auth/tenant.go`
- Test: `internal/auth/tenant_test.go` (build tag `integration`)
- Create: `internal/ratelimit/limiter.go`
- Test: `internal/ratelimit/limiter_test.go`
- Create: `scripts/seed_tenants.go`

**Interfaces:**
- Consumes: `core.Tenant` (Task 2), `dynamodb.Client` (Task 6 pattern).
- Produces: `auth.Store` interface `{ResolveAPIKey(ctx context.Context, apiKey string) (core.Tenant, error)}`; `auth.NewDynamoStore(client *dynamodb.Client, tableName string) auth.Store`; `auth.HashAPIKey(key string) string` (SHA-256 hex, usado tanto pelo resolve quanto pelo script de seed); `ratelimit.Limiter interface{Allow(tenantID string, rpm int) (allowed bool, retryAfterSeconds int)}`; `ratelimit.NewInMemoryLimiter() ratelimit.Limiter` — usados pelo pipeline completo na Task 9.

- [ ] **Step 1: Escrever o teste de `HashAPIKey` e `ResolveAPIKey` (integração, falhando)**

Create `internal/auth/tenant_test.go`:
```go
//go:build integration

package auth

import (
	"context"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

func newTestDynamoClient(t *testing.T) *dynamodb.Client {
	t.Helper()
	endpoint := os.Getenv("AWS_ENDPOINT_URL")
	if endpoint == "" {
		endpoint = "http://localhost:4566"
	}
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion("us-east-1"))
	if err != nil {
		t.Fatalf("loading AWS config: %v", err)
	}
	return dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) { o.BaseEndpoint = &endpoint })
}

func TestResolveAPIKey_FindsSeededTenant(t *testing.T) {
	client := newTestDynamoClient(t)
	ctx := context.Background()

	hash := HashAPIKey("test-key-123")
	item, _ := attributevalue.MarshalMap(map[string]any{
		"api_key_hash":         hash,
		"tenant_id":            "tenant-abc",
		"tier":                 "free",
		"rpm_limit":            10,
		"monthly_token_budget": 100000,
	})
	if _, err := client.PutItem(ctx, &dynamodb.PutItemInput{TableName: strPtr("tenants"), Item: item}); err != nil {
		t.Fatalf("seeding tenant: %v", err)
	}

	store := NewDynamoStore(client, "tenants")
	tenant, err := store.ResolveAPIKey(ctx, "test-key-123")
	if err != nil {
		t.Fatalf("ResolveAPIKey: %v", err)
	}
	if tenant.ID != "tenant-abc" || tenant.Tier != "free" || tenant.RPMLimit != 10 {
		t.Fatalf("unexpected tenant: %+v", tenant)
	}
}

func TestResolveAPIKey_UnknownKeyReturnsError(t *testing.T) {
	client := newTestDynamoClient(t)
	store := NewDynamoStore(client, "tenants")
	if _, err := store.ResolveAPIKey(context.Background(), "does-not-exist"); err == nil {
		t.Fatalf("expected error for unknown API key")
	}
}

func strPtr(s string) *string { return &s }
```

- [ ] **Step 2: Rodar o teste de integração e ver falhar**

Run: `docker compose up -d localstack && go test -tags integration ./internal/auth/... -v`
Expected: FAIL — `undefined: HashAPIKey`, `undefined: NewDynamoStore`.

- [ ] **Step 3: Implementar `internal/auth/tenant.go`**

```go
package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/vicentemoura/ai-gateway/internal/core"
)

// ErrTenantNotFound is returned when no tenant matches the given API key.
var ErrTenantNotFound = errors.New("auth: tenant not found for api key")

// HashAPIKey returns the SHA-256 hex digest of an API key. Keys are never
// stored or logged in plaintext; only this hash is persisted, as the pk of
// the tenants table.
func HashAPIKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

// Store resolves an API key to its Tenant.
type Store interface {
	ResolveAPIKey(ctx context.Context, apiKey string) (core.Tenant, error)
}

type dynamoStore struct {
	client    *dynamodb.Client
	tableName string
}

// NewDynamoStore builds a Store backed by the tenants DynamoDB table (pk api_key_hash).
func NewDynamoStore(client *dynamodb.Client, tableName string) Store {
	return &dynamoStore{client: client, tableName: tableName}
}

type tenantItem struct {
	APIKeyHash         string `dynamodbav:"api_key_hash"`
	TenantID           string `dynamodbav:"tenant_id"`
	Tier               string `dynamodbav:"tier"`
	RPMLimit           int    `dynamodbav:"rpm_limit"`
	MonthlyTokenBudget int    `dynamodbav:"monthly_token_budget"`
}

func (s *dynamoStore) ResolveAPIKey(ctx context.Context, apiKey string) (core.Tenant, error) {
	hash := HashAPIKey(apiKey)
	key, err := attributevalue.MarshalMap(map[string]string{"api_key_hash": hash})
	if err != nil {
		return core.Tenant{}, fmt.Errorf("auth: marshaling key: %w", err)
	}
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{TableName: &s.tableName, Key: key})
	if err != nil {
		return core.Tenant{}, fmt.Errorf("auth: querying tenant: %w", err)
	}
	if out.Item == nil {
		return core.Tenant{}, ErrTenantNotFound
	}
	var item tenantItem
	if err := attributevalue.UnmarshalMap(out.Item, &item); err != nil {
		return core.Tenant{}, fmt.Errorf("auth: unmarshaling tenant: %w", err)
	}
	return core.Tenant{
		ID:                 item.TenantID,
		APIKeyHash:         item.APIKeyHash,
		Tier:               item.Tier,
		RPMLimit:           item.RPMLimit,
		MonthlyTokenBudget: item.MonthlyTokenBudget,
	}, nil
}
```

- [ ] **Step 4: Rodar o teste de integração e ver passar**

Run: `go test -tags integration ./internal/auth/... -v`
Expected: PASS nos dois casos (tabela `tenants` já existe desde a Task 6 Step 10).

- [ ] **Step 5: Escrever o script de seed de tenants**

Create `scripts/seed_tenants.go`:
```go
//go:build ignore

package main

import (
	"context"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/vicentemoura/ai-gateway/internal/auth"
)

// Run with: go run scripts/seed_tenants.go
// Seeds one tenant per tier for local dev/testing against LocalStack.
func main() {
	endpoint := os.Getenv("AWS_ENDPOINT_URL")
	if endpoint == "" {
		endpoint = "http://localhost:4566"
	}
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion("us-east-1"))
	if err != nil {
		log.Fatalf("loading AWS config: %v", err)
	}
	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) { o.BaseEndpoint = &endpoint })

	tenants := []struct {
		apiKey             string
		tenantID           string
		tier               string
		rpmLimit           int
		monthlyTokenBudget int
	}{
		{"dev-free-key", "tenant-free", "free", 10, 50000},
		{"dev-standard-key", "tenant-standard", "standard", 30, 500000},
		{"dev-premium-key", "tenant-premium", "premium", 60, 2000000},
	}

	for _, t := range tenants {
		item, err := attributevalue.MarshalMap(map[string]any{
			"api_key_hash":         auth.HashAPIKey(t.apiKey),
			"tenant_id":            t.tenantID,
			"tier":                 t.tier,
			"rpm_limit":            t.rpmLimit,
			"monthly_token_budget": t.monthlyTokenBudget,
		})
		if err != nil {
			log.Fatalf("marshaling tenant %s: %v", t.tenantID, err)
		}
		if _, err := client.PutItem(context.Background(), &dynamodb.PutItemInput{TableName: strPtr("tenants"), Item: item}); err != nil {
			log.Fatalf("seeding tenant %s: %v", t.tenantID, err)
		}
		log.Printf("seeded tenant %s (api key: %s)", t.tenantID, t.apiKey)
	}
}

func strPtr(s string) *string { return &s }
```

- [ ] **Step 6: Rodar o script de seed manualmente**

Run: `go run scripts/seed_tenants.go`
Expected: 3 linhas de log confirmando `tenant-free`, `tenant-standard`, `tenant-premium` gravados no LocalStack.

- [ ] **Step 7: Escrever o teste de `ratelimit.Limiter` (falhando, sem integração)**

Create `internal/ratelimit/limiter_test.go`:
```go
package ratelimit

import "testing"

func TestInMemoryLimiter_AllowsUpToRPMThenBlocks(t *testing.T) {
	l := NewInMemoryLimiter()

	allowed, _ := l.Allow("tenant-1", 1)
	if !allowed {
		t.Fatalf("expected first request to be allowed")
	}

	allowed, retryAfter := l.Allow("tenant-1", 1)
	if allowed {
		t.Fatalf("expected second immediate request to be blocked with rpm=1")
	}
	if retryAfter <= 0 {
		t.Fatalf("expected a positive Retry-After, got %d", retryAfter)
	}
}

func TestInMemoryLimiter_TracksTenantsIndependently(t *testing.T) {
	l := NewInMemoryLimiter()

	if allowed, _ := l.Allow("tenant-a", 1); !allowed {
		t.Fatalf("expected tenant-a first request allowed")
	}
	if allowed, _ := l.Allow("tenant-b", 1); !allowed {
		t.Fatalf("expected tenant-b first request allowed independently of tenant-a")
	}
}
```

- [ ] **Step 8: Rodar o teste e ver falhar**

Run: `go test ./internal/ratelimit/... -v`
Expected: FAIL — `undefined: NewInMemoryLimiter`.

- [ ] **Step 9: Implementar `internal/ratelimit/limiter.go`**

```go
package ratelimit

import (
	"math"
	"sync"

	"golang.org/x/time/rate"
)

// Limiter decides whether a tenant may make a request right now, given its
// requests-per-minute limit.
type Limiter interface {
	// Allow reports whether the request is permitted. When false, retryAfterSeconds
	// is the integer number of seconds the caller should wait before retrying.
	Allow(tenantID string, rpm int) (allowed bool, retryAfterSeconds int)
}

type inMemoryLimiter struct {
	limiters sync.Map // tenantID -> *rate.Limiter
}

// NewInMemoryLimiter returns a per-instance token-bucket limiter keyed by
// tenant id. This is deliberately per-instance (see Mini-ADR "rate limit em
// memória"): a second gateway replica multiplies the effective limit.
func NewInMemoryLimiter() Limiter {
	return &inMemoryLimiter{}
}

func (l *inMemoryLimiter) Allow(tenantID string, rpm int) (bool, int) {
	limiterAny, _ := l.limiters.LoadOrStore(tenantID, rate.NewLimiter(rate.Limit(float64(rpm)/60.0), rpm))
	limiter := limiterAny.(*rate.Limiter)

	if limiter.Allow() {
		return true, 0
	}
	reservation := limiter.Reserve()
	delay := reservation.Delay()
	reservation.Cancel()
	retryAfter := int(math.Ceil(delay.Seconds()))
	if retryAfter < 1 {
		retryAfter = 1
	}
	return false, retryAfter
}
```

- [ ] **Step 10: Adicionar `golang.org/x/time/rate` e rodar o teste**

Run:
```bash
go get golang.org/x/time/rate
go test ./internal/ratelimit/... -v
```
Expected: PASS nos dois casos.

- [ ] **Step 11: Commit**

```bash
git add internal/auth internal/ratelimit scripts
git commit -m "feat(auth,ratelimit): add DynamoDB tenant resolution and per-tenant rate limiting

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 8: `budget` — reserve/settle atômico com TTL e teste de concorrência

**Files:**
- Create: `internal/budget/budget.go`
- Test: `internal/budget/budget_test.go` (build tag `integration`)

**Interfaces:**
- Consumes: `core.Reservation` (Task 2), `dynamodb.Client`.
- Produces: `budget.ErrBudgetExceeded error`; `budget.Store interface{Reserve(ctx context.Context, tenantID, period string, estimatedTokens, monthlyLimit int) (core.Reservation, error); Settle(ctx context.Context, reservation core.Reservation, realTokens int) error}`; `budget.NewDynamoStore(client *dynamodb.Client, budgetsTable, reservationsTable string) budget.Store`; `budget.EstimateTokens(promptBytes int, maxTokens int) int` (fórmula S1 do spec: `promptBytes/4 + maxTokens`, ou `+1024` se `maxTokens<=0`) — usados pelo pipeline completo na Task 9.

- [ ] **Step 1: Escrever o teste de `EstimateTokens` (falhando, sem integração)**

Create `internal/budget/budget_test.go` (parte não-integration no topo do arquivo, sem build tag — o restante do arquivo com os testes de integração fica atrás da tag, então este teste específico vai num arquivo irmão sem tag):

Create `internal/budget/estimate_test.go`:
```go
package budget

import "testing"

func TestEstimateTokens_UsesPromptBytesAndMaxTokens(t *testing.T) {
	got := EstimateTokens(400, 200)
	want := 400/4 + 200
	if got != want {
		t.Fatalf("EstimateTokens(400, 200) = %d, want %d", got, want)
	}
}

func TestEstimateTokens_DefaultsMaxTokensTo1024WhenAbsent(t *testing.T) {
	got := EstimateTokens(400, 0)
	want := 400/4 + 1024
	if got != want {
		t.Fatalf("EstimateTokens(400, 0) = %d, want %d", got, want)
	}
}
```

- [ ] **Step 2: Rodar o teste e ver falhar**

Run: `go test ./internal/budget/... -run TestEstimateTokens -v`
Expected: FAIL — `undefined: EstimateTokens`.

- [ ] **Step 3: Implementar a parte de estimativa em `internal/budget/budget.go`**

```go
package budget

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"

	"github.com/vicentemoura/ai-gateway/internal/core"
)

// ErrBudgetExceeded is returned by Reserve when the tenant's monthly budget
// would be exceeded by the estimated tokens for this request.
var ErrBudgetExceeded = errors.New("budget: monthly token budget exceeded")

// EstimateTokens implements spec assumption S1: a pessimistic pre-call
// estimate of tokens this request will consume, used by Reserve before the
// backend call happens.
func EstimateTokens(promptBytes int, maxTokens int) int {
	if maxTokens <= 0 {
		maxTokens = 1024
	}
	return promptBytes/4 + maxTokens
}
```

- [ ] **Step 4: Rodar o teste e ver passar**

Run: `go test ./internal/budget/... -run TestEstimateTokens -v`
Expected: PASS.

- [ ] **Step 5: Escrever o teste de `Reserve`/`Settle` e o teste de concorrência (integração, falhando)**

Create `internal/budget/store_test.go`:
```go
//go:build integration

package budget

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/vicentemoura/ai-gateway/internal/core"
)

func newTestDynamoClient(t *testing.T) *dynamodb.Client {
	t.Helper()
	endpoint := os.Getenv("AWS_ENDPOINT_URL")
	if endpoint == "" {
		endpoint = "http://localhost:4566"
	}
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion("us-east-1"))
	if err != nil {
		t.Fatalf("loading AWS config: %v", err)
	}
	return dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) { o.BaseEndpoint = &endpoint })
}

func TestReserveThenSettle_AdjustsUsedByDelta(t *testing.T) {
	client := newTestDynamoClient(t)
	store := NewDynamoStore(client, "budgets", "reservations")
	ctx := context.Background()
	tenantID := "tenant-budget-test-1"

	reservation, err := store.Reserve(ctx, tenantID, "2026-09", 1000, 100000)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if reservation.EstimatedTokens != 1000 {
		t.Fatalf("unexpected reservation: %+v", reservation)
	}

	if err := store.Settle(ctx, reservation, 800); err != nil {
		t.Fatalf("Settle: %v", err)
	}
}

func TestReserve_RejectsWhenOverBudget(t *testing.T) {
	client := newTestDynamoClient(t)
	store := NewDynamoStore(client, "budgets", "reservations")
	ctx := context.Background()
	tenantID := "tenant-budget-test-2"

	if _, err := store.Reserve(ctx, tenantID, "2026-09", 900, 1000); err != nil {
		t.Fatalf("first Reserve should succeed: %v", err)
	}
	if _, err := store.Reserve(ctx, tenantID, "2026-09", 200, 1000); err != ErrBudgetExceeded {
		t.Fatalf("expected ErrBudgetExceeded, got %v", err)
	}
}

func TestReserve_ConcurrentRequestsNeverExceedLimit(t *testing.T) {
	client := newTestDynamoClient(t)
	store := NewDynamoStore(client, "budgets", "reservations")
	ctx := context.Background()
	tenantID := "tenant-budget-race"
	const limit = 1000
	const perRequest = 30
	const goroutines = 50 // 50 * 30 = 1500 > limit: some must be rejected

	var accepted int64
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := store.Reserve(ctx, tenantID, "2026-09", perRequest, limit); err == nil {
				atomic.AddInt64(&accepted, 1)
			}
		}()
	}
	wg.Wait()

	if accepted*perRequest > limit {
		t.Fatalf("accepted %d reservations of %d tokens each (%d total), exceeds limit %d", accepted, perRequest, accepted*perRequest, limit)
	}
	if accepted == 0 {
		t.Fatalf("expected at least one reservation to be accepted")
	}
}

var _ core.Reservation // keep core imported for the Reservation type used above
```

- [ ] **Step 6: Rodar o teste de integração e ver falhar**

Run: `docker compose up -d localstack && go test -tags integration -race ./internal/budget/... -v`
Expected: FAIL — `undefined: NewDynamoStore`.

- [ ] **Step 7: Implementar `Reserve`/`Settle` em `internal/budget/budget.go`**

Append to `internal/budget/budget.go`:
```go
// Store reserves estimated tokens atomically before a backend call, and
// settles the reservation with the real usage afterward.
type Store interface {
	Reserve(ctx context.Context, tenantID, period string, estimatedTokens, monthlyLimit int) (core.Reservation, error)
	Settle(ctx context.Context, reservation core.Reservation, realTokens int) error
}

type dynamoStore struct {
	client            *dynamodb.Client
	budgetsTable      string
	reservationsTable string
}

// NewDynamoStore builds a Store backed by the budgets table (pk tenant_id, sk
// period) and a reservations table used to track orphaned reservations (TTL 15m).
func NewDynamoStore(client *dynamodb.Client, budgetsTable, reservationsTable string) Store {
	return &dynamoStore{client: client, budgetsTable: budgetsTable, reservationsTable: reservationsTable}
}

func (s *dynamoStore) Reserve(ctx context.Context, tenantID, period string, estimatedTokens, monthlyLimit int) (core.Reservation, error) {
	key, err := attributevalue.MarshalMap(map[string]string{"tenant_id": tenantID, "period": period})
	if err != nil {
		return core.Reservation{}, fmt.Errorf("budget: marshaling key: %w", err)
	}
	exprValues, err := attributevalue.MarshalMap(map[string]any{
		":est":   estimatedTokens,
		":limit": monthlyLimit,
	})
	if err != nil {
		return core.Reservation{}, fmt.Errorf("budget: marshaling expression values: %w", err)
	}

	_, err = s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 &s.budgetsTable,
		Key:                       key,
		UpdateExpression:          strPtr("ADD used :est"),
		ConditionExpression:       strPtr("attribute_not_exists(used) OR used + :est <= :limit"),
		ExpressionAttributeValues: exprValues,
	})
	if err != nil {
		var condFailed *types.ConditionalCheckFailedException
		if errors.As(err, &condFailed) {
			return core.Reservation{}, ErrBudgetExceeded
		}
		return core.Reservation{}, fmt.Errorf("budget: reserving tokens: %w", err)
	}

	reservationID := uuid.NewString()
	ttl := time.Now().Add(15 * time.Minute).Unix()
	reservationItem, err := attributevalue.MarshalMap(map[string]any{
		"reservation_id":   reservationID,
		"tenant_id":        tenantID,
		"period":           period,
		"estimated_tokens": estimatedTokens,
		"ttl":              ttl,
	})
	if err != nil {
		return core.Reservation{}, fmt.Errorf("budget: marshaling reservation item: %w", err)
	}
	if _, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{TableName: &s.reservationsTable, Item: reservationItem}); err != nil {
		return core.Reservation{}, fmt.Errorf("budget: recording reservation: %w", err)
	}

	return core.Reservation{ID: reservationID, TenantID: tenantID, Period: period, EstimatedTokens: estimatedTokens}, nil
}

func (s *dynamoStore) Settle(ctx context.Context, reservation core.Reservation, realTokens int) error {
	delta := realTokens - reservation.EstimatedTokens

	key, err := attributevalue.MarshalMap(map[string]string{"tenant_id": reservation.TenantID, "period": reservation.Period})
	if err != nil {
		return fmt.Errorf("budget: marshaling settle key: %w", err)
	}
	exprValues, err := attributevalue.MarshalMap(map[string]any{":delta": delta})
	if err != nil {
		return fmt.Errorf("budget: marshaling settle expression values: %w", err)
	}
	if _, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 &s.budgetsTable,
		Key:                       key,
		UpdateExpression:          strPtr("ADD used :delta"),
		ExpressionAttributeValues: exprValues,
	}); err != nil {
		return fmt.Errorf("budget: settling tokens: %w", err)
	}

	reservationKey, err := attributevalue.MarshalMap(map[string]string{"reservation_id": reservation.ID})
	if err != nil {
		return fmt.Errorf("budget: marshaling reservation key: %w", err)
	}
	if _, err := s.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{TableName: &s.reservationsTable, Key: reservationKey}); err != nil {
		return fmt.Errorf("budget: deleting reservation: %w", err)
	}
	return nil
}

func strPtr(s string) *string { return &s }
```

- [ ] **Step 8: Rodar o teste de integração e ver passar**

Run: `go test -tags integration -race ./internal/budget/... -v`
Expected: PASS nos três casos, incluindo o de concorrência com `-race` limpo (nenhuma data race, e `accepted*perRequest <= limit`). A tabela `reservations` (pk `reservation_id`, TTL em `ttl`) já foi criada na Task 6 Step 9 (`infra/main.tf`, módulo `reservations_table`) — nenhum passo de infraestrutura adicional é necessário aqui.

- [ ] **Step 9: Commit**

```bash
git add internal/budget infra
git commit -m "feat(budget): add atomic reserve/settle with orphan-reservation TTL

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 9: Backend OpenRouter, `router` (alias+tier → cascata) e `resilience` (retry/fallback)

**Files:**
- Create: `internal/backend/openrouter/client.go`
- Create: `internal/backend/openrouter/testdata/chat_success.json`
- Create: `internal/backend/openrouter/testdata/chat_error.json`
- Test: `internal/backend/openrouter/client_test.go`
- Create: `internal/router/router.go`
- Test: `internal/router/router_test.go`
- Create: `internal/resilience/resilience.go`
- Test: `internal/resilience/resilience_test.go`

**Interfaces:**
- Consumes: `config.Routing`/`AliasTiers`/`BackendTargetConfig` (Task 1), `core.BackendTarget`/`ChatRequest`/`ChatResponse`/`BackendError`/`ErrorClass` (Task 2).
- Produces: `openrouter.NewClient(baseURL, apiKey string) *openrouter.Client` com `Chat`/`ChatStream` na mesma assinatura do `ollama.Client` (Task 3/4); `router.Resolve(routing *config.Routing, alias, tier string) ([]core.BackendTarget, error)` (retorna `ErrUnknownModel` se alias não existir, valida que `provider/model` direto só é aceito com `tier == "premium"`); `resilience.Backend interface{Chat(ctx, model string, req core.ChatRequest) (core.ChatResponse, error)}` e `resilience.FullBackend interface{resilience.Backend; core.StreamBackend}` (adaptador comum satisfeito por `ollama.Client`, `openrouter.Client` e, a partir da Task 10, `gemini.Client`); `resilience.Call(ctx context.Context, backends map[string]resilience.Backend, targets []core.BackendTarget, req core.ChatRequest) (core.ChatResponse, core.BackendTarget, []resilience.Attempt, error)` retornando `ErrAllBackendsFailed`; `resilience.CallStream(ctx context.Context, targets []core.BackendTarget, resolve func(core.BackendTarget) (core.StreamBackend, error), req core.ChatRequest, onChunk func(core.ChatChunk) error) (core.Usage, []resilience.Attempt, error)` retornando `ErrStreamFailedAfterFirstByte` quando o erro ocorre depois do primeiro byte já entregue — usados pelo `api` a partir da Task 11 para substituir o target fixo da Task 5, tanto no caminho não-streaming quanto no streaming.

- [ ] **Step 1: Fixtures do OpenRouter**

Create `internal/backend/openrouter/testdata/chat_success.json`:
```json
{
  "id": "gen-123",
  "model": "openai/gpt-4o-mini",
  "choices": [
    {
      "index": 0,
      "message": {"role": "assistant", "content": "Olá! Como posso ajudar?"},
      "finish_reason": "stop"
    }
  ],
  "usage": {"prompt_tokens": 15, "completion_tokens": 9, "total_tokens": 24}
}
```

Create `internal/backend/openrouter/testdata/chat_error.json`:
```json
{
  "error": {"message": "Rate limit exceeded", "code": 429}
}
```

- [ ] **Step 2: Escrever o teste do client OpenRouter (falhando)**

Create `internal/backend/openrouter/client_test.go`:
```go
package openrouter

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/vicentemoura/ai-gateway/internal/core"
)

func TestChat_Success_IncludesUsage(t *testing.T) {
	fixture, err := os.ReadFile("testdata/chat_success.json")
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}
	var capturedBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer test-key" {
			t.Fatalf("expected Authorization header with test-key, got %q", r.Header.Get("Authorization"))
		}
		json.NewDecoder(r.Body).Decode(&capturedBody)
		w.Header().Set("Content-Type", "application/json")
		w.Write(fixture)
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "test-key")
	req := core.ChatRequest{RequestID: "req-1", Messages: []core.Message{{Role: "user", Content: "oi"}}}

	resp, err := client.Chat(context.Background(), "openai/gpt-4o-mini", req)
	if err != nil {
		t.Fatalf("Chat returned error: %v", err)
	}
	if resp.Message.Content != "Olá! Como posso ajudar?" {
		t.Fatalf("unexpected content: %q", resp.Message.Content)
	}
	if resp.Usage.PromptTokens != 15 || resp.Usage.CompletionTokens != 9 {
		t.Fatalf("unexpected usage: %+v", resp.Usage)
	}
}

func TestChat_429_IsTransientError(t *testing.T) {
	fixture, err := os.ReadFile("testdata/chat_error.json")
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		w.Write(fixture)
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "test-key")
	req := core.ChatRequest{RequestID: "req-2", Messages: []core.Message{{Role: "user", Content: "oi"}}}

	_, err = client.Chat(context.Background(), "openai/gpt-4o-mini", req)
	be, ok := err.(*core.BackendError)
	if !ok {
		t.Fatalf("expected *core.BackendError, got %T", err)
	}
	if be.Class != core.Transient {
		t.Fatalf("expected Transient for 429, got %v", be.Class)
	}
}
```

- [ ] **Step 3: Rodar o teste e ver falhar**

Run: `go test ./internal/backend/openrouter/... -v`
Expected: FAIL — `undefined: NewClient`.

- [ ] **Step 4: Implementar `internal/backend/openrouter/client.go`**

```go
package openrouter

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/vicentemoura/ai-gateway/internal/core"
)

// Client talks to OpenRouter's OpenAI-compatible /v1/chat/completions endpoint.
type Client struct {
	baseURL string
	apiKey  string
	http    *http.Client
}

func NewClient(baseURL, apiKey string) *Client {
	return &Client{baseURL: baseURL, apiKey: apiKey, http: &http.Client{}}
}

type streamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

type requestBody struct {
	Model         string         `json:"model"`
	Messages      []core.Message `json:"messages"`
	Stream        bool           `json:"stream"`
	StreamOptions *streamOptions `json:"stream_options,omitempty"`
}

type responseBody struct {
	Choices []struct {
		Message      core.Message `json:"message"`
		Delta        core.Message `json:"delta"`
		FinishReason string       `json:"finish_reason"`
	} `json:"choices"`
	Usage *struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
	} `json:"usage"`
	Error *struct {
		Message string `json:"message"`
		Code    int    `json:"code"`
	} `json:"error"`
}

func classify(status int) core.ErrorClass {
	if status >= 500 || status == http.StatusTooManyRequests {
		return core.Transient
	}
	return core.Permanent
}

// Chat performs a single non-streaming call to OpenRouter.
func (c *Client) Chat(ctx context.Context, model string, req core.ChatRequest) (core.ChatResponse, error) {
	body, err := json.Marshal(requestBody{Model: model, Messages: req.Messages, Stream: false})
	if err != nil {
		return core.ChatResponse{}, &core.BackendError{Class: core.Permanent, Err: fmt.Errorf("openrouter: marshaling request: %w", err)}
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return core.ChatResponse{}, &core.BackendError{Class: core.Permanent, Err: fmt.Errorf("openrouter: building request: %w", err)}
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return core.ChatResponse{}, &core.BackendError{Class: core.Transient, Err: fmt.Errorf("openrouter: request failed: %w", err)}
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return core.ChatResponse{}, &core.BackendError{Class: core.Transient, Err: fmt.Errorf("openrouter: reading response: %w", err)}
	}
	if resp.StatusCode >= 400 {
		return core.ChatResponse{}, &core.BackendError{Class: classify(resp.StatusCode), Status: resp.StatusCode, Err: fmt.Errorf("openrouter: %s", string(raw))}
	}

	var parsed responseBody
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return core.ChatResponse{}, &core.BackendError{Class: core.Transient, Err: fmt.Errorf("openrouter: parsing response: %w", err)}
	}
	if len(parsed.Choices) == 0 {
		return core.ChatResponse{}, &core.BackendError{Class: core.Transient, Err: fmt.Errorf("openrouter: no choices in response")}
	}

	usage := core.Usage{}
	if parsed.Usage != nil {
		usage = core.Usage{PromptTokens: parsed.Usage.PromptTokens, CompletionTokens: parsed.Usage.CompletionTokens}
	}
	return core.ChatResponse{
		Message:      parsed.Choices[0].Message,
		FinishReason: parsed.Choices[0].FinishReason,
		Usage:        usage,
	}, nil
}

// ChatStream performs a streaming call. OpenRouter requires
// stream_options.include_usage=true to get a final usage chunk (spec S1).
func (c *Client) ChatStream(ctx context.Context, model string, req core.ChatRequest) (<-chan core.ChatChunk, <-chan error) {
	chunks := make(chan core.ChatChunk)
	errs := make(chan error, 1)

	go func() {
		defer close(chunks)
		defer close(errs)

		body, err := json.Marshal(requestBody{Model: model, Messages: req.Messages, Stream: true, StreamOptions: &streamOptions{IncludeUsage: true}})
		if err != nil {
			errs <- &core.BackendError{Class: core.Permanent, Err: fmt.Errorf("openrouter: marshaling request: %w", err)}
			return
		}
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/chat/completions", bytes.NewReader(body))
		if err != nil {
			errs <- &core.BackendError{Class: core.Permanent, Err: fmt.Errorf("openrouter: building request: %w", err)}
			return
		}
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("Authorization", "Bearer "+c.apiKey)

		resp, err := c.http.Do(httpReq)
		if err != nil {
			errs <- &core.BackendError{Class: core.Transient, Err: fmt.Errorf("openrouter: request failed: %w", err)}
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 400 {
			raw, _ := io.ReadAll(resp.Body)
			errs <- &core.BackendError{Class: classify(resp.StatusCode), Status: resp.StatusCode, Err: fmt.Errorf("openrouter: %s", string(raw))}
			return
		}

		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" || !strings.HasPrefix(line, "data: ") {
				continue
			}
			data := strings.TrimPrefix(line, "data: ")
			if data == "[DONE]" {
				return
			}
			var parsed responseBody
			if err := json.Unmarshal([]byte(data), &parsed); err != nil {
				errs <- &core.BackendError{Class: core.Transient, Err: fmt.Errorf("openrouter: parsing stream chunk: %w", err)}
				return
			}
			if parsed.Usage != nil {
				chunks <- core.ChatChunk{Usage: &core.Usage{PromptTokens: parsed.Usage.PromptTokens, CompletionTokens: parsed.Usage.CompletionTokens}}
				continue
			}
			if len(parsed.Choices) > 0 {
				select {
				case chunks <- core.ChatChunk{Delta: parsed.Choices[0].Delta.Content, FinishReason: parsed.Choices[0].FinishReason}:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return chunks, errs
}
```

- [ ] **Step 5: Rodar o teste e ver passar**

Run: `go test ./internal/backend/openrouter/... -v`
Expected: PASS nos dois casos.

- [ ] **Step 6: Gravar fixture real com curl (requer `OPENROUTER_API_KEY` com teto de gasto configurado, conforme spec)**

Run:
```bash
curl -s https://openrouter.ai/api/v1/chat/completions \
  -H "Authorization: Bearer $OPENROUTER_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"model":"openai/gpt-4o-mini","messages":[{"role":"user","content":"oi"}]}' \
  | tee internal/backend/openrouter/testdata/chat_success.json
```
Expected: resposta real gravada; ajustar a asserção de conteúdo exato do Step 2 para checar `resp.Message.Content != ""` (texto real não é determinístico), como feito na Task 3.

- [ ] **Step 7: Escrever o teste de `router.Resolve` (falhando)**

Create `internal/router/router_test.go`:
```go
package router

import (
	"testing"

	"github.com/vicentemoura/ai-gateway/internal/config"
)

func testRouting() *config.Routing {
	return &config.Routing{
		Aliases: map[string]config.AliasTiers{
			"nuva/fast": {Tiers: map[string][]config.BackendTargetConfig{
				"free":     {{Provider: "ollama", Model: "qwen2.5-coder:1.5b"}},
				"standard": {{Provider: "ollama", Model: "qwen2.5-coder:1.5b"}, {Provider: "openrouter", Model: "openai/gpt-4o-mini"}},
				"premium":  {{Provider: "openrouter", Model: "openai/gpt-4o-mini"}},
			}},
		},
	}
}

func TestResolve_KnownAliasReturnsOrderedCascade(t *testing.T) {
	targets, err := Resolve(testRouting(), "nuva/fast", "standard")
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}
	if len(targets) != 2 || targets[0].Provider != "ollama" || targets[1].Provider != "openrouter" {
		t.Fatalf("unexpected targets: %+v", targets)
	}
}

func TestResolve_UnknownAliasReturnsErrUnknownModel(t *testing.T) {
	if _, err := Resolve(testRouting(), "nuva/does-not-exist", "standard"); err != ErrUnknownModel {
		t.Fatalf("expected ErrUnknownModel, got %v", err)
	}
}

func TestResolve_DirectBackendNameOnlyAllowedInPremium(t *testing.T) {
	targets, err := Resolve(testRouting(), "ollama/qwen2.5-coder:1.5b", "premium")
	if err != nil {
		t.Fatalf("expected direct backend name to resolve in premium tier: %v", err)
	}
	if len(targets) != 1 || targets[0].Provider != "ollama" || targets[0].Model != "qwen2.5-coder:1.5b" {
		t.Fatalf("unexpected targets: %+v", targets)
	}

	if _, err := Resolve(testRouting(), "ollama/qwen2.5-coder:1.5b", "free"); err != ErrUnknownModel {
		t.Fatalf("expected direct backend name to be rejected outside premium, got %v", err)
	}
}
```

- [ ] **Step 8: Rodar o teste e ver falhar**

Run: `go test ./internal/router/... -v`
Expected: FAIL — `undefined: Resolve`, `undefined: ErrUnknownModel`.

- [ ] **Step 9: Implementar `internal/router/router.go`**

```go
package router

import (
	"errors"
	"strings"

	"github.com/vicentemoura/ai-gateway/internal/config"
	"github.com/vicentemoura/ai-gateway/internal/core"
)

// ErrUnknownModel is returned when the requested alias (or direct backend
// name) cannot be resolved to a cascade for the given tier.
var ErrUnknownModel = errors.New("router: unknown model or alias")

// Resolve converts a "model" field from the request (a gateway alias like
// "nuva/fast", or a direct "provider/model" name) plus the caller's tier
// into an ordered cascade of backend targets to try in order.
func Resolve(routing *config.Routing, model, tier string) ([]core.BackendTarget, error) {
	if alias, ok := routing.Aliases[model]; ok {
		cascade, ok := alias.Tiers[tier]
		if !ok || len(cascade) == 0 {
			return nil, ErrUnknownModel
		}
		return toTargets(cascade), nil
	}

	// Direct backend name ("provider/model"), only allowed for tier "premium".
	if tier != "premium" {
		return nil, ErrUnknownModel
	}
	provider, backendModel, ok := strings.Cut(model, "/")
	if !ok {
		return nil, ErrUnknownModel
	}
	if _, known := routing.Providers[provider]; !known {
		return nil, ErrUnknownModel
	}
	return []core.BackendTarget{{Provider: provider, Model: backendModel}}, nil
}

func toTargets(cascade []config.BackendTargetConfig) []core.BackendTarget {
	targets := make([]core.BackendTarget, 0, len(cascade))
	for _, c := range cascade {
		targets = append(targets, core.BackendTarget{Provider: c.Provider, Model: c.Model})
	}
	return targets
}
```

- [ ] **Step 10: Rodar o teste e ver passar**

Run: `go test ./internal/router/... -v`
Expected: PASS nos três casos.

- [ ] **Step 11: Escrever o teste de `resilience.Call` (falhando)**

Create `internal/resilience/resilience_test.go`:
```go
package resilience

import (
	"context"
	"errors"
	"testing"

	"github.com/vicentemoura/ai-gateway/internal/core"
)

type fakeBackend struct {
	calls   int
	results []struct {
		resp core.ChatResponse
		err  error
	}
}

func (f *fakeBackend) Chat(ctx context.Context, model string, req core.ChatRequest) (core.ChatResponse, error) {
	i := f.calls
	f.calls++
	if i >= len(f.results) {
		i = len(f.results) - 1
	}
	return f.results[i].resp, f.results[i].err
}

func TestCall_SucceedsOnFirstBackend(t *testing.T) {
	ollama := &fakeBackend{results: []struct {
		resp core.ChatResponse
		err  error
	}{{resp: core.ChatResponse{Message: core.Message{Content: "ok"}}}}}

	backends := map[string]Backend{"ollama": ollama}
	targets := []core.BackendTarget{{Provider: "ollama", Model: "qwen2.5-coder:1.5b"}}

	resp, target, attempts, err := Call(context.Background(), backends, targets, core.ChatRequest{})
	if err != nil {
		t.Fatalf("Call returned error: %v", err)
	}
	if resp.Message.Content != "ok" || target.Provider != "ollama" {
		t.Fatalf("unexpected result: resp=%+v target=%+v", resp, target)
	}
	if len(attempts) != 1 {
		t.Fatalf("expected 1 attempt, got %d", len(attempts))
	}
	if ollama.calls != 1 {
		t.Fatalf("expected exactly 1 call on success, got %d", ollama.calls)
	}
}

func TestCall_RetriesTransientThenFallsBackToNextTarget(t *testing.T) {
	transientErr := &core.BackendError{Class: core.Transient, Status: 503, Err: errors.New("unavailable")}
	ollama := &fakeBackend{results: []struct {
		resp core.ChatResponse
		err  error
	}{{err: transientErr}, {err: transientErr}, {err: transientErr}}}
	openrouter := &fakeBackend{results: []struct {
		resp core.ChatResponse
		err  error
	}{{resp: core.ChatResponse{Message: core.Message{Content: "fallback ok"}}}}}

	backends := map[string]Backend{"ollama": ollama, "openrouter": openrouter}
	targets := []core.BackendTarget{{Provider: "ollama", Model: "m1"}, {Provider: "openrouter", Model: "m2"}}

	resp, target, attempts, err := Call(context.Background(), backends, targets, core.ChatRequest{})
	if err != nil {
		t.Fatalf("Call returned error: %v", err)
	}
	if resp.Message.Content != "fallback ok" || target.Provider != "openrouter" {
		t.Fatalf("unexpected result: resp=%+v target=%+v", resp, target)
	}
	if ollama.calls != 2 {
		t.Fatalf("expected exactly 2 attempts on ollama (cap of 2), got %d", ollama.calls)
	}
	if len(attempts) != 3 {
		t.Fatalf("expected 3 attempts recorded (2 ollama + 1 openrouter), got %d: %+v", len(attempts), attempts)
	}
}

func TestCall_PermanentErrorSkipsRetryGoesToNextTarget(t *testing.T) {
	permanentErr := &core.BackendError{Class: core.Permanent, Status: 400, Err: errors.New("bad request")}
	ollama := &fakeBackend{results: []struct {
		resp core.ChatResponse
		err  error
	}{{err: permanentErr}}}
	openrouter := &fakeBackend{results: []struct {
		resp core.ChatResponse
		err  error
	}{{resp: core.ChatResponse{Message: core.Message{Content: "fallback ok"}}}}}

	backends := map[string]Backend{"ollama": ollama, "openrouter": openrouter}
	targets := []core.BackendTarget{{Provider: "ollama", Model: "m1"}, {Provider: "openrouter", Model: "m2"}}

	_, target, _, err := Call(context.Background(), backends, targets, core.ChatRequest{})
	if err != nil {
		t.Fatalf("Call returned error: %v", err)
	}
	if target.Provider != "openrouter" {
		t.Fatalf("expected fallback to openrouter, got %+v", target)
	}
	if ollama.calls != 1 {
		t.Fatalf("expected exactly 1 attempt on permanent error (no retry), got %d", ollama.calls)
	}
}

func TestCall_AllBackendsFail_ReturnsErrAllBackendsFailed(t *testing.T) {
	transientErr := &core.BackendError{Class: core.Transient, Status: 503, Err: errors.New("unavailable")}
	failing := &fakeBackend{results: []struct {
		resp core.ChatResponse
		err  error
	}{{err: transientErr}, {err: transientErr}}}

	backends := map[string]Backend{"ollama": failing}
	targets := []core.BackendTarget{{Provider: "ollama", Model: "m1"}}

	_, _, attempts, err := Call(context.Background(), backends, targets, core.ChatRequest{})
	if !errors.Is(err, ErrAllBackendsFailed) {
		t.Fatalf("expected ErrAllBackendsFailed, got %v", err)
	}
	if len(attempts) != 2 {
		t.Fatalf("expected 2 attempts recorded, got %d", len(attempts))
	}
}
```

- [ ] **Step 12: Rodar o teste e ver falhar**

Run: `go test ./internal/resilience/... -v`
Expected: FAIL — `undefined: Backend`, `undefined: Call`, `undefined: ErrAllBackendsFailed`.

- [ ] **Step 13: Implementar `internal/resilience/resilience.go`**

```go
package resilience

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/vicentemoura/ai-gateway/internal/core"
)

// ErrAllBackendsFailed is returned when every target in the cascade failed.
var ErrAllBackendsFailed = errors.New("resilience: all backends failed")

// ErrStreamFailedAfterFirstByte is returned by CallStream when a backend
// fails after at least one chunk with a non-empty Delta was already
// delivered to the caller's onChunk. Past that point, no retry or fallback
// is attempted: the client has already received partial output, so mixing
// in a second backend's output would be incoherent.
var ErrStreamFailedAfterFirstByte = errors.New("resilience: backend stream failed after first byte was sent")

// maxAttemptsPerBackend caps retries on the same backend before moving to
// the next target in the cascade (spec: teto de 2 tentativas por backend).
const maxAttemptsPerBackend = 2

// Backend is the common interface satisfied by ollama.Client and openrouter.Client
// (and gemini.Client from Task 10) for the purposes of resilience.Call.
type Backend interface {
	Chat(ctx context.Context, model string, req core.ChatRequest) (core.ChatResponse, error)
}

// FullBackend is satisfied by every backend adapter (ollama.Client,
// openrouter.Client, gemini.Client): it can be called both non-streaming
// (Backend) and streaming (core.StreamBackend). Pipeline.Backends (Task 11)
// is keyed by provider name and holds values of this type.
type FullBackend interface {
	Backend
	core.StreamBackend
}

// Attempt records one call made while resolving a request, for the
// all_backends_failed error body and for usage_event.attempts.
type Attempt struct {
	Provider string
	Model    string
	Status   int
	Err      error
}

// Call tries each target in order. For a given target, it retries up to
// maxAttemptsPerBackend times with exponential backoff + jitter, but only
// when the error is Transient; a Permanent error moves immediately to the
// next target. If every target fails, it returns ErrAllBackendsFailed
// alongside every Attempt made, for the 502 response body.
func Call(ctx context.Context, backends map[string]Backend, targets []core.BackendTarget, req core.ChatRequest) (core.ChatResponse, core.BackendTarget, []Attempt, error) {
	var attempts []Attempt

	for _, target := range targets {
		backend, ok := backends[target.Provider]
		if !ok {
			attempts = append(attempts, Attempt{Provider: target.Provider, Model: target.Model, Err: errors.New("resilience: no backend registered for provider")})
			continue
		}

		for attempt := 0; attempt < maxAttemptsPerBackend; attempt++ {
			resp, err := backend.Chat(ctx, target.Model, req)
			if err == nil {
				attempts = append(attempts, Attempt{Provider: target.Provider, Model: target.Model, Status: 200})
				return resp, target, attempts, nil
			}

			var be *core.BackendError
			status := 0
			if errors.As(err, &be) {
				status = be.Status
			}
			attempts = append(attempts, Attempt{Provider: target.Provider, Model: target.Model, Status: status, Err: err})

			if !errors.As(err, &be) || !be.IsTransient() {
				break // permanent error: stop retrying this backend, move to next target
			}
			if attempt == maxAttemptsPerBackend-1 {
				break // exhausted retries on this backend, move to next target
			}
			backoff(attempt)
		}
	}

	return core.ChatResponse{}, core.BackendTarget{}, attempts, ErrAllBackendsFailed
}

func backoff(attempt int) {
	base := 200 * time.Millisecond * time.Duration(1<<attempt)
	jitter := time.Duration(rand.Int63n(int64(base / 2)))
	time.Sleep(base + jitter)
}

// CallStream tries each target's ChatStream in order. Before the first
// chunk with a non-empty Delta is delivered to onChunk, it follows the same
// retry/fallback policy as Call: up to maxAttemptsPerBackend retries on
// Transient errors, then falls back to the next target; a Permanent error
// moves straight to the next target. Once the first byte has been sent to
// onChunk, no more retry or fallback happens — any later error stops the
// stream immediately with ErrStreamFailedAfterFirstByte, wrapping the
// underlying error, alongside whatever core.Usage the stream reported
// before failing (zero value if it failed before any usage chunk arrived).
func CallStream(ctx context.Context, targets []core.BackendTarget, resolve func(core.BackendTarget) (core.StreamBackend, error), req core.ChatRequest, onChunk func(core.ChatChunk) error) (core.Usage, []Attempt, error) {
	var attempts []Attempt

	for _, target := range targets {
		backend, err := resolve(target)
		if err != nil {
			attempts = append(attempts, Attempt{Provider: target.Provider, Model: target.Model, Err: err})
			continue
		}

		for attempt := 0; attempt < maxAttemptsPerBackend; attempt++ {
			usage, firstByteSent, streamErr := runStream(ctx, backend, target.Model, req, onChunk)

			if streamErr == nil {
				attempts = append(attempts, Attempt{Provider: target.Provider, Model: target.Model, Status: 200})
				return usage, attempts, nil
			}

			var be *core.BackendError
			status := 0
			if errors.As(streamErr, &be) {
				status = be.Status
			}
			attempts = append(attempts, Attempt{Provider: target.Provider, Model: target.Model, Status: status, Err: streamErr})

			if firstByteSent {
				return usage, attempts, fmt.Errorf("%w: %v", ErrStreamFailedAfterFirstByte, streamErr)
			}

			if !errors.As(streamErr, &be) || !be.IsTransient() {
				break // permanent error before first byte: move to next target
			}
			if attempt == maxAttemptsPerBackend-1 {
				break // exhausted retries on this backend before first byte: move to next target
			}
			backoff(attempt)
		}
	}

	return core.Usage{}, attempts, ErrAllBackendsFailed
}

// runStream drains one ChatStream call, forwarding every chunk with a
// non-empty Delta (and every chunk carrying Usage, even delta-less ones) to
// onChunk. It reports whether at least one Delta chunk was sent before any
// error, and the Usage from the last chunk that carried one (Gemini bundles
// Delta and Usage in the same final chunk; Ollama/OpenRouter send a
// separate delta-less usage chunk — both shapes are handled the same way
// here, since the Usage capture is unconditional and independent of the
// Delta-forwarding branch).
func runStream(ctx context.Context, backend core.StreamBackend, model string, req core.ChatRequest, onChunk func(core.ChatChunk) error) (core.Usage, bool, error) {
	chunks, errs := backend.ChatStream(ctx, model, req)
	var usage core.Usage
	firstByteSent := false

	for chunk := range chunks {
		if chunk.Delta != "" {
			if err := onChunk(chunk); err != nil {
				return usage, firstByteSent, err
			}
			firstByteSent = true
		} else if chunk.Usage != nil {
			if err := onChunk(chunk); err != nil {
				return usage, firstByteSent, err
			}
		}
		if chunk.Usage != nil {
			usage = *chunk.Usage
		}
	}
	if err := <-errs; err != nil {
		return usage, firstByteSent, err
	}
	return usage, firstByteSent, nil
}
```

- [ ] **Step 14: Rodar o teste e ver passar**

Run: `go test ./internal/resilience/... -v`
Expected: PASS nos quatro casos de `Call` (nota: o teste de retry gasta ~200ms+400ms de backoff real; se ficar lento demais em CI, reduzir `maxAttemptsPerBackend`'s backoff apenas em teste não é permitido pelo plano — aceitar a lentidão de alguns segundos, é um teste unitário, não de carga).

- [ ] **Step 15: Escrever o teste de `CallStream` (falhando)**

Append to `internal/resilience/resilience_test.go`:
```go
type fakeStreamBackend struct {
	chunks []core.ChatChunk
	err    error
}

func (f *fakeStreamBackend) ChatStream(ctx context.Context, model string, req core.ChatRequest) (<-chan core.ChatChunk, <-chan error) {
	chunks := make(chan core.ChatChunk, len(f.chunks))
	errs := make(chan error, 1)
	for _, c := range f.chunks {
		chunks <- c
	}
	close(chunks)
	errs <- f.err
	close(errs)
	return chunks, errs
}

func TestCallStream_SucceedsOnFirstTarget(t *testing.T) {
	backend := &fakeStreamBackend{chunks: []core.ChatChunk{
		{Delta: "ol"},
		{Delta: "a", Usage: &core.Usage{PromptTokens: 5, CompletionTokens: 2}},
	}}
	resolve := func(target core.BackendTarget) (core.StreamBackend, error) { return backend, nil }
	targets := []core.BackendTarget{{Provider: "ollama", Model: "m1"}}

	var received []string
	onChunk := func(c core.ChatChunk) error {
		received = append(received, c.Delta)
		return nil
	}

	usage, attempts, err := CallStream(context.Background(), targets, resolve, core.ChatRequest{}, onChunk)
	if err != nil {
		t.Fatalf("CallStream returned error: %v", err)
	}
	if len(received) != 2 || received[0] != "ol" || received[1] != "a" {
		t.Fatalf("unexpected received deltas: %+v", received)
	}
	if usage.PromptTokens != 5 || usage.CompletionTokens != 2 {
		t.Fatalf("unexpected usage: %+v", usage)
	}
	if len(attempts) != 1 || attempts[0].Status != 200 {
		t.Fatalf("unexpected attempts: %+v", attempts)
	}
}
```

- [ ] **Step 16: Rodar o teste e ver falhar**

Run: `go test ./internal/resilience/... -run TestCallStream -v`
Expected: FAIL — `undefined: CallStream`.

- [ ] **Step 17: Rodar o teste e ver passar**

Run: `go test ./internal/resilience/... -v`
Expected: PASS em todos os casos, incluindo os quatro de `Call` e o novo de `CallStream`. (`CallStream` já foi implementada no Step 13 junto com `Call`; este step só confirma que a implementação cobre também o caminho de sucesso em streaming — os dois cenários de fallback/erro-após-primeiro-byte descritos no design ficam cobertos pelos testes de nível HTTP na Task 11, que exercitam `CallStream` através do pipeline completo.)

- [ ] **Step 18: Commit**

```bash
git add internal/backend/openrouter internal/router internal/resilience
git commit -m "feat(router,resilience,backend/openrouter): add alias routing, retry/fallback (incl. streaming) and OpenRouter adapter

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 10: Backend Gemini

**Files:**
- Create: `internal/backend/gemini/client.go`
- Create: `internal/backend/gemini/testdata/generate_success.json`
- Create: `internal/backend/gemini/testdata/generate_stream.ndjson`
- Test: `internal/backend/gemini/client_test.go`

**Interfaces:**
- Consumes: `core.ChatRequest`/`Message`/`ChatResponse`/`ChatChunk`/`Usage`/`BackendError` (Task 2).
- Produces: `gemini.NewClient(baseURL, apiKey string) *gemini.Client` com `Chat(ctx, model string, req core.ChatRequest) (core.ChatResponse, error)` e `ChatStream(ctx, model string, req core.ChatRequest) (<-chan core.ChatChunk, <-chan error)`, mesma forma dos outros backends — satisfaz `resilience.Backend` (Task 9) e é registrado no pipeline completo na Task 11.

- [ ] **Step 1: Fixtures do Gemini**

Create `internal/backend/gemini/testdata/generate_success.json`:
```json
{
  "candidates": [
    {
      "content": {
        "role": "model",
        "parts": [{"text": "Olá! Como posso ajudar?"}]
      },
      "finishReason": "STOP"
    }
  ],
  "usageMetadata": {
    "promptTokenCount": 10,
    "candidatesTokenCount": 7,
    "totalTokenCount": 17
  }
}
```

Create `internal/backend/gemini/testdata/generate_stream.ndjson` (SSE-shaped, uma linha `data: {...}` por chunk, como `streamGenerateContent?alt=sse` devolve):
```
data: {"candidates":[{"content":{"role":"model","parts":[{"text":"Ol"}]}}]}

data: {"candidates":[{"content":{"role":"model","parts":[{"text":"á!"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":7,"totalTokenCount":17}}

```

- [ ] **Step 2: Escrever o teste do client Gemini (falhando)**

Create `internal/backend/gemini/client_test.go`:
```go
package gemini

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/vicentemoura/ai-gateway/internal/core"
)

func TestChat_Success_ParsesCandidateAndUsageMetadata(t *testing.T) {
	fixture, err := os.ReadFile("testdata/generate_success.json")
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "generateContent") {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		if r.URL.Query().Get("key") != "test-key" {
			t.Fatalf("expected key=test-key query param, got %q", r.URL.RawQuery)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(fixture)
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "test-key")
	req := core.ChatRequest{RequestID: "req-1", Messages: []core.Message{{Role: "user", Content: "oi"}}}

	resp, err := client.Chat(context.Background(), "gemini-1.5-pro", req)
	if err != nil {
		t.Fatalf("Chat returned error: %v", err)
	}
	if resp.Message.Content != "Olá! Como posso ajudar?" {
		t.Fatalf("unexpected content: %q", resp.Message.Content)
	}
	if resp.Usage.PromptTokens != 10 || resp.Usage.CompletionTokens != 7 {
		t.Fatalf("unexpected usage: %+v", resp.Usage)
	}
	if resp.FinishReason != "STOP" {
		t.Fatalf("unexpected finish reason: %q", resp.FinishReason)
	}
}

func TestChatStream_EmitsDeltasThenFinalUsage(t *testing.T) {
	fixture, err := os.ReadFile("testdata/generate_stream.ndjson")
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "streamGenerateContent") {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write(fixture)
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "test-key")
	req := core.ChatRequest{RequestID: "req-2", Messages: []core.Message{{Role: "user", Content: "oi"}}}

	chunks, errs := client.ChatStream(context.Background(), "gemini-1.5-pro", req)

	var deltas []string
	var sawUsage bool
	for chunk := range chunks {
		if chunk.Usage != nil {
			sawUsage = true
			if chunk.Usage.PromptTokens != 10 || chunk.Usage.CompletionTokens != 7 {
				t.Fatalf("unexpected final usage: %+v", chunk.Usage)
			}
			continue
		}
		deltas = append(deltas, chunk.Delta)
	}
	if err := <-errs; err != nil {
		t.Fatalf("unexpected stream error: %v", err)
	}
	if len(deltas) != 2 || deltas[0] != "Ol" || deltas[1] != "á!" {
		t.Fatalf("unexpected deltas: %+v", deltas)
	}
	if !sawUsage {
		t.Fatalf("expected a final chunk carrying usageMetadata")
	}
}
```

- [ ] **Step 3: Rodar o teste e ver falhar**

Run: `go test ./internal/backend/gemini/... -v`
Expected: FAIL — `undefined: NewClient`.

- [ ] **Step 4: Implementar `internal/backend/gemini/client.go`**

```go
package gemini

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/vicentemoura/ai-gateway/internal/core"
)

// Client talks to Gemini's generateContent / streamGenerateContent endpoints.
type Client struct {
	baseURL string
	apiKey  string
	http    *http.Client
}

func NewClient(baseURL, apiKey string) *Client {
	return &Client{baseURL: baseURL, apiKey: apiKey, http: &http.Client{}}
}

type part struct {
	Text string `json:"text"`
}

type content struct {
	Role  string `json:"role,omitempty"`
	Parts []part `json:"parts"`
}

type requestBody struct {
	Contents []content `json:"contents"`
}

type candidate struct {
	Content      content `json:"content"`
	FinishReason string  `json:"finishReason"`
}

type usageMetadata struct {
	PromptTokenCount     int `json:"promptTokenCount"`
	CandidatesTokenCount int `json:"candidatesTokenCount"`
}

type responseBody struct {
	Candidates    []candidate    `json:"candidates"`
	UsageMetadata *usageMetadata `json:"usageMetadata"`
}

func toContents(messages []core.Message) []content {
	contents := make([]content, 0, len(messages))
	for _, m := range messages {
		role := m.Role
		if role == "assistant" {
			role = "model" // Gemini uses "model", not "assistant"
		}
		contents = append(contents, content{Role: role, Parts: []part{{Text: m.Content}}})
	}
	return contents
}

func classify(status int) core.ErrorClass {
	if status >= 500 || status == http.StatusTooManyRequests {
		return core.Transient
	}
	return core.Permanent
}

// Chat performs a single non-streaming call to Gemini's generateContent.
func (c *Client) Chat(ctx context.Context, model string, req core.ChatRequest) (core.ChatResponse, error) {
	body, err := json.Marshal(requestBody{Contents: toContents(req.Messages)})
	if err != nil {
		return core.ChatResponse{}, &core.BackendError{Class: core.Permanent, Err: fmt.Errorf("gemini: marshaling request: %w", err)}
	}
	url := fmt.Sprintf("%s/v1beta/models/%s:generateContent?key=%s", c.baseURL, model, c.apiKey)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return core.ChatResponse{}, &core.BackendError{Class: core.Permanent, Err: fmt.Errorf("gemini: building request: %w", err)}
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return core.ChatResponse{}, &core.BackendError{Class: core.Transient, Err: fmt.Errorf("gemini: request failed: %w", err)}
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return core.ChatResponse{}, &core.BackendError{Class: core.Transient, Err: fmt.Errorf("gemini: reading response: %w", err)}
	}
	if resp.StatusCode >= 400 {
		return core.ChatResponse{}, &core.BackendError{Class: classify(resp.StatusCode), Status: resp.StatusCode, Err: fmt.Errorf("gemini: %s", string(raw))}
	}

	var parsed responseBody
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return core.ChatResponse{}, &core.BackendError{Class: core.Transient, Err: fmt.Errorf("gemini: parsing response: %w", err)}
	}
	if len(parsed.Candidates) == 0 || len(parsed.Candidates[0].Content.Parts) == 0 {
		return core.ChatResponse{}, &core.BackendError{Class: core.Transient, Err: fmt.Errorf("gemini: no candidates in response")}
	}

	usage := core.Usage{}
	if parsed.UsageMetadata != nil {
		usage = core.Usage{PromptTokens: parsed.UsageMetadata.PromptTokenCount, CompletionTokens: parsed.UsageMetadata.CandidatesTokenCount}
	}
	return core.ChatResponse{
		Message:      core.Message{Role: "assistant", Content: parsed.Candidates[0].Content.Parts[0].Text},
		FinishReason: parsed.Candidates[0].FinishReason,
		Usage:        usage,
	}, nil
}

// ChatStream performs a streaming call to Gemini's streamGenerateContent?alt=sse.
func (c *Client) ChatStream(ctx context.Context, model string, req core.ChatRequest) (<-chan core.ChatChunk, <-chan error) {
	chunks := make(chan core.ChatChunk)
	errs := make(chan error, 1)

	go func() {
		defer close(chunks)
		defer close(errs)

		body, err := json.Marshal(requestBody{Contents: toContents(req.Messages)})
		if err != nil {
			errs <- &core.BackendError{Class: core.Permanent, Err: fmt.Errorf("gemini: marshaling request: %w", err)}
			return
		}
		url := fmt.Sprintf("%s/v1beta/models/%s:streamGenerateContent?alt=sse&key=%s", c.baseURL, model, c.apiKey)
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			errs <- &core.BackendError{Class: core.Permanent, Err: fmt.Errorf("gemini: building request: %w", err)}
			return
		}
		httpReq.Header.Set("Content-Type", "application/json")

		resp, err := c.http.Do(httpReq)
		if err != nil {
			errs <- &core.BackendError{Class: core.Transient, Err: fmt.Errorf("gemini: request failed: %w", err)}
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 400 {
			raw, _ := io.ReadAll(resp.Body)
			errs <- &core.BackendError{Class: classify(resp.StatusCode), Status: resp.StatusCode, Err: fmt.Errorf("gemini: %s", string(raw))}
			return
		}

		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" || !strings.HasPrefix(line, "data: ") {
				continue
			}
			data := strings.TrimPrefix(line, "data: ")
			var parsed responseBody
			if err := json.Unmarshal([]byte(data), &parsed); err != nil {
				errs <- &core.BackendError{Class: core.Transient, Err: fmt.Errorf("gemini: parsing stream chunk: %w", err)}
				return
			}
			if len(parsed.Candidates) == 0 {
				continue
			}
			cand := parsed.Candidates[0]
			var text string
			if len(cand.Content.Parts) > 0 {
				text = cand.Content.Parts[0].Text
			}
			var usage *core.Usage
			if parsed.UsageMetadata != nil {
				usage = &core.Usage{PromptTokens: parsed.UsageMetadata.PromptTokenCount, CompletionTokens: parsed.UsageMetadata.CandidatesTokenCount}
			}
			select {
			case chunks <- core.ChatChunk{Delta: text, FinishReason: cand.FinishReason, Usage: usage}:
			case <-ctx.Done():
				return
			}
		}
	}()

	return chunks, errs
}
```

Este adaptador é o único dos três cujo chunk final carrega `Delta` (o último pedaço de texto) **e** `Usage` juntos — Ollama e OpenRouter emitem o usage num chunk separado, sem delta. `resilience.CallStream` (Task 9) e `resilience.runStream` tratam isso de forma genérica: todo chunk com `Delta != ""` é sempre encaminhado ao `onChunk` primeiro, e separadamente, sempre que `chunk.Usage != nil`, o valor é capturado como o usage final da chamada — nessa ordem, um chunk do Gemini com os dois campos preenchidos passa por ambos os caminhos sem perder o texto nem o usage.

- [ ] **Step 5: Rodar o teste e ver passar**

Run: `go test ./internal/backend/gemini/... -v`
Expected: PASS nos dois casos.

- [ ] **Step 6: Gravar fixtures reais com curl (requer `GEMINI_API_KEY`)**

Run:
```bash
curl -s "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-pro:generateContent?key=$GEMINI_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"contents":[{"role":"user","parts":[{"text":"oi"}]}]}' \
  | tee internal/backend/gemini/testdata/generate_success.json

curl -s "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-pro:streamGenerateContent?alt=sse&key=$GEMINI_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"contents":[{"role":"user","parts":[{"text":"oi"}]}]}' \
  | tee internal/backend/gemini/testdata/generate_stream.ndjson
```
Expected: fixtures reais gravadas; ajustar as asserções de conteúdo exato do Step 2 para checar não-vazio em vez de texto literal (modelo real não é determinístico), como nas Tasks 3 e 9.

- [ ] **Step 7: Commit**

```bash
git add internal/backend/gemini
git commit -m "feat(backend/gemini): add generateContent and SSE streaming adapter

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 11: Ligar o pipeline completo (auth → ratelimit → budget → router → resilience → trace) em `/v1/chat/completions`

**Files:**
- Create: `internal/api/pipeline.go`
- Test: `internal/api/pipeline_test.go`
- Modify: `internal/api/chat.go`
- Modify: `cmd/gateway/main.go`

**Interfaces:**
- Consumes: `auth.Store` (Task 7), `ratelimit.Limiter` (Task 7), `budget.Store`/`EstimateTokens` (Task 8), `router.Resolve` (Task 9), `resilience.Backend`/`FullBackend`/`Call`/`CallStream`/`Attempt`/`ErrAllBackendsFailed`/`ErrStreamFailedAfterFirstByte` (Task 9), `core.StreamBackend` (Task 2), `trace.Store`/`EventType` (Task 6), `ollama.Client`/`openrouter.Client`/`gemini.Client` (Tasks 3/4/9/10).
- Produces: `api.Pipeline{Auth auth.Store; RateLimit ratelimit.Limiter; Budget budget.Store; Routing *config.Routing; Backends map[string]resilience.FullBackend; Trace trace.Store}`; `api.NewPipelineChatHandler(p *api.Pipeline) http.Handler` — substitui `NewChatHandler` da Task 5 como handler real de `/v1/chat/completions`, com cascata/retry/fallback completos tanto no caminho não-streaming (`resilience.Call`) quanto no streaming (`resilience.CallStream`); `api.WriteError` ganha o `429` com `Retry-After` e o `502 all_backends_failed` com `attempts` no corpo; o streaming comunica falha em banda, via evento SSE `backend_stream_failed`, já que o header HTTP `200` e `Content-Type: text/event-stream` já foram enviados antes de qualquer erro poder ocorrer.

- [ ] **Step 1: Escrever o teste do pipeline completo (falhando)**

Create `internal/api/pipeline_test.go`:
```go
package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/vicentemoura/ai-gateway/internal/budget"
	"github.com/vicentemoura/ai-gateway/internal/config"
	"github.com/vicentemoura/ai-gateway/internal/core"
	"github.com/vicentemoura/ai-gateway/internal/ratelimit"
	"github.com/vicentemoura/ai-gateway/internal/resilience"
	"github.com/vicentemoura/ai-gateway/internal/trace"
)

type fakeAuth struct{ tenant core.Tenant }

func (f *fakeAuth) ResolveAPIKey(ctx context.Context, apiKey string) (core.Tenant, error) {
	if apiKey != "valid-key" {
		return core.Tenant{}, context.DeadlineExceeded // any non-nil error signals "not found" for this fake
	}
	return f.tenant, nil
}

type fakeLimiter struct{ allow bool }

func (f *fakeLimiter) Allow(tenantID string, rpm int) (bool, int) {
	if f.allow {
		return true, 0
	}
	return false, 5
}

type fakeBudget struct{ reserveErr error }

func (f *fakeBudget) Reserve(ctx context.Context, tenantID, period string, estimatedTokens, monthlyLimit int) (core.Reservation, error) {
	if f.reserveErr != nil {
		return core.Reservation{}, f.reserveErr
	}
	return core.Reservation{ID: "r1", TenantID: tenantID, Period: period, EstimatedTokens: estimatedTokens}, nil
}
func (f *fakeBudget) Settle(ctx context.Context, reservation core.Reservation, realTokens int) error { return nil }

type fakeTrace struct{}

func (f *fakeTrace) RecordRequest(ctx context.Context, requestID, tenantID, alias string) error { return nil }
func (f *fakeTrace) RecordEvent(ctx context.Context, requestID string, seq int, eventType trace.EventType, node string, payload map[string]any) error {
	return nil
}

type fakeChatBackend struct{ content string }

func (f *fakeChatBackend) Chat(ctx context.Context, model string, req core.ChatRequest) (core.ChatResponse, error) {
	return core.ChatResponse{Message: core.Message{Role: "assistant", Content: f.content}, Usage: core.Usage{PromptTokens: 5, CompletionTokens: 3}, FinishReason: "stop"}, nil
}

func (f *fakeChatBackend) ChatStream(ctx context.Context, model string, req core.ChatRequest) (<-chan core.ChatChunk, <-chan error) {
	chunks := make(chan core.ChatChunk)
	errs := make(chan error, 1)
	close(chunks)
	close(errs)
	return chunks, errs
}

// fakeStreamBackend is a resilience.FullBackend whose ChatStream plays back
// a fixed sequence of chunks and then a fixed terminal error (nil for a
// clean end of stream). Chat is not exercised by the streaming tests.
type fakeStreamBackend struct {
	streamChunks []core.ChatChunk
	streamErr    error
}

func (f *fakeStreamBackend) Chat(ctx context.Context, model string, req core.ChatRequest) (core.ChatResponse, error) {
	return core.ChatResponse{}, errors.New("fakeStreamBackend.Chat is not used by streaming tests")
}

func (f *fakeStreamBackend) ChatStream(ctx context.Context, model string, req core.ChatRequest) (<-chan core.ChatChunk, <-chan error) {
	chunks := make(chan core.ChatChunk, len(f.streamChunks))
	errs := make(chan error, 1)
	for _, c := range f.streamChunks {
		chunks <- c
	}
	close(chunks)
	errs <- f.streamErr
	close(errs)
	return chunks, errs
}

func testRouting() *config.Routing {
	return &config.Routing{Aliases: map[string]config.AliasTiers{
		"nuva/fast": {Tiers: map[string][]config.BackendTargetConfig{"free": {{Provider: "ollama", Model: "qwen2.5-coder:1.5b"}}}},
	}}
}

// testRoutingWithFallback gives tier "standard" a two-target cascade, used
// by the streaming fallback tests below.
func testRoutingWithFallback() *config.Routing {
	return &config.Routing{Aliases: map[string]config.AliasTiers{
		"nuva/fast": {Tiers: map[string][]config.BackendTargetConfig{
			"standard": {{Provider: "ollama", Model: "m1"}, {Provider: "openrouter", Model: "m2"}},
		}},
	}}
}

func TestPipeline_HappyPath_ReturnsCompletionAndSettlesBudget(t *testing.T) {
	p := &Pipeline{
		Auth:      &fakeAuth{tenant: core.Tenant{ID: "tenant-1", Tier: "free", RPMLimit: 10, MonthlyTokenBudget: 100000}},
		RateLimit: &fakeLimiter{allow: true},
		Budget:    &fakeBudget{},
		Routing:   testRouting(),
		Backends:  map[string]resilience.FullBackend{"ollama": &fakeChatBackend{content: "ok"}},
		Trace:     &fakeTrace{},
	}
	handler := RequestIDMiddleware(NewPipelineChatHandler(p))

	reqBody := `{"model":"nuva/fast","messages":[{"role":"user","content":"oi"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(reqBody))
	req.Header.Set("Authorization", "Bearer valid-key")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestPipeline_MissingAuth_Returns401(t *testing.T) {
	p := &Pipeline{Auth: &fakeAuth{}, RateLimit: &fakeLimiter{allow: true}, Budget: &fakeBudget{}, Routing: testRouting(), Trace: &fakeTrace{}}
	handler := RequestIDMiddleware(NewPipelineChatHandler(p))

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"nuva/fast","messages":[]}`))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestPipeline_RateLimited_Returns429WithRetryAfter(t *testing.T) {
	p := &Pipeline{
		Auth:      &fakeAuth{tenant: core.Tenant{ID: "tenant-1", Tier: "free"}},
		RateLimit: &fakeLimiter{allow: false},
		Budget:    &fakeBudget{},
		Routing:   testRouting(),
		Trace:     &fakeTrace{},
	}
	handler := RequestIDMiddleware(NewPipelineChatHandler(p))

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"nuva/fast","messages":[{"role":"user","content":"oi"}]}`))
	req.Header.Set("Authorization", "Bearer valid-key")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", rec.Code)
	}
	if rec.Header().Get("Retry-After") != "5" {
		t.Fatalf("expected Retry-After: 5, got %q", rec.Header().Get("Retry-After"))
	}
}

func TestPipeline_BudgetExceeded_Returns402(t *testing.T) {
	p := &Pipeline{
		Auth:      &fakeAuth{tenant: core.Tenant{ID: "tenant-1", Tier: "free"}},
		RateLimit: &fakeLimiter{allow: true},
		Budget:    &fakeBudget{reserveErr: budget.ErrBudgetExceeded},
		Routing:   testRouting(),
		Trace:     &fakeTrace{},
	}
	handler := RequestIDMiddleware(NewPipelineChatHandler(p))

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"nuva/fast","messages":[{"role":"user","content":"oi"}]}`))
	req.Header.Set("Authorization", "Bearer valid-key")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusPaymentRequired {
		t.Fatalf("expected 402, got %d", rec.Code)
	}
}

func TestPipeline_UnknownAlias_Returns400(t *testing.T) {
	p := &Pipeline{
		Auth:      &fakeAuth{tenant: core.Tenant{ID: "tenant-1", Tier: "free"}},
		RateLimit: &fakeLimiter{allow: true},
		Budget:    &fakeBudget{},
		Routing:   testRouting(),
		Trace:     &fakeTrace{},
	}
	handler := RequestIDMiddleware(NewPipelineChatHandler(p))

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"nuva/does-not-exist","messages":[{"role":"user","content":"oi"}]}`))
	req.Header.Set("Authorization", "Bearer valid-key")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
	var body map[string]map[string]string
	json.Unmarshal(rec.Body.Bytes(), &body)
	if body["error"]["type"] != "unknown_model" {
		t.Fatalf("unexpected error type: %+v", body)
	}
}

func TestPipeline_AllBackendsFailed_Returns502WithAttempts(t *testing.T) {
	p := &Pipeline{
		Auth:      &fakeAuth{tenant: core.Tenant{ID: "tenant-1", Tier: "free"}},
		RateLimit: &fakeLimiter{allow: true},
		Budget:    &fakeBudget{},
		Routing:   testRouting(),
		Backends:  map[string]resilience.FullBackend{}, // no backend registered for "ollama"
		Trace:     &fakeTrace{},
	}
	handler := RequestIDMiddleware(NewPipelineChatHandler(p))

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"nuva/fast","messages":[{"role":"user","content":"oi"}]}`))
	req.Header.Set("Authorization", "Bearer valid-key")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("expected 502, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestPipeline_Streaming_FallsBackBeforeFirstByte(t *testing.T) {
	transientErr := &core.BackendError{Class: core.Transient, Status: 503, Err: errors.New("unavailable")}
	first := &fakeStreamBackend{streamErr: transientErr}
	second := &fakeStreamBackend{streamChunks: []core.ChatChunk{
		{Delta: "ok"},
		{Usage: &core.Usage{PromptTokens: 4, CompletionTokens: 2}},
	}}

	p := &Pipeline{
		Auth:      &fakeAuth{tenant: core.Tenant{ID: "tenant-1", Tier: "standard"}},
		RateLimit: &fakeLimiter{allow: true},
		Budget:    &fakeBudget{},
		Routing:   testRoutingWithFallback(),
		Backends:  map[string]resilience.FullBackend{"ollama": first, "openrouter": second},
		Trace:     &fakeTrace{},
	}
	handler := RequestIDMiddleware(NewPipelineChatHandler(p))

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"nuva/fast","stream":true,"messages":[{"role":"user","content":"oi"}]}`))
	req.Header.Set("Authorization", "Bearer valid-key")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"content":"ok"`) {
		t.Fatalf("expected the fallback backend's delta in the stream, got: %s", body)
	}
	if !strings.Contains(body, "data: [DONE]") {
		t.Fatalf("expected the DONE sentinel, got: %s", body)
	}
	if strings.Contains(body, "backend_stream_failed") {
		t.Fatalf("did not expect a stream error event when the fallback target succeeds, got: %s", body)
	}
}

func TestPipeline_Streaming_StopsWithoutFallbackAfterFirstByte(t *testing.T) {
	failsAfterTwoChunks := &fakeStreamBackend{
		streamChunks: []core.ChatChunk{{Delta: "he"}, {Delta: "llo"}},
		streamErr:    &core.BackendError{Class: core.Transient, Status: 503, Err: errors.New("connection dropped")},
	}
	neverCalled := &fakeStreamBackend{streamChunks: []core.ChatChunk{{Delta: "should not appear"}}}

	p := &Pipeline{
		Auth:      &fakeAuth{tenant: core.Tenant{ID: "tenant-1", Tier: "standard"}},
		RateLimit: &fakeLimiter{allow: true},
		Budget:    &fakeBudget{},
		Routing:   testRoutingWithFallback(),
		Backends:  map[string]resilience.FullBackend{"ollama": failsAfterTwoChunks, "openrouter": neverCalled},
		Trace:     &fakeTrace{},
	}
	handler := RequestIDMiddleware(NewPipelineChatHandler(p))

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"nuva/fast","stream":true,"messages":[{"role":"user","content":"oi"}]}`))
	req.Header.Set("Authorization", "Bearer valid-key")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	body := rec.Body.String()
	if !strings.Contains(body, `"content":"he"`) || !strings.Contains(body, `"content":"llo"`) {
		t.Fatalf("expected both deltas from the failing backend before the error, got: %s", body)
	}
	if !strings.Contains(body, "backend_stream_failed") {
		t.Fatalf("expected a backend_stream_failed error event, got: %s", body)
	}
	if strings.Contains(body, "should not appear") {
		t.Fatalf("expected no fallback to the second backend after the first byte was sent, got: %s", body)
	}
	if !strings.Contains(body, "data: [DONE]") {
		t.Fatalf("expected the DONE sentinel even after the stream error, got: %s", body)
	}
}
```

- [ ] **Step 2: Rodar o teste e ver falhar**

Run: `go test ./internal/api/... -run TestPipeline -v`
Expected: FAIL — `undefined: Pipeline`, `undefined: NewPipelineChatHandler`.

- [ ] **Step 3: Implementar `internal/api/pipeline.go`**

```go
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/vicentemoura/ai-gateway/internal/auth"
	"github.com/vicentemoura/ai-gateway/internal/budget"
	"github.com/vicentemoura/ai-gateway/internal/config"
	"github.com/vicentemoura/ai-gateway/internal/core"
	"github.com/vicentemoura/ai-gateway/internal/ratelimit"
	"github.com/vicentemoura/ai-gateway/internal/resilience"
	"github.com/vicentemoura/ai-gateway/internal/router"
	"github.com/vicentemoura/ai-gateway/internal/trace"
)

// Pipeline wires every module the real /v1/chat/completions handler needs.
// This replaces the Task 5 NewChatHandler, which only called a fixed Ollama model.
type Pipeline struct {
	Auth      auth.Store
	RateLimit ratelimit.Limiter
	Budget    budget.Store
	Routing   *config.Routing
	Backends  map[string]resilience.FullBackend
	Trace     trace.Store
}

// NewPipelineChatHandler is the real handler for POST /v1/chat/completions:
// auth -> ratelimit -> budget.Reserve -> router.Resolve -> resilience.Call/CallStream -> budget.Settle,
// recording a trace_event at each stage. Both the non-streaming and the
// streaming path get the full retry/fallback cascade; see
// resilience.CallStream for the streaming-specific rule about stopping
// fallback once the first byte has reached the client.
func NewPipelineChatHandler(p *Pipeline) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		requestID := RequestIDFromContext(ctx)
		seq := 0
		emit := func(eventType trace.EventType, node string, payload map[string]any) {
			seq++
			p.Trace.RecordEvent(ctx, requestID, seq, eventType, node, payload)
		}

		apiKey := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if apiKey == "" {
			WriteError(w, requestID, http.StatusUnauthorized, "unauthorized", "missing Authorization header")
			return
		}
		tenant, err := p.Auth.ResolveAPIKey(ctx, apiKey)
		if err != nil {
			emit(trace.Auth, "auth", map[string]any{"status": "denied"})
			WriteError(w, requestID, http.StatusUnauthorized, "unauthorized", "invalid API key")
			return
		}
		emit(trace.Auth, "auth", map[string]any{"tenant_id": tenant.ID, "tier": tenant.Tier})

		if allowed, retryAfter := p.RateLimit.Allow(tenant.ID, tenant.RPMLimit); !allowed {
			emit(trace.RateLimit, "ratelimit", map[string]any{"tenant_id": tenant.ID, "retry_after": retryAfter})
			w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
			WriteError(w, requestID, http.StatusTooManyRequests, "rate_limited", "too many requests")
			return
		}
		emit(trace.RateLimit, "ratelimit", map[string]any{"tenant_id": tenant.ID, "status": "allowed"})

		var body chatCompletionRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			WriteError(w, requestID, http.StatusBadRequest, "invalid_request", "malformed JSON body")
			return
		}
		messages := make([]core.Message, 0, len(body.Messages))
		promptBytes := 0
		for _, m := range body.Messages {
			messages = append(messages, core.Message{Role: m.Role, Content: m.Content})
			promptBytes += len(m.Content)
		}
		chatReq := core.ChatRequest{RequestID: requestID, TenantID: tenant.ID, Alias: body.Model, Tier: tenant.Tier, Messages: messages, Stream: body.Stream}

		period := time.Now().UTC().Format("2006-01")
		estimated := budget.EstimateTokens(promptBytes, 0)
		reservation, err := p.Budget.Reserve(ctx, tenant.ID, period, estimated, tenant.MonthlyTokenBudget)
		if err != nil {
			emit(trace.BudgetReserve, "budget", map[string]any{"tenant_id": tenant.ID, "status": "denied"})
			WriteError(w, requestID, http.StatusPaymentRequired, "budget_exceeded", "monthly token budget exceeded")
			return
		}
		emit(trace.BudgetReserve, "budget", map[string]any{"tenant_id": tenant.ID, "reservation_id": reservation.ID, "estimated_tokens": estimated})

		p.Trace.RecordRequest(ctx, requestID, tenant.ID, body.Model)

		targets, err := router.Resolve(p.Routing, body.Model, tenant.Tier)
		if err != nil {
			emit(trace.Route, "router", map[string]any{"status": "unknown_model"})
			WriteError(w, requestID, http.StatusBadRequest, "unknown_model", "unknown alias or backend model for this tier")
			return
		}
		emit(trace.Route, "router", map[string]any{"targets": len(targets)})

		if body.Stream {
			serveStreamPipeline(w, r, p, targets, chatReq, requestID, reservation, emit)
			return
		}

		resp, target, attempts, err := resilience.Call(ctx, toBackends(p.Backends), targets, chatReq)
		for _, a := range attempts {
			status := "ok"
			if a.Err != nil {
				status = "error"
			}
			emit(trace.BackendAttempt, "resilience", map[string]any{"provider": a.Provider, "model": a.Model, "status": status})
		}
		if err != nil {
			p.Budget.Settle(ctx, reservation, 0)
			emit(trace.Error, "resilience", map[string]any{"reason": "all_backends_failed"})
			writeAllBackendsFailed(w, requestID, attempts)
			return
		}
		emit(trace.BackendResult, "resilience", map[string]any{"provider": target.Provider, "model": target.Model})

		realTokens := resp.Usage.PromptTokens + resp.Usage.CompletionTokens
		p.Budget.Settle(ctx, reservation, realTokens)
		emit(trace.BudgetSettle, "budget", map[string]any{"tenant_id": tenant.ID, "real_tokens": realTokens})

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id":      requestID,
			"object":  "chat.completion",
			"choices": []map[string]any{{"index": 0, "message": map[string]string{"role": "assistant", "content": resp.Message.Content}, "finish_reason": resp.FinishReason}},
			"usage":   map[string]int{"prompt_tokens": resp.Usage.PromptTokens, "completion_tokens": resp.Usage.CompletionTokens},
		})
	})
}

// toBackends narrows a map[string]resilience.FullBackend to the
// map[string]resilience.Backend shape resilience.Call expects. Every
// FullBackend value already satisfies Backend, so this is a plain copy.
func toBackends(full map[string]resilience.FullBackend) map[string]resilience.Backend {
	out := make(map[string]resilience.Backend, len(full))
	for k, v := range full {
		out[k] = v
	}
	return out
}

// serveStreamPipeline runs the streaming cascade through resilience.CallStream,
// forwarding every chunk to the client as SSE as it arrives. It settles the
// budget with whatever core.Usage the stream reported (partial if it failed
// mid-stream, full on success) and records trace_events for every attempt
// and for a terminal failure. Because the SSE headers (200 + text/event-stream)
// are already flushed before any backend is called, every failure — including
// "all backends failed before any byte was sent" — is communicated in-band as
// an SSE error event followed by the [DONE] sentinel, never as an HTTP error status.
func serveStreamPipeline(w http.ResponseWriter, r *http.Request, p *Pipeline, targets []core.BackendTarget, chatReq core.ChatRequest, requestID string, reservation core.Reservation, emit func(trace.EventType, string, map[string]any)) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		WriteError(w, requestID, http.StatusInternalServerError, "streaming_unsupported", "response writer does not support flushing")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)

	resolve := func(target core.BackendTarget) (core.StreamBackend, error) {
		backend, ok := p.Backends[target.Provider]
		if !ok {
			return nil, fmt.Errorf("resilience: no backend registered for provider %q", target.Provider)
		}
		return backend, nil
	}

	onChunk := func(chunk core.ChatChunk) error {
		select {
		case <-r.Context().Done():
			return r.Context().Err()
		default:
		}
		payload, _ := json.Marshal(map[string]any{
			"id":      requestID,
			"object":  "chat.completion.chunk",
			"choices": []map[string]any{{"index": 0, "delta": map[string]string{"content": chunk.Delta}, "finish_reason": chunk.FinishReason}},
		})
		fmt.Fprintf(w, "data: %s\n\n", payload)
		flusher.Flush()
		return nil
	}

	usage, attempts, streamErr := resilience.CallStream(r.Context(), targets, resolve, chatReq, onChunk)
	for _, a := range attempts {
		status := "ok"
		if a.Err != nil {
			status = "error"
		}
		emit(trace.BackendAttempt, "resilience", map[string]any{"provider": a.Provider, "model": a.Model, "status": status})
	}

	realTokens := usage.PromptTokens + usage.CompletionTokens
	p.Budget.Settle(r.Context(), reservation, realTokens)
	emit(trace.BudgetSettle, "budget", map[string]any{"tenant_id": chatReq.TenantID, "real_tokens": realTokens})

	if streamErr != nil {
		reason := "all_backends_failed"
		errType := "all_backends_failed"
		if errors.Is(streamErr, resilience.ErrStreamFailedAfterFirstByte) {
			reason = "stream_failed_after_first_byte"
			errType = "backend_stream_failed"
		}
		emit(trace.Error, "resilience", map[string]any{"reason": reason})
		payload, _ := json.Marshal(map[string]any{"error": map[string]string{"type": errType, "message": streamErr.Error(), "request_id": requestID}})
		fmt.Fprintf(w, "data: %s\n\n", payload)
		flusher.Flush()
	}

	fmt.Fprint(w, "data: [DONE]\n\n")
	flusher.Flush()
}

func writeAllBackendsFailed(w http.ResponseWriter, requestID string, attempts []resilience.Attempt) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadGateway)
	attemptBodies := make([]map[string]any, 0, len(attempts))
	for _, a := range attempts {
		errMsg := ""
		if a.Err != nil {
			errMsg = a.Err.Error()
		}
		attemptBodies = append(attemptBodies, map[string]any{"provider": a.Provider, "model": a.Model, "status": a.Status, "error": errMsg})
	}
	json.NewEncoder(w).Encode(map[string]any{
		"error":    map[string]any{"type": "all_backends_failed", "message": "every backend in the cascade failed", "request_id": requestID},
		"attempts": attemptBodies,
	})
}
```

- [ ] **Step 4: Rodar o teste e ver passar**

Run: `go test ./internal/api/... -v`
Expected: PASS em todos os casos (happy path, 401, 429 com `Retry-After: 5`, 402, 400 `unknown_model`, 502 `all_backends_failed`, streaming com fallback antes do primeiro byte, streaming que para sem fallback depois do primeiro byte).

- [ ] **Step 5: Ligar o pipeline real em `cmd/gateway/main.go`**

Replace `main.go` body (from Task 5) with:
```go
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/vicentemoura/ai-gateway/internal/api"
	"github.com/vicentemoura/ai-gateway/internal/auth"
	"github.com/vicentemoura/ai-gateway/internal/backend/gemini"
	"github.com/vicentemoura/ai-gateway/internal/backend/ollama"
	"github.com/vicentemoura/ai-gateway/internal/backend/openrouter"
	"github.com/vicentemoura/ai-gateway/internal/budget"
	"github.com/vicentemoura/ai-gateway/internal/config"
	"github.com/vicentemoura/ai-gateway/internal/ratelimit"
	"github.com/vicentemoura/ai-gateway/internal/resilience"
	"github.com/vicentemoura/ai-gateway/internal/trace"
)

func main() {
	logger := trace.NewLogger(os.Stdout)
	ctx := context.Background()

	routing, err := config.LoadRouting("config/routing.yaml")
	if err != nil {
		logger.Error("loading routing config", "error", err)
		os.Exit(1)
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion("us-east-1"))
	if err != nil {
		logger.Error("loading AWS config", "error", err)
		os.Exit(1)
	}
	endpoint := os.Getenv("AWS_ENDPOINT_URL")
	dynamoClient := dynamodb.NewFromConfig(awsCfg, func(o *dynamodb.Options) {
		if endpoint != "" {
			o.BaseEndpoint = &endpoint
		}
	})

	ollamaBaseURL := os.Getenv("OLLAMA_BASE_URL")
	if ollamaBaseURL == "" {
		ollamaBaseURL = routing.Providers["ollama"].BaseURL
	}

	backends := map[string]resilience.FullBackend{
		"ollama":     ollama.NewClient(ollamaBaseURL),
		"openrouter": openrouter.NewClient(routing.Providers["openrouter"].BaseURL, os.Getenv(routing.Providers["openrouter"].APIKeyEnv)),
		"gemini":     gemini.NewClient(routing.Providers["gemini"].BaseURL, os.Getenv(routing.Providers["gemini"].APIKeyEnv)),
	}

	pipeline := &api.Pipeline{
		Auth:      auth.NewDynamoStore(dynamoClient, "tenants"),
		RateLimit: ratelimit.NewInMemoryLimiter(),
		Budget:    budget.NewDynamoStore(dynamoClient, "budgets", "reservations"),
		Routing:   routing,
		Backends:  backends,
		Trace:     trace.NewDynamoStore(dynamoClient, "requests", "trace_events"),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})
	mux.Handle("POST /v1/chat/completions", api.NewPipelineChatHandler(pipeline))

	handler := api.RequestIDMiddleware(mux)

	addr := ":8080"
	logger.Info("starting ai-gateway", "addr", addr)
	if err := http.ListenAndServe(addr, handler); err != nil {
		logger.Error("server stopped", "error", err)
		os.Exit(1)
	}
}
```

- [ ] **Step 6: Rodar build e vet**

Run:
```bash
go build ./...
go vet ./...
```
Expected: build e vet sem erro.

- [ ] **Step 7: Commit**

```bash
git add internal/api cmd/gateway
git commit -m "feat(api): wire auth, ratelimit, budget, router and resilience into the chat pipeline

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 12: `usage` → SQS com DLQ, `stats` e endpoints `/readyz`, `/v1/models`, `/stats`

**Files:**
- Create: `internal/usage/usage.go`
- Test: `internal/usage/usage_test.go` (build tag `integration`)
- Create: `internal/stats/stats.go`
- Test: `internal/stats/stats_test.go`
- Create: `infra/modules/sqs/main.tf`
- Create: `infra/modules/sqs/variables.tf`
- Modify: `infra/main.tf`
- Modify: `internal/api/pipeline.go`
- Modify: `cmd/gateway/main.go`

**Interfaces:**
- Consumes: `core.Usage`/`Tenant`/`BackendTarget` (Task 2), `resilience.Attempt` (Task 9), `sqs.Client`.
- Produces: `usage.Event{EventVersion int, RequestID, TenantID, Tier, Alias, Provider, Model string, PromptTokens, CompletionTokens, TTFTMs, LatencyMs int, Status, ErrorClass string, Attempts []usage.AttemptRecord, TS string}`; `usage.Publisher interface{Publish(ctx context.Context, event usage.Event) error}`; `usage.NewSQSPublisher(client *sqs.Client, queueURL string) usage.Publisher`; `stats.Recorder interface{Record(route, model, tenantID string, latencyMs int, tokens int); Snapshot() stats.Report}` com `stats.Report` trazendo p50/p90/p99 de latência e tokens por rota/modelo/tenant numa janela deslizante em memória; `stats.NewInMemoryRecorder(window time.Duration) stats.Recorder`.

- [ ] **Step 1: Escrever o teste de `stats.Recorder` (falhando, sem integração)**

Create `internal/stats/stats_test.go`:
```go
package stats

import "testing"

func TestRecorder_ComputesPercentilesPerRoute(t *testing.T) {
	r := NewInMemoryRecorder(0) // window=0 means "keep everything", for deterministic tests
	for _, latency := range []int{10, 20, 30, 40, 50, 60, 70, 80, 90, 100} {
		r.Record("nuva/fast", "qwen2.5-coder:1.5b", "tenant-1", latency, 50)
	}

	report := r.Snapshot()
	route, ok := report.Routes["nuva/fast"]
	if !ok {
		t.Fatalf("expected route nuva/fast in report, got %+v", report)
	}
	if route.Count != 10 {
		t.Fatalf("expected 10 samples, got %d", route.Count)
	}
	if route.P50LatencyMs < 40 || route.P50LatencyMs > 60 {
		t.Fatalf("unexpected p50: %d", route.P50LatencyMs)
	}
	if route.P99LatencyMs < 90 {
		t.Fatalf("unexpected p99: %d", route.P99LatencyMs)
	}
}

func TestRecorder_EmptyReportHasNoRoutes(t *testing.T) {
	r := NewInMemoryRecorder(0)
	report := r.Snapshot()
	if len(report.Routes) != 0 {
		t.Fatalf("expected no routes recorded yet, got %+v", report.Routes)
	}
}
```

- [ ] **Step 2: Rodar o teste e ver falhar**

Run: `go test ./internal/stats/... -v`
Expected: FAIL — `undefined: NewInMemoryRecorder`.

- [ ] **Step 3: Implementar `internal/stats/stats.go`**

```go
package stats

import (
	"sort"
	"sync"
	"time"
)

// RouteStats is the computed percentile summary for one route (alias/model pair).
type RouteStats struct {
	Count        int
	P50LatencyMs int
	P90LatencyMs int
	P99LatencyMs int
}

// Report is the full /stats response body.
type Report struct {
	Routes map[string]RouteStats
}

type sample struct {
	latencyMs int
	tokens    int
	at        time.Time
}

// Recorder accumulates latency/token samples per route and computes
// percentiles over a sliding time window (window=0 keeps all samples,
// useful for deterministic tests).
type Recorder interface {
	Record(route, model, tenantID string, latencyMs, tokens int)
	Snapshot() Report
}

type inMemoryRecorder struct {
	mu      sync.Mutex
	window  time.Duration
	samples map[string][]sample // keyed by route
}

func NewInMemoryRecorder(window time.Duration) Recorder {
	return &inMemoryRecorder{window: window, samples: make(map[string][]sample)}
}

func (r *inMemoryRecorder) Record(route, model, tenantID string, latencyMs, tokens int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.samples[route] = append(r.samples[route], sample{latencyMs: latencyMs, tokens: tokens, at: time.Now()})
}

func (r *inMemoryRecorder) Snapshot() Report {
	r.mu.Lock()
	defer r.mu.Unlock()

	report := Report{Routes: make(map[string]RouteStats)}
	cutoff := time.Time{}
	if r.window > 0 {
		cutoff = time.Now().Add(-r.window)
	}

	for route, samples := range r.samples {
		var latencies []int
		for _, s := range samples {
			if r.window > 0 && s.at.Before(cutoff) {
				continue
			}
			latencies = append(latencies, s.latencyMs)
		}
		if len(latencies) == 0 {
			continue
		}
		sort.Ints(latencies)
		report.Routes[route] = RouteStats{
			Count:        len(latencies),
			P50LatencyMs: percentile(latencies, 50),
			P90LatencyMs: percentile(latencies, 90),
			P99LatencyMs: percentile(latencies, 99),
		}
	}
	return report
}

func percentile(sorted []int, p int) int {
	if len(sorted) == 0 {
		return 0
	}
	idx := (p * len(sorted)) / 100
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
```

- [ ] **Step 4: Rodar o teste e ver passar**

Run: `go test ./internal/stats/... -v`
Expected: PASS nos dois casos.

- [ ] **Step 5: Escrever o teste de `usage.Publisher` (integração, falhando)**

Create `internal/usage/usage_test.go`:
```go
//go:build integration

package usage

import (
	"context"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

func newTestSQSClient(t *testing.T) (*sqs.Client, string) {
	t.Helper()
	endpoint := os.Getenv("AWS_ENDPOINT_URL")
	if endpoint == "" {
		endpoint = "http://localhost:4566"
	}
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion("us-east-1"))
	if err != nil {
		t.Fatalf("loading AWS config: %v", err)
	}
	client := sqs.NewFromConfig(cfg, func(o *sqs.Options) { o.BaseEndpoint = &endpoint })

	out, err := client.GetQueueUrl(context.Background(), &sqs.GetQueueUrlInput{QueueName: strPtr("usage-events")})
	if err != nil {
		t.Fatalf("getting queue url (did terraform apply run?): %v", err)
	}
	return client, *out.QueueUrl
}

func strPtr(s string) *string { return &s }

func TestPublish_SendsEventVersionOneJSON(t *testing.T) {
	client, queueURL := newTestSQSClient(t)
	publisher := NewSQSPublisher(client, queueURL)

	event := Event{
		EventVersion:     1,
		RequestID:        "req-1",
		TenantID:         "tenant-1",
		Tier:             "free",
		Alias:            "nuva/fast",
		Provider:         "ollama",
		Model:            "qwen2.5-coder:1.5b",
		PromptTokens:     10,
		CompletionTokens: 5,
		LatencyMs:        120,
		Status:           "ok",
		TS:               "2026-09-19T12:00:00Z",
	}
	if err := publisher.Publish(context.Background(), event); err != nil {
		t.Fatalf("Publish: %v", err)
	}
}
```

- [ ] **Step 6: Rodar o teste de integração e ver falhar**

Run: `docker compose up -d localstack && go test -tags integration ./internal/usage/... -v`
Expected: FAIL — `undefined: NewSQSPublisher`, e também falha em `GetQueueUrl` até a fila existir (resolvido no Step 9 com Terraform).

- [ ] **Step 7: Implementar `internal/usage/usage.go`**

```go
package usage

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// AttemptRecord is one backend attempt, as recorded in usage_event.attempts.
type AttemptRecord struct {
	Provider  string `json:"provider"`
	Model     string `json:"model"`
	Status    string `json:"status"`
	LatencyMs int    `json:"latency_ms"`
}

// Event is usage_event v1, published to SQS for every completed or failed request.
// It never contains message content.
type Event struct {
	EventVersion     int             `json:"event_version"`
	RequestID        string          `json:"request_id"`
	TenantID         string          `json:"tenant_id"`
	Tier             string          `json:"tier"`
	Alias            string          `json:"alias"`
	Provider         string          `json:"provider"`
	Model            string          `json:"model"`
	PromptTokens     int             `json:"prompt_tokens"`
	CompletionTokens int             `json:"completion_tokens"`
	TTFTMs           int             `json:"ttft_ms"`
	LatencyMs        int             `json:"latency_ms"`
	Status           string          `json:"status"`
	ErrorClass       string          `json:"error_class"`
	Attempts         []AttemptRecord `json:"attempts"`
	TS               string          `json:"ts"`
}

// Publisher sends a usage_event to the queue.
type Publisher interface {
	Publish(ctx context.Context, event Event) error
}

type sqsPublisher struct {
	client   *sqs.Client
	queueURL string
}

// NewSQSPublisher builds a Publisher backed by the given SQS queue URL.
func NewSQSPublisher(client *sqs.Client, queueURL string) Publisher {
	return &sqsPublisher{client: client, queueURL: queueURL}
}

func (p *sqsPublisher) Publish(ctx context.Context, event Event) error {
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("usage: marshaling event: %w", err)
	}
	bodyStr := string(body)
	_, err = p.client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: &p.queueURL, MessageBody: &bodyStr})
	if err != nil {
		return fmt.Errorf("usage: sending message: %w", err)
	}
	return nil
}
```

- [ ] **Step 8: Adicionar a dependência SQS**

Run: `go get github.com/aws/aws-sdk-go-v2/service/sqs`

- [ ] **Step 9: Criar a fila `usage-events` com DLQ via Terraform**

Create `infra/modules/sqs/variables.tf`:
```hcl
variable "queue_name" {
  type = string
}
```

Create `infra/modules/sqs/main.tf`:
```hcl
resource "aws_sqs_queue" "dlq" {
  name = "${var.queue_name}-dlq"
}

resource "aws_sqs_queue" "this" {
  name = var.queue_name

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = 5
  })
}

output "queue_url" {
  value = aws_sqs_queue.this.id
}

output "dlq_url" {
  value = aws_sqs_queue.dlq.id
}
```

Add to `infra/main.tf` (from Task 6):
```hcl
module "usage_events_queue" {
  source     = "./modules/sqs"
  queue_name = "usage-events"
}
```

- [ ] **Step 10: Aplicar e rodar o teste de integração**

Run:
```bash
cd infra && tflocal apply -auto-approve && cd ..
go test -tags integration ./internal/usage/... -v
```
Expected: PASS.

- [ ] **Step 11: Integrar `usage.Publisher` e `stats.Recorder` no pipeline**

Modify `internal/api/pipeline.go`: add `Usage usage.Publisher` and `Stats stats.Recorder` fields to `Pipeline`, and after the `budget.Settle` call in the non-streaming success path, insert:
```go
start := time.Now() // capture at the top of NewPipelineChatHandler's closure, before auth
// ... (existing pipeline code) ...
latencyMs := int(time.Since(start).Milliseconds())
errorClass := ""
status := "ok"
if err != nil {
    status = "error"
    errorClass = "transient"
}
attemptRecords := make([]usage.AttemptRecord, 0, len(attempts))
for _, a := range attempts {
    attemptStatus := "ok"
    if a.Err != nil {
        attemptStatus = "error"
    }
    attemptRecords = append(attemptRecords, usage.AttemptRecord{Provider: a.Provider, Model: a.Model, Status: attemptStatus})
}
p.Usage.Publish(ctx, usage.Event{
    EventVersion:     1,
    RequestID:        requestID,
    TenantID:         tenant.ID,
    Tier:             tenant.Tier,
    Alias:            body.Model,
    Provider:         target.Provider,
    Model:            target.Model,
    PromptTokens:     resp.Usage.PromptTokens,
    CompletionTokens: resp.Usage.CompletionTokens,
    LatencyMs:        latencyMs,
    Status:           status,
    ErrorClass:       errorClass,
    Attempts:         attemptRecords,
    TS:               time.Now().UTC().Format(time.RFC3339),
})
p.Stats.Record(body.Model, target.Model, tenant.ID, latencyMs, resp.Usage.PromptTokens+resp.Usage.CompletionTokens)
emit(trace.UsagePublish, "usage", map[string]any{"tenant_id": tenant.ID})
```
Place `start := time.Now()` as the very first line inside the `http.HandlerFunc` closure in `NewPipelineChatHandler`, and add the same `p.Usage.Publish`/`emit(trace.UsagePublish, ...)` call (with `status: "error"`, `error_class: "transient"`, empty `Provider`/`Model`) right before `writeAllBackendsFailed` in the `all_backends_failed` branch, so failed requests also get a `usage_event`. Add imports `"time"`, `"github.com/vicentemoura/ai-gateway/internal/usage"`, `"github.com/vicentemoura/ai-gateway/internal/stats"`.

- [ ] **Step 12: Adicionar `/readyz`, `/v1/models` e `/stats` em `cmd/gateway/main.go`**

Modify `main.go`: add to the `Pipeline` construction `Usage: usage.NewSQSPublisher(sqsClient, usageQueueURL)` and `Stats: stats.NewInMemoryRecorder(5 * time.Minute)`, building `sqsClient := sqs.NewFromConfig(awsCfg, ...)` the same way `dynamoClient` is built, and resolving `usageQueueURL` via `sqsClient.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: strPtr("usage-events")})` at startup (fail fast with `logger.Error` + `os.Exit(1)` if it errors). Then register the new routes on `mux`:
```go
mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
	if _, err := dynamoClient.DescribeTable(r.Context(), &dynamodb.DescribeTableInput{TableName: strPtr("tenants")}); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(`{"status":"store unavailable"}`))
		return
	}
	if _, err := ollamaClient.Tags(r.Context()); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(`{"status":"no backend reachable"}`))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"ready"}`))
})

mux.HandleFunc("GET /v1/models", func(w http.ResponseWriter, r *http.Request) {
	names, err := ollamaClient.Tags(r.Context())
	if err != nil {
		names = nil
	}
	data := make([]map[string]string, 0, len(names)+len(routing.Aliases))
	for alias := range routing.Aliases {
		data = append(data, map[string]string{"id": alias})
	}
	for _, name := range names {
		data = append(data, map[string]string{"id": "ollama/" + name})
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"object": "list", "data": data})
})

mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(statsRecorder.Snapshot())
})
```
Extract the `ollama.NewClient(ollamaBaseURL)` call into a named variable `ollamaClient` (reused both by `backends["ollama"]` and by these new routes), extract `stats.NewInMemoryRecorder(5 * time.Minute)` into `statsRecorder` (reused both by `pipeline.Stats` and the `/stats` route), and add `strPtr` as a small local helper `func strPtr(s string) *string { return &s }`. Add imports `"encoding/json"` and `"github.com/aws/aws-sdk-go-v2/service/sqs"`.

- [ ] **Step 13: Rodar todos os testes e o build**

Run:
```bash
go build ./...
go vet ./...
go test ./... -race
go test -tags integration ./... -v
```
Expected: tudo verde.

- [ ] **Step 14: Commit**

```bash
git add internal/usage internal/stats internal/api cmd/gateway infra
git commit -m "feat(usage,stats): publish usage_event to SQS with DLQ and add /readyz, /v1/models, /stats

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 13: OTel — spans e métricas, collector, dashboard Grafana

**Files:**
- Create: `internal/trace/otel.go`
- Test: `internal/trace/otel_test.go`
- Modify: `internal/api/pipeline.go`
- Modify: `cmd/gateway/main.go`
- Create: `deploy/otel-collector-config.yaml`
- Create: `deploy/tempo.yaml`
- Create: `deploy/prometheus.yml`
- Create: `deploy/grafana/dashboards/gateway.json`

**Interfaces:**
- Consumes: `go.opentelemetry.io/otel`, `otel/sdk/trace`, `otel/sdk/metric`, `otlptracehttp`.
- Produces: `trace.NewTracerProvider(ctx context.Context, otlpEndpoint string) (*sdktrace.TracerProvider, error)`; `trace.NewMeterProvider(ctx context.Context, otlpEndpoint string) (*sdkmetric.MeterProvider, error)`; `trace.Tracer() oteltrace.Tracer` (nome fixo `"ai-gateway"`); `trace.Metrics{RequestsTotal metric.Int64Counter, LatencyMs metric.Float64Histogram, TokensTotal metric.Int64Counter, TTFTMs metric.Float64Histogram}`; `trace.NewMetrics(meter metric.Meter) (*trace.Metrics, error)` — usados pelo `Pipeline` (Task 11) para envolver cada estágio (`auth`, `budget.reserve`, `route`, `backend.call`, `budget.settle`, `usage.publish`) num span filho.

- [ ] **Step 1: Escrever o teste de `NewMetrics` (falhando)**

Create `internal/trace/otel_test.go`:
```go
package trace

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/sdk/metric"
)

func TestNewMetrics_RegistersAllFourInstruments(t *testing.T) {
	provider := metric.NewMeterProvider()
	defer provider.Shutdown(context.Background())
	meter := provider.Meter("ai-gateway-test")

	m, err := NewMetrics(meter)
	if err != nil {
		t.Fatalf("NewMetrics returned error: %v", err)
	}
	if m.RequestsTotal == nil || m.LatencyMs == nil || m.TokensTotal == nil || m.TTFTMs == nil {
		t.Fatalf("expected all four instruments to be non-nil: %+v", m)
	}

	// Smoke-test that recording doesn't panic.
	m.RequestsTotal.Add(context.Background(), 1)
	m.LatencyMs.Record(context.Background(), 42.0)
	m.TokensTotal.Add(context.Background(), 10)
	m.TTFTMs.Record(context.Background(), 5.0)
}
```

- [ ] **Step 2: Rodar o teste e ver falhar**

Run: `go test ./internal/trace/... -run TestNewMetrics -v`
Expected: FAIL — `undefined: NewMetrics`.

- [ ] **Step 3: Adicionar as dependências OTel**

Run:
```bash
go get go.opentelemetry.io/otel go.opentelemetry.io/otel/sdk go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp
```

- [ ] **Step 4: Implementar `internal/trace/otel.go`**

```go
package trace

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
)

const tracerName = "ai-gateway"

// NewTracerProvider builds an OTLP-over-HTTP tracer provider pointed at the
// collector. Spans emitted: auth, budget.reserve, route, backend.call,
// budget.settle, usage.publish, one request span as their parent.
func NewTracerProvider(ctx context.Context, otlpEndpoint string) (*sdktrace.TracerProvider, error) {
	exporter, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpoint(otlpEndpoint), otlptracehttp.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("trace: creating OTLP trace exporter: %w", err)
	}
	provider := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter))
	otel.SetTracerProvider(provider)
	return provider, nil
}

// NewMeterProvider builds an OTLP-over-HTTP meter provider for
// requests_total, latency_ms, tokens_total and ttft_ms.
func NewMeterProvider(ctx context.Context, otlpEndpoint string) (*sdkmetric.MeterProvider, error) {
	exporter, err := otlpmetrichttp.New(ctx, otlpmetrichttp.WithEndpoint(otlpEndpoint), otlpmetrichttp.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("trace: creating OTLP metric exporter: %w", err)
	}
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter)))
	otel.SetMeterProvider(provider)
	return provider, nil
}

// Tracer returns the gateway's named tracer for creating request/stage spans.
func Tracer() oteltrace.Tracer {
	return otel.Tracer(tracerName)
}

// Metrics holds the four counters/histograms the spec requires.
type Metrics struct {
	RequestsTotal metric.Int64Counter
	LatencyMs     metric.Float64Histogram
	TokensTotal   metric.Int64Counter
	TTFTMs        metric.Float64Histogram
}

// NewMetrics registers the four instruments on the given meter.
func NewMetrics(meter metric.Meter) (*Metrics, error) {
	requestsTotal, err := meter.Int64Counter("requests_total")
	if err != nil {
		return nil, fmt.Errorf("trace: creating requests_total counter: %w", err)
	}
	latencyMs, err := meter.Float64Histogram("latency_ms")
	if err != nil {
		return nil, fmt.Errorf("trace: creating latency_ms histogram: %w", err)
	}
	tokensTotal, err := meter.Int64Counter("tokens_total")
	if err != nil {
		return nil, fmt.Errorf("trace: creating tokens_total counter: %w", err)
	}
	ttftMs, err := meter.Float64Histogram("ttft_ms")
	if err != nil {
		return nil, fmt.Errorf("trace: creating ttft_ms histogram: %w", err)
	}
	return &Metrics{RequestsTotal: requestsTotal, LatencyMs: latencyMs, TokensTotal: tokensTotal, TTFTMs: ttftMs}, nil
}
```

- [ ] **Step 5: Rodar o teste e ver passar**

Run: `go test ./internal/trace/... -v`
Expected: PASS em todos os casos do pacote (`NewLogger`, `NewMetrics`, e os de integração se `-tags integration` for usado).

- [ ] **Step 6: Envolver os estágios do pipeline em spans**

Modify `internal/api/pipeline.go`: add `Tracer oteltrace.Tracer` and `Metrics *trace.Metrics` fields to `Pipeline`. At the top of the `http.HandlerFunc` closure in `NewPipelineChatHandler`, replace `ctx := r.Context()` with:
```go
ctx, requestSpan := p.Tracer.Start(r.Context(), "chat.completions")
defer requestSpan.End()
```
Wrap each existing stage with a child span, e.g. for auth:
```go
_, authSpan := p.Tracer.Start(ctx, "auth")
tenant, err := p.Auth.ResolveAPIKey(ctx, apiKey)
authSpan.End()
```
Apply the same `p.Tracer.Start(ctx, "<name>")` / `span.End()` pattern around: `budget.reserve` (the `p.Budget.Reserve` call), `route` (the `router.Resolve` call), `backend.call` (the `resilience.Call` call), `budget.settle` (the `p.Budget.Settle` call), `usage.publish` (the `p.Usage.Publish` call) — five additional child spans, matching exactly the spec's span list `auth, budget.reserve, route, backend.call, budget.settle, usage.publish`. After building the final response (success path), call `p.Metrics.RequestsTotal.Add(ctx, 1)`, `p.Metrics.LatencyMs.Record(ctx, float64(latencyMs))`, `p.Metrics.TokensTotal.Add(ctx, int64(realTokens))`; in the `all_backends_failed` path, still call `p.Metrics.RequestsTotal.Add(ctx, 1)` and `p.Metrics.LatencyMs.Record(ctx, float64(latencyMs))` (tokens stay 0). Add imports `oteltrace "go.opentelemetry.io/otel/trace"` and `"github.com/vicentemoura/ai-gateway/internal/trace"` (if not already imported for `trace.EventType`).

- [ ] **Step 7: Iniciar os providers em `main.go`**

Modify `cmd/gateway/main.go`: near the top of `main`, before building `pipeline`, add:
```go
otlpEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
if otlpEndpoint == "" {
	otlpEndpoint = "localhost:4318"
}
tracerProvider, err := trace.NewTracerProvider(ctx, otlpEndpoint)
if err != nil {
	logger.Error("starting tracer provider", "error", err)
	os.Exit(1)
}
defer tracerProvider.Shutdown(ctx)

meterProvider, err := trace.NewMeterProvider(ctx, otlpEndpoint)
if err != nil {
	logger.Error("starting meter provider", "error", err)
	os.Exit(1)
}
defer meterProvider.Shutdown(ctx)

metrics, err := trace.NewMetrics(meterProvider.Meter("ai-gateway"))
if err != nil {
	logger.Error("registering metrics", "error", err)
	os.Exit(1)
}
```
Add `Tracer: trace.Tracer(), Metrics: metrics,` to the `api.Pipeline{...}` literal.

- [ ] **Step 8: Rodar build, vet e testes**

Run:
```bash
go build ./...
go vet ./...
go test ./... -race
```
Expected: tudo verde. (O gateway sem `otel-collector` rodando ainda sobe — os exporters OTLP falham silenciosamente em background por design do SDK, conforme Global Constraints: "não é dependência de runtime".)

- [ ] **Step 9: Criar a config do OTel Collector**

Create `deploy/otel-collector-config.yaml`:
```yaml
receivers:
  otlp:
    protocols:
      http:
        endpoint: 0.0.0.0:4318

exporters:
  otlp/tempo:
    endpoint: tempo:4317
    tls:
      insecure: true
  prometheus:
    endpoint: 0.0.0.0:8889

service:
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [otlp/tempo]
    metrics:
      receivers: [otlp]
      exporters: [prometheus]
```

- [ ] **Step 10: Criar a config do Tempo**

Create `deploy/tempo.yaml`:
```yaml
server:
  http_listen_port: 3200

distributor:
  receivers:
    otlp:
      protocols:
        grpc:

storage:
  trace:
    backend: local
    local:
      path: /tmp/tempo/traces
```

- [ ] **Step 11: Criar a config do Prometheus**

Create `deploy/prometheus.yml`:
```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: otel-collector
    static_configs:
      - targets: ["otel-collector:8889"]
```

- [ ] **Step 12: Criar o dashboard Grafana provisionado**

Create `deploy/grafana/dashboards/gateway.json`:
```json
{
  "title": "ai-gateway",
  "timezone": "browser",
  "schemaVersion": 39,
  "panels": [
    {
      "id": 1,
      "title": "Requests total",
      "type": "stat",
      "gridPos": {"h": 8, "w": 8, "x": 0, "y": 0},
      "targets": [{"expr": "sum(requests_total)"}]
    },
    {
      "id": 2,
      "title": "Latency p99 (ms)",
      "type": "timeseries",
      "gridPos": {"h": 8, "w": 8, "x": 8, "y": 0},
      "targets": [{"expr": "histogram_quantile(0.99, sum(rate(latency_ms_bucket[5m])) by (le))"}]
    },
    {
      "id": 3,
      "title": "Tokens total",
      "type": "stat",
      "gridPos": {"h": 8, "w": 8, "x": 16, "y": 0},
      "targets": [{"expr": "sum(tokens_total)"}]
    },
    {
      "id": 4,
      "title": "TTFT p99 (ms)",
      "type": "timeseries",
      "gridPos": {"h": 8, "w": 8, "x": 0, "y": 8},
      "targets": [{"expr": "histogram_quantile(0.99, sum(rate(ttft_ms_bucket[5m])) by (le))"}]
    }
  ]
}
```

- [ ] **Step 13: Subir a stack e checar visualmente**

Run: `docker compose up -d`
Expected: `gateway`, `localstack`, `otel-collector`, `tempo`, `prometheus`, `grafana` sobem sem erro (`docker compose ps` mostra todos `running`). Acessar `http://localhost:3000` (Grafana) e confirmar que o dashboard `ai-gateway` aparece provisionado (login padrão `admin`/`admin`).

- [ ] **Step 14: Commit**

```bash
git add internal/trace internal/api cmd/gateway deploy
git commit -m "feat(trace): add OTel spans and metrics with collector, Tempo, Prometheus and Grafana dashboard

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 14: Terraform `ecs_fargate`, `tflocal plan` no CI e doc de `apply` real

**Files:**
- Create: `infra/modules/ecs_fargate/main.tf`
- Create: `infra/modules/ecs_fargate/variables.tf`
- Modify: `infra/main.tf`
- Create: `docs/deploy-fargate.md`

**Interfaces:**
- Consumes: nada de Go (só Terraform); reutiliza os módulos `dynamodb` (Task 6) e `sqs` (Task 12) já registrados em `infra/main.tf`.
- Produces: módulo `infra/modules/ecs_fargate` parametrizável por `image`, `container_port`, `cpu`, `memory`; documentação do `apply` real em `docs/deploy-fargate.md`, referenciada pelo README (Task 15).

- [ ] **Step 1: Escrever o módulo `ecs_fargate`**

Create `infra/modules/ecs_fargate/variables.tf`:
```hcl
variable "cluster_name" {
  type    = string
  default = "ai-gateway"
}

variable "service_name" {
  type    = string
  default = "gateway"
}

variable "image" {
  type = string
}

variable "container_port" {
  type    = number
  default = 8080
}

variable "cpu" {
  type    = number
  default = 256
}

variable "memory" {
  type    = number
  default = 512
}
```

Create `infra/modules/ecs_fargate/main.tf`:
```hcl
resource "aws_ecs_cluster" "this" {
  name = var.cluster_name
}

resource "aws_ecs_task_definition" "this" {
  family                   = var.service_name
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = tostring(var.cpu)
  memory                   = tostring(var.memory)

  container_definitions = jsonencode([
    {
      name      = var.service_name
      image     = var.image
      essential = true
      portMappings = [
        { containerPort = var.container_port, protocol = "tcp" }
      ]
    }
  ])
}

resource "aws_ecs_service" "this" {
  name            = var.service_name
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.this.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets = [] # filled in at apply time with a real VPC subnet; LocalStack
                 # plan does not need a real subnet, only a real `apply` does.
  }
}

output "cluster_name" {
  value = aws_ecs_cluster.this.name
}
```

- [ ] **Step 2: Registrar o módulo em `infra/main.tf`**

Add to `infra/main.tf` (after the `sqs` module from Task 12):
```hcl
module "gateway_service" {
  source         = "./modules/ecs_fargate"
  image          = "ai-gateway:latest"
  container_port = 8080
}
```

- [ ] **Step 3: Rodar `tflocal plan` localmente**

Run:
```bash
docker compose up -d localstack
cd infra && tflocal init -upgrade && tflocal plan && cd ..
```
Expected: plan sem erro, mostrando a criação do cluster ECS, task definition e serviço (LocalStack Community simula esses recursos o suficiente para `plan`; `apply` completo em ECS real precisa de VPC/subnets reais, documentado no Step 5).

- [ ] **Step 4: Confirmar que o job `terraform-plan` do CI (criado na Task 1) cobre o novo módulo**

Run: `git diff --stat infra/` para revisão manual — nenhuma mudança de CI é necessária, o job `terraform-plan` já roda `tflocal init && tflocal plan` na raiz de `infra/`, que agora inclui o módulo `ecs_fargate`.

- [ ] **Step 5: Documentar o `apply` real em Fargate**

Create `docs/deploy-fargate.md`:
```markdown
# Deploy real em ECS Fargate (fora do LocalStack)

Este documento cobre o `apply` real, feito manualmente — o CI só roda `tflocal plan` contra LocalStack (ver `.github/workflows/ci.yml`, job `terraform-plan`).

## Pré-requisitos

1. Conta AWS real com credenciais configuradas (`aws configure` ou variáveis `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`).
2. Uma VPC com ao menos uma subnet pública (ou usar a VPC default da conta).
3. Imagem do gateway publicada em um registro acessível pela conta (ECR, por exemplo) — `docker build -t <ecr-repo>:latest . && docker push <ecr-repo>:latest`.

## Passos

1. Editar `infra/main.tf`: trocar o provider `aws` (bloco `endpoints`, usado só para LocalStack) por um provider `aws` sem `endpoints`/`skip_*`, apontando para a região real.
2. Preencher `network_configuration.subnets` em `infra/modules/ecs_fargate/main.tf` com os IDs de subnet reais (via variável nova `subnet_ids`, passada pelo módulo em `infra/main.tf`).
3. Trocar `image = "ai-gateway:latest"` pela URI real da imagem no ECR.
4. Rodar:
   ```bash
   cd infra
   terraform init
   terraform plan -out=plan.tfplan
   terraform apply plan.tfplan
   ```
5. Depois de validar, **destruir os recursos reais** se este for só um teste (spec "Riscos": "rodar suíte de budget contra tabela real uma vez, destruir depois"):
   ```bash
   terraform destroy
   ```

## Limitações conhecidas

- LocalStack Community não simula IAM roles/policies com fidelidade total; o `apply` real pode exigir uma `aws_iam_role` de execução de task ECS que o `plan` local não valida.
- Este repo não provisiona VPC/subnets — assume-se uma já existente na conta.
```

- [ ] **Step 6: Commit**

```bash
git add infra docs/deploy-fargate.md
git commit -m "feat(infra): add ecs_fargate module and real-apply deployment doc

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 15: README final

**Files:**
- Create: `README.md`

**Interfaces:**
- Consumes: todo o código e config das Tasks 0–14 (arquitetura, tabelas, endpoints, limitações).
- Produces: `README.md` na raiz do repo, documento final do projeto — nenhum código novo.

- [ ] **Step 1: Escrever a seção de arquitetura**

Create `README.md` starting with:
```markdown
# ai-gateway

Gateway HTTP OpenAI-compatible em Go que roteia `POST /v1/chat/completions` entre Ollama (local), OpenRouter e Gemini por alias e tier de tenant, com autenticação por API key, rate limit, reserva de orçamento de tokens antes da chamada, retry/fallback entre provedores, tracing de ponta a ponta e publicação de eventos de uso.

Repo 1 do Portfólio AI Infra. Spec completo: `docs/superpowers/specs/2026-09-19-ai-gateway-design.md`. Plano de implementação: `docs/superpowers/plans/2026-09-19-ai-gateway.md`.

## Arquitetura

```
Client
  │  POST /v1/chat/completions
  ▼
[RequestIDMiddleware] ── gera X-Request-Id (uuid v4)
  ▼
[api.Pipeline]
  │
  ├─ auth.Store.ResolveAPIKey ─────► DynamoDB `tenants`
  ├─ ratelimit.Limiter.Allow ──────► token bucket em memória (por tenant)
  ├─ budget.Store.Reserve ─────────► DynamoDB `budgets` (UpdateItem condicional) + `reservations` (TTL 15min)
  ├─ router.Resolve ───────────────► config/routing.yaml (alias+tier → cascata)
  ├─ resilience.Call ──────────────► backend/{ollama,openrouter,gemini} com retry+fallback
  ├─ budget.Store.Settle ──────────► ajusta `used` pelo delta real
  ├─ usage.Publisher.Publish ──────► SQS `usage-events` (+ DLQ)
  └─ trace.Store.RecordEvent ──────► DynamoDB `trace_events` (a cada estágio acima)
  ▼
Response (JSON ou SSE) + X-Request-Id
```

Cada requisição também abre um span OTel `chat.completions` com filhos `auth`, `budget.reserve`, `route`, `backend.call`, `budget.settle`, `usage.publish`, exportados via OTLP para o Collector → Tempo (traces) e Prometheus (métricas `requests_total`, `latency_ms`, `tokens_total`, `ttft_ms`), visualizados no Grafana (`deploy/grafana/dashboards/gateway.json`).
```

- [ ] **Step 2: Escrever a tabela "item do post → onde no código"**

Append to `README.md`:
```markdown
## Item do post de origem → onde no código

| Item do post | Onde no código |
|---|---|
| Porta única OpenAI-compatible | `internal/api/pipeline.go` (`NewPipelineChatHandler`), `internal/api/chat.go` |
| Política de tráfego (não é só um endpoint) | `internal/router/router.go` + `config/routing.yaml` |
| Reserva de orçamento antes da chamada | `internal/budget/budget.go` (`Reserve`/`Settle`) |
| Rate limit por chave | `internal/ratelimit/limiter.go` |
| Fallback entre provedores | `internal/resilience/resilience.go` (`Call`) |
| Identificador de requisição ponta a ponta | `internal/api/requestid.go`, propagado em `trace_events` |
| Evento de uso para cobrança/auditoria externa | `internal/usage/usage.go` (`usage_event` v1 → SQS) |
```

- [ ] **Step 3: Escrever a tabela "nota da Pós → decisão"**

Append to `README.md`:
```markdown
## Nota da Pós IA Aplicada → decisão neste repo

| Nota | Decisão |
|---|---|
| Módulo 02/01 — Gateway Roteador de Modelos (OpenRouter) | Alias + tier → cascata estática em `config/routing.yaml`, resolvida por `internal/router` (Mini-ADR "alias + cascata por tier") |
| Módulo 03/07 — Segurança de API: Auth e Rate Limiting no MCP | API key por header `Authorization: Bearer`, hash SHA-256 nunca em texto puro (`internal/auth.HashAPIKey`); rate limit em `internal/ratelimit` |
| Módulo 08/05 — Observabilidade, Implantação Híbrida e Model Tiering | Tiers `free/standard/premium` em `config/routing.yaml`; observabilidade OTel completa em `internal/trace/otel.go` |
| Módulo 04b/07 — Observabilidade e Limites de Autonomia | `X-Request-Id` + `trace_events` de ponta a ponta (`internal/trace/store.go`); nenhuma subtarefa é agente autônomo (roteamento/budget/rate limit são regras finitas, ver spec) |
| Proposta — Padrões extraídos da Pós IA Aplicada | Logger com chaves proibidas testado (`internal/trace/logger.go`), erro tipado `transient/permanent` nunca tratado por `startsWith`, budget com `ConditionExpression` atômica (nunca check-then-act em dois passos) |
```

- [ ] **Step 4: Escrever "como rodar"**

Append to `README.md`:
```markdown
## Como rodar

Pré-requisitos: ver Task 0 do plano de implementação (Go 1.27, Docker Compose, Terraform, `tflocal`, `awslocal`, Ollama local com `qwen2.5-coder:1.5b`, `gh auth status`).

```bash
# 1. Subir a infra local (LocalStack + observabilidade)
docker compose up -d localstack otel-collector tempo prometheus grafana

# 2. Provisionar as tabelas/filas no LocalStack
cd infra && tflocal init && tflocal apply -auto-approve && cd ..

# 3. Semear tenants de teste
go run scripts/seed_tenants.go

# 4. Rodar o gateway (fora do compose, para usar o Ollama do host diretamente)
go run cmd/gateway/main.go

# 5. Chamar
curl http://localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer dev-free-key" \
  -H "Content-Type: application/json" \
  -d '{"model":"nuva/fast","messages":[{"role":"user","content":"oi"}]}'
```

Testes: `go test ./... -race` (unitários) e `go test -tags integration ./...` (precisa do LocalStack rodando).
```

- [ ] **Step 5: Escrever "limitações conhecidas" e "fase 2"**

Append to `README.md`:
```markdown
## Limitações conhecidas

- **Rate limit é por instância** (Mini-ADR "rate limit em memória, não em Redis"): rodar duas réplicas do gateway multiplica o limite efetivo por tenant. Sinal de mudança: segunda réplica em produção real.
- **Streaming para de dar fallback depois do primeiro byte** (Task 9/11, `resilience.CallStream`): antes de qualquer chunk chegar ao cliente, streaming tem a mesma cascata de retry/fallback do caminho não-streaming; depois do primeiro byte entregue, um erro no backend encerra o stream (evento SSE `backend_stream_failed` + `[DONE]`, `settle` com tokens parciais) em vez de trocar de provedor no meio da resposta — misturar texto de dois modelos na mesma resposta seria incoerente para quem está lendo.
- **LocalStack Community**: sem paridade total de IAM/TTL do DynamoDB; validar contra AWS real antes de publicar (ver `docs/deploy-fargate.md` e spec "Riscos e pontos de validação").
- **Budget pessimista**: a estimativa de tokens (`prompt_bytes/4 + max_tokens`) pode recusar um tenant com saldo real ainda disponível. Sinal de revisão: >5% das recusas `402` com saldo real >10% do budget por 4 semanas (Mini-ADR "budget reservado antes da chamada").
- **Uma instância, sem multi-região**: fora de escopo deste repo (repo 4, `finops-control-plane`, cobre failover multi-região).

## Fase 2

Cache semântico (embeddings + vector store) para prompts repetidos — hoje fora de escopo. Entra quando o `llm-loadgen` (repo 2 do portfólio) mostrar taxa de prompts repetidos que justifique o custo de manter um índice vetorial. Aprovado por Vicente em 19/09/2026 (ver spec, seção "Metade 1 — Narrativa").
```

- [ ] **Step 6: Commit**

```bash
git add README.md
git commit -m "docs: add final README with architecture, traceability tables and known limitations

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```
</content>
