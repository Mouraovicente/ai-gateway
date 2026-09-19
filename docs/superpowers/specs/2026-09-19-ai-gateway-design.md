---
title: Arquitetura — ai-gateway
description: Desenho do repo 1 do Portfólio AI Infra — gateway multi-modelo em Go, OpenAI-compatible, com tenant, budget, cascata por tier, observabilidade e eventos de uso em AWS (LocalStack).
updated: 2026-09-19
status: aprovada
author: claude
tags:
  - portfolio
  - ai-infra
  - arquitetura
sources:
  - Projetos/Portfólio AI Infra/00 - Portfólio AI Infra.md
  - Projetos/Portfólio AI Infra/Backlog — 15 projetos de AI Infra (Suraj Sharma).md
  - Conhecimento/Pós IA Aplicada/Módulo 02 - Integração com APIs de LLMs/01 - Gateway Roteador de Modelos (OpenRouter).md
  - Conhecimento/Pós IA Aplicada/Módulo 03 - MCP/07 - Segurança de API - Auth e Rate Limiting no MCP.md
  - Conhecimento/Pós IA Aplicada/Módulo 08 - TrialForge/05 - Arquitetura Enterprise - Observabilidade, Implantação Híbrida e Model Tiering.md
  - Conhecimento/Pós IA Aplicada/Módulo 04b - OpsPilot/07 - Observabilidade e Limites de Autonomia.md
  - Conhecimento/Infra e Padrões/Proposta — Padrões extraídos da Pós IA Aplicada.md
---
# Arquitetura — ai-gateway

Repo 1 de [[00 - Portfólio AI Infra]]. Cobre itens 6 (gateway multi-modelo) e 13 (observabilidade) do [[Backlog — 15 projetos de AI Infra (Suraj Sharma)|backlog]]. "Cliente" aqui é o próprio Vicente e quem lê o repo (recrutador, engenheiro avaliando o portfólio).

## Metade 1 — Narrativa

O `ai-gateway` é uma porta única para modelos de linguagem. Uma aplicação fala com ele no mesmo formato da API da OpenAI e não precisa saber se a resposta veio de um modelo rodando no computador local (Ollama), do OpenRouter ou do Gemini. O gateway decide isso por uma política escrita em código: cada cliente (tenant) tem uma chave, um plano (tier) e um orçamento mensal de tokens. Pedidos baratos vão primeiro para o modelo local e caem para um provedor pago só se o local falhar. Pedidos marcados como "importantes" vão direto para o modelo forte, sem tentar o barato.

Por que desenhado assim: o post de origem diz que "produção não é um endpoint, é uma política de tráfego". O curso da Pós ensinou exatamente isso em duas notas (roteador OpenRouter e Model Tiering), mas em nível de aplicação. Aqui a política vira infraestrutura: reserva de orçamento antes da chamada, limite de requisições por chave, fallback entre provedores, identificador de requisição de ponta a ponta e um evento de uso publicado para quem quiser cobrar ou auditar (o repo 4, em Java, vai consumir).

O que fica de fora de propósito: painel e cobrança em dinheiro (repo 4, `finops-control-plane`, itens 2 e 14), multi-região (drill do repo 4). Cache semântico (Módulo 08/04 do curso) é **fase 2 deste repo**: entra quando o `llm-loadgen` mostrar taxa de prompts repetidos que justifique embeddings e store vetorial. Aprovado por Vicente em 19/09/2026.

O que precisa ser confirmado antes de codar está em "Riscos e validação".

## Metade 2 — Working-memory técnica

### Requisitos e restrições

Funcionais, em ordem:
1. `POST /v1/chat/completions` compatível com OpenAI, com e sem `stream: true` (SSE), aceitando `model` como alias do gateway (`nuva/fast`, `nuva/smart`) ou nome direto de backend.
2. Autenticação por API key (`Authorization: Bearer <key>`), resolvendo tenant, tier e orçamento.
3. Reserva de orçamento de tokens **antes** da chamada ao provedor (estimativa por tamanho do prompt + `max_tokens`), acerto depois com o uso real; sem `await` entre checar e debitar (operação atômica no store).
4. Rate limit por chave (requisições/minuto) com `429` e `Retry-After`.
5. Roteamento: alias + tier definem cascata ordenada de backends; retry com backoff só em erro transitório; fallback para o próximo backend; teto de 2 tentativas por backend.
6. `X-Request-Id` gerado no início, devolvido no header e no corpo de erro, propagado ao provedor quando ele aceita.
7. Persistir `requests` (metadados) e `trace_events` (sequência, tipo, nó, payload de metadados) por requisição.
8. Publicar `usage_event` por requisição concluída ou falhada.
9. `GET /healthz`, `GET /readyz` (checa store e pelo menos um backend), `GET /v1/models`, `GET /stats` (percentis de latência e tokens por rota/modelo/tenant, janela em memória).
10. OpenTelemetry: trace por requisição com spans `auth`, `budget.reserve`, `route`, `backend.call`, `budget.settle`, `usage.publish`; métricas `requests_total`, `latency_ms`, `tokens_total`, `ttft_ms`.

Volume: portfólio. Dezenas de req/s no load test (repo 2), unidades em uso normal. Uma instância. Não desenhar para multi-instância agora (ver ADR de rate limit).

Orçamento de operação: zero até deploy real. LocalStack Community, Ollama local, OpenRouter só com chave e teto de gasto configurado na conta.

O que já existe: Docker Desktop, Go 1.23.2 (Windows), Ollama com `qwen2.5-coder:1.5b`, `phi4-mini`, `gemma4-coder`; LiteLLM rodando em `nuvatech_dev_litellm` (referência de comportamento, não dependência); `gh` autenticado. Faltam: Terraform, `tflocal`, `awscli-local`, Podman (não necessário neste repo). Go precisa subir para a versão estável atual antes do primeiro commit.

Suposições:
- S1. Tokens estimados no `reserve` = `len(prompt_bytes)/4 + max_tokens` (ou 1024 se ausente). Acerto no `settle` com `usage` do provedor; Ollama devolve `prompt_eval_count`/`eval_count`; OpenRouter exige `stream_options.include_usage: true` no streaming.
- S2. Orçamento mensal por tenant em tokens, não em dinheiro. Conversão para $/1M tokens é do repo 4.
- S3. Tabela de preços por modelo vive em arquivo de config versionado, não em banco.
- S4. Tiers: `free` (só Ollama, sem fallback pago), `standard` (Ollama → OpenRouter barato), `premium` (`nuva/smart` direto no forte). Nomes e limites em config.
- S5. Sem multi-região neste repo. Failover entre AWS e GCP é do repo 4.

Agente ou regra: **nenhuma subtarefa é agente**. Roteamento, budget, rate limit, retry são regras finitas que cobrem 100% dos casos. O LLM só aparece como backend opaco atrás do gateway. Dos cinco blocos de referência: Gateway = este repo (**existe** ao fim); Orquestrador determinístico = pipeline de middlewares (**existe**); Modelo+Tools = backends externos (**existe**, fora do repo); Approval Gate = **não se aplica** (nenhuma ação irreversível; anotar no README); Observabilidade = **existe** (requestId, trace_events, OTel).

### Módulos e contratos

| Módulo | Responsabilidade (uma frase) | Entrada | Saída |
|---|---|---|---|
| `api` | Traduz HTTP OpenAI-compatible em `ChatRequest` e devolve `ChatResponse`/SSE | HTTP | `ChatRequest{requestId, tenant, alias, messages, params, stream}` |
| `auth` | Resolve API key em `Tenant{id, tier, rpmLimit, monthlyTokenBudget}` | header Bearer | `Tenant` ou `401` |
| `ratelimit` | Decide se a chave pode passar agora | `tenant.id`, instante | permitido / `429 + Retry-After` |
| `budget` | Reserva tokens estimados atomicamente e acerta com uso real | `tenant.id, mes, estimado` / `reservationId, real` | `Reservation` ou `402 budget_exceeded` |
| `router` | Converte `alias + tier` em lista ordenada de `BackendTarget{provider, model}` | `alias`, `tier` | `[]BackendTarget` ou `400 unknown_model` |
| `backend/*` | Adaptadores Ollama, OpenRouter, Gemini falando `ChatRequest` → `ChatChunk`/`ChatResponse` com `Usage` | `ChatRequest`, target | stream de `ChatChunk`, `Usage`, erro classificado `transient/permanent` |
| `resilience` | Retry com backoff e fallback ao próximo target, teto 2 por backend | `[]BackendTarget`, chamada | primeiro sucesso ou `502 all_backends_failed` com lista tentada |
| `trace` | Grava `requests` e `trace_events`; logger com chaves proibidas | eventos do pipeline | linhas no store; logs só metadados |
| `usage` | Publica `usage_event` v1 | `Usage`, `Tenant`, target, status | mensagem SQS |
| `stats` | Percentis em janela deslizante para `/stats` | eventos concluídos | JSON |

Fronteiras críticas:
- `api → budget`: `reserve(tenantId, period, estimatedTokens) → (reservationId, ok)`; `settle(reservationId, realTokens)`. Atômico no store (DynamoDB `UpdateItem` com `ConditionExpression`).
- `resilience → backend`: erro tipado. `transient` = timeout, 5xx, 429 do provedor, conexão recusada; `permanent` = 4xx de validação, modelo inexistente, chave inválida. Só `transient` gera retry/fallback.
- `usage → SQS`: JSON `usage_event` v1: `{event_version, request_id, tenant_id, tier, alias, provider, model, prompt_tokens, completion_tokens, ttft_ms, latency_ms, status, error_class, attempts:[{provider, model, status, latency_ms}], ts}`. Nunca contém mensagens.
- `trace`: `trace_events(request_id, seq, type, node, payload_json)` com `type` ∈ `auth, ratelimit, budget_reserve, route, backend_attempt, backend_result, budget_settle, usage_publish, error`. `payload_json` só metadados; teste garante ausência de `messages`, `content`, `prompt`, `answer`.

### Stack e desvios do default

Defaults da casa (Next.js+Vercel, Make, API Claude) não se aplicam: não há interface, não é integração low-code, não é agente. Desvios registrados:
- **Go, stdlib `net/http` (Go 1.22+ mux), sem framework.** Objetivo do repo é mostrar Go; framework esconde o que se quer mostrar. Libs: `aws-sdk-go-v2` (DynamoDB, SQS), `go.opentelemetry.io/otel` + exporter OTLP, `golang.org/x/time/rate`. Testes com `testing` + `httptest`; `testcontainers-go` opcional para LocalStack.
- **DynamoDB** para tenants, budget e `requests`/`trace_events`. Um store só, condição atômica nativa, serverless no deploy real. Tabelas: `tenants` (pk `api_key_hash`), `budgets` (pk `tenant_id`, sk `period`), `requests` (pk `request_id`), `trace_events` (pk `request_id`, sk `seq`).
- **SQS** para `usage_event`. Desacopla do repo 4; fila DLQ desde o início.
- **OTel Collector + Grafana Tempo + Prometheus + Grafana** no `docker-compose` de dev (mesma família do Módulo 01/10 do curso). Não é dependência de runtime: gateway funciona sem collector.
- **LocalStack Community** via compose; `tflocal` no CI; Terraform em `infra/` com módulos `dynamodb`, `sqs`, `ecs-fargate` (este último só `plan` no CI, `apply` manual documentado).
- **Ollama** como backend `free`; **OpenRouter** e **Gemini** como pagos. Ordem e modelos em `config/routing.yaml`.
- **Docker compose** (não Podman) neste repo, decisão da nota-mãe.

### Mini-ADRs

### Mini-ADR: budget reservado antes da chamada, com estimativa
**Contexto:** debitar depois permite estourar orçamento em paralelo; checar-e-debitar em dois passos tem corrida.
**Decisão:** `reserve` atômico com estimativa pessimista antes do backend; `settle` acerta com uso real; reserva órfã expira por TTL.
**Alternativas descartadas:** débito pós-chamada (corrida); lock distribuído (complexidade sem ganho numa tabela com condição nativa).
**Consequências:** tenant no limite pode ser recusado com folga ainda existente (estimativa pessimista). Fica fácil auditar: toda reserva vira `trace_event`. Eixo inegociável: nunca ultrapassar orçamento. Sinal de mudança: se >5% das recusas `402` ocorrerem com saldo real >10% do budget por 4 semanas, revisar fórmula de estimativa.

### Mini-ADR: rate limit em memória, não em Redis
**Contexto:** uma instância no escopo; Redis adiciona serviço e custo.
**Decisão:** token bucket por chave em memória (`x/time/rate`); interface `Limiter` para trocar depois.
**Alternativas descartadas:** ElastiCache/Redis (multi-instância que não existe); DynamoDB (latência por request).
**Consequências:** limite é por instância; ao escalar horizontalmente, o limite efetivo multiplica. Sinal de mudança: segunda réplica em produção.

### Mini-ADR: alias + cascata por tier, com regra fixa para o forte
**Contexto:** roteador dinâmico por custo/latência é difícil de testar e explicar; alias estático perde tiering.
**Decisão:** `alias × tier → []BackendTarget` em YAML versionado; `nuva/smart` em tier `premium` vai direto ao forte, sem tentar barato.
**Alternativas descartadas:** roteador dinâmico (não determinístico); só alias (sem budget por tier).
**Consequências:** política auditável em diff de config; adicionar provedor é adaptador + linha de YAML. Eixo inegociável: determinismo da rota dado (alias, tier, saúde dos backends). Sinal de mudança: pedido real de roteamento por latência medida (repo 2 pode gerar o dado).

### Riscos e pontos de validação

| Risco | Impacto | Validação concreta |
|---|---|---|
| Contagem de tokens em streaming difere por provedor (OpenRouter só com `include_usage`; Gemini nativo tem formato próprio; Ollama campos próprios) | budget errado, $/1M errado no repo 4 | Spike de 1h: uma chamada streaming em cada provedor, gravar payload final de `usage`; fixar em testes de contrato com fixtures reais |
| LocalStack Community sem paridade total em DynamoDB TTL/condições | teste verde local, comportamento diferente na AWS | Rodar suíte de `budget` contra tabela real uma vez antes de publicar (`terraform apply` temporário, destruir depois) |
| Ollama lento/frio no Windows derruba TTFT e dispara fallback pago à toa | custo inesperado, métricas enganosas | `readyz` faz warm-up; timeout de TTFT separado do timeout total; fallback pago só em tier que permite |
| Go 1.23.2 desatualizado; libs OTel/AWS podem exigir versão maior | build quebra | Atualizar Go antes do `go mod init`; fixar `go` directive no `go.mod` |
| Chave OpenRouter/Gemini vazando em log ou fixture | segredo público em repo público | Logger com chaves proibidas testado; `gitleaks` no pre-commit e no CI; chaves só por env |
| SSE proxy com backpressure e cancelamento de cliente | goroutine leak, budget reservado sem settle | Teste com cliente que fecha no meio: settle com tokens parciais, `trace_event error` com `client_closed` |

### Passos de implementação (ordem)

1. Bootstrap: atualizar Go, `go mod init`, layout `cmd/gateway`, `internal/{api,auth,ratelimit,budget,router,backend,resilience,trace,usage,stats,config}`, `config/routing.yaml`, `docker-compose.yml` (LocalStack, Ollama opcional externo, OTel stack), CI (`go vet`, `go test -race`, `gitleaks`, `golangci-lint`).
2. Contratos internos: `ChatRequest`, `ChatChunk`, `Usage`, `BackendTarget`, erro tipado; testes de tabela.
3. Backend Ollama (não streaming, depois streaming) com fixtures reais; `/v1/models`; `/healthz`.
4. `api` OpenAI-compatible passando direto para um target fixo; SSE; `X-Request-Id`; teste `httptest` end-to-end.
5. `trace` + logger com chaves proibidas (teste de lista) + `requests`/`trace_events` em DynamoDB LocalStack.
6. `auth` + `ratelimit` (tenants em DynamoDB, seed via script).
7. `budget` reserve/settle atômico + TTL de reserva órfã; teste de concorrência (`-race`, 50 goroutines no limite).
8. `router` + `resilience` (YAML, cascata por tier, retry/fallback, classificação de erro); backends OpenRouter e Gemini.
9. `usage` → SQS + DLQ; `stats`.
10. OTel spans e métricas; dashboard Grafana provisionado em JSON.
11. Terraform `infra/` + `tflocal` no CI; doc de `apply` real em Fargate.
12. README com arquitetura, tabela "item do post → onde no código", "nota da Pós → decisão", benchmark inicial (repo 2 refina), limitações conhecidas.

Gates antes de publicar: `engenharia:revisao-codigo`, `engenharia:security-audit` (categoria L), checklist `engenharia:agente-em-producao` no que couber (requestId, logger, limites), Codex review.

## Deviations

(vazio; preenchido por `engenharia:implementacao`)

---

*Este documento alimenta a implementação (`engenharia:implementacao`). Não há orçamento comercial: projeto de portfólio, sem `pricing-calculator` nem `proposta-tecnica`.*
