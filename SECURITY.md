# Security Policy

## Reporting a vulnerability

Please **do not** open a public issue for a security problem.

Report it privately through GitHub's ["Report a vulnerability"](https://github.com/Mouraovicente/ai-gateway/security/advisories/new)
form, or by email to <mouraovicente1@gmail.com> with `[ai-gateway security]`
in the subject.

Useful in a report: what you did, what happened, what you expected, and the
smallest reproduction you have (a `curl` is perfect). Please do not include
third-party data or anyone's real API key.

Expect an acknowledgement within 7 days and, for anything confirmed, a fix
or a written decision within 30 days. This is a portfolio project maintained
by one person in his own time — that is the honest turnaround, not an SLA.

## Scope

In scope: this repository's code — the gateway itself (`cmd/`, `internal/`),
the Terraform modules under `infra/`, the container image, and the CI
workflow.

Out of scope:

- `docker-compose.yml` and everything it starts. It is a local development
  environment, documented as such: Grafana has a throwaway admin password,
  LocalStack accepts any credentials, the OTLP receiver accepts any
  telemetry. Every port is bound to `127.0.0.1`.
- `config/tenants.dev.yaml`, `scripts/seed_tenants.go` and `bench/`. The API
  keys there (`dev-free-key`, `bench-key`, ...) are deliberately public
  fakes for local development.
- Findings that require an attacker to already control the operator's
  configuration (`config/routing.yaml`, environment variables, AWS
  credentials).
- Reports produced only by a scanner, with no described impact.

## No bug bounty

There is no bug bounty and no payment for reports. Credit in the fix commit
is offered gladly if you want it.

## Known gaps

Tracked, deliberate, and documented in [`docs/security.md`](docs/security.md):
API key hashing has no pepper, and there is no key revocation/expiry field
yet. Both need a `tenants` table migration and are listed there rather than
quietly omitted.
