# ai-gateway

Gateway LLM compatível com OpenAI, em Go. Visão geral no `README.md`, documentação em `docs/`.

## Agente remoto (Antigravity Remote Agents, Codex cloud)

Vale para agente que roda em VM limpa na nuvem, só com o que está no GitHub.

- Sem acesso ao vault Second Brain, a arquivos `.env*` nem a bancos locais. Se a tarefa depender disso, pare e pergunte.
- Trabalhe em branch nova a partir de `main` e abra pull request. Nunca faça push direto em `main` e nunca mescle o PR.
- Não rode deploy, migração em banco real nem dispare workflow do GitHub Actions.
- Nunca escreva segredo, token ou dado de cliente em código, teste, commit ou descrição de PR.
- Use Go 1.27.
- Não rode os testes de integração (`-tags integration`): dependem de LocalStack e Terraform. O CI roda no PR.
- Repositório público: nada de dado real de cliente, chave ou endereço interno.

Antes de abrir o PR, rode e informe o resultado no PR:

```bash
go vet ./...
go test -race ./...
```
