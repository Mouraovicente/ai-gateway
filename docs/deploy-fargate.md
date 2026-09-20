# Deploy real em ECS Fargate (fora do LocalStack)

Este documento cobre o `apply` real, feito manualmente contra uma conta AWS de verdade. O CI só roda `tflocal plan` contra LocalStack, com `enable_fargate = false` (ver `.github/workflows/ci.yml`, job `terraform-plan`) — LocalStack Community não emula ECS/ALB o suficiente para validar esses recursos, então o módulo `ecs_fargate` fica fora do plano do CI por padrão.

## Pré-requisitos

1. Conta AWS real com credenciais configuradas (`aws configure` ou variáveis `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`).
2. Uma VPC com ao menos duas subnets (idealmente públicas, para o ALB). **Não passe security group nenhum**: o módulo cria o par certo sozinho — um SG no ALB aceitando 443/80 da internet e um SG na task aceitando a porta 8080 **só do SG do ALB**, nunca de um CIDR. Isso é o que impede alguém de falar direto com a porta 8080 da task, contornando o TLS (e qualquer WAF futuro). Se você precisa mesmo usar SGs já existentes, passe `alb_security_group_ids` **e** `task_security_group_ids` (os dois, não vazios — o módulo recusa o apply com só um, porque o outro cairia no SG default da VPC).
3. Um repositório ECR para a imagem do gateway.
4. Um certificado ACM para o domínio do gateway, **na mesma região** do ALB (`us-east-1` nos exemplos abaixo — ACM é regional, um certificado emitido em outra região não aparece como opção pro listener HTTPS). Peça e valide por DNS antes do apply:

   ```bash
   aws acm request-certificate \
     --domain-name gateway.example.com \
     --validation-method DNS \
     --region us-east-1
   # anote o CertificateArn retornado, e o(s) registro(s) CNAME de validação
   # (aws acm describe-certificate --certificate-arn <arn> --region us-east-1)
   # crie o(s) CNAME(s) no seu provedor de DNS e espere o status virar ISSUED:
   aws acm wait certificate-validated --certificate-arn <arn> --region us-east-1
   ```

   O ALB (`infra/modules/ecs_fargate`) sempre termina TLS: o listener HTTPS:443 exige esse certificado (`acm_certificate_arn`), e o listener HTTP:80 só existe para redirecionar 301 pra HTTPS — não há fallback HTTP puro. `enable_fargate=true` sem `acm_certificate_arn` falha o apply.

## Build e push da imagem para o ECR

```bash
aws ecr create-repository --repository-name ai-gateway --region us-east-1   # uma vez só
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin <account-id>.dkr.ecr.us-east-1.amazonaws.com

docker build -t ai-gateway:latest .
docker tag ai-gateway:latest <account-id>.dkr.ecr.us-east-1.amazonaws.com/ai-gateway:latest
docker push <account-id>.dkr.ecr.us-east-1.amazonaws.com/ai-gateway:latest
```

## TRUST_PROXY

O módulo sempre coloca a task atrás do ALB que ele mesmo cria, e o ALB sempre define `X-Forwarded-For`. Por isso `infra/modules/ecs_fargate/main.tf` já injeta `TRUST_PROXY=true` no `environment` da task definition — sem isso, o limiter pré-auth vê o IP do próprio ALB para todo mundo e um único bucket de 60 falhas de auth/min derruba `/v1/chat/completions` e `/stats` para todos os tenants.

## Secrets (chaves dos provedores)

As chaves dos provedores pagos (`OPENROUTER_API_KEY`, `GEMINI_API_KEY` — os nomes vêm de `api_key_env` em `config/routing.yaml`) nunca vão como `environment` plano na task definition. Crie-as no Secrets Manager e referencie por ARN:

```bash
aws secretsmanager create-secret --name ai-gateway/openrouter-api-key --secret-string '<chave>'
aws secretsmanager create-secret --name ai-gateway/gemini-api-key --secret-string '<chave>'
```

Depois, passe os ARNs pela variável raiz `provider_secret_arns` (chave = nome da env var, valor = ARN do secret) — nunca editando `main.tf` direto. Ou por `-var`:

```bash
-var 'provider_secret_arns={OPENROUTER_API_KEY="arn:aws:secretsmanager:us-east-1:<account-id>:secret:ai-gateway/openrouter-api-key-XXXXXX",GEMINI_API_KEY="arn:aws:secretsmanager:us-east-1:<account-id>:secret:ai-gateway/gemini-api-key-XXXXXX"}'
```

ou num `terraform.tfvars` local (já coberto por `*.tfvars` no `.gitignore` — não versione esse arquivo):

```hcl
provider_secret_arns = {
  OPENROUTER_API_KEY = "arn:aws:secretsmanager:us-east-1:<account-id>:secret:ai-gateway/openrouter-api-key-XXXXXX"
  GEMINI_API_KEY      = "arn:aws:secretsmanager:us-east-1:<account-id>:secret:ai-gateway/gemini-api-key-XXXXXX"
}
```

A execution role do módulo já tem permissão `secretsmanager:GetSecretValue` restrita a esses ARNs (ver `infra/modules/ecs_fargate/main.tf`).

## Apply

1. Use `terraform` puro (não `tflocal`) com credenciais reais exportadas: `tflocal` sempre sobrepõe os `endpoints` do LocalStack, `terraform` puro resolve pro endpoint real da AWS.
2. Passe as variáveis de rede, o repositório ECR e a imagem real:

   ```bash
   cd infra
   terraform init
   terraform plan \
     -var enable_fargate=true \
     -var image=<account-id>.dkr.ecr.us-east-1.amazonaws.com/ai-gateway:latest \
     -var ecr_repository_arn=arn:aws:ecr:us-east-1:<account-id>:repository/ai-gateway \
     -var acm_certificate_arn=arn:aws:acm:us-east-1:<account-id>:certificate/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx \
     -var vpc_id=vpc-xxxxxxxx \
     -var 'subnet_ids=["subnet-aaaa","subnet-bbbb"]' \
     -var 'security_group_ids=["sg-xxxxxxxx"]' \
     -out=plan.tfplan
   terraform apply plan.tfplan
   ```

   `ecr_repository_arn` e `acm_certificate_arn` são obrigatórios junto com `enable_fargate=true`: o primeiro escopa a permissão de pull de imagem da execution role a esse repositório específico (em vez de `Resource = "*"`); o segundo é o certificado que o listener HTTPS:443 do ALB termina — sem ele o apply falha.

3. Pegue o DNS do ALB no output `alb_dns_name` do módulo (`terraform output -module=gateway_service` ou o output do apply) e confirme o serviço de pé:

   ```bash
   curl -i http://<alb_dns_name>/readyz
   # esperado: HTTP/1.1 200 OK, body {"status":"ready"}
   ```

   Pode levar 1-2 minutos até o health check do target group considerar a task healthy.

## Custo

Fargate (512 CPU / 1024 MB, 1 task, `us-east-1`) + ALB ficam ligados 24/7 na casa de **algumas dezenas de dólares por mês** (ALB tem custo fixo por hora + por LCU, mesmo com tráfego baixo; a task Fargate cobra por vCPU/memória-hora). Não é caro, mas também não é gratuito — não deixe rodando sem necessidade.

## Destruir depois de testar

Se o apply foi só um teste (ex.: validar a suíte de budget contra tabelas reais uma vez), destrua tudo em seguida:

```bash
terraform destroy \
  -var enable_fargate=true \
  -var image=<account-id>.dkr.ecr.us-east-1.amazonaws.com/ai-gateway:latest \
  -var ecr_repository_arn=arn:aws:ecr:us-east-1:<account-id>:repository/ai-gateway \
  -var acm_certificate_arn=arn:aws:acm:us-east-1:<account-id>:certificate/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx \
  -var vpc_id=vpc-xxxxxxxx \
  -var 'subnet_ids=["subnet-aaaa","subnet-bbbb"]' \
  -var 'security_group_ids=["sg-xxxxxxxx"]'
```

## Limitações conhecidas

- LocalStack Community não simula ECS/ALB/IAM com fidelidade suficiente para validar esses recursos — por isso o CI só roda `tflocal plan` com `enable_fargate=false` (só DynamoDB + SQS) e uma validação estrutural (`terraform validate -var enable_fargate=true`), sem `plan`/`apply` contra LocalStack.
- Este repo não provisiona VPC/subnets — assume-se que já existem na conta (passe os IDs via `-var`). Os security groups, esses sim, o módulo cria (ALB e task separados); veja Pré-requisitos.
- Este repo também não provisiona o certificado ACM nem os registros DNS de validação — peça e valide manualmente (seção Pré-requisitos) antes do apply.
