data "aws_region" "current" {}

resource "aws_ecs_cluster" "this" {
  name = var.cluster_name
}

resource "aws_cloudwatch_log_group" "this" {
  name              = "/ecs/${var.service_name}"
  retention_in_days = var.log_retention_days
}

# --- Task execution role: pulls the image from ECR, ships logs, reads secrets ---

resource "aws_iam_role" "execution" {
  name = "${var.service_name}-execution"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "execution" {
  name = "${var.service_name}-execution"
  role = aws_iam_role.execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat([
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents",
        ]
        Resource = "${aws_cloudwatch_log_group.this.arn}:*"
      },
      {
        Effect   = "Allow"
        Action   = ["ecr:GetAuthorizationToken"]
        Resource = "*"
      }
      ],
      var.ecr_repository_arn == null ? [] : [{
        Effect = "Allow"
        Action = [
          "ecr:BatchCheckLayerAvailability",
          "ecr:GetDownloadUrlForLayer",
          "ecr:BatchGetImage",
        ]
        Resource = var.ecr_repository_arn
      }],
      length(var.secrets) == 0 ? [] : [{
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue", "ssm:GetParameters"]
        Resource = values(var.secrets)
      }]
    )
  })
}

# --- Task role: what the running container itself is allowed to call ---

resource "aws_iam_role" "task" {
  name = "${var.service_name}-task"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "task" {
  name = "${var.service_name}-task"
  role = aws_iam_role.task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat(
      length(var.dynamodb_table_arns) == 0 ? [] : [{
        Effect = "Allow"
        Action = [
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:UpdateItem",
          "dynamodb:DeleteItem",
          "dynamodb:Query",
          "dynamodb:DescribeTable",
        ]
        Resource = var.dynamodb_table_arns
      }],
      var.usage_queue_arn == null ? [] : [{
        Effect   = "Allow"
        Action   = ["sqs:SendMessage", "sqs:GetQueueUrl"]
        Resource = var.usage_queue_arn
      }]
    )
  })
}

resource "aws_ecs_task_definition" "this" {
  family                   = var.service_name
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = tostring(var.cpu)
  memory                   = tostring(var.memory)
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task.arn

  container_definitions = jsonencode([
    {
      name      = var.service_name
      image     = var.image
      essential = true
      portMappings = [
        { containerPort = var.container_port, protocol = "tcp" }
      ]
      # TRUST_PROXY=true: this module always fronts the task with the ALB it
      # creates, which sets X-Forwarded-For on every request. Without it the
      # pre-auth IP limiter sees the ALB's own IP for all callers and one
      # bucket of 60 auth failures 429s every tenant. See docs/deploy-fargate.md.
      environment = [
        for k, v in merge({ TRUST_PROXY = "true" }, var.environment) : { name = k, value = v }
      ]
      # Provider API keys and other secrets are injected via valueFrom
      # references (Secrets Manager ARN or SSM parameter ARN), never as
      # plain environment values. See docs/deploy-fargate.md.
      secrets = [
        for k, v in var.secrets : { name = k, valueFrom = v }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.this.name
          "awslogs-region"        = data.aws_region.current.name
          "awslogs-stream-prefix" = var.service_name
        }
      }
    }
  ])
}

# --- Security groups: one for the ALB (public), one for the task (private) ---
#
# The task SG allows the container port from the ALB SG *only*, never from a
# CIDR. That is what stops anyone from talking to the task directly (and
# bypassing TLS and any future WAF) when the task ends up with a public IP.

locals {
  create_security_groups = length(var.alb_security_group_ids) == 0 && length(var.task_security_group_ids) == 0
  alb_security_groups    = local.create_security_groups ? [aws_security_group.alb[0].id] : var.alb_security_group_ids
  task_security_groups   = local.create_security_groups ? [aws_security_group.task[0].id] : var.task_security_group_ids
}

resource "aws_security_group" "alb" {
  count       = local.create_security_groups ? 1 : 0
  name        = "${var.service_name}-alb"
  description = "Public ingress to the ai-gateway ALB (443, and 80 for the redirect)"
  vpc_id      = var.vpc_id

  ingress {
    description = "HTTPS from the internet"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = var.alb_ingress_cidr_blocks
  }

  ingress {
    description = "HTTP, redirected to HTTPS by the listener"
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = var.alb_ingress_cidr_blocks
  }

  egress {
    description = "To the task"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "task" {
  count       = local.create_security_groups ? 1 : 0
  name        = "${var.service_name}-task"
  description = "ai-gateway task: container port from the ALB security group only"
  vpc_id      = var.vpc_id

  ingress {
    description     = "Gateway port, from the ALB only"
    from_port       = var.container_port
    to_port         = var.container_port
    protocol        = "tcp"
    security_groups = [aws_security_group.alb[0].id]
  }

  egress {
    description = "Outbound to AWS APIs and LLM providers"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# --- ALB in front of the service, health-checked on /readyz ---

resource "aws_lb" "this" {
  name               = "${var.service_name}-alb"
  load_balancer_type = "application"
  internal           = false
  subnets            = var.subnet_ids
  security_groups    = local.alb_security_groups

  lifecycle {
    precondition {
      condition     = local.create_security_groups || (length(var.alb_security_group_ids) > 0 && length(var.task_security_group_ids) > 0)
      error_message = "Supply both alb_security_group_ids and task_security_group_ids, or neither (the module then creates the pair itself). One alone would leave the other on the VPC default security group."
    }
  }
}

resource "aws_lb_target_group" "this" {
  name        = "${var.service_name}-tg"
  port        = var.container_port
  protocol    = "HTTP"
  vpc_id      = var.vpc_id
  target_type = "ip"

  health_check {
    path                = "/readyz"
    matcher             = "200"
    interval            = 30
    timeout             = 5
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }
}

resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_lb.this.arn
  port              = 80
  protocol          = "HTTP"

  # Always redirect plaintext to HTTPS: acm_certificate_arn is required
  # (see variables.tf), so there is never a case where this module serves
  # the gateway over plain HTTP.
  default_action {
    type = "redirect"
    redirect {
      port        = "443"
      protocol    = "HTTPS"
      status_code = "HTTP_301"
    }
  }
}

resource "aws_lb_listener" "https" {
  load_balancer_arn = aws_lb.this.arn
  port              = 443
  protocol          = "HTTPS"
  ssl_policy        = "ELBSecurityPolicy-TLS13-1-2-2021-06"
  certificate_arn   = var.acm_certificate_arn

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.this.arn
  }
}

resource "aws_ecs_service" "this" {
  name            = var.service_name
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.this.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets         = var.subnet_ids
    security_groups = local.task_security_groups
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.this.arn
    container_name   = var.service_name
    container_port   = var.container_port
  }

  depends_on = [aws_lb_listener.http, aws_lb_listener.https]
}

output "cluster_name" {
  value = aws_ecs_cluster.this.name
}

output "alb_dns_name" {
  value = aws_lb.this.dns_name
}
