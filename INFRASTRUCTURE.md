# Infrastructure Setup Guide

Everything needed to provision the Unified E-Commerce Data Platform infrastructure from scratch.
Follow this guide top to bottom — each step depends on the one before it.

---

## What gets provisioned

| Layer | Service | Region |
|---|---|---|
| Source database | AWS RDS PostgreSQL db.t3.micro | eu-west-1 |
| Raw landing zone | S3 ecommerce-lakehouse-{account}-raw-01 | eu-west-1 |
| Bronze Delta tables | S3 ecommerce-lakehouse-{account}-bronze-01 | eu-west-1 |
| Silver Delta tables | S3 ecommerce-lakehouse-{account}-silver-01 | eu-west-1 |
| Gold Delta tables | S3 ecommerce-lakehouse-{account}-gold-01 | eu-west-1 |
| Streaming broker | AWS MSK kafka.m5.large | eu-west-1 — spin up when needed |
| CDC connector | Debezium on EC2 t3.small | eu-west-1 — spin up when needed |
| Orchestration | Airflow MWAA | eu-west-1 — spin up when needed |
| Compute | Databricks on AWS | eu-west-1 — next step |

> **Cost rule:** Only RDS and S3 run continuously. MSK, EC2, and MWAA are
> provisioned before a session and destroyed after. This keeps idle cost near zero.

---

## Prerequisites

```bash
# AWS CLI configured
aws sts get-caller-identity

# Terraform installed
terraform version

# In the project folder
cd "/path/to/ecommerce-lakehouse"
```

---

## Step 1 — Environment file

Create `.env` in the project root. This file is gitignored — never commit it.

```bash
cp .env.example .env
# Fill in your actual values
```

Load before running any command:

```bash
export $(cat .env | grep -v "#" | xargs)
```

---

## Step 2 — RDS PostgreSQL

### What it is
The core OLTP database. Every order, customer, payment, and inventory record lives here.
Configured for CDC from day one so Debezium can read the WAL.

### Provision

```bash
cd terraform
export TF_VAR_db_password="YourPassword123"
export TF_VAR_db_username="postgres_admin"
terraform init
terraform apply -target=aws_db_instance.postgres
```

Takes ~5 minutes. Watch for `Creation complete`.

### Verify

```bash
aws rds describe-db-instances \
  --query 'DBInstances[*].[DBInstanceIdentifier,DBInstanceStatus]' \
  --output table \
  --region eu-west-1
```

Expected: `staff-de-journey-postgres | available`

### Reset password if forgotten

```bash
aws rds modify-db-instance \
  --db-instance-identifier staff-de-journey-postgres \
  --master-user-password "NewPassword123" \
  --apply-immediately \
  --region eu-west-1
```

Password rules: 8+ characters, no `@` `/` `"` or spaces.

### Stop to save cost when not developing

```bash
aws rds stop-db-instance \
  --db-instance-identifier staff-de-journey-postgres \
  --region eu-west-1
```

### Key config decisions

| Setting | Value | Why |
|---|---|---|
| Instance class | db.t3.micro | Free tier eligible — 750 hrs/month |
| WAL level | logical | Required for Debezium CDC |
| Public access | true (dev only) | Set false in production |
| Encryption | true | Always on — costs nothing |
| Region | eu-west-1 | Must match all other resources |

---

## Step 3 — S3 Medallion Buckets

### What they are
Four buckets — one per medallion layer. Data flows Raw → Bronze → Silver → Gold.
Raw is immutable. Bronze is ingested. Silver is cleaned. Gold is business-ready.

### Why four separate buckets (not prefixes)

| Reason | Detail |
|---|---|
| Access control | Analysts get Gold read-only. Engineers get Bronze/Silver read-write. |
| Lifecycle policies | Raw goes to Glacier after 90 days. Gold stays hot forever. |
| Cost visibility | AWS Cost Explorer shows cost per layer separately. |
| Blast radius | A pipeline bug fills one bucket, not all four. |

### Provision via AWS CLI

> Terraform had region conflict issues during this project build (buckets created
> in wrong region after repeated destroy/recreate cycles). Use AWS CLI directly.

```bash
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)

for layer in raw-01 bronze-01 silver-01 gold-01; do
  aws s3api create-bucket \
    --bucket ecommerce-lakehouse-${ACCOUNT}-${layer} \
    --region eu-west-1 \
    --create-bucket-configuration LocationConstraint=eu-west-1
  echo "Created: ecommerce-lakehouse-${ACCOUNT}-${layer}"
done
```

### Apply security and versioning

```bash
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)

for layer in raw-01 bronze-01 silver-01 gold-01; do
  NAME="ecommerce-lakehouse-${ACCOUNT}-${layer}"

  aws s3api put-bucket-versioning \
    --bucket $NAME \
    --versioning-configuration Status=Enabled \
    --region eu-west-1

  aws s3api put-bucket-encryption \
    --bucket $NAME \
    --server-side-encryption-configuration \
    '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}' \
    --region eu-west-1

  aws s3api put-public-access-block \
    --bucket $NAME \
    --public-access-block-configuration \
    "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true" \
    --region eu-west-1

  echo "✅ $NAME configured"
done
```

### Verify

```bash
aws s3 ls --region eu-west-1 | grep ecommerce-lakehouse
```

Expected: 4 buckets listed with creation timestamps.

### Bucket naming explained

```
ecommerce-lakehouse - 467091806172 - bronze - 01
      ↑                    ↑           ↑       ↑
  project name        account ID    layer   version
```

Account ID guarantees global uniqueness. `-01` leaves room for parallel environments.

---

## Step 4 — MSK Kafka (spin up when needed)

> Only provision MSK when working on streaming sources (04, 05, 15, 16).
> Cost: ~$0.63/hr. Always destroy after the session.

### Provision

```bash
cd terraform
export TF_VAR_db_password="YourPassword123"
terraform apply -target=aws_msk_cluster.main
```

Takes ~25 minutes.

### Destroy after session

```bash
terraform destroy -target=aws_msk_cluster.main
```

---

## Step 5 — EC2 Debezium Host (spin up when needed)

> Only needed for CDC work (Source 02). Stop the instance between sessions.

### Start

```bash
aws ec2 start-instances \
  --instance-ids i-042dbb1daaa085576 \
  --region eu-west-1
```

### SSH

```bash
ssh -i ~/.ssh/staff-de-journey-key.pem ec2-user@56.228.75.74
```

### Start Kafka Connect + register Debezium connector

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto
/opt/debezium/kafka/bin/connect-distributed.sh \
  /opt/debezium/connect-distributed.properties > /tmp/connect.log 2>&1 &
sleep 15
curl -s -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @/opt/debezium/connector.json
```

### Stop after session

```bash
aws ec2 stop-instances \
  --instance-ids i-042dbb1daaa085576 \
  --region eu-west-1
```

---

## Full teardown

Destroys everything except S3 buckets (which retain data).

```bash
cd terraform
export TF_VAR_db_password="YourPassword123"
terraform destroy
```

To also delete S3 buckets:

```bash
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
for layer in raw-01 bronze-01 silver-01 gold-01; do
  aws s3 rb s3://ecommerce-lakehouse-${ACCOUNT}-${layer} --force --region eu-west-1
done
```

---

## Cost summary

| Resource | Cost | When |
|---|---|---|
| RDS db.t3.micro | ~$0/month free tier | Always running |
| S3 (all 4 buckets) | ~$0.023/GB/month | Always running |
| MSK kafka.m5.large | ~$0.63/hr | Only when streaming |
| EC2 t3.small | ~$0.023/hr | Only when CDC active |
| MWAA smallest | ~$0.49/hr | Only when orchestrating |
| Databricks | ~$2-3/session | Only when pipelines run |

**Per demo session cost: ~$2-3**
**Idle cost (RDS + S3 only): ~$0-5/month**

---

## Lessons learned during this build

**S3 bucket region conflicts**
If you create S3 buckets in the wrong region and then change the Terraform region variable,
Terraform gets confused trying to manage buckets it thinks are in the new region but are
actually in the old one. Fix: manually delete the old buckets with
`aws s3 rb --force --region old-region` before applying with the new region.

**S3 OperationAborted on repeated create/delete**
AWS needs up to 60 minutes to release a bucket name after deletion if the same name
was created and deleted multiple times in quick succession. Fix: add a `-01` or timestamp
suffix to the bucket name to avoid the conflict entirely.

**Terraform state vs reality**
When Terraform times out or is force-cancelled (Ctrl+C), the state file may not match
what actually exists in AWS. Always run `terraform plan` before `terraform apply` and
check for drift. Use `terraform state rm` to remove resources that exist in state
but not in AWS.

**Password rules for RDS**
AWS RDS master password: minimum 8 characters. The characters `@`, `/`, `"`, and space
are not allowed. This catches out most people on first attempt.

**Never commit credentials**
Add to `.gitignore` before the first commit:
```
.env
*.tfstate
*.tfstate.backup
terraform.tfvars
*.csv
*.docx
.DS_Store
**/.DS_Store
.terraform/
```
