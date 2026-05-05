# Infrastructure — Terraform

Two independent Terraform modules provision the cloud resources used by the PaySim pipeline.

```
infra/terraform/
├── gcp/          # GCS landing bucket + fraud-loader service account
└── snowflake/    # Database, schemas, warehouse, roles, grants
```

---

## Quick start

### Prerequisites
| Tool | Min version |
|------|-------------|
| Terraform | 1.5 |
| gcloud CLI | any recent | (GCP module)
| Snowflake key-pair set up | — | (Snowflake module)

---

## GCP module

```bash
cd infra/terraform/gcp

cp terraform.tfvars.example terraform.tfvars
# edit terraform.tfvars with your real project ID

gcloud auth application-default login   # or set GOOGLE_APPLICATION_CREDENTIALS

terraform init
terraform plan
terraform apply
```

**Resources created**

| Resource | Name pattern |
|----------|-------------|
| GCS bucket | `<project>-paysim-landing` |
| IAM bindings | `objectCreator` + `objectViewer` on the bucket for the pre-existing `fraud-loader` SA |

> The `fraud-loader` service account and its key (`secrets/fraud-loader-key.json`) are  
> pre-existing — Terraform does **not** create or manage them. The email is derived  
> automatically from the project ID: `fraud-loader@<project>.iam.gserviceaccount.com`.

---

## Snowflake module

```bash
cd infra/terraform/snowflake

cp terraform.tfvars.example terraform.tfvars
# edit terraform.tfvars with your org, account, user

terraform init
terraform plan
terraform apply
```

**Resources created**

| Resource | Name |
|----------|------|
| Database | `FRAUD_DB` |
| Schemas | `RAW` · `KITCHEN` · `MARTS` |
| Warehouse | `FRAUD_WH` (X-Small, auto-suspend 5 min) |
| Roles | `FRAUD_LOADER` · `FRAUD_ANALYST` |
| Grants | Loader: write to RAW/KITCHEN/MARTS · Analyst: read MARTS |

Auth uses the existing RSA key-pair at `secrets/snowflake_key.p8`.

---

## Security notes

* `terraform.tfvars` is git-ignored — never commit it.
* `secrets/fraud-loader-key.json` is also git-ignored — keep it local only.
* Terraform derives the SA email from the project ID — no IAM API call is made, so `iam.googleapis.com` does not need to be enabled just to run `plan`/`apply`.
* In production, replace the key file with **Workload Identity Federation** (no exported keys needed).

