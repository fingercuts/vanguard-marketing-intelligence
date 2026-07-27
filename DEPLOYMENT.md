# Cloud Deployment & Infrastructure Guide

This guide details the continuous deployment architecture, security credentials mapping, and setup operations required to deploy the Vanguard Marketing Streamlit Dashboard to Google Cloud Run.

---

## 1. CD Architecture Overview

```
GitHub Repository (Push)
        ↓
GitHub Actions Runner
        ↓ (OIDC Identity Federation token exchange)
Google Cloud IAM (Role assume)
        ↓
Google Artifact Registry (Docker push)
        ↓
Google Cloud Run (Scale-to-zero serving)
```

---

## 2. GCP Infrastructure Setup Instructions

Follow these commands to configure the Google Cloud environment.

### A. Enable Required APIs
Ensure the container build, registry, and serverless compute APIs are active:
```bash
gcloud services enable \
    artifactregistry.googleapis.com \
    run.googleapis.com \
    iamcredentials.googleapis.com
```

### B. Create Google Artifact Registry (GAR)
Create a secure Docker registry to host the container images:
```bash
gcloud artifacts repositories create vanguard-docker-registry \
    --repository-format=docker \
    --location=us-central1 \
    --description="Vanguard Marketing Platform Container Images"
```

### C. Configure Workload Identity Federation (OIDC)
Avoid storing static IAM Service Account keys in GitHub Secrets. Set up secure OpenID Connect mapping:

1. Create the Identity Pool:
   ```bash
   gcloud iam workload-identity-pools create github-actions-pool \
       --location="global" \
       --display-name="GitHub Actions Pool"
   ```

2. Create the Identity Provider:
   ```bash
   gcloud iam workload-identity-pools providers create-oidc github-provider \
       --workload-identity-pool="github-actions-pool" \
       --location="global" \
       --issuer-uri="https://token.actions.githubusercontent.com" \
       --attribute-mapping="google.subject=assertion.subject,attribute.actor=assertion.actor,attribute.repository=assertion.repository"
   ```

3. Create the Deployer Service Account:
   ```bash
   gcloud iam service-accounts create github-actions-deployer \
       --display-name="GitHub Actions CD Deployer"
   ```

4. Grant access to the GitHub Repository:
   ```bash
   gcloud iam service-accounts add-iam-policy-binding github-actions-deployer@vanguard-marketing-prod.iam.gserviceaccount.com \
       --role="roles/iam.workloadIdentityUser" \
       --member="principalSet://iam.googleapis.com/projects/123456789/locations/global/workloadIdentityPools/github-actions-pool/attribute.repository/YOUR_GITHUB_ORG/marketing-data-projects"
   ```

5. Assign execution permissions:
   * **Artifact Registry Writer**: to push images.
   * **Cloud Run Developer**: to deploy services.

---

## 3. Cloud Run Host Sizing
The dashboard container is deployed with optimized resources to prevent idle charges:
- **CPU**: `1 vCPU`
- **Memory**: `512 MiB`
- **Auto-Scaling**: Max 2 instances, Min 0 instances. When there are no active users browsing, Cloud Run scales to **0 active containers**, resulting in **$0.00 base cost**.
