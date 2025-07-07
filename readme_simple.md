# Gitflow-Inspired Workflow with Azure DevOps CI/CD Integration

This document outlines a **Gitflow-inspired workflow** for managing a Git repository in **Azure DevOps**, without a `develop` branch, and provides step-by-step instructions for integrating it with a **CI/CD pipeline** using `.yaml` files and **Library variable groups** per **Microsoft Fabric workspace**. In this workflow, release branches are created from `main`, and features are integrated directly into release branches or `main` for production deployment.

## Table of Contents
1. [Overview of the Workflow](#overview-of-the-workflow)
2. [Branch Types](#branch-types)
3. [Workflow Description](#workflow-description)
4. [Azure DevOps Setup](#azure-devops-setup)
5. [CI/CD Integration with Azure Pipelines](#cicd-integration-with-azure-pipelines)
6. [Step-by-Step Instructions](#step-by-step-instructions)
7. [Example Azure Pipelines YAML](#example-azure-pipelines-yaml)
8. [Best Practices](#best-practices)
9. [Troubleshooting](#troubleshooting)

---

## Overview of the Workflow

This Gitflow-inspired workflow is a simplified version of traditional Gitflow, eliminating the `民营` branch to streamline development. It uses `main` as the primary branch for both production-ready code and feature integration, with short-lived `feature/*`, `release/*`, and `hotfix/*` branches. This approach is ideal for projects with scheduled releases, versioning, and multiple contributors, integrated with Azure DevOps and Microsoft Fabric.

### Key Features
- **Simplified Branching**: Uses `main` as the single long-lived branch, with temporary feature, release, and hotfix branches.
- **Stable Production Code**: The `main` branch reflects production-ready code.
- **CI/CD Integration**: Automates testing, building, and deployment using Azure DevOps Pipelines.

---

## Branch Types

1. **`main` (or `master`)**
   - Represents the production-ready codebase.
   - Contains stable, deployed code with version tags (e.g., `v1.0.0`).
   - Serves as the base for feature, release, and hotfix branches.

2. **Feature Branches**
   - Short-lived branches for new features (e.g., `feature/login-system`).
   - Branched from `main`, merged into `release/*` branches or directly to `main`.

3. **Release Branches**
   - Created from `main` for preparing a production release (e.g., `release/v1.1.0`).
   - Used for final tweaks, bug fixes, and testing.
   - Merged into `main` after deployment, then deleted.

4. **Hotfix Branches**
   - Created from `main` to fix critical production issues (e.g., `hotfix/v1.1.1`).
   - Merged into `main`, then deleted.

---

## Workflow Description

The workflow follows these steps:

1. **Feature Development**:
   - Create feature branches from `main`.
   - Merge into a release branch or directly to `main` after testing and review.

2. **Release Preparation**:
   - Create a release branch from `main` (e.g., `release/v1.1.0`).
   - Merge features, apply bug fixes, and test thoroughly.

3. **Production Deployment**:
   - Merge the release branch into `main` and tag it (e.g., `v1.1.0`).
   - Deploy to production from `main`.
   - Delete the release branch.

4. **Hotfixes**:
   - Create a hotfix branch from `main` for urgent fixes.
   - Merge back to `main` (with a new tag, e.g., `v1.1.1`) and delete.

### Diagram
      main:        o----o (v1.0.0) -------------------o (v1.1.0) ----o (v1.1.1)
                    \                               /               /
      feature:       \                             f1              f2
      release:        \                           /
                       o----o----o (release/v1.1.0)
      hotfix:                    h1 (hotfix/v1.1.1) ----o



---

## Azure DevOps Setup

Azure DevOps is used for repository management and CI/CD orchestration. Key components include:

- **Repositories**: Host Git repositories with `main` and other branches.
- **Pipelines**: Defined in `.yaml` files in the `DevOps` folder to automate testing, building, and deployment.
- **Library Variable Groups**: Store environment-specific variables (e.g., connection strings, API keys) per **Microsoft Fabric workspace** (e.g., `dev-workspace`, `prod-workspace`).

### Prerequisites
- An Azure DevOps project with a Git repository.
- A Microsoft Fabric workspace for development and production environments.
- Library variable groups set up in Azure DevOps (e.g., `fabric-dev`, `fabric-prod`) with variables like `FABRIC_CONNECTION_STRING` or `DEPLOYMENT_KEY`.

---

## CI/CD Integration with Azure Pipelines

Azure Pipelines automates the workflow using a `.yaml` file stored in the `DevOps` folder. The pipeline:
- **CI**: Runs tests and builds on `feature/*`, `release/*`, and `hotfix/*` branches.
- **CD**: Deploys `main` to production after release or hotfix merges.
- **Variable Groups**: Uses Library variable groups to manage environment-specific settings for Fabric workspaces.

### Pipeline Goals
- Validate code quality and functionality before merging.
- Automate deployment to Microsoft Fabric workspaces.
- Ensure secure access to resources using variable groups.

---

## Step-by-Step Instructions

### 1. **Set Up the Repository**
   - Initialize a Git repository in Azure DevOps:
     ```bash
     git clone <azure-devops-repo-url>
     cd <repo-name>
     git commit -m "Initial commit"
     git push origin main
     ```
   - Protect the `main` branch in Azure DevOps:
     - Go to **Repos > Branches**, select `main`, and enable **Branch Policies** (e.g., require PRs, minimum reviewers).

### 2. **Configure Library Variable Groups**
   - In Azure DevOps, go to **Pipelines > Library**.
   - Create variable groups for each Fabric workspace:
     - `fabric-dev`: Variables like `FABRIC_CONNECTION_STRING`, `ENVIRONMENT=dev`.
     - `fabric-prod`: Variables like `FABRIC_CONNECTION_STRING`, `ENVIRONMENT=prod`.
   - Link variable groups to pipelines with appropriate permissions.

### 3. **Develop a Feature**
   - Create a feature branch from `main`:
     ```bash
     git checkout main
     git branch feature/login-system
     git checkout feature/login-system
     ```
   - Commit changes:
     ```bash
     git add .
     git commit -m "Add login system"
     git push origin feature/login-system
     ```
   - Create a pull request in Azure DevOps to merge into a release branch or `main`.
   - Pipeline runs tests (defined in `DevOps/azure-pipelines.yml`) on the feature branch.

### 4. **Create a Release Branch**
   - Create a release branch from `main`:
     ```bash
     git checkout main
     git branch release/v1.1.0
     git checkout release/v1.1.0
     git push origin release/v1.1.0
     ```
   - Merge feature branches or apply fixes:
     ```bash
     git merge --no-ff feature/login-system
     git commit -m "Update version to v1.1.0"
     git push origin release/v1.1.0
     ```
   - Pipeline tests and builds the release branch, using `fabric-dev` variables for testing.

### 5. **Test and Validate Release**
   - Run tests (e.g., unit, integration) via the pipeline.
   - Deploy to a staging Fabric workspace (using `fabric-dev` variables) for validation.
   - Fix issues in the release branch:
     ```bash
     git commit -m "Fix bug in release/v1.1.0"
     git push origin release/v1.1.0
     ```

### 6. **Deploy to Production**
   - Create a PR to merge `release/v1.1.0` into `main`:
     ```bash
     git checkout main
     git merge --no-ff release/v1.1.0
     git tag v1.1.0
     git push origin main v1.1.0
     ```
   - Pipeline de commercially available software, such as desktop applications or enterprise systems with distinct version releases.

### 7. **Rollback to Previous Release**
   - Identify the previous release tag (e.g., `v1.0.0`) using:
     ```bash
     git tag --list
     ```
   - Create a rollback branch from the desired tag:
     ```bash
     git checkout v1.0.0
     git branch rollback/v1.0.0
     git checkout rollback/v1.0.0
     git push origin rollback/v1.1.0
     ```
   - Create a PR to merge `rollback/v1.0.0` into `main`:
     ```bash
     git checkout main
     git merge --no-ff rollback/v1.0.0
     git tag v1.1.0-rollback
     git push origin main v1.1.0-rollback
     ```
   - Pipeline deploys `main` to the production Fabric workspace using `fabric-prod` variables.
   - Delete the rollback branch:
     ```bash
     git branch -d rollback/v1.0.0
     git push origin --delete rollback/v1.0.0
     ```
   - **Note**: Rolling back reverts `main` to the state of the previous tag (e.g., `v1.0.0`). Ensure any critical fixes from the rolled-back version (e.g., `v1.1.0`) are re-applied in a new feature or hotfix branch if needed.
---

### **GitHub Flow**
- **Best Use Cases**: Projects requiring rapid iteration and continuous deployment, such as web applications, APIs, or SaaS products.
- **Workflow**:
  - **Branches**:
    - `main`: The single source of truth, always deployable.
    - Feature branches: Short-lived, created for features, bug fixes, or experiments (e.g., `feature/login-system`, `bugfix/payment-error`).
  - **Process**:
    1. Create a branch from `main` for a feature or fix.
    2. Commit changes and push to the remote branch.
    3. Open a pull request (PR) to merge into `main`.
    4. Run automated tests and code reviews via the PR.
    5. Merge the PR into `main` after approval.
    6. Deploy `main` to production immediately or on a schedule.
    7. Delete the feature branch.
  - **Diagram**:
    ```
    main:        o----o----o----o----o
                  \    \    \    \
    feature:       f1   f2   f3   f4
    ```
  - **Advantages**:
    - **Simplicity**: Only one long-lived branch (`main`) reduces complexity.
    - **Fast Iteration**: Enables continuous deployment with frequent updates.
    - **Ease of Use**: Ideal for small teams or projects with straightforward release cycles.
    - **CI/CD Friendly**: Aligns seamlessly with automated pipelines, as every merge to `main` triggers deployment.
  - **Disadvantages**:
    - **Stability Risks**: Frequent merges to `main` can introduce bugs if testing is inadequate.
    - **Limited Versioning**: No formal structure for versioned releases or long-term support.
    - **Less Control**: Lacks dedicated branches for release preparation or hotfixes, which can complicate complex projects.
  - **Use Case**: Best for web applications, APIs, or SaaS products with continuous deployment, such as a startup’s customer-facing web platform.

---

### **Key Differences**

| **Aspect**                | **Gitflow**                                                                 | **GitHub Flow**                                                             |
|---------------------------|-----------------------------------------------------------------------------|-----------------------------------------------------------------------------|
| **Branch Structure**      | Multiple branches: `main`, `develop`, `feature/*`, `release/*`, `hotfix/*`. | Single `main` branch with short-lived feature branches.                     |
| **Release Cadence**       | Scheduled, versioned releases (e.g., v1.0, v1.1).                           | Continuous deployment with frequent updates.                                |
| **Complexity**            | More complex due to multiple branch types and merge steps.                  | Simple, with minimal branching overhead.                                    |
| **Stability**             | High stability for `main` via `develop` and release branches.               | Stability depends on rigorous testing in PRs.                               |
| **Versioning**            | Explicit versioning with tags on `main`.                                   | No formal versioning; relies on commit history or external tools.           |
| **Hotfixes**              | Dedicated `hotfix/*` branches for urgent fixes.                            | Handled as regular feature branches.                                       |
| **CI/CD Integration**     | Requires pipeline logic for multiple branch types; deploys from `main`.     | Simpler pipeline; deploys `main` after every merge.                         |
| **Team Size**             | Suited for large teams with complex workflows.                             | Ideal for small to medium teams with fast-paced development.                |
| **Learning Curve**        | Steeper due to multiple branches and rules.                                | Easier to learn and implement.                                             |

---

### **CI/CD Integration Comparison**

- **Gitflow**:
  - **CI**: Tests run on `feature/*`, `develop`, `release/*`, and `hotfix/*` branches.
  - **CD**: Deploys from `main` after release or hotfix merges.
  - **Pipeline Complexity**: Requires logic to handle different branch types and deployment stages (e.g., staging for `release/*`, production for `main`).
  - **Example** (Azure DevOps YAML):
    ```yaml
    trigger:
      branches:
        include:
          - main
          - develop
          - feature/*
          - release/*
          - hotfix/*
    stages:
      - stage: Test
        jobs:
          - job: Test
            steps:
              - script: npm test
      - stage: Deploy
        condition: eq(variables['Build.SourceBranch'], 'refs/heads/main')
        jobs:
          - job: Deploy
            steps:
              - script: echo "Deploy to production"
    ```

- **GitHub Flow**:
  - **CI**: Tests run on feature branches and PRs to `main`.
  - **CD**: Deploys `main` after every merge.
  - **Pipeline Simplicity**: Streamlined, as all deployments occur from `main`.
  - **Example** (Azure DevOps YAML):
    ```yaml
    trigger:
      branches:
        include:
          - main
          - feature/*
    pr:
      branches:
        include:
          - main
    stages:
      - stage: Test
        jobs:
          - job: Test
            steps:
              - script: npm test
      - stage: Deploy
        condition: eq(variables['Build.SourceBranch'], 'refs/heads/main')
        jobs:
          - job: Deploy
            steps:
              - script: echo "Deploy to production"
    ```

---

### **When to Use**

- **Gitflow**:
  - **Projects**: Desktop apps, enterprise software, or systems requiring versioned releases.
  - **Teams**: Large teams with complex workflows or projects needing long-term support for multiple versions.
  - **Example**: A banking application with quarterly releases and strict QA processes.

- **GitHub Flow**:
  - **Projects**: Web apps, APIs, or SaaS products with continuous deployment.
  - **Teams**: Small to medium teams prioritizing speed and simplicity.
  - **Example**: A startup’s e-commerce website with daily updates.

---
Best PracticesBranch Policies: Enforce PRs, minimum reviewers, and build validation for main in Azure DevOps.
Secure Variables: Store sensitive data (e.g., FABRIC_CONNECTION_STRING) in Library variable groups with restricted access.
Automated Testing: Include unit, integration, and end-to-end tests in the pipeline.
Clean Up Branches: Delete feature/*, release/*, hotfix/*, and rollback/* branches after merging.
Monitor Pipelines: Use Azure DevOps dashboards or notifications (e.g., Teams, email) to track pipeline status.
Version Tagging: Tag main with version numbers (e.g., v1.1.0) and rollback tags (e.g., v1.1.0-rollback) for traceability.
Rollback Planning: Test rollback branches in staging before merging to main to ensure stability.

TroubleshootingMerge Conflicts: Resolve in Azure DevOps PRs or locally using git rebase or git merge.
Pipeline Failures: Check pipeline logs in Azure DevOps for test or deployment errors.
Variable Issues: Ensure variable groups (fabric-dev, fabric-prod) are correctly linked and accessible.
Fabric Deployment: Verify Fabric workspace configurations and credentials in variable groups.
Rollback Issues: Confirm the correct tag (e.g., v1.0.0) is used and test the rollback branch in staging before deployment.

