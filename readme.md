# Gitflow Workflow with Azure DevOps CI/CD Integration

This document outlines the **Gitflow workflow** for managing a Git repository in **Azure DevOps** and provides step-by-step instructions for integrating it with a **CI/CD pipeline** using `.yaml` files and **Library variable groups** per **Microsoft Fabric workspace**. The workflow follows the traditional Gitflow model, emphasizing structured branching for development, releases, and hotfixes.

## Table of Contents
1. [Overview of Gitflow](#overview-of-gitflow)
2. [Branch Types](#branch-types)
3. [Gitflow Workflow](#gitflow-workflow)
4. [Azure DevOps Setup](#azure-devops-setup)
5. [CI/CD Integration with Azure Pipelines](#cicd-integration-with-azure-pipelines)
6. [Step-by-Step Instructions](#step-by-step-instructions)
7. [Example Azure Pipelines YAML](#example-azure-pipelines-yaml)
8. [Best Practices](#best-practices)
9. [Troubleshooting](#troubleshooting)

---

## Overview of Gitflow

Gitflow is a robust Git branching model that organizes development, release preparation, and maintenance tasks using defined branches. It is ideal for projects with scheduled releases, versioning, and multiple contributors. This guide uses the traditional Gitflow approach, where release branches are created from `develop` and merged into `main` and `develop` after deployment.

### Key Features
- **Structured Branching**: Separates feature development, release preparation, and production fixes.
- **Stable Production Code**: The `main` branch reflects production-ready code.
- **CI/CD Integration**: Automates testing, building, and deployment using Azure DevOps Pipelines.

---

## Branch Types

1. **`main` (or `master`)**
   - Represents the production-ready codebase.
   - Contains only stable, deployed code with version tags (e.g., `v1.0.0`).

2. **`develop`**
   - Integration branch for ongoing development.
   - Contains tested features preparing for the next release.

3. **Feature Branches**
   - Short-lived branches for new features (e.g., `feature/login-system`).
   - Branched from `develop`, merged back into `develop`.

4. **Release Branches**
   - Created from `develop` for preparing a production release (e.g., `release/v1.1.0`).
   - Used for final tweaks, bug fixes, and testing.
   - Merged into `main` and `develop` after deployment, then deleted.

5. **Hotfix Branches**
   - Created from `main` to fix critical production issues (e.g., `hotfix/v1.1.1`).
   - Merged into `main` and `develop`, then deleted.

---

## Gitflow Workflow

The workflow follows these steps:

1. **Feature Development**:
   - Create feature branches from `develop`.
   - Merge back to `develop` after testing and code review.

2. **Release Preparation**:
   - Create a release branch from `develop` (e.g., `release/v1.1.0`).
   - Apply bug fixes, documentation updates, and version bumps.
   - Test thoroughly.

### Diagram
    main:        o----o (v1.0.0) -------------------o (v1.1.0)
                  \                               /
    hotfix:        \                             h1 (hotfix/v1.1.1) ----o (v1.1.1)
                    \                                          
    develop:         o----o----o----o----o----o----o----o-------o
                           \                 /
    feature:               f1               f2
    release:                         o----o (release/v1.1.0)





---

## Azure DevOps Setup

Azure DevOps is used for repository management and CI/CD orchestration. Key components include:

- **Repositories**: Host Git repositories with `main`, `develop`, and other branches.
- **Pipelines**: Defined in `.yaml` files to automate testing, building, and deployment.
- **Library Variable Groups**: Store environment-specific variables (e.g., connection strings, API keys) per **Microsoft Fabric workspace** (e.g., `dev-workspace`, `prod-workspace`).

### Prerequisites
- An Azure DevOps project with a Git repository.
- A Microsoft Fabric workspace for development and production environments.
- Library variable groups set up in Azure DevOps (e.g., `fabric-dev`, `fabric-prod`) with variables like `FABRIC_CONNECTION_STRING` or `DEPLOYMENT_KEY`.

---

## CI/CD Integration with Azure Pipelines

Azure Pipelines automates the Gitflow process using `.yaml` files stored in the repository. The pipeline:
- **CI**: Runs tests and builds on `feature/*`, `develop`, `release/*`, and `hotfix/*` branches.
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
     git branch develop
     git push origin main develop
     ```
   - Protect `main` and `develop` branches in Azure DevOps:
     - Go to **Repos > Branches**, select `main` and `develop`, and enable **Branch Policies** (e.g., require PRs, minimum reviewers).

### 2. **Configure Library Variable Groups**
   - In Azure DevOps, go to **Pipelines > Library**.
   - Create variable groups for each Fabric workspace:
     - `fabric-dev`: Variables like `FABRIC_CONNECTION_STRING`, `ENVIRONMENT=dev`.
     - `fabric-prod`: Variables like `FABRIC_CONNECTION_STRING`, `ENVIRONMENT=prod`.
   - Link variable groups to pipelines with appropriate permissions.

### 3. **Develop a Feature**
   - Create a feature branch from `develop`:
     ```bash
     git checkout develop
     git branch feature/login-system
     git checkout feature/login-system
     ```
   - Commit changes:
     ```bash
     git add .
     git commit -m "Add login system"
     git push origin feature/login-system
     ```
   - Create a pull request in Azure DevOps to merge into `develop`.
   - Pipeline runs tests (defined in `azure-pipelines.yml`) on the feature branch.

### 4. **Create a Release Branch**
   - Create a release branch from `develop`:
     ```bash
     git checkout develop
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
   - Pipeline deploys `main` to the production Fabric workspace using `fabric-prod` variables.
   - Merge the release branch into `develop`:
     ```bash
     git checkout develop
     git merge --no-ff release/v1.1.0
     git push origin develop
     ```
   - Delete the release branch:
     ```bash
     git branch -d release/v1.1.0
     git push origin --delete release/v1.1.0
     ```

### 7. **Handle Hotfixes**
   - Create a hotfix branch from `main`:
     ```bash
     git checkout main
     git branch hotfix/v1.1.1
     git checkout hotfix/v1.1.1
     git push origin hotfix/v1.1.1
     ```
   - Apply fixes:
     ```bash
     git commit -m "Fix critical bug in v1.1.1"
     git push origin hotfix/v1.1.1
     ```
   - Pipeline tests the hotfix branch using `fabric-dev` variables.
   - Merge into `main` and tag:
     ```bash
     git checkout main
     git merge --no-ff hotfix/v1.1.1
     git tag v1.1.1
     git push origin main v1.1.1
     ```
   - Pipeline deploys `main` to production using `fabric-prod` variables.
   - Merge into `develop`:
     ```bash
     git checkout develop
     git merge --no-ff hotfix/v1.1.1
     git push origin develop
     ```
   - Delete the hotfix branch:
     ```bash
     git branch -d hotfix/v1.1.1
     git push origin --delete hotfix/v1.1.1
     ```

---

## Example Azure Pipelines YAML

Create a file named `azure-pipelines.yml` in the repository root:

```yaml
trigger:
  branches:
    include:
      - main
      - develop
      - feature/*
      - release/*
      - hotfix/*

pr:
  branches:
    include:
      - develop
      - release/*

variables:
  - group: fabric-dev  # Default variable group for testing
  - name: isMain
    value: ${{ eq(variables['Build.SourceBranch'], 'refs/heads/main') }}

stages:
  - stage: Test
    displayName: 'Run Tests and Build'
    jobs:
      - job: TestAndBuild
        pool:
          vmImage: 'ubuntu-latest'
        steps:
          - checkout: self
          - task: UseNode@1
            inputs:
              version: '16.x'
          - script: |
              npm install
              npm test
            displayName: 'Run Tests'
          - script: |
              npm run lint
            displayName: 'Lint Code'
          - script: |
              npm run build
            displayName: 'Build Artifact'
          - publish: $(System.DefaultWorkingDirectory)/dist
            artifact: build-output
            displayName: 'Publish Build Artifact'

  - stage: DeployStaging
    displayName: 'Deploy to Staging'
    condition: and(succeeded(), ne(variables['Build.SourceBranch'], 'refs/heads/main'))
    jobs:
      - deployment: DeployStaging
        environment: 'staging'
        pool:
          vmImage: 'ubuntu-latest'
        variables:
          - group: fabric-dev
        strategy:
          runOnce:
            deploy:
              steps:
              - checkout: self
              - script: |
                  echo "Deploying to Fabric dev workspace with $(FABRIC_CONNECTION_STRING)"
                  # Add Fabric deployment commands (e.g., az fabric deploy)
                displayName: 'Deploy to Staging'

  - stage: DeployProduction
    displayName: 'Deploy to Production'
    condition: and(succeeded(), eq(variables['isMain'], true))
    jobs:
      - deployment: DeployProduction
        environment: 'production'
        pool:
          vmImage: 'ubuntu-latest'
        variables:
          - group: fabric-prod
        strategy:
          runOnce:
            deploy:
              steps:
              - checkout: self
              - script: |
                  echo "Deploying to Fabric prod workspace with $(FABRIC_CONNECTION_STRING)"
                  # Add Fabric deployment commands (e.g., az fabric deploy)
                displayName: 'Deploy to Production'