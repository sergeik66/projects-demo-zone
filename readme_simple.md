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
