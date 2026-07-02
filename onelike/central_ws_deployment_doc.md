# Microsoft Fabric – Centralized Workspace Deployment Model

**Version**: 1.0  
**Date**: June 2026  
**Owner**: [Your Team / Data Platform]  
**Status**: Recommended Enterprise Pattern

## 1. Executive Summary

This model uses a **single central Service Principal** for governance and ownership combined with **per-workspace managed identities** for secure, secret-less runtime execution. It provides strong separation of duties while enabling scalable, governed data platforms across multiple workspaces.

## 2. Key Components

| Component                    | Role                                      | Ownership / Permissions                          | Primary Use Case                          |
|-----------------------------|-------------------------------------------|--------------------------------------------------|-------------------------------------------|
| **Central Admin SP**        | Governance & Ownership                    | Admin on every workspace + Owner of all artifacts | CI/CD, deployment pipelines, admin tasks |
| **Workspace Identity** (per workspace) | Runtime execution                        | Viewer / Contributor (as needed) + OneLake roles | Pipelines, notebooks, shortcuts, Dataflows |
| **OneLake Security**        | Granular data access control              | Applied to Workspace Identities & external users | RLS, CLS, folder/table-level restrictions |
| **Workspaces**              | Logical boundaries                        | Managed by Central SP                            | Bronze / Silver / Gold or domain separation |

## 3. Permission Model

- **Central Service Principal**
  - Assigned **Admin** role in every workspace.
  - Set as **Owner** of all items (Lakehouses, Semantic Models, Pipelines, etc.).
  - Can manage workspace settings, capacities, and perform all administrative actions.

- **Workspace Identities**
  - Created automatically via Workspace Settings or REST API (`POST /v1/workspaces/{id}/provisionIdentity`).
  - Used for authentication in connections (shortcuts to ADLS Gen2, pipelines, etc.).
  - Assigned minimal roles in target workspaces (often **Viewer** + specific OneLake security roles for restrictions).
  - Enable **Trusted Workspace Access** for firewall-protected storage.

- **OneLake Security**
  - Central SP (as Admin) bypasses restrictions and has full access.
  - Workspace Identities are subject to OneLake roles when assigned Viewer-level access.
  - Recommended for cross-workspace shortcuts and external consumption.

## 4. Data Flow & Cross-Workspace Access

- Workspace Identities authenticate shortcuts and pipelines between workspaces.
- Central SP handles deployment and ownership.
- OneLake security policies travel with the data via shortcuts.

## 5. Benefits

- **Security**: No long-lived secrets for runtime identities; centralized ownership reduces orphaned artifacts.
- **Governance**: Single point of control for deployments and auditing.
- **Scalability**: Easy to add new workspaces/domains.
- **Operational Excellence**: Secret-less authentication for pipelines and shortcuts.
- **Compliance**: Clear audit trail (who owns what, who runs what).

## 6. Implementation Steps

1. Create/register the **Central Admin Service Principal** in Entra ID.
2. Enable required tenant settings (Service principals can use Fabric APIs, etc.).
3. For each workspace:
   - Assign Central SP as **Admin**.
   - Provision **Workspace Identity**.
   - Configure Workspace Identity in connections/shortcuts.
4. Set Central SP as **Owner** of existing and new artifacts (via portal or API where supported).
5. Define **OneLake security roles** for Workspace Identities and other principals.
6. Update pipelines/notebooks to use Workspace Identity authentication.
7. Test cross-workspace shortcuts and data flows.
8. Monitor via Microsoft Purview audit logs and Fabric activity logs.

## 7. Best Practices

- Use **least privilege** for Workspace Identities (prefer Viewer + OneLake roles over Contributor).
- Never use personal accounts as owners of production artifacts.
- Combine with **Trusted Workspace Access** for ADLS Gen2 shortcuts.
- Version-control all deployment scripts and use the Central SP for CI/CD (Azure DevOps / GitHub Actions).
- Regularly review workspace identity usage and OneLake security roles.
- Document workspace purpose and data domains clearly.

## 8. Limitations & Considerations

- Workspace Identities are tied to the workspace lifecycle.
- Some features may still require the Central SP for full administrative actions.
- Cross-tenant scenarios are not supported for Workspace Identities.
- Test thoroughly in a non-production capacity before rolling out.

## 9. References

- [Workspace identity – Microsoft Fabric](https://learn.microsoft.com/en-us/fabric/security/workspace-identity)
- [Provision Identity REST API](https://learn.microsoft.com/en-us/rest/api/fabric/core/workspaces/provision-identity)
- [OneLake security overview](https://learn.microsoft.com/en-us/fabric/onelake/security/get-started-security)
- [Authenticate with workspace identity](https://learn.microsoft.com/en-us/fabric/security/workspace-identity-authenticate)

---

**Next Steps**  
Would you like me to expand any section (e.g., detailed migration script examples, PowerShell/Python code for provisioning identities, or a more detailed security matrix)?
