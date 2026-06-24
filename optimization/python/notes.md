Key Production Features Included

Safe upserts with granular API + batch fallback + ETag support
Retry logic with exponential backoff + 429 handling
Comprehensive error handling with Fabric requestId
Full support for RLS (constraints.rows) and CLS (constraints.columns)
Member management (Entra ID users/groups/service principals + virtual fabricItemMembers)
Specific helpers for the exact roles you requested (PII_FULL_ACCESS / NO_PII_COLUMNS)
Clean separation of concerns and extensibility

Notes / Recommendations

Test thoroughly in a non-production workspace first (feature is relatively new).
Consider deleting or editing the DefaultReader role if you want strict role-based access only.
For very large numbers of roles, prefer the batch list_roles() + _put_full_roles() pattern.
Monitor Fabric capacity and API throttling.

This class is ready to be dropped into a Fabric notebook, Azure DevOps pipeline step, or local automation script. Let me know if you need enhancements (e.g., YAML/JSON config loader, diff reporting, or integration with your existing secrets/Key Vault patterns).

 pii_full_members = [
        {"objectId": "06743794-d68d-49e0-9f3f-70fe952b28af", "objectType": "Group", "tenantId": tenant_id}
    ]

    [{'objectId': '06743794-d68d-49e0-9f3f-70fe952b28af', 'objectType': 'Group', 'tenantId': '[REDACTED]'}]
