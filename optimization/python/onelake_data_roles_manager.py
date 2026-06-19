"""
Production-ready manager for Microsoft Fabric OneLake Data Access Roles.

Features:
- Create / update / delete roles (including PII_FULL_ACCESS and NO_PII_COLUMNS patterns)
- Add / remove members (Microsoft Entra users, groups, service principals)
- Manage table/folder level access (Object-Level Security)
- Row-Level Security (RLS) via T-SQL predicates
- Column-Level Security (CLS) via permitted columns per table
- Uses batch + granular single-role APIs with safe merge/upsert patterns
- Service Principal / Managed Identity / DefaultAzureCredential support
- Retry logic, ETag optimistic concurrency, structured logging, proper error handling

Prerequisites:
- pip install azure-identity requests
- Service Principal must be added as **Member** (or higher) in the target workspace
- Tenant admin must have enabled Service Principals in Fabric Developer settings
- Scope used: https://api.fabric.microsoft.com/.default
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict, List, Optional, Union
from urllib.parse import quote

import requests
from azure.core.credentials import TokenCredential
from azure.identity import ClientSecretCredential, DefaultAzureCredential

logger = logging.getLogger(__name__)


class OneLakeSecurityError(Exception):
    """Base exception for OneLake security operations."""
    pass


class OneLakeDataAccessRoleManager:
    """
    Manages OneLake Data Access Roles (security roles) for a Fabric item
    (typically a Lakehouse or Mirrored Database).

    Example initialization with Service Principal:
        cred = ClientSecretCredential(
            tenant_id="your-tenant-id",
            client_id="your-client-id",
            client_secret="your-client-secret"
        )
        manager = OneLakeDataAccessRoleManager(
            credential=cred,
            workspace_id="...",
            item_id="..."  # Lakehouse item ID
        )
    """

    def __init__(
        self,
        credential: TokenCredential,
        workspace_id: str,
        item_id: str,
        base_url: str = "https://api.fabric.microsoft.com/v1",
        max_retries: int = 5,
        retry_backoff: float = 1.5,
    ):
        self.credential = credential
        self.workspace_id = workspace_id
        self.item_id = item_id
        self.base_url = base_url.rstrip("/")
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        self.session = requests.Session()

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _get_access_token(self) -> str:
        """Get Entra ID token for Fabric API."""
        token = self.credential.get_token("https://api.fabric.microsoft.com/.default")
        return token.token

    def _get_headers(self, etag: Optional[str] = None, extra: Optional[Dict] = None) -> Dict[str, str]:
        headers = {
            "Authorization": f"Bearer {self._get_access_token()}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if etag:
            headers["If-Match"] = etag
        if extra:
            headers.update(extra)
        return headers

    def _request(
        self,
        method: str,
        url: str,
        *,
        json_body: Optional[Dict] = None,
        params: Optional[Dict] = None,
        headers: Optional[Dict] = None,
        timeout: int = 60,
    ) -> requests.Response:
        """Robust request with retry, logging, and error handling."""
        attempt = 0
        last_exception = None

        while attempt < self.max_retries:
            attempt += 1
            try:
                resp = self.session.request(
                    method=method,
                    url=url,
                    json=json_body,
                    params=params,
                    headers=headers or self._get_headers(),
                    timeout=timeout,
                )

                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", 5))
                    logger.warning(f"Rate limited. Retrying after {retry_after}s (attempt {attempt})")
                    time.sleep(retry_after)
                    continue

                if resp.status_code >= 400:
                    self._handle_error_response(resp)

                return resp

            except requests.exceptions.RequestException as e:
                last_exception = e
                wait = self.retry_backoff ** attempt
                logger.warning(f"Request failed (attempt {attempt}/{self.max_retries}): {e}. Retrying in {wait:.1f}s")
                time.sleep(wait)

        raise OneLakeSecurityError(f"Request failed after {self.max_retries} attempts") from last_exception

    def _handle_error_response(self, resp: requests.Response):
        try:
            error = resp.json()
            request_id = error.get("requestId") or resp.headers.get("x-ms-request-id")
            message = error.get("message", str(error))
        except Exception:
            message = resp.text
            request_id = resp.headers.get("x-ms-request-id")

        logger.error(f"Fabric API error {resp.status_code}: {message} | RequestId: {request_id}")
        raise OneLakeSecurityError(f"Fabric API error {resp.status_code}: {message} (RequestId: {request_id})")

    def _build_role(
        self,
        name: str,
        decision_rules: Optional[List[Dict]] = None,
        members: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Build a minimal valid role object."""
        role: Dict[str, Any] = {
            "name": name,
            "kind": "Policy",
        }
        if decision_rules:
            role["decisionRules"] = decision_rules
        if members:
            role["members"] = members
        return role

    # ------------------------------------------------------------------ #
    # Core Role Operations (Granular + Batch fallback)
    # ------------------------------------------------------------------ #
    def list_roles(self) -> List[Dict[str, Any]]:
        """List all data access roles for the item."""
        url = f"{self.base_url}/workspaces/{self.workspace_id}/items/{self.item_id}/dataAccessRoles"
        resp = self._request("GET", url)
        return resp.json().get("value", [])

    def get_role(self, role_name: str) -> Optional[Dict[str, Any]]:
        """Get a single role by name (tries granular endpoint first)."""
        # Try granular GET
        try:
            encoded_name = quote(role_name, safe="")
            url = f"{self.base_url}/workspaces/{self.workspace_id}/items/{self.item_id}/dataAccessRoles/{encoded_name}"
            resp = self._request("GET", url)
            return resp.json()
        except OneLakeSecurityError:
            pass  # Fall back to list

        # Fallback: filter from full list
        for role in self.list_roles():
            if role.get("name") == role_name:
                return role
        return None

    def upsert_role(
        self,
        role: Dict[str, Any],
        conflict_policy: str = "Overwrite",
    ) -> Dict[str, Any]:
        """
        Create or update a single role using the granular API (preferred).
        Falls back to batch merge if needed.
        """
        url = f"{self.base_url}/workspaces/{self.workspace_id}/items/{self.item_id}/dataAccessRoles"
        params = {"dataAccessRoleConflictPolicy": conflict_policy}

        try:
            resp = self._request(
                "POST",
                url,
                json_body=role,
                params=params,
            )
            etag = resp.headers.get("ETag")
            logger.info(f"Upserted role '{role.get('name')}' successfully. ETag: {etag}")
            return {"status": resp.status_code, "etag": etag, "role": resp.json() if resp.content else None}
        except OneLakeSecurityError as e:
            # Fallback to safe batch merge
            logger.warning(f"Granular upsert failed, falling back to batch merge: {e}")
            return self._upsert_role_via_batch(role)

    def _upsert_role_via_batch(self, new_role: Dict[str, Any]) -> Dict[str, Any]:
        """Safe batch upsert: GET current → merge single role → PUT."""
        current_roles = self.list_roles()
        role_name = new_role["name"]

        updated = False
        for i, existing in enumerate(current_roles):
            if existing.get("name") == role_name:
                current_roles[i] = new_role
                updated = True
                break

        if not updated:
            current_roles.append(new_role)

        return self._put_full_roles(current_roles)

    def _put_full_roles(self, roles: List[Dict[str, Any]], dry_run: bool = False) -> Dict[str, Any]:
        """Internal batch PUT (full replacement with ETag support)."""
        url = f"{self.base_url}/workspaces/{self.workspace_id}/items/{self.item_id}/dataAccessRoles"
        if dry_run:
            url += "?dryRun=true"

        # Get current ETag if possible (best effort)
        etag = None
        try:
            list_resp = self._request("GET", url.replace("?dryRun=true", ""))
            etag = list_resp.headers.get("ETag")
        except Exception:
            pass

        payload = {"value": roles}
        resp = self._request(
            "PUT",
            url,
            json_body=payload,
            headers=self._get_headers(etag=etag),
        )
        return {"status": resp.status_code, "etag": resp.headers.get("ETag")}

    def delete_role(self, role_name: str) -> bool:
        """Delete a single role (tries granular DELETE first)."""
        encoded_name = quote(role_name, safe="")
        url = f"{self.base_url}/workspaces/{self.workspace_id}/items/{self.item_id}/dataAccessRoles/{encoded_name}"

        try:
            self._request("DELETE", url)
            logger.info(f"Deleted role '{role_name}'")
            return True
        except OneLakeSecurityError:
            # Fallback: remove from batch and PUT
            logger.info(f"Granular delete not available or failed. Using batch removal for '{role_name}'")
            current = self.list_roles()
            filtered = [r for r in current if r.get("name") != role_name]
            if len(filtered) == len(current):
                logger.warning(f"Role '{role_name}' not found")
                return False
            self._put_full_roles(filtered)
            return True

    # ------------------------------------------------------------------ #
    # High-level convenience methods
    # ------------------------------------------------------------------ #
    def add_or_update_members(
        self,
        role_name: str,
        entra_members: Optional[List[Dict[str, str]]] = None,
        fabric_item_members: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """
        Add or replace members in a role.
        entra_members example:
            [{"objectId": "...", "objectType": "Group", "tenantId": "..."}]
        """
        role = self.get_role(role_name)
        if not role:
            raise OneLakeSecurityError(f"Role '{role_name}' does not exist. Create it first.")

        members = role.get("members", {})
        if entra_members:
            members["microsoftEntraMembers"] = entra_members
        if fabric_item_members:
            members["fabricItemMembers"] = fabric_item_members

        role["members"] = members
        return self.upsert_role(role)

    def remove_member(self, role_name: str, object_id: str) -> bool:
        """Remove a specific Entra member (user/group/SP) from a role."""
        role = self.get_role(role_name)
        if not role:
            return False

        members = role.get("members", {})
        entra = members.get("microsoftEntraMembers", [])
        new_entra = [m for m in entra if m.get("objectId") != object_id]

        if len(new_entra) == len(entra):
            return False  # Not found

        members["microsoftEntraMembers"] = new_entra
        role["members"] = members
        self.upsert_role(role)
        return True

    def set_table_or_folder_access(
        self,
        role_name: str,
        paths: List[str],
        action: str = "Read",
    ) -> Dict[str, Any]:
        """
        Set the Path permission for a role (Object-Level Security).
        paths examples: ["*"], ["/Tables/Sales"], ["/Files/bronze"]
        """
        decision_rule = {
            "effect": "Permit",
            "permission": [
                {"attributeName": "Path", "attributeValueIncludedIn": paths},
                {"attributeName": "Action", "attributeValueIncludedIn": [action]},
            ],
        }

        role = self.get_role(role_name) or self._build_role(role_name)
        role["decisionRules"] = [decision_rule]
        return self.upsert_role(role)

    def add_column_level_security(
        self,
        role_name: str,
        table_path: str,
        permitted_columns: List[str],
        column_action: str = "Read",
    ) -> Dict[str, Any]:
        """
        Add/update Column-Level Security (CLS) for a specific table.
        Only the listed columns will be visible.
        """
        role = self.get_role(role_name) or self._build_role(role_name)

        if "decisionRules" not in role or not role["decisionRules"]:
            role["decisionRules"] = [{"effect": "Permit", "permission": [
                {"attributeName": "Path", "attributeValueIncludedIn": ["*"]},
                {"attributeName": "Action", "attributeValueIncludedIn": ["Read"]}
            ]}]

        rule = role["decisionRules"][0]
        constraints = rule.setdefault("constraints", {})
        columns = constraints.setdefault("columns", [])

        # Remove existing entry for this table if present
        columns = [c for c in columns if c.get("tablePath") != table_path]
        columns.append({
            "tablePath": table_path,
            "columnNames": permitted_columns,
            "columnEffect": "Permit",
            "columnAction": [column_action],
        })
        constraints["columns"] = columns
        rule["constraints"] = constraints
        role["decisionRules"][0] = rule

        return self.upsert_role(role)

    def add_row_level_security(
        self,
        role_name: str,
        table_path: str,
        tsql_predicate: str,
    ) -> Dict[str, Any]:
        """
        Add/update Row-Level Security (RLS) using a T-SQL predicate.
        Example predicate: "Country = 'US' OR Department IN ('Finance', 'HR')"
        """
        role = self.get_role(role_name) or self._build_role(role_name)

        if "decisionRules" not in role or not role["decisionRules"]:
            role["decisionRules"] = [{"effect": "Permit", "permission": [
                {"attributeName": "Path", "attributeValueIncludedIn": ["*"]},
                {"attributeName": "Action", "attributeValueIncludedIn": ["Read"]}
            ]}]

        rule = role["decisionRules"][0]
        constraints = rule.setdefault("constraints", {})
        rows = constraints.setdefault("rows", [])

        # Remove existing for this table
        rows = [r for r in rows if r.get("tablePath") != table_path]
        rows.append({
            "tablePath": table_path,
            "value": tsql_predicate,
        })
        constraints["rows"] = rows
        rule["constraints"] = constraints
        role["decisionRules"][0] = rule

        return self.upsert_role(role)

    # ------------------------------------------------------------------ #
    # Convenience: PII role patterns requested by user
    # ------------------------------------------------------------------ #
    def ensure_pii_full_access_role(
        self,
        members: List[Dict[str, str]],
        paths: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Create/update PII_FULL_ACCESS role (sees everything, including PII columns).
        """
        paths = paths or ["*"]
        decision_rules = [{
            "effect": "Permit",
            "permission": [
                {"attributeName": "Path", "attributeValueIncludedIn": paths},
                {"attributeName": "Action", "attributeValueIncludedIn": ["Read"]},
            ],
        }]
        role = self._build_role("PII_FULL_ACCESS", decision_rules=decision_rules)
        role["members"] = {"microsoftEntraMembers": members}
        return self.upsert_role(role)

    def ensure_no_pii_columns_role(
        self,
        members: List[Dict[str, str]],
        pii_tables_and_safe_columns: Dict[str, List[str]],
        paths: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Create/update NO_PII_COLUMNS role with Column-Level Security.
        Example:
            pii_tables_and_safe_columns = {
                "/Tables/Customers": ["CustomerId", "Name", "City"],
                "/Tables/Employees": ["EmployeeId", "Department"]
            }
        """
        paths = paths or ["*"]
        decision_rules = [{
            "effect": "Permit",
            "permission": [
                {"attributeName": "Path", "attributeValueIncludedIn": paths},
                {"attributeName": "Action", "attributeValueIncludedIn": ["Read"]},
            ],
        }]

        role = self._build_role("NO_PII_COLUMNS", decision_rules=decision_rules)
        role["members"] = {"microsoftEntraMembers": members}

        # Apply CLS constraints
        constraints: Dict[str, Any] = {"columns": []}
        for table_path, safe_cols in pii_tables_and_safe_columns.items():
            constraints["columns"].append({
                "tablePath": table_path,
                "columnNames": safe_cols,
                "columnEffect": "Permit",
                "columnAction": ["Read"],
            })
        role["decisionRules"][0]["constraints"] = constraints

        return self.upsert_role(role)


# ---------------------------------------------------------------------- #
# Example usage
# ---------------------------------------------------------------------- #
if __name__ == "__main__":
    # Example with Service Principal
    credential = ClientSecretCredential(
        tenant_id="YOUR_TENANT_ID",
        client_id="YOUR_CLIENT_ID",
        client_secret="YOUR_CLIENT_SECRET",
    )

    manager = OneLakeDataAccessRoleManager(
        credential=credential,
        workspace_id="YOUR_WORKSPACE_ID",
        item_id="YOUR_LAKEHOUSE_ITEM_ID",
    )

    # 1. Create PII_FULL_ACCESS role
    pii_full_members = [
        {"objectId": "GROUP-OR-USER-OBJECT-ID", "objectType": "Group", "tenantId": "YOUR_TENANT_ID"}
    ]
    manager.ensure_pii_full_access_role(members=pii_full_members)

    # 2. Create NO_PII_COLUMNS role with CLS
    no_pii_members = [
        {"objectId": "ANOTHER-GROUP-OBJECT-ID", "objectType": "Group", "tenantId": "YOUR_TENANT_ID"}
    ]
    safe_columns = {
        "/Tables/Customers": ["CustomerKey", "FullName", "City", "State"],
        "/Tables/Orders": ["OrderKey", "OrderDate", "Amount"],
    }
    manager.ensure_no_pii_columns_role(
        members=no_pii_members,
        pii_tables_and_safe_columns=safe_columns
    )

    # 3. Add RLS example
    manager.add_row_level_security(
        role_name="NO_PII_COLUMNS",
        table_path="/Tables/Orders",
        tsql_predicate="Region = 'West'"
    )

    # 4. Add/remove members later
    manager.add_or_update_members(
        role_name="PII_FULL_ACCESS",
        entra_members=[{"objectId": "NEW-USER-ID", "objectType": "User", "tenantId": "..."}]
    )

    print("OneLake security roles configured successfully.")
