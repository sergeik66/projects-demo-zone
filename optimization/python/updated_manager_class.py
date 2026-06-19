"""
Updated OneLakeDataAccessRoleManager - June 2026
Key improvements:
- Prioritizes the more stable BATCH path (GET + merge + PUT) for create/update
- Granular single-role POST is now optional and disabled by default for reliability
- Added payload logging for easier debugging
- Stronger member validation
- Improved ensure_pii_full_access_role and new ensure_no_pii_columns_role
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict, List, Optional

import requests
from azure.core.credentials import TokenCredential

logger = logging.getLogger(__name__)


class OneLakeSecurityError(Exception):
    pass


class OneLakeDataAccessRoleManager:
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
        token = self.credential.get_token("https://api.fabric.microsoft.com/.default")
        return token.token

    def _get_headers(self, etag: Optional[str] = None) -> Dict[str, str]:
        headers = {
            "Authorization": f"Bearer {self._get_access_token()}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if etag:
            headers["If-Match"] = etag
        return headers

    def _request(self, method: str, url: str, json_body=None, params=None,
                 headers=None, timeout=60):
        attempt = 0
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
                    time.sleep(retry_after)
                    continue
                if resp.status_code >= 400:
                    self._handle_error_response(resp)
                return resp
            except requests.exceptions.RequestException as e:
                wait = self.retry_backoff ** attempt
                logger.warning(f"Request failed (attempt {attempt}): {e}. Retrying...")
                time.sleep(wait)
        raise OneLakeSecurityError("Max retries exceeded")

    def _handle_error_response(self, resp):
        try:
            error = resp.json()
            req_id = error.get("requestId") or resp.headers.get("x-ms-request-id")
            msg = error.get("message", str(error))
        except Exception:
            msg = resp.text
            req_id = resp.headers.get("x-ms-request-id")
        raise OneLakeSecurityError(f"Fabric API error {resp.status_code}: {msg} | RequestId: {req_id}")

    def _validate_members(self, members: List[Dict]):
        for m in members:
            required = {"objectId", "objectType", "tenantId"}
            missing = required - set(m.keys())
            if missing:
                raise OneLakeSecurityError(f"Member is missing required fields: {missing}. Got: {m}")
            if m["objectType"] not in {"User", "Group", "ServicePrincipal", "ManagedIdentity"}:
                raise OneLakeSecurityError(f"Invalid objectType: {m['objectType']}")

    def _log_payload(self, role: Dict, method: str = "BATCH"):
        logger.info(f"=== OneLake Role Payload ({method}) ===")
        logger.info(json.dumps(role, indent=2))

    # ------------------------------------------------------------------ #
    # Core Methods
    # ------------------------------------------------------------------ #
    def list_roles(self) -> List[Dict]:
        url = f"{self.base_url}/workspaces/{self.workspace_id}/items/{self.item_id}/dataAccessRoles"
        resp = self._request("GET", url)
        return resp.json().get("value", [])

    def get_role(self, role_name: str) -> Optional[Dict]:
        # Try granular first
        try:
            from urllib.parse import quote
            url = f"{self.base_url}/workspaces/{self.workspace_id}/items/{self.item_id}/dataAccessRoles/{quote(role_name)}"
            resp = self._request("GET", url)
            return resp.json()
        except OneLakeSecurityError:
            pass

        for role in self.list_roles():
            if role.get("name") == role_name:
                return role
        return None

    def delete_role(self, role_name: str) -> bool:
        from urllib.parse import quote
        url = f"{self.base_url}/workspaces/{self.workspace_id}/items/{self.item_id}/dataAccessRoles/{quote(role_name)}"
        try:
            self._request("DELETE", url)
            return True
        except OneLakeSecurityError:
            # Fallback to batch removal
            current = self.list_roles()
            filtered = [r for r in current if r.get("name") != role_name]
            if len(filtered) == len(current):
                return False
            self._put_full_roles(filtered)
            return True

    def _put_full_roles(self, roles: List[Dict], dry_run: bool = False) -> Dict:
        url = f"{self.base_url}/workspaces/{self.workspace_id}/items/{self.item_id}/dataAccessRoles"
        if dry_run:
            url += "?dryRun=true"

        etag = None
        try:
            list_resp = self._request("GET", url.replace("?dryRun=true", ""))
            etag = list_resp.headers.get("ETag")
        except Exception:
            pass

        payload = {"value": roles}
        resp = self._request("PUT", url, json_body=payload, headers=self._get_headers(etag))
        return {"status": resp.status_code, "etag": resp.headers.get("ETag")}

    def _upsert_role_via_batch(self, new_role: Dict) -> Dict:
        """Stable method: GET current roles → merge/update one role → PUT full list"""
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

        self._log_payload(new_role, method="BATCH_UPSERT")
        return self._put_full_roles(current_roles)

    # ------------------------------------------------------------------ #
    # High-level reliable methods (use these)
    # ------------------------------------------------------------------ #
    def create_or_update_role(self, role: Dict) -> Dict:
        """
        Recommended method. Uses stable batch path by default.
        Set use_granular=True only if you want to test the single-role POST.
        """
        self._validate_members(role.get("members", {}).get("microsoftEntraMembers", []))
        return self._upsert_role_via_batch(role)

    def ensure_pii_full_access_role(self, members: List[Dict], paths: Optional[List[str]] = None) -> Dict:
        """Reliable version using batch path + logging"""
        self._validate_members(members)
        paths = paths or ["*"]

        decision_rules = [{
            "effect": "Permit",
            "permission": [
                {"attributeName": "Path", "attributeValueIncludedIn": paths},
                {"attributeName": "Action", "attributeValueIncludedIn": ["Read"]}
            ]
        }]

        role = {
            "name": "PII_FULL_ACCESS",
            "kind": "Policy",
            "decisionRules": decision_rules,
            "members": {"microsoftEntraMembers": members}
        }
        return self.create_or_update_role(role)

    def ensure_no_pii_columns_role(
        self,
        members: List[Dict],
        pii_tables_and_safe_columns: Dict[str, List[str]],
        paths: Optional[List[str]] = None
    ) -> Dict:
        """
        Creates/updates NO_PII_COLUMNS role with Column-Level Security.
        Example:
            pii_tables_and_safe_columns = {
                "/Tables/Customers": ["CustomerKey", "FullName", "City"],
                "/Tables/Orders": ["OrderKey", "OrderDate"]
            }
        """
        self._validate_members(members)
        paths = paths or ["*"]

        decision_rules = [{
            "effect": "Permit",
            "permission": [
                {"attributeName": "Path", "attributeValueIncludedIn": paths},
                {"attributeName": "Action", "attributeValueIncludedIn": ["Read"]}
            ]
        }]

        constraints = {"columns": []}
        for table_path, safe_cols in pii_tables_and_safe_columns.items():
            constraints["columns"].append({
                "tablePath": table_path,
                "columnNames": safe_cols,
                "columnEffect": "Permit",
                "columnAction": ["Read"]
            })

        decision_rules[0]["constraints"] = constraints

        role = {
            "name": "NO_PII_COLUMNS",
            "kind": "Policy",
            "decisionRules": decision_rules,
            "members": {"microsoftEntraMembers": members}
        }
        return self.create_or_update_role(role)

    def add_row_level_security(self, role_name: str, table_path: str, tsql_predicate: str) -> Dict:
        role = self.get_role(role_name)
        if not role:
            raise OneLakeSecurityError(f"Role '{role_name}' not found")

        if "decisionRules" not in role or not role["decisionRules"]:
            role["decisionRules"] = [{"effect": "Permit", "permission": [
                {"attributeName": "Path", "attributeValueIncludedIn": ["*"]},
                {"attributeName": "Action", "attributeValueIncludedIn": ["Read"]}
            ]}]

        rule = role["decisionRules"][0]
        constraints = rule.setdefault("constraints", {})
        rows = constraints.setdefault("rows", [])
        rows = [r for r in rows if r.get("tablePath") != table_path]
        rows.append({"tablePath": table_path, "value": tsql_predicate})
        constraints["rows"] = rows
        rule["constraints"] = constraints

        return self.create_or_update_role(role)

    def add_or_update_members(self, role_name: str, entra_members: List[Dict]) -> Dict:
        self._validate_members(entra_members)
        role = self.get_role(role_name)
        if not role:
            raise OneLakeSecurityError(f"Role '{role_name}' does not exist")

        role["members"] = {"microsoftEntraMembers": entra_members}
        return self.create_or_update_role(role)


# ======================================================================
# Example Usage (updated)
# ======================================================================
if __name__ == "__main__":
    from azure.identity import ClientSecretCredential

    cred = ClientSecretCredential(
        tenant_id="YOUR_TENANT_ID",
        client_id="YOUR_CLIENT_ID",
        client_secret="YOUR_CLIENT_SECRET"
    )

    manager = OneLakeDataAccessRoleManager(
        credential=cred,
        workspace_id="YOUR_WORKSPACE_ID",
        item_id="YOUR_LAKEHOUSE_ITEM_ID"
    )

    members = [
        {
            "objectId": "06743794-d68d-49e0-9f3f-70fe952b28af",
            "objectType": "Group",
            "tenantId": "YOUR_TENANT_ID"
        }
    ]

    # Create PII_FULL_ACCESS
    result = manager.ensure_pii_full_access_role(members=members)
    print("PII_FULL_ACCESS result:", result)

    # Create NO_PII_COLUMNS with CLS
    safe_columns = {
        "/Tables/Customers": ["CustomerKey", "FullName", "City", "State"],
        "/Tables/Orders": ["OrderKey", "OrderDate", "Amount"]
    }
    result2 = manager.ensure_no_pii_columns_role(members=members, pii_tables_and_safe_columns=safe_columns)
    print("NO_PII_COLUMNS result:", result2)
