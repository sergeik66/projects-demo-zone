import json
import logging
import re
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
        use_preview: bool = True,
        max_retries: int = 5,
        retry_backoff: float = 1.5,
    ):
        self.credential = credential
        self.workspace_id = workspace_id
        self.item_id = item_id
        self.base_url = base_url.rstrip("/")
        self.use_preview = use_preview
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        self.session = requests.Session()

    # ------------------------------------------------------------------ #
    # Validation
    # ------------------------------------------------------------------ #
    @staticmethod
    def _validate_role_name(name: str):
        """OneLake role names currently only allow letters and numbers."""
        if not re.match(r'^[A-Za-z0-9]+$', name):
            raise OneLakeSecurityError(
                f"Invalid role name '{name}'. "
                "Role names can only contain letters (A-Z, a-z) and numbers (0-9). "
                "No underscores, hyphens, spaces, or special characters are allowed."
            )

    def _validate_members(self, members: List[Dict]):
        for m in members:
            required = {"objectId", "objectType", "tenantId"}
            missing = required - set(m.keys())
            if missing:
                raise OneLakeSecurityError(f"Member missing required fields: {missing}")
            if m["objectType"] not in {"User", "Group", "ServicePrincipal", "ManagedIdentity"}:
                raise OneLakeSecurityError(f"Invalid objectType: {m['objectType']}")

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _get_role_base_url(self) -> str:
        url = f"{self.base_url}/workspaces/{self.workspace_id}/items/{self.item_id}/dataAccessRoles"
        if self.use_preview:
            url += "?preview=true"
        return url

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

    def _request(self, method: str, url: str, json_body=None, params=None, headers=None, timeout=60):
        attempt = 0
        while attempt < self.max_retries:
            attempt += 1
            try:
                resp = self.session.request(
                    method=method, url=url, json=json_body, params=params,
                    headers=headers or self._get_headers(), timeout=timeout
                )
                if resp.status_code == 429:
                    time.sleep(int(resp.headers.get("Retry-After", 5)))
                    continue
                if resp.status_code >= 400:
                    self._handle_error_response(resp)
                return resp
            except requests.exceptions.RequestException as e:
                time.sleep(self.retry_backoff ** attempt)
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

    def _log_payload(self, payload: Dict, action: str):
        logger.info(f"\n=== SENDING PAYLOAD ({action}) ===")
        logger.info(json.dumps(payload, indent=2))

    # ------------------------------------------------------------------ #
    # Core Methods
    # ------------------------------------------------------------------ #
    def list_roles(self) -> List[Dict]:
        resp = self._request("GET", self._get_role_base_url())
        return resp.json().get("value", [])

    def delete_role(self, role_name: str) -> bool:
        from urllib.parse import quote
        url = f"{self.base_url}/workspaces/{self.workspace_id}/items/{self.item_id}/dataAccessRoles/{quote(role_name)}"
        if self.use_preview:
            url += "?preview=true"
        try:
            self._request("DELETE", url)
            return True
        except OneLakeSecurityError:
            current = self.list_roles()
            filtered = [r for r in current if r.get("name") != role_name]
            if len(filtered) == len(current):
                return False
            return self._put_full_roles(filtered)

    def _put_full_roles(self, roles: List[Dict]) -> Dict:
        url = self._get_role_base_url()
        etag = None
        try:
            list_resp = self._request("GET", self._get_role_base_url())
            etag = list_resp.headers.get("ETag")
        except Exception:
            pass

        payload = {"value": roles}
        self._log_payload(payload, "BATCH_PUT")
        resp = self._request("PUT", url, json_body=payload, headers=self._get_headers(etag))
        return {"status": resp.status_code, "etag": resp.headers.get("ETag")}

    def _upsert_role_via_batch(self, new_role: Dict) -> Dict:
        self._validate_role_name(new_role["name"])   # ← Added validation
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

    # ------------------------------------------------------------------ #
    # High-level Methods (now with name validation)
    # ------------------------------------------------------------------ #
    def create_or_update_role(self, role: Dict) -> Dict:
        self._validate_members(role.get("members", {}).get("microsoftEntraMembers", []))
        return self._upsert_role_via_batch(role)

    def ensure_pii_full_access_role(self, members: List[Dict], paths: Optional[List[str]] = None) -> Dict:
        self._validate_members(members)
        paths = paths or ["*"]

        role = {
            "name": "PIIFullAccess",   # ← No underscore
            "kind": "Policy",
            "decisionRules": [{
                "effect": "Permit",
                "permission": [
                    {"attributeName": "Path", "attributeValueIncludedIn": paths},
                    {"attributeName": "Action", "attributeValueIncludedIn": ["Read"]}
                ]
            }],
            "members": {"microsoftEntraMembers": members}
        }
        return self.create_or_update_role(role)

    def ensure_no_pii_columns_role(
        self, members: List[Dict], pii_tables_and_safe_columns: Dict[str, List[str]]
    ) -> Dict:
        self._validate_members(members)
        paths = ["*"]

        constraints = {"columns": []}
        for table_path, cols in pii_tables_and_safe_columns.items():
            constraints["columns"].append({
                "tablePath": table_path,
                "columnNames": cols,
                "columnEffect": "Permit",
                "columnAction": ["Read"]
            })

        role = {
            "name": "NoPiiColumns",   # ← No underscore
            "kind": "Policy",
            "decisionRules": [{
                "effect": "Permit",
                "permission": [
                    {"attributeName": "Path", "attributeValueIncludedIn": paths},
                    {"attributeName": "Action", "attributeValueIncludedIn": ["Read"]}
                ],
                "constraints": constraints
            }],
            "members": {"microsoftEntraMembers": members}
        }
        return self.create_or_update_role(role)


# ======================================================================
# Example usage with valid role names
# ======================================================================
if __name__ == "__main__":
    from azure.identity import ClientSecretCredential

    cred = ClientSecretCredential(tenant_id=..., client_id=..., client_secret=...)

    manager = OneLakeDataAccessRoleManager(
        credential=cred,
        workspace_id="YOUR_WORKSPACE_ID",
        item_id="YOUR_LAKEHOUSE_ITEM_ID",
        use_preview=True
    )

    members = [{"objectId": "...", "objectType": "Group", "tenantId": "..."}]

    # Use names without underscores or special characters
    manager.ensure_pii_full_access_role(members=members)
    manager.ensure_no_pii_columns_role(
        members=members,
        pii_tables_and_safe_columns={"/Tables/Customers": ["CustomerKey", "FullName", "City"]}
    )
