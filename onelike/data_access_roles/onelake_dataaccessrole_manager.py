# Usage in notebook
manager = OneLakeDataAccessRoleManager(...)

pii_safe_columns = build_pii_safe_columns_dict(
    spark=spark,
    pii_metadata_table_fqn="metadata_lakehouse.dataset_pii_list",
    data=data   # from Ingest.metrics()
)

if pii_safe_columns:
    manager.sync_no_pii_columns_role(
        members=your_members,
        pii_tables_and_safe_columns=pii_safe_columns
    )


import json
import logging
import re
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import requests
from azure.core.credentials import TokenCredential
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


# ============================================================
# STANDALONE HELPER FUNCTION
# ============================================================
def build_pii_safe_columns_dict(
    spark,
    pii_metadata_table_fqn: str,
    data: Optional[dict] = None,
    incoming_table_names: Optional[List[str]] = None,
    default_schema: str = "dbo"
) -> Dict[str, List[str]]:
    """
    Builds visible (non-PII) columns for the NoPiiColumns role.
    Logic: visible_columns = all_columns - PII columns (where cd_level = 'CD3')
    """

    logger.info("🔍 Building visible (non-PII) columns...")

    table_columns: Dict[str, List[str]] = {}

    # Preferred: Use columns from Ingest class result
    if data:
        table_columns = data.get("table_columns", {})
        if not table_columns and "tables" in data:
            table_columns = {
                t["table_name"]: t.get("columns", [])
                for t in data.get("tables", [])
            }

    if not table_columns:
        logger.warning("No table_columns found in data. Cannot build PII safe columns.")
        return {}

    # Get PII columns from metadata
    pii_df = spark.read.format("delta").table(pii_metadata_table_fqn)
    pii_columns_df = pii_df.filter(F.col("cd_level") == "CD3")

    pii_by_table = (
        pii_columns_df.groupBy("table_name")
        .agg(F.collect_set("column_name").alias("pii_columns"))
        .collect()
    )
    pii_dict = {row["table_name"]: set(row["pii_columns"]) for row in pii_by_table}

    result: Dict[str, List[str]] = {}
    tables_to_process = incoming_table_names or list(table_columns.keys())

    for table_name in tables_to_process:
        all_columns = table_columns.get(table_name, [])
        if not all_columns:
            continue

        pii_columns = pii_dict.get(table_name, set())
        visible_columns = sorted(set(all_columns) - pii_columns)

        if visible_columns:
            table_path = f"/Tables/{default_schema}/{table_name}"
            result[table_path] = visible_columns
            logger.info(f"   {table_name}: {len(visible_columns)} visible columns "
                        f"(hidden PII: {len(pii_columns)})")

    return result


# ============================================================
# MAIN CLASS
# ============================================================
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

    # ------------------ Internal Helpers ------------------
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

    # ------------------ Core Role Operations ------------------
    def list_roles(self) -> List[Dict]:
        url = f"{self.base_url}/workspaces/{self.workspace_id}/items/{self.item_id}/dataAccessRoles"
        if self.use_preview:
            url += "?preview=true"
        resp = self._request("GET", url)
        return resp.json().get("value", [])

    def get_role(self, role_name: str) -> Optional[Dict]:
        try:
            url = f"{self.base_url}/workspaces/{self.workspace_id}/items/{self.item_id}/dataAccessRoles/{quote(role_name)}"
            if self.use_preview:
                url += "?preview=true"
            resp = self._request("GET", url)
            return resp.json()
        except OneLakeSecurityError:
            pass

        for role in self.list_roles():
            if role.get("name") == role_name:
                return role
        return None

    def delete_role(self, role_name: str) -> bool:
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
            self._put_full_roles(filtered)
            return True

    def _put_full_roles(self, roles: List[Dict]) -> Dict:
        url = f"{self.base_url}/workspaces/{self.workspace_id}/items/{self.item_id}/dataAccessRoles"
        if self.use_preview:
            url += "?preview=true"

        etag = None
        try:
            list_resp = self._request("GET", url)
            etag = list_resp.headers.get("ETag")
        except Exception:
            pass

        payload = {"value": roles}
        resp = self._request("PUT", url, json_body=payload, headers=self._get_headers(etag))
        return {"status": resp.status_code, "etag": resp.headers.get("ETag")}

    def _upsert_role_via_batch(self, new_role: Dict) -> Dict:
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

    # ------------------ General Smart Sync ------------------
    def sync_role(
        self,
        role_name: str,
        members: List[Dict],
        decision_rules: List[Dict],
        role_kind: str = "Policy"
    ) -> bool:
        """General-purpose smart sync with change detection."""
        logger.info(f"🔄 Starting smart sync for role: {role_name}")

        current_role = self.get_role(role_name)

        if not current_role:
            new_role = {
                "name": role_name,
                "kind": role_kind,
                "decisionRules": decision_rules,
                "members": {"microsoftEntraMembers": members}
            }
            self._upsert_role_via_batch(new_role)
            logger.info(f"✅ Role '{role_name}' created")
            return True

        current_decision = current_role.get("decisionRules", [])
        current_members = current_role.get("members", {}).get("microsoftEntraMembers", [])

        if current_decision == decision_rules and current_members == members:
            logger.info(f"✅ Role '{role_name}' is already up to date")
            return False

        updated_role = {
            "name": role_name,
            "kind": role_kind,
            "decisionRules": decision_rules,
            "members": {"microsoftEntraMembers": members}
        }
        self._upsert_role_via_batch(updated_role)
        logger.info(f"✅ Role '{role_name}' synced successfully")
        return True

    # ------------------ Specialized NoPiiColumns Sync ------------------
    def sync_no_pii_columns_role(
        self,
        members: List[Dict],
        pii_tables_and_safe_columns: Optional[Dict[str, List[str]]] = None,
        spark=None,
        pii_metadata_table_fqn: Optional[str] = None,
        incoming_table_names: Optional[List[str]] = None,
        role_name: str = "NoPiiColumns",
        default_schema: str = "dbo"
    ) -> bool:
        """Convenience method for NoPiiColumns role with smart sync."""

        if pii_tables_and_safe_columns is None:
            if spark is None or pii_metadata_table_fqn is None:
                raise OneLakeSecurityError(
                    "Provide either 'pii_tables_and_safe_columns' or 'spark' + 'pii_metadata_table_fqn'"
                )
            pii_tables_and_safe_columns = build_pii_safe_columns_dict(
                spark=spark,
                pii_metadata_table_fqn=pii_metadata_table_fqn,
                incoming_table_names=incoming_table_names,
                default_schema=default_schema
            )

        if not pii_tables_and_safe_columns:
            logger.info("No tables require PII column restrictions.")
            return False

        column_rules = []
        for table_path, visible_cols in pii_tables_and_safe_columns.items():
            column_rules.append({
                "tablePath": table_path,
                "columnNames": visible_cols,
                "columnEffect": "Permit",
                "columnAction": ["Read"]
            })

        decision_rules = [{
            "effect": "Permit",
            "permission": [
                {"attributeName": "Path", "attributeValueIncludedIn": ["*"]},
                {"attributeName": "Action", "attributeValueIncludedIn": ["Read"]}
            ],
            "constraints": {"columns": column_rules}
        }]

        return self.sync_role(
            role_name=role_name,
            members=members,
            decision_rules=decision_rules
        )

    # ------------------ Convenience Methods ------------------
    def create_or_update_role(self, role: Dict) -> Dict:
        return self._upsert_role_via_batch(role)
