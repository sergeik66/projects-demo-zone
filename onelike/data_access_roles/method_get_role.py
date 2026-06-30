# ==================== ADD THESE TWO METHODS ====================

def list_roles(self) -> List[Dict]:
    """List all data access roles for the item."""
    url = f"{self.base_url}/workspaces/{self.workspace_id}/items/{self.item_id}/dataAccessRoles"
    if getattr(self, "use_preview", False):
        url += "?preview=true"
    resp = self._request("GET", url)
    return resp.json().get("value", [])


def get_role(self, role_name: str) -> Optional[Dict]:
    """
    Get a single data access role by name.
    Tries granular endpoint first, falls back to full list.
    """
    from urllib.parse import quote

    try:
        url = f"{self.base_url}/workspaces/{self.workspace_id}/items/{self.item_id}/dataAccessRoles/{quote(role_name)}"
        if getattr(self, "use_preview", False):
            url += "?preview=true"
        resp = self._request("GET", url)
        return resp.json()
    except OneLakeSecurityError:
        pass

    # Fallback
    for role in self.list_roles():
        if role.get("name") == role_name:
            return role
    return None
