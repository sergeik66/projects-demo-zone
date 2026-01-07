import json
import os
import requests
import yaml
import sempy.fabric as fabric
import fsspec

ws_name  = spark.conf.get("trident.workspace.name").upper()
prd_ws = "-PRD" in ws_name

workspace_id = fabric.get_workspace_id()
lakehouse_id = fabric.get_lakehouse_id()
config_file_name = "create_shortcuts_config"
config_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Files/shortcuts_config/"
config_path = config_path + (config_file_name + "_prd" if prd_ws else config_file_name) + ".yaml"

def load_config(path: str) -> dict:
    storage_options = {
            "account_name": "onelake",
            "account_host": "onelake.dfs.fabric.microsoft.com",
        }
    onelake_fs = fsspec.filesystem("abfss", **storage_options)
    if not mssparkutils.fs.exists(path):
        raise FileNotFoundError(f"Config file not found: {path}")

    with onelake_fs.open(path, "r") as f:
        return yaml.safe_load(f)

def get_bearer_token() -> str:
    # Use Fabric's identity token for REST calls
    return notebookutils.credentials.getToken("pbi")

def resolve_ids(cfg: dict) -> tuple[str, str]:
    ws_id = cfg.get("workspaceId")
    lh_id = cfg.get("lakehouseId")
    if ws_id and lh_id:
        return ws_id, lh_id

    # Auto-detect current notebook's workspace and lakehouse
    ws_id = fabric.get_workspace_id()
    lh_id = fabric.get_lakehouse_id()
    if not ws_id or not lh_id:
        raise RuntimeError("Could not auto-detect workspace/lakehouse IDs.")
    return ws_id, lh_id

def shortcut_exists(base_url: str, headers: dict, ws_id: str, lh_id: str, path: str, name: str) -> bool:
    # GET /shortcuts/{path}/{name}
    url = f"{base_url}/v1/workspaces/{ws_id}/items/{lh_id}/shortcuts/{path}/{name}"
    resp = requests.get(url, headers=headers, timeout=60)
    return resp.status_code == 200

def create_shortcut(base_url: str, headers: dict, ws_id: str, lh_id: str,
                    body: dict, conflict_policy: str = "Abort") -> requests.Response:
    url = f"{base_url}/v1/workspaces/{ws_id}/items/{lh_id}/shortcuts?shortcutConflictPolicy={conflict_policy}"
    return requests.post(url, headers=headers, json=body, timeout=120)

def validate_shortcut_body(entry: dict) -> dict:
    """
    Ensures the body conforms to the REST API:
    { "path": "...", "name": "...", "target": { <oneOf target types> } }
    """
    name = entry.get("name")
    path = entry.get("path")
    target = entry.get("target")

    if not name or not isinstance(name, str):
        raise ValueError("Shortcut 'name' is required and must be a string.")
    if not path or not isinstance(path, str) or not (path.startswith("Files") or path.startswith("Tables")):
        raise ValueError("Shortcut 'path' must start with 'Files' or 'Tables'.")
    if not target or not isinstance(target, dict) or len(target.keys()) != 1:
        raise ValueError("Shortcut 'target' must specify exactly one supported type (e.g., 'adlsGen2', 'oneLake').")

    return {"name": name, "path": path, "target": target}

def run(config_path: str):
    cfg = load_config(config_path)

    ws_id, lh_id = resolve_ids(cfg)
    conflict_policy = cfg.get("shortcutConflictPolicy", "Abort")
    skip_if_exists = bool(cfg.get("skipIfExists", False))

    token = get_bearer_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    base_url = "https://api.fabric.microsoft.com"

    shortcuts = cfg.get("shortcuts", [])
    if not isinstance(shortcuts, list) or not shortcuts:
        raise ValueError("No 'shortcuts' defined in config.")

    created = []
    skipped = []
    errors = []

    for i, entry in enumerate(shortcuts, start=1):
        try:
            body = validate_shortcut_body(entry)
            path = body["path"]
            name = body["name"]

            if skip_if_exists and shortcut_exists(base_url, headers, ws_id, lh_id, path, name):
                skipped.append({"name": name, "path": path, "reason": "exists"})
                print(f"[SKIP] {path}/{name} already exists.")
                continue

            resp = create_shortcut(base_url, headers, ws_id, lh_id, body, conflict_policy)
            if resp.status_code in (200, 201):
                created.append({"name": name, "path": path, "status": resp.status_code})
                print(f"[OK] Created/Updated: {path}/{name} (HTTP {resp.status_code})")
            else:
                # Capture error body
                try:
                    err = resp.json()
                except Exception:
                    err = {"text": resp.text}
                errors.append({"name": name, "path": path, "status": resp.status_code, "error": err})
                print(f"[ERR] {path}/{name} (HTTP {resp.status_code}) -> {err}")

        except Exception as ex:
            errors.append({"entry": entry, "error": str(ex)})
            print(f"[EX] Failed entry #{i}: {ex}")

    summary = {
        "created_count": len(created),
        "skipped_count": len(skipped),
        "error_count": len(errors),
        "created": created,
        "skipped": skipped,
        "errors": errors,
    }
    print("\n=== Summary ===")
    print(json.dumps(summary, indent=2))
    return summary

res = run(config_path)
