"""
create_shortcuts.py - Production-ready shortcut creation script for Microsoft Fabric

This script creates OneLake shortcuts in a Fabric lakehouse based on a YAML configuration file.
Designed to be idempotent, robust, and suitable for scheduled notebook runs in production.
"""

import json
import logging
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass

import yaml
import requests
import sempy.fabric as fabric
import fsspec
from notebookutils import mssparkutils

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


@dataclass
class ShortcutResult:
    name: str
    path: str
    status_code: Optional[int] = None
    action: str = "unknown"  # created, skipped, error, dry_run
    error: Optional[str] = None


def load_config(path: str) -> Dict[str, Any]:
    """Load YAML config from OneLake using fsspec."""
    storage_options = {
        "account_name": "onelake",
        "account_host": "onelake.dfs.fabric.microsoft.com",
    }
    fs = fsspec.filesystem("abfss", **storage_options)

    if not fs.exists(path):
        raise FileNotFoundError(f"Config file not found at: {path}")

    logger.info(f"Loading config from {path}")
    with fs.open(path, "r") as f:
        return yaml.safe_load(f)


def get_bearer_token() -> str:
    """Get Fabric identity token for API authentication."""
    try:
        # Preferred method in Fabric notebooks
        return mssparkutils.credentials.getToken("pbi")
    except Exception as e:
        logger.warning(f"notebookutils failed: {e}. Falling back to sempy.fabric")
        return fabric.get_access_token()


def resolve_workspace_lakehouse(cfg: dict) -> Tuple[str, str]:
    """Resolve workspace and lakehouse IDs from config or current context."""
    ws_id = cfg.get("workspaceId")
    lh_id = cfg.get("lakehouseId")

    if ws_id and lh_id:
        return ws_id, lh_id

    ws_id = fabric.get_workspace_id()
    lh_id = fabric.get_lakehouse_id()

    if not ws_id or not lh_id:
        raise RuntimeError("Unable to resolve workspaceId and lakehouseId. Specify in config or run in attached lakehouse.")

    logger.info(f"Auto-detected workspaceId={ws_id}, lakehouseId={lh_id}")
    return ws_id, lh_id


def validate_shortcut_entry(entry: dict, index: int) -> dict:
    """Validate a single shortcut entry against Fabric API requirements."""
    required_fields = ["name", "path", "target"]
    for field in required_fields:
        if field not in entry:
            raise ValueError(f"Shortcut #{index + 1}: Missing required field '{field}'")

    name = entry["name"]
    path = entry["path"]
    target = entry["target"]

    if not isinstance(name, str) or not name.strip():
        raise ValueError(f"Shortcut #{index + 1}: 'name' must be a non-empty string")

    if not isinstance(path, str) or not (path.startswith("Files/") or path.startswith("Tables/")):
        raise ValueError(f"Shortcut #{index + 1}: 'path' must start with 'Files/' or 'Tables/'")

    if not isinstance(target, dict) or len(target) != 1:
        raise ValueError(f"Shortcut #{index + 1}: 'target' must be a dict with exactly one key (e.g., adlsGen2, oneLake)")

    valid_target_types = {"adlsGen2", "oneLake", "s3"}  # Add more as supported
    target_type = next(iter(target))
    if target_type not in valid_target_types:
        raise ValueError(f"Shortcut #{index + 1}: Unsupported target type '{target_type}'")

    return {"name": name.strip(), "path": path, "target": target}


def shortcut_exists(base_url: str, headers: dict, ws_id: str, lh_id: str, path: str, name: str) -> bool:
    """Check if a shortcut already exists."""
    url = f"{base_url}/v1/workspaces/{ws_id}/items/{lh_id}/shortcuts/{path}/{name}"
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        return resp.status_code == 200
    except requests.RequestException as e:
        logger.warning(f"Failed to check existence of {path}/{name}: {e}")
        return False  # Assume not exists to avoid blocking creation


def create_shortcut(
    base_url: str,
    headers: dict,
    ws_id: str,
    lh_id: str,
    body: dict,
    conflict_policy: str = "Abort"
) -> requests.Response:
    """Create a single shortcut."""
    url = f"{base_url}/v1/workspaces/{ws_id}/items/{lh_id}/shortcuts?shortcutConflictPolicy={conflict_policy}"
    return requests.post(url, headers=headers, json=body, timeout=120)


def run(
    config_path: str,
    dry_run: bool = False,
    default_conflict_policy: str = "Abort",
    force_create: bool = False
) -> Dict[str, Any]:
    """
    Main execution function.
    
    Args:
        config_path: Path to config YAML in OneLake
        dry_run: If True, only validate and log what would be done
        default_conflict_policy: Default policy if not specified per shortcut
        force_create: Ignore existing shortcuts and recreate
    """
    cfg = load_config(config_path)
    ws_id, lh_id = resolve_workspace_lakehouse(cfg)

    token = get_bearer_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    base_url = "https://api.fabric.microsoft.com"

    shortcuts_cfg = cfg.get("shortcuts", [])
    if not shortcuts_cfg:
        raise ValueError("No 'shortcuts' list found in configuration.")

    results: List[ShortcutResult] = []

    logger.info(f"Processing {len(shortcuts_cfg)} shortcut(s) in {'DRY-RUN' if dry_run else 'LIVE'} mode")

    for i, entry in enumerate(shortcuts_cfg):
        try:
            body = validate_shortcut_entry(entry, i)

            path = body["path"]
            name = body["name"]
            conflict_policy = entry.get("conflictPolicy", default_conflict_policy)

            log_prefix = f"{path}/{name}"

            if not force_create and not dry_run:
                if shortcut_exists(base_url, headers, ws_id, lh_id, path, name):
                    results.append(ShortcutResult(name=name, path=path, action="skipped", error="Already exists"))
                    logger.info(f"[SKIP] {log_prefix} already exists")
                    continue

            if dry_run:
                results.append(ShortcutResult(name=name, path=path, action="dry_run"))
                logger.info(f"[DRY-RUN] Would create {log_prefix} (policy: {conflict_policy})")
                continue

            resp = create_shortcut(base_url, headers, ws_id, lh_id, body, conflict_policy)

            if resp.status_code in (200, 201):
                action = "created" if resp.status_code == 201 else "updated"
                results.append(ShortcutResult(name=name, path=path, status_code=resp.status_code, action=action))
                logger.info(f"[OK] {action.capitalize()} {log_prefix} (HTTP {resp.status_code})")
            else:
                error_detail = resp.text
                try:
                    error_detail = resp.json()
                except:
                    pass
                results.append(ShortcutResult(
                    name=name, path=path, status_code=resp.status_code,
                    action="error", error=str(error_detail)
                ))
                logger.error(f"[ERROR] {log_prefix} failed (HTTP {resp.status_code}): {error_detail}")

        except Exception as ex:
            results.append(ShortcutResult(
                name=entry.get("name", f"entry_{i+1}"),
                path=entry.get("path", "unknown"),
                action="error",
                error=str(ex)
            ))
            logger.exception(f"[EXCEPTION] Failed processing shortcut #{i+1}")

    # Summary
    summary = {
        "total": len(shortcuts_cfg),
        "created": len([r for r in results if r.action == "created"]),
        "updated": len([r for r in results if r.action == "updated"]),
        "skipped": len([r for r in results if r.action == "skipped"]),
        "dry_run": len([r for r in results if r.action == "dry_run"]),
        "errors": len([r for r in results if r.action == "error"]),
        "details": [
            {
                "path": f"{r.path}/{r.name}",
                "action": r.action,
                "status_code": r.status_code,
                "error": r.error
            } for r in results
        ]
    }

    logger.info("\n=== Execution Summary ===")
    logger.info(json.dumps(summary, indent=2))

    return summary


# =============================================================================
# Notebook Entry Point
# =============================================================================

# Detect environment (PRD vs non-PRD)
ws_name = spark.conf.get("trident.workspace.name", "").upper()
is_prd = "-PRD" in ws_name
config_file_name = "create_shortcuts_config"
config_variant = "_prd" if is_prd else ""

workspace_id = fabric.get_workspace_id()
lakehouse_id = fabric.get_lakehouse_id()

config_path = (
    f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/"
    f"{lakehouse_id}/Files/shortcuts_config/"
    f"{config_file_name}{config_variant}.yaml"
)

# Run in live mode by default in production, allow dry-run via parameter if needed
# You can parameterize this in Fabric using notebook parameters
dry_run_mode = False  # Set to True for testing
force_recreate = False

res = run(
    config_path=config_path,
    dry_run=dry_run_mode,
    default_conflict_policy="Abort",  # or "Replace" if you want to overwrite
    force_create=force_recreate
)

# Optional: display nice table in notebook
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

details_df = spark.createDataFrame(res["details"])
display(details_df)
