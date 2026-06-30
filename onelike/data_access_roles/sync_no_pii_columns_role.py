from typing import List, Dict, Optional, Set
from pyspark.sql import functions as F
import logging

logger = logging.getLogger(__name__)


def sync_no_pii_columns_role(
    spark,
    pii_metadata_table_fqn: str,
    manager: "OneLakeDataAccessRoleManager",
    members: List[Dict],
    incoming_table_names: Optional[List[str]] = None,
    role_name: str = "NoPiiColumns"
) -> bool:
    """
    Intelligently syncs the NoPiiColumns role.

    Features:
    - Only updates the role if there are actual changes (new tables, changed columns, or removed tables)
    - Removes tables that no longer have PII (cd_level != 'CD3')
    - Uses merge logic instead of blind overwrite
    """

    logger.info(f"🔄 Starting smart sync for role: {role_name}")

    # ============================================================
    # 1. Build DESIRED state from metadata (excluding CD3)
    # ============================================================
    pii_df = spark.read.format("delta").table(pii_metadata_table_fqn)

    safe_df = (
        pii_df
        .filter(F.col("cd_level") != "CD3")
        .select("table_name", "column_name")
    )

    if incoming_table_names:
        safe_df = safe_df.filter(F.col("table_name").isin(incoming_table_names))

    grouped = (
        safe_df
        .groupBy("table_name")
        .agg(F.collect_list("column_name").alias("safe_columns"))
    )

    desired: Dict[str, List[str]] = {}
    for row in grouped.collect():
        table_path = f"/Tables/{row['table_name']}"
        desired[table_path] = sorted(row["safe_columns"])  # sort for stable comparison

    logger.info(f"   Desired state contains {len(desired)} tables")

    # ============================================================
    # 2. Get CURRENT role state
    # ============================================================
    current_role = manager.get_role(role_name)

    if not current_role:
        logger.info(f"   Role '{role_name}' does not exist. Creating new role...")
        if desired:
            manager.ensure_no_pii_columns_role(members=members, pii_tables_and_safe_columns=desired)
            logger.info("✅ Role created successfully")
            return True
        else:
            logger.info("ℹ️ No tables require PII protection. Nothing to create.")
            return False

    # Extract current column constraints
    current_constraints = {}
    decision_rules = current_role.get("decisionRules", [])
    if decision_rules:
        constraints = decision_rules[0].get("constraints", {})
        for col_rule in constraints.get("columns", []):
            current_constraints[col_rule["tablePath"]] = sorted(col_rule.get("columnNames", []))

    logger.info(f"   Current role contains {len(current_constraints)} tables")

    # ============================================================
    # 3. Detect differences (New / Changed / Removed)
    # ============================================================
    needs_update = False
    final_column_rules = []

    # Check for new tables or changed columns
    for table_path, desired_cols in desired.items():
        current_cols = current_constraints.get(table_path, [])

        if set(desired_cols) != set(current_cols):
            needs_update = True
            logger.info(f"   🔄 Change detected for {table_path}")
            if not current_cols:
                logger.info(f"      → New table added")
            else:
                added = set(desired_cols) - set(current_cols)
                removed = set(current_cols) - set(desired_cols)
                if added:
                    logger.info(f"      → Columns added: {added}")
                if removed:
                    logger.info(f"      → Columns removed: {removed}")

        final_column_rules.append({
            "tablePath": table_path,
            "columnNames": desired_cols,
            "columnEffect": "Permit",
            "columnAction": ["Read"]
        })

    # Check for tables that should be REMOVED (no longer have PII)
    tables_to_remove = set(current_constraints.keys()) - set(desired.keys())
    if tables_to_remove:
        needs_update = True
        logger.info(f"   🗑️ Tables to be removed from role (no longer have PII): {tables_to_remove}")

    # ============================================================
    # 4. Perform update only if needed
    # ============================================================
    if not needs_update:
        logger.info("✅ No changes detected. Role is already up to date.")
        return False

    # Build updated role payload using merge approach
    updated_role = {
        "name": role_name,
        "kind": "Policy",
        "decisionRules": [{
            "effect": "Permit",
            "permission": [
                {"attributeName": "Path", "attributeValueIncludedIn": ["*"]},
                {"attributeName": "Action", "attributeValueIncludedIn": ["Read"]}
            ],
            "constraints": {
                "columns": final_column_rules
            }
        }],
        "members": {"microsoftEntraMembers": members}
    }

    logger.info("📝 Applying changes to role...")
    manager.create_or_update_role(updated_role)
    logger.info("✅ Role updated successfully with smart merge")

    return True
