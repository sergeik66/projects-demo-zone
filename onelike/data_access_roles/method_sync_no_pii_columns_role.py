# Add this method to the OneLakeDataAccessRoleManager class

def sync_no_pii_columns_role(
    self,
    members: List[Dict],
    pii_tables_and_safe_columns: Optional[Dict[str, List[str]]] = None,
    spark=None,
    pii_metadata_table_fqn: Optional[str] = None,
    incoming_table_names: Optional[List[str]] = None,
    role_name: str = "NoPiiColumns"
) -> bool:
    """
    Smart sync for the NoPiiColumns role with change detection and merge logic.
    Can either accept a pre-built dictionary or dynamically build it from metadata.
    """
    from pyspark.sql import functions as F

    logger.info(f"🔄 Starting smart sync for role: {role_name}")

    # Build desired state if not provided
    if pii_tables_and_safe_columns is None:
        if spark is None or pii_metadata_table_fqn is None:
            raise OneLakeSecurityError(
                "Either provide 'pii_tables_and_safe_columns' or both 'spark' and 'pii_metadata_table_fqn'"
            )

        pii_df = spark.read.format("delta").table(pii_metadata_table_fqn)

        safe_df = pii_df.filter(F.col("cd_level") != "CD3").select("table_name", "column_name")

        if incoming_table_names:
            safe_df = safe_df.filter(F.col("table_name").isin(incoming_table_names))

        grouped = safe_df.groupBy("table_name").agg(F.collect_list("column_name").alias("safe_columns"))

        pii_tables_and_safe_columns = {}
        for row in grouped.collect():
            table_path = f"/Tables/{row['table_name']}"
            pii_tables_and_safe_columns[table_path] = sorted(row["safe_columns"])

    if not pii_tables_and_safe_columns:
        logger.info("ℹ️ No tables require PII column restrictions.")
        return False

    # Get current role
    current_role = self.get_role(role_name)

    if not current_role:
        logger.info(f"Role '{role_name}' does not exist. Creating it...")
        self.ensure_no_pii_columns_role(members=members, pii_tables_and_safe_columns=pii_tables_and_safe_columns)
        return True

    # Extract current constraints
    current_constraints = {}
    decision_rules = current_role.get("decisionRules", [{}])
    if decision_rules:
        for col_rule in decision_rules[0].get("constraints", {}).get("columns", []):
            current_constraints[col_rule["tablePath"]] = sorted(col_rule.get("columnNames", []))

    # Detect changes
    needs_update = False
    final_column_rules = []

    for table_path, desired_cols in pii_tables_and_safe_columns.items():
        current_cols = current_constraints.get(table_path, [])
        if set(desired_cols) != set(current_cols):
            needs_update = True
            logger.info(f"Change detected on {table_path}")

        final_column_rules.append({
            "tablePath": table_path,
            "columnNames": desired_cols,
            "columnEffect": "Permit",
            "columnAction": ["Read"]
        })

    # Detect tables to remove
    tables_to_remove = set(current_constraints.keys()) - set(pii_tables_and_safe_columns.keys())
    if tables_to_remove:
        needs_update = True
        logger.info(f"Tables to remove from role: {tables_to_remove}")

    if not needs_update:
        logger.info("✅ No changes detected. Role is up to date.")
        return False

    # Build and apply updated role
    updated_role = {
        "name": role_name,
        "kind": "Policy",
        "decisionRules": [{
            "effect": "Permit",
            "permission": [
                {"attributeName": "Path", "attributeValueIncludedIn": ["*"]},
                {"attributeName": "Action", "attributeValueIncludedIn": ["Read"]}
            ],
            "constraints": {"columns": final_column_rules}
        }],
        "members": {"microsoftEntraMembers": members}
    }

    self.create_or_update_role(updated_role)
    logger.info("✅ Role synced successfully with smart merge")
    return True
