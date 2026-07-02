# Usage in Notebook
data = (
    SparkEngine.ingest(...)
    ...
    .metrics()
)

pii_safe_columns = build_pii_safe_columns_dict(
    spark=spark,
    pii_metadata_table_fqn="metadata_lakehouse.dataset_pii_list",
    data=data
)

if pii_safe_columns:
    manager.sync_no_pii_columns_role(
        members=NO_PII_MEMBERS,
        pii_tables_and_safe_columns=pii_safe_columns
    )

def build_pii_safe_columns_dict(
    spark,
    pii_metadata_table_fqn: str,
    data: dict,                              # ← Pass the full data object from ingestion
    incoming_table_names: Optional[List[str]] = None
) -> Dict[str, List[str]]:
    """
    Builds visible columns using column information from the Ingest class result.
    """

    logger.info("🔍 Building visible (non-PII) columns from ingestion metadata...")

    # Support both "table_columns" and "tables" structure
    table_columns = data.get("table_columns", {})

    if not table_columns and "tables" in data:
        table_columns = {
            t["table_name"]: t["columns"] 
            for t in data.get("tables", [])
        }

    if not table_columns:
        logger.warning("No table_columns found in ingestion result.")
        return {}

    # Get PII columns from metadata table
    pii_df = spark.read.format("delta").table(pii_metadata_table_fqn)

    pii_columns_df = (
        pii_df
        .filter(F.col("cd_level") == "CD3")
        .select("table_name", "column_name")
    )

    pii_by_table = (
        pii_columns_df.groupBy("table_name")
        .agg(F.collect_set("column_name").alias("pii_columns"))
        .collect()
    )
    pii_dict = {row["table_name"]: set(row["pii_columns"]) for row in pii_by_table}

    result = {}
    tables_to_process = incoming_table_names or list(table_columns.keys())

    for table_name in tables_to_process:
        all_columns = table_columns.get(table_name, [])
        if not all_columns:
            continue

        pii_columns = pii_dict.get(table_name, set())
        visible_columns = sorted(set(all_columns) - pii_columns)

        if visible_columns:
            table_path = f"/Tables/{data.get('metadata', {}).get('runOutput', {}).get('deltaSchema', 'dbo')}/{table_name}"
            result[table_path] = visible_columns

            logger.info(f"   {table_name}: {len(visible_columns)} visible columns "
                        f"(hidden PII: {len(pii_columns)})")

    return result
