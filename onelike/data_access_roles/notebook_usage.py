# After ingestion
try:
        # Get list of tables processed in this run
        ingested_tables = data.get("tables_processed", []) or []

        if ingested_tables:
            logger.info("🔐 Syncing NoPiiColumns role with latest metadata...")

            updated = manager.sync_no_pii_columns_role(
                members=NO_PII_MEMBERS,
                spark=spark,
                pii_metadata_table_fqn=PII_METADATA_TABLE,
                incoming_table_names=ingested_tables,
                role_name="NoPiiColumns"
            )

            if updated:
                logger.info("✅ NoPiiColumns role was updated")
            else:
                logger.info("ℹ️ NoPiiColumns role was already up to date")
        else:
            logger.info("ℹ️ No tables processed in this run. Skipping role sync.")

    except Exception as role_error:
        # Role sync failure should NOT fail the main pipeline
        logger.warning(f"⚠️ Failed to sync NoPiiColumns role (non-fatal): {role_error}")

# After successful ingestion
try:
    from one_lake_security import OneLakeDataAccessRoleManager

    manager = OneLakeDataAccessRoleManager(
        credential=your_credential,
        workspace_id=target_workspace_id,
        item_id=target_lakehouse_id,
        use_preview=True
    )

    no_pii_members = [
        {"objectId": "your-group-object-id", "objectType": "Group", "tenantId": "your-tenant-id"}
    ]

    # Get list of tables processed in this run (customize as needed)
    ingested_tables = data.get("tables_processed", []) or []

    updated = sync_no_pii_columns_role(
        spark=spark,
        pii_metadata_table_fqn="metadata_lakehouse.dataset_pii_list",
        manager=manager,
        members=no_pii_members,
        incoming_table_names=ingested_tables,
        role_name="NoPiiColumns"
    )

    if updated:
        logger.info("🔐 NoPiiColumns role was updated")
    else:
        logger.info("ℹ️ NoPiiColumns role was already up to date")

except Exception as role_error:
    logger.warning(f"⚠️ Failed to sync NoPiiColumns role (non-fatal): {role_error}")
