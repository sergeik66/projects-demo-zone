Fabric notebook that **creates OneLake shortcuts** in the current lakehouse based on a YAML configuration file.

The script is production-ready, idempotent, logs clearly, supports dry-run mode, and includes proper error handling and summary reporting.

## Purpose

Automate creation of shortcuts to external OneLake items (other lakehouses/datasets) under the `Files/` root of the current lakehouse.

Typical use case:
- Create shortcuts to curated/policy/raw datasets from multiple source lakehouses
- Run in both DEV/TEST and PRD environments using different config variants (`_prd.yaml`)

## Features

- Auto-detects current workspace & lakehouse (or uses explicit IDs from config)
- Supports `oneLake`, `adlsGen2`, `s3` target types (extensible)
- Idempotent: skips existing shortcuts by default (`skipIfExists: true`)
- Conflict policy: `Abort` (default) or `CreateOrOverwrite` per shortcut or globally
- Dry-run mode (`dry_run=True`) — validates without making changes
- Detailed logging + structured JSON summary
- Nice display table of results in notebook
- PRD / non-PRD config switching (`shortcuts_config.yaml` vs `shortcuts_config_prd.yaml`)

## Prerequisites

- Microsoft Fabric notebook environment
- Attached lakehouse
- Config file stored in:  
  `Files/shortcuts_config/shortcuts_config.yaml`  
  or `Files/shortcuts_config/shortcuts_config_prd.yaml` (for PRD workspaces)

## Configuration File Example (`shortcuts_config.yaml`)

```yaml
workspaceId: "ab08da5e-0f71-423b-a811-bd0af21f182b"      # optional
lakehouseId: "7c6d771a-3b6f-4042-8a89-1a885973a93c"     # optional

shortcutConflictPolicy: "CreateOrOverwrite"
skipIfExists: true

shortcuts:
  - name: "prdandprc_policy_dp_datasets"
    path: "Files/"
    target:
      oneLake:
        workspaceId: "02c3d55e-485c-419b-b587-21a51aeb261e"
        itemId: "6740d2cc-6489-41d9-af20-315d92df9c07"
        path: "Files/datasets/policy_dp"

  - name: "claims_handling_iis_transaction_datasets"
    path: "Files/"
    target:
      oneLake:
        workspaceId: "1e139fac-e1b9-4677-8d87-4c14fff3724a"
        itemId: "6dc7b1ae-adc8-4a59-937a-a04332f85054"
        path: "Files/datasets/iis_transaction"
```
## How to Use
1. Place your config file in the lakehouse:
```
Files/shortcuts_config/shortcuts_config.yaml
or _prd variant for production workspaces
```
2. Attach the notebook to your lakehouse.
3. Run all cells (or just the last one).
4. Optional parameters (edit before run):
```
dry_run_mode = False       # Set True to simulate without creating
force_recreate = False     # Force recreate even if exists
```
5. Check the summary table at the end:
```
2025-01-15 10:45:12,345 [INFO] Processing 8 shortcut(s) in LIVE mode
2025-01-15 10:45:13,120 [INFO] [OK] Created Files//prdandprc_policy_dp_datasets (HTTP 201)
2025-01-15 10:45:14,890 [INFO] [SKIP] Files//claims_handling_iis_transaction_datasets already exists

=== Execution Summary ===
{
  "total": 8,
  "created": 5,
  "skipped": 3,
  "errors": 0,
  ...
}
```
## Related Notebooks / Pipeline
- load_datasets_metadata – loads .json metadata files from created shortcuts into catalog.datasets
- pipeline - none
## Recommendations
- Use CreateOrOverwrite in development, Abort in production
- Add notebook parameter support for dry_run_mode and force_recreate
- Monitor errors via Fabric monitoring or add email notification on failure
