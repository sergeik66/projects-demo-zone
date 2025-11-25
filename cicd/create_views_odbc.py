import re
import requests
import pyodbc
import logging
from typing import List, Dict

# ────────────────────────────────
# FABRIC CREDENTIALS – SET THESE ONCE
# ────────────────────────────────
TENANT_ID       = "<YOUR_TENANT_ID>"        # e.g. 12345678-1234-1234-1234-1234567890ab
CLIENT_ID       = "<YOUR_CLIENT_ID>"        # Service Principal / App Registration ID
CLIENT_SECRET   = "<YOUR_CLIENT_SECRET>"    # Secret from App Registration
WORKSPACE_ID    = "<YOUR_WORKSPACE_ID>"     # From Fabric workspace URL
LAKEHOUSE_ID    = "<YOUR_LAKEHOUSE_ID>"     # From lakehouse > Settings > Properties

# Optional: Set these via notebook parameters or secrets manager (recommended in prod)
# For now, replace the placeholders above or use spark.conf.get() if passed in

VIEWS_GLOB_PATTERN = f"abfss://{LAKEHOUSE_NAME}@onelake.dfs.fabric.microsoft.com/Files/**/*.sql"

def _get_fabric_access_token() -> str:
    token_url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://api.fabric.microsoft.com/.default"
    }
    resp = requests.post(token_url, data=data)
    resp.raise_for_status()
    return resp.json()["access_token"]

def _get_sql_endpoint_connection_string(access_token: str) -> str:
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/lakehouses/{LAKEHOUSE_ID}"
    headers = {"Authorization": f"Bearer {access_token}"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()["properties"]["sqlEndpoint"]["connectionString"]

def execute_create_views_via_odbc(spark: SparkSession):
    """
    Executes all view scripts found under Files/ directly on the SQL analytics endpoint
    using Fabric REST API + pyodbc → guarantees visibility in SQL endpoint.
    """
    logger.info("Starting view deployment via SQL Endpoint (ODBC)")

    try:
        # 1. Get access token
        logger.info("Acquiring Fabric access token...")
        token = _get_fabric_access_token()

        # 2. Get connection string
        logger.info("Retrieving SQL endpoint connection string...")
        conn_str = _get_sql_endpoint_connection_string(token)
        logger.info(f"Connection string retrieved (server: {conn_str.split(';')[1] if ';' in conn_str else 'hidden'})")

        # 3. Discover and sort view files
        df = spark.read.text(VIEWS_GLOB_PATTERN, wholetext=True) \
                  .withColumn("file_path", spark.sql.functions.input_file_name())

        if df.isEmpty():
            logger.info("No .sql files found under Files/ – skipping")
            return

        candidates = []
        for row in df.collect():
            content = row["value"]
            path = row["file_path"]
            filename = path.split("/")[-1]

            if re.search(r"\bCREATE\s+(?:OR\s+(?:ALTER|REPLACE)\s+)?VIEW\b", content, re.IGNORECASE):
                # Normalize to CREATE OR REPLACE VIEW for SQL endpoint
                normalized = re.sub(
                    r"CREATE\s+(?:OR\s+ALTER\s+)?VIEW",
                    "CREATE OR REPLACE VIEW",
                    content,
                    count=1,
                    flags=re.IGNORECASE
                )
                candidates.append({
                    "filename": filename,
                    "content": normalized
                })

        if not candidates:
            logger.info("No view definitions found")
            return

        # Sort by filename for correct dependency order
        candidates.sort(key=lambda x: x["filename"].lower())
        logger.info(f"Found {len(candidates)} view(s) → executing in order:")
        for c in candidates:
            logger.info(f"  → {c['filename']}")

        # 4. Connect via pyodbc and execute
        conn = pyodbc.connect(conn_str, autocommit=True)
        cursor = conn.cursor()
        errors: List[Dict] = []
        success_count = 0

        for script in candidates:
            try:
                logger.info(f"Executing via SQL endpoint: {script['filename']}")
                cursor.execute(script["content"])
                logger.info(f"SUCCESS: {script['filename']}")
                success_count += 1
            except Exception as e:
                err_msg = str(e)
                logger.error(f"FAILED: {script['filename']} | {err_msg}")
                errors.append({"filename": script["filename"], "error": err_msg})
            finally:
                # Always commit per statement (some drivers need it)
                try:
                    conn.commit()
                except:
                    pass

        cursor.close()
        conn.close()

        # 5. Final report
        if errors:
            summary = "\n".join([f"  • {e['filename']} → {e['error'][:200]}" for e in errors])
            msg = f"""
VIEW DEPLOYMENT FAILED ({len(errors)} error(s))
Successful: {success_count} | Failed: {len(errors)}
Details:
{summary}
"""
            logger.error(msg)
            raise RuntimeError(f"View deployment failed. {len(errors)} view(s) had errors.")
        else:
            logger.info(f"All {success_count} views deployed successfully via SQL endpoint!")

    except Exception as e:
        logger.error(f"Critical failure in execute_create_views_via_odbc(): {str(e)}")
        raise
