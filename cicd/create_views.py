import re
from typing import List, Dict

# Configuration – control error behavior here
FAIL_ON_VIEW_ERROR = True          # Set to False if you want to continue even if some views fail
COLLECT_ALL_ERRORS = True          # Always True – gives you full error report even when failing fast


def execute_create_views(spark: SparkSession):
    """
    Executes all CREATE OR ALTER VIEW scripts found under Files/
    in alphabetical order by filename.
    Handles errors gracefully and fails the notebook with a clear summary
    if any view fails and FAIL_ON_VIEW_ERROR is True.
    """
    logger.info("Starting view deployment (ordered by filename)")

    try:
        df = spark.read.text(f"abfss://{LAKEHOUSE_NAME}@onelake.dfs.fabric.microsoft.com/Files/**/*.sql", wholetext=True) \
                  .withColumn("file_path", spark.sql.functions.input_file_name())

        if df.isEmpty():
            logger.info("No .sql files found under Files/ – nothing to do")
            return

        rows = df.collect()
        candidates = []

        for row in rows:
            content = row["value"]
            file_path = row["file_path"]
            filename = file_path.split("/")[-1]

            if re.search(r"\bCREATE\s+OR\s+ALTER\s+VIEW\b", content, re.IGNORECASE):
                candidates.append({
                    "filename": filename,
                    "file_path": file_path,
                    "content": content
                })

        if not candidates:
            logger.info("No CREATE OR ALTER VIEW scripts found")
            return

        # Sort by filename (01_, 02_, 10_ → correct order)
        candidates.sort(key=lambda x: x["filename"].lower())

        logger.info(f"Found {len(candidates)} view script(s). Execution order:")
        for c in candidates:
            logger.info(f"  {c['filename']}")

        # Execute and collect results
        errors: List[Dict] = []
        successful = 0

        for script in candidates:
            try:
                logger.info(f"Executing → {script['filename']}")
                spark.sql(script["content"])
                logger.info(f"Success: {script['filename']}")
                successful += 1
            except Exception as e:
                error_msg = str(e).replace("\n", " ").replace("\r", "")
                error_entry = {
                    "filename": script["filename"],
                    "file_path": script["file_path"],
                    "error": error_msg
                }
                errors.append(error_entry)
                logger.error(f"Failed: {script['filename']} | Error: {error_msg}")

        # Final reporting
        logger.info(f"View deployment complete: {successful} succeeded, {len(errors)} failed")

        if errors:
            summary = "\n".join([
                f"  • {e['filename']} → {e['error'][:200]}{'...' if len(e['error']) > 200 else ''}"
                for e in errors
            ])
            full_error_message = f"""
╔══════════════════════════════════════════════════════════════╗
║                     VIEW DEPLOYMENT FAILED                     ║
╚══════════════════════════════════════════════════════════════╝
Total views processed : {len(candidates)}
Successful            : {successful}
Failed                : {len(errors)}

Failed scripts:
{summary}

To continue anyway, set FAIL_ON_VIEW_ERROR = False in the notebook.
"""
            logger.error(full_error_message)

            if FAIL_ON_VIEW_ERROR:
                raise RuntimeError(f"View deployment failed ({len(errors)} error(s)). See logs above.")

        else:
            logger.info("All views deployed successfully!")

    except Exception as e:
        logger.error(f"Unexpected error in execute_create_views(): {str(e)}")
        raise
