""" First Notebook """

lakehouse_id = "e07f913f-34ad-4395-9f91-97d2e055c6d3"
sheet_name = "Rule Master ES_HO_PD"
skip_rows = 1
dq_template_excel_file_name = "Distribution Delegated Authority - PBI_RPT_DPR_001_E&S_HO_POLICY_DATA - DQ Rule Book_E&S HO.xlsx"
dq_template_json_file_name = "dq_template_output.json"
workspace_id = "36fa502d-ee99-4479-84ef-4f6278542c0f"

excel_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Files/data_quality/{dq_template_excel_file_name}"
json_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Files/data_quality/{dq_template_json_file_name}"
sheet_name = sheet_name
skip_rows = skip_rows


from pyspark.sql.window import Window

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

pd_df = pd.read_excel(
    io=excel_path,
    sheet_name=sheet_name,
    skiprows=skip_rows,
    header=0,
    dtype=str,
).dropna(how="all")

df = spark.createDataFrame(pd_df)

# Define window specification (Optional: You can partition by a column if needed)
window_spec = Window.orderBy("DQ Active Flag")
df = df.withColumn("DQ Active Flag",lit(1))\
       .withColumn("DQ Effective Date",current_timestamp())\
       .withColumn("DQ Expiration Date",lit("2099-12-31 23:59:59").cast("timestamp"))

df = df.filter((col("Group") != "not active"))
# Add row_number column
df = df.withColumn("#", row_number().over(window_spec))

df = df.select(
    struct(
        col("#").alias("dq_rule_master_key"),
        col("DQ Rule ID").alias("dq_rule_id"),
        col("Data Product Name").alias("data_product_name"),
        col("Sub Domain Name").alias("sub_domain_name"),
        col("DQ Rule Description").alias("dq_rule_description"),
        col("DQ Rule Constraint").alias("dq_rule_constraint"),
        col("DQ Rule Dimension").alias("dq_rule_dimension"),
        col("DQ Screen Type").alias("dq_screen_type"),
        col("DQ Rule Applicable Lakehouse").alias("dq_rule_applicable_lakehouse"),
        col("DQ Rule Applicable Schema").alias("dq_rule_applicable_schema"),
        col("DQ Rule Applicable Object").alias("dq_rule_applicable_object"),
        col("DQ Rule Applicable Attribute").alias("dq_rule_applicable_attribute"),
        col("DQ Rule Failure Action").alias("dq_rule_failure_action"),
        col("DQ Rule Severity Score").alias("dq_rule_severity_score"),
        col("DQ Active Flag").alias("is_current_flag"),
        col("DQ Effective Date").alias("row_effective_date"),
        col("DQ Expiration Date").alias("row_expiration_date")
    ).alias("dq_json")
)

dq_rule_constraint_schema = {
    "type": "object",
    "properties": {
        "type": {"type": "string"},
        "kwargs": {"type": "object"},
        "meta": {"type": "object"},
    },
    "required": ["type", "kwargs"],
}

dq_json = df.select(to_json(collect_list("dq_json"))).collect()[0][0]
dq_json = json.loads(dq_json)
for idx in dq_json:
    idx["dq_rule_constraint"] = json.loads(idx["dq_rule_constraint"])
    validate(instance=idx["dq_rule_constraint"], schema=dq_rule_constraint_schema)

storage_options = {
    "account_name": "onelake",
    "account_host": "onelake.dfs.fabric.microsoft.com",
}
onelake_fs = fsspec.filesystem("abfss", **storage_options)

with onelake_fs.open(json_path, "w") as json_file:
    json.dump(dq_json, json_file, indent=4)

print("DQ template conversion complete.")
print(json_path)


""" Second Notebook """
lh_metadata_name = "den_lhw_pdi_001_metadata"
lh_observability_name = "den_lhw_pdi_001_observability"
dq_template_json_file_name = "dq_template_output.json"
from spark_engine.common.lakehouse import LakehouseManager

metadata_lakehouse = LakehouseManager(lh_metadata_name)

observability_lakehouse = LakehouseManager(lh_observability_name)

if observability_lakehouse.check_if_table_exists("dim_dq_rule_master", "data_quality"):

    (

        spark.read

        .option("multiLine", True)

        .json(f"{metadata_lakehouse.lakehouse_path}/Files/data_quality/{dq_template_json_file_name}")

        .selectExpr(

            "cast(dq_rule_master_key as int) as dq_rule_master_key",

            "dq_rule_id",

            "data_product_name",

            "sub_domain_name",

            "dq_rule_description",

            "to_json(dq_rule_constraint) as dq_rule_constraint",

            "dq_rule_dimension",

            "dq_screen_type",

            "dq_rule_applicable_lakehouse",

            "dq_rule_applicable_schema",

            "dq_rule_applicable_object",

            "dq_rule_applicable_attribute",

            "dq_rule_failure_action",

            "cast(dq_rule_severity_score as double) as dq_rule_severity_score",

            "cast(is_current_flag as boolean) as is_current_flag",

            "cast(row_effective_date as timestamp) as row_effective_date",

            "cast(row_expiration_date as timestamp) as row_expiration_date"

        )

        .write.mode("overwrite")

        .save(f"{observability_lakehouse.lakehouse_path}/Tables/data_quality/dim_dq_rule_master")

    )
