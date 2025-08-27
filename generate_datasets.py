from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, concat_ws, sha2
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, FloatType, LongType
from datetime import datetime, timezone

# Initialize Spark session
spark = SparkSession.builder.appName("GenerateCDCDatasets").getOrCreate()

# Define schema based on provided error message
schema = StructType([
    StructField("code", StringType(), False),
    StructField("primeseq", StringType(), False),
    StructField("state", StringType(), False),
    StructField("effdate", TimestampType(), False),
    StructField("classcode", StringType(), False),
    StructField("seqcode", StringType(), False),
    StructField("territory", StringType(), False),
    StructField("exmod", FloatType(), True),
    StructField("minprem", FloatType(), True),
    StructField("exposure", FloatType(), True),
    StructField("premium", FloatType(), True),
    StructField("rate", FloatType(), True),
    StructField("xcluamt", FloatType(), True),
    StructField("losscons", FloatType(), True),
    StructField("form", StringType(), True),
    StructField("paragraph", StringType(), True),
    StructField("volcomp", StringType(), False),
    StructField("hazard", StringType(), True),
    StructField("cvrg", StringType(), True),
    StructField("disctype", StringType(), True),
    StructField("classuffix", StringType(), True),
    StructField("diffprem", FloatType(), True),
    StructField("diffrate", FloatType(), True),
    StructField("rateeffdate", TimestampType(), False),
    StructField("losscost", FloatType(), True),
    StructField("dl_rowhash", StringType(), True),
    StructField("dl_partitionkey", StringType(), True),
    StructField("dl_iscurrent", IntegerType(), True),
    StructField("dl_recordstartdateutc", TimestampType(), True),
    StructField("dl_recordenddateutc", TimestampType(), True),
    StructField("dl_createddateutc", TimestampType(), True),
    StructField("dl_lastmodifiedutc", TimestampType(), True),
    StructField("dl_sourcefilepath", StringType(), True),
    StructField("dl_sourcefilename", StringType(), True),
    StructField("dl_eltid", StringType(), True),
    StructField("dl_runid", StringType(), True),
    StructField("dl_is_deleted", IntegerType(), True),
])

# Schema for source data includes CDC columns
source_schema = StructType(schema.fields + [
    StructField("__$operation", IntegerType(), False),
    StructField("__$start_lsn", LongType(), True),
    StructField("__$seqval", LongType(), True),
    StructField("row_num", IntegerType(), True),
])

# Common metadata
batch_time = datetime.now(timezone.utc)
elt_id = "test_elt_001"
run_id = "test_run_001"
source_filepath = "/path/to/source"
source_filename = "cdc_data.parquet"

# Helper function to compute dl_rowhash
def compute_rowhash(row, columns):
    values = [str(row[col]) for col in columns]
    return sha2(concat_ws("|", *values), 256)

# Target dataset (existing Delta table)
target_data = [
    {
        "code": "POL001",
        "primeseq": "SEQ001",
        "state": "CA",
        "effdate": datetime(2025, 1, 1, tzinfo=timezone.utc),
        "classcode": "CL001",
        "seqcode": "SC001",
        "territory": "TER001",
        "exmod": 1.05,
        "minprem": 1000.0,
        "exposure": 50000.0,
        "premium": 2500.0,
        "rate": 0.05,
        "xcluamt": 0.0,
        "losscons": 0.0,
        "form": "F001",
        "paragraph": "P001",
        "volcomp": "VC001",
        "hazard": "H001",
        "cvrg": "CV001",
        "disctype": "DT001",
        "classuffix": "CS001",
        "diffprem": 0.0,
        "diffrate": 0.0,
        "rateeffdate": datetime(2025, 1, 1, tzinfo=timezone.utc),
        "losscost": 0.02,
        "dl_partitionkey": "20250101",
        "dl_iscurrent": 1,
        "dl_recordstartdateutc": batch_time,
        "dl_recordenddateutc": None,
        "dl_createddateutc": batch_time,
        "dl_lastmodifiedutc": batch_time,
        "dl_sourcefilepath": source_filepath,
        "dl_sourcefilename": source_filename,
        "dl_eltid": elt_id,
        "dl_runid": run_id,
        "dl_is_deleted": 0,
    },
    {
        "code": "POL002",
        "primeseq": "SEQ002",
        "state": "NY",
        "effdate": datetime(2025, 2, 1, tzinfo=timezone.utc),
        "classcode": "CL002",
        "seqcode": "SC002",
        "territory": "TER002",
        "exmod": 1.10,
        "minprem": 1500.0,
        "exposure": 60000.0,
        "premium": 3000.0,
        "rate": 0.06,
        "xcluamt": 0.0,
        "losscons": 0.0,
        "form": "F002",
        "paragraph": "P002",
        "volcomp": "VC002",
        "hazard": "H002",
        "cvrg": "CV002",
        "disctype": "DT002",
        "classuffix": "CS002",
        "diffprem": 0.0,
        "diffrate": 0.0,
        "rateeffdate": datetime(2025, 2, 1, tzinfo=timezone.utc),
        "losscost": 0.03,
        "dl_partitionkey": "20250201",
        "dl_iscurrent": 1,
        "dl_recordstartdateutc": batch_time,
        "dl_recordenddateutc": None,
        "dl_createddateutc": batch_time,
        "dl_lastmodifiedutc": batch_time,
        "dl_sourcefilepath": source_filepath,
        "dl_sourcefilename": source_filename,
        "dl_eltid": elt_id,
        "dl_runid": run_id,
        "dl_is_deleted": 0,
    }
]

# Compute dl_rowhash for target data
data_columns = [
    "code", "primeseq", "state", "effdate", "classcode", "seqcode", "territory",
    "exmod", "minprem", "exposure", "premium", "rate", "xcluamt", "losscons",
    "form", "paragraph", "volcomp", "hazard", "cvrg", "disctype", "classuffix",
    "diffprem", "diffrate", "rateeffdate", "losscost"
]
for row in target_data:
    row["dl_rowhash"] = compute_rowhash(row, data_columns)

# Create target DataFrame
target_df = spark.createDataFrame(target_data, schema=schema)
target_df.write.format("parquet").save("/tmp/target_data.parquet")

# Source dataset (CDC changes)
source_data = [
    # Insert: New record
    {
        "code": "POL003",
        "primeseq": "SEQ003",
        "state": "TX",
        "effdate": datetime(2025, 3, 1, tzinfo=timezone.utc),
        "classcode": "CL003",
        "seqcode": "SC003",
        "territory": "TER003",
        "exmod": 1.08,
        "minprem": 1200.0,
        "exposure": 55000.0,
        "premium": 2750.0,
        "rate": 0.055,
        "xcluamt": 0.0,
        "losscons": 0.0,
        "form": "F003",
        "paragraph": "P003",
        "volcomp": "VC003",
        "hazard": "H003",
        "cvrg": "CV003",
        "disctype": "DT003",
        "classuffix": "CS003",
        "diffprem": 0.0,
        "diffrate": 0.0,
        "rateeffdate": datetime(2025, 3, 1, tzinfo=timezone.utc),
        "losscost": 0.025,
        "dl_partitionkey": "20250301",
        "dl_iscurrent": 1,
        "dl_recordstartdateutc": batch_time,
        "dl_recordenddateutc": None,
        "dl_createddateutc": batch_time,
        "dl_lastmodifiedutc": batch_time,
        "dl_sourcefilepath": source_filepath,
        "dl_sourcefilename": source_filename,
        "dl_eltid": elt_id,
        "dl_runid": run_id,
        "dl_is_deleted": 0,
        "__$operation": 2,  # Insert
        "__$start_lsn": 1001,
        "__$seqval": 1,
        "row_num": 1
    },
    # Update: Modify POL001
    {
        "code": "POL001",
        "primeseq": "SEQ001",
        "state": "CA",
        "effdate": datetime(2025, 1, 1, tzinfo=timezone.utc),
        "classcode": "CL001",
        "seqcode": "SC001",
        "territory": "TER001",
        "exmod": 1.07,  # Updated value
        "minprem": 1100.0,  # Updated value
        "exposure": 52000.0,
        "premium": 2600.0,
        "rate": 0.05,
        "xcluamt": 0.0,
        "losscons": 0.0,
        "form": "F001",
        "paragraph": "P001",
        "volcomp": "VC001",
        "hazard": "H001",
        "cvrg": "CV001",
        "disctype": "DT001",
        "classuffix": "CS001",
        "diffprem": 0.0,
        "diffrate": 0.0,
        "rateeffdate": datetime(2025, 1, 1, tzinfo=timezone.utc),
        "losscost": 0.02,
        "dl_partitionkey": "20250101",
        "dl_iscurrent": 1,
        "dl_recordstartdateutc": batch_time,
        "dl_recordenddateutc": None,
        "dl_createddateutc": batch_time,
        "dl_lastmodifiedutc": batch_time,
        "dl_sourcefilepath": source_filepath,
        "dl_sourcefilename": source_filename,
        "dl_eltid": elt_id,
        "dl_runid": run_id,
        "dl_is_deleted": 0,
        "__$operation": 4,  # Update
        "__$start_lsn": 1002,
        "__$seqval": 2,
        "row_num": 1
    },
    # Delete: Mark POL002 as deleted
    {
        "code": "POL002",
        "primeseq": "SEQ002",
        "state": "NY",
        "effdate": datetime(2025, 2, 1, tzinfo=timezone.utc),
        "classcode": "CL002",
        "seqcode": "SC002",
        "territory": "TER002",
        "exmod": None,  # Null for delete
        "minprem": None,
        "exposure": None,
        "premium": None,
        "rate": None,
        "xcluamt": None,
        "losscons": None,
        "form": None,
        "paragraph": None,
        "volcomp": "VC002",
        "hazard": None,
        "cvrg": None,
        "disctype": None,
        "classuffix": None,
        "diffprem": None,
        "diffrate": None,
        "rateeffdate": datetime(2025, 2, 1, tzinfo=timezone.utc),
        "losscost": None,
        "dl_partitionkey": "20250201",
        "dl_iscurrent": 0,
        "dl_recordstartdateutc": batch_time,
        "dl_recordenddateutc": None,
        "dl_createddateutc": batch_time,
        "dl_lastmodifiedutc": batch_time,
        "dl_sourcefilepath": source_filepath,
        "dl_sourcefilename": source_filename,
        "dl_eltid": elt_id,
        "dl_runid": run_id,
        "dl_is_deleted": 1,
        "__$operation": 1,  # Delete
        "__$start_lsn": 1003,
        "__$seqval": 3,
        "row_num": 1
    }
]

# Compute dl_rowhash for source data
for row in source_data:
    row["dl_rowhash"] = compute_rowhash(row, data_columns)

# Create source DataFrame
source_df = spark.createDataFrame(source_data, schema=source_schema)
source_df.write.format("parquet").save("/tmp/source_data.parquet")

# Stop Spark session
spark.stop()
