from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("HandleLateArrivingFacts").getOrCreate()

# Sample Dimension Table (SCD Type 2 with effective dates)
dim_data = [
    (1, "CustomerA", "New York", "2023-01-01 10:00:00", "2023-06-01 09:00:00", True),
    (1, "CustomerA", "Boston", "2023-06-01 09:00:01", "9999-12-31 23:59:59", True),
    (2, "CustomerB", "Chicago", "2023-01-01 10:00:00", "9999-12-31 23:59:59", True)
]
dim_columns = ["customer_id", "customer_name", "location", "effective_start", "effective_end", "is_current"]
dim_df = spark.createDataFrame(dim_data, dim_columns)

# Sample Fact Table (with some late-arriving records)
fact_data = [
    (101, 1, 500.0, "2023-05-01 12:00:00"),  # Late-arriving, should match New York
    (102, 1, 700.0, "2023-07-01 15:00:00"),  # Should match Boston
    (103, 2, 300.0, "2023-02-01 08:00:00")   # Matches Chicago
]
fact_columns = ["fact_id", "customer_id", "amount", "transaction_time"]
fact_df = spark.createDataFrame(fact_data, fact_columns)

# Convert timestamp strings to timestamp type
dim_df = dim_df.withColumn("effective_start", col("effective_start").cast("timestamp")) \
               .withColumn("effective_end", col("effective_end").cast("timestamp"))
fact_df = fact_df.withColumn("transaction_time", col("transaction_time").cast("timestamp"))

# Step 1: Join Fact and Dimension tables to align facts with correct dimension version
# Alias columns to avoid ambiguity
fact_dim_joined = fact_df.alias("f").join(
    dim_df.alias("d"),
    (col("f.customer_id") == col("d.customer_id")) &
    (col("f.transaction_time").between(col("d.effective_start"), col("d.effective_end"))),
    "left"
)

# Step 2: Handle cases where no matching dimension record is found
# Create a window to get the latest dimension record per customer_id
window_spec = Window.partitionBy("customer_id").orderBy(col("effective_start").desc())

# Add a fallback dimension record for unmatched facts
dim_latest = dim_df.withColumn("row_num", row_number().over(window_spec)) \
                   .filter(col("row_num") == 1) \
                   .drop("row_num")

# Join unmatched facts with the latest dimension record
unmatched_facts = fact_dim_joined.filter(col("d.customer_id").isNull()) \
                                .select([col(f"f.{c}") for c in fact_df.columns]) \
                                .join(
                                    dim_latest.alias("dl"),
                                    col("f.customer_id") == col("dl.customer_id"),
                                    "left"
                                )

# Step 3: Combine matched and unmatched results
final_result = fact_dim_joined.filter(col("d.customer_id").isNotNull()) \
                             .union(unmatched_facts)

# Step 4: Select relevant columns for output
output = final_result.select(
    col("f.fact_id").alias("fact_id"),
    col("f.customer_id").alias("customer_id"),
    col("f.amount").alias("amount"),
    col("f.transaction_time").alias("transaction_time"),
    col("d.customer_name").alias("customer_name"),
    col("d.location").alias("location")
)

# Show the result
output.show(truncate=False)

# Stop Spark session
spark.stop()
