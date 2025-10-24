# Databricks notebook source
# MAGIC %md
# MAGIC # Create SCD Type 2 Table from Change Data Feed
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Reads the Change Data Feed from the feature_comparison table
# MAGIC 2. Creates a Slowly Changing Dimension (SCD) Type 2 table
# MAGIC 3. Maintains historical records with effective dates and current flags

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, row_number, lead, when
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create SCD Type 2 Table Schema

# COMMAND ----------

# Create the SCD Type 2 table if it doesn't exist
spark.sql("""
CREATE TABLE IF NOT EXISTS this_is_for_tahir_only.robocars.feature_comparison_history (
    core_feature_id INT,
    car_manufacturer STRING,
    car_name STRING,
    car_model STRING,
    core_feature STRING,
    core_feature_news STRING,
    news_updated TIMESTAMP,
    effective_start_date TIMESTAMP,
    effective_end_date TIMESTAMP,
    is_current BOOLEAN,
    surrogate_key BIGINT
)
USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Change Data Feed

# COMMAND ----------

# Get the latest version of the source table
latest_version = spark.sql("""
    DESCRIBE HISTORY this_is_for_tahir_only.robocars.feature_comparison
    LIMIT 1
""").collect()[0]["version"]

print(f"Latest version of source table: {latest_version}")

# COMMAND ----------

# Determine the starting version for CDF
# Check if we have already processed some changes
try:
    max_processed_version_df = spark.sql("""
        SELECT MAX(surrogate_key) as max_key
        FROM this_is_for_tahir_only.robocars.feature_comparison_history
    """)

    max_key = max_processed_version_df.collect()[0]["max_key"]

    if max_key is None:
        # First run - read from version 0
        start_version = 0
        print("First run - processing from version 0")
    else:
        # Calculate the last processed version from surrogate key
        # Note: This is a simplified approach. In production, you'd track this separately.
        last_processed_version_df = spark.sql("""
            SELECT MAX(effective_start_date) as last_update
            FROM this_is_for_tahir_only.robocars.feature_comparison_history
        """)

        # For this example, we'll read all changes from version 0
        # In production, you'd track the last processed version
        start_version = 0
        print(f"Incremental run - processing from version {start_version}")

except Exception as e:
    # Table is empty or doesn't exist yet
    start_version = 0
    print(f"Table empty or error: {e}. Processing from version 0")

# COMMAND ----------

# Read the Change Data Feed
# Note: For the first run, we'll read the current state and insert it as initial records
try:
    cdf_df = spark.read.format("delta") \
        .option("readChangeFeed", "true") \
        .option("startingVersion", start_version) \
        .table("this_is_for_tahir_only.robocars.feature_comparison")

    print(f"Read {cdf_df.count()} change records from CDF")
    display(cdf_df.limit(10))

except Exception as e:
    print(f"Error reading CDF (might be first run): {e}")
    # For first run, read the current state
    cdf_df = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process Changes into SCD Type 2 Format

# COMMAND ----------

# Check if this is the initial load
is_initial_load = spark.table("this_is_for_tahir_only.robocars.feature_comparison_history").count() == 0

if is_initial_load:
    print("Performing initial load...")

    # Read current state of the source table
    current_state_df = spark.table("this_is_for_tahir_only.robocars.feature_comparison")

    # Create initial SCD Type 2 records
    from pyspark.sql.functions import monotonically_increasing_id

    scd_df = current_state_df.select(
        col("core_feature_id"),
        col("car_manufacturer"),
        col("car_name"),
        col("car_model"),
        col("core_feature"),
        col("core_feature_news"),
        col("news_updated"),
        col("news_updated").alias("effective_start_date"),
        lit(None).cast("timestamp").alias("effective_end_date"),
        lit(True).alias("is_current")
    ).withColumn("surrogate_key", monotonically_increasing_id())

    # Write initial records
    scd_df.write.format("delta").mode("append").saveAsTable(
        "this_is_for_tahir_only.robocars.feature_comparison_history"
    )

    print(f"Initial load complete. Inserted {scd_df.count()} records.")

else:
    print("Processing incremental changes...")

    if cdf_df is not None and cdf_df.count() > 0:
        # Filter for insert and update operations
        changes_df = cdf_df.filter(
            col("_change_type").isin(["insert", "update_postimage"])
        )

        if changes_df.count() > 0:
            # Get the current SCD table
            scd_delta_table = DeltaTable.forName(spark, "this_is_for_tahir_only.robocars.feature_comparison_history")

            # For each changed record, we need to:
            # 1. Close out the current record (set effective_end_date and is_current = false)
            # 2. Insert a new record with the new values

            # Process each change
            from pyspark.sql.functions import monotonically_increasing_id, max as sql_max

            # Get the max surrogate key
            max_key_df = spark.sql("""
                SELECT COALESCE(MAX(surrogate_key), 0) as max_key
                FROM this_is_for_tahir_only.robocars.feature_comparison_history
            """)
            max_key = max_key_df.collect()[0]["max_key"]

            # Prepare new records
            window_spec = Window.partitionBy("core_feature_id").orderBy(col("_commit_timestamp"))

            new_records = changes_df.select(
                col("core_feature_id"),
                col("car_manufacturer"),
                col("car_name"),
                col("car_model"),
                col("core_feature"),
                col("core_feature_news"),
                col("news_updated"),
                col("_commit_timestamp").alias("effective_start_date"),
                lit(None).cast("timestamp").alias("effective_end_date"),
                lit(True).alias("is_current"),
                (row_number().over(window_spec) + lit(max_key)).alias("surrogate_key")
            )

            # Close out current records for changed feature_ids
            changed_ids = [row.core_feature_id for row in new_records.select("core_feature_id").distinct().collect()]

            for feature_id in changed_ids:
                # Get the effective_start_date of the new record
                new_start_date = new_records.filter(col("core_feature_id") == feature_id) \
                    .select("effective_start_date").first()["effective_start_date"]

                # Update the current record
                scd_delta_table.update(
                    condition=f"core_feature_id = {feature_id} AND is_current = true",
                    set={
                        "effective_end_date": f"cast('{new_start_date.isoformat()}' as timestamp)",
                        "is_current": "false"
                    }
                )

            # Insert new records
            new_records.write.format("delta").mode("append").saveAsTable(
                "this_is_for_tahir_only.robocars.feature_comparison_history"
            )

            print(f"Processed {new_records.count()} change records")

        else:
            print("No changes to process")
    else:
        print("No CDF data to process")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify SCD Type 2 Table

# COMMAND ----------

# Display the SCD Type 2 table
print("Current records (is_current = true):")
display(spark.sql("""
    SELECT
        core_feature_id,
        car_manufacturer,
        car_name,
        core_feature,
        core_feature_news,
        effective_start_date,
        effective_end_date,
        is_current,
        surrogate_key
    FROM this_is_for_tahir_only.robocars.feature_comparison_history
    WHERE is_current = true
    ORDER BY core_feature_id
"""))

# COMMAND ----------

# Show historical records for a sample feature
print("\nExample: Historical records for feature_id 1:")
display(spark.sql("""
    SELECT
        core_feature_id,
        car_manufacturer,
        car_name,
        core_feature,
        core_feature_news,
        effective_start_date,
        effective_end_date,
        is_current,
        surrogate_key
    FROM this_is_for_tahir_only.robocars.feature_comparison_history
    WHERE core_feature_id = 1
    ORDER BY effective_start_date DESC
"""))

# COMMAND ----------

# Summary statistics
print("\nSummary Statistics:")
summary_df = spark.sql("""
    SELECT
        COUNT(*) as total_records,
        COUNT(DISTINCT core_feature_id) as unique_features,
        SUM(CASE WHEN is_current THEN 1 ELSE 0 END) as current_records,
        SUM(CASE WHEN NOT is_current THEN 1 ELSE 0 END) as historical_records
    FROM this_is_for_tahir_only.robocars.feature_comparison_history
""")

display(summary_df)

# COMMAND ----------

print("SCD Type 2 table processing complete!")
