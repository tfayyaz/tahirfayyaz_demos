# Databricks notebook source
# MAGIC %md
# MAGIC # Get All Feature IDs
# MAGIC
# MAGIC This notebook reads all core_feature_id values from the Unity Catalog table
# MAGIC and passes them to dbutils.jobs.taskValues for use in a for-each loop.

# COMMAND ----------

from pyspark.sql import SparkSession

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Feature IDs from Table

# COMMAND ----------

# Read all core_feature_id values from the table
feature_ids_df = spark.sql("""
    SELECT DISTINCT core_feature_id
    FROM this_is_for_tahir_only.robocars.feature_comparison
    ORDER BY core_feature_id
""")

# COMMAND ----------

# Convert to list of integers
feature_ids = [row.core_feature_id for row in feature_ids_df.collect()]

print(f"Found {len(feature_ids)} feature IDs")
print(f"Feature IDs range: {min(feature_ids)} to {max(feature_ids)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set Task Values for For-Each Loop

# COMMAND ----------

# Set the feature IDs as a task value
# The for-each task will iterate over this list
dbutils.jobs.taskValues.set(key="feature_ids", value=feature_ids)

print(f"Successfully set task value 'feature_ids' with {len(feature_ids)} IDs")

# COMMAND ----------

# Display the feature IDs for verification
display(feature_ids_df)
