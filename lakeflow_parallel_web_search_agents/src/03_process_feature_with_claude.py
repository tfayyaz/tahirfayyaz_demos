# Databricks notebook source
# MAGIC %md
# MAGIC # Process Feature with Claude API Web Search
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Takes a core_feature_id as a parameter
# MAGIC 2. Reads that row from the Unity Catalog table
# MAGIC 3. Uses Claude API with web search to check for new news
# MAGIC 4. Updates the row with new news if found

# COMMAND ----------

# MAGIC %pip install anthropic

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql import SparkSession
from delta.tables import DeltaTable
import anthropic
import os
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Parameters

# COMMAND ----------

# Get the core_feature_id parameter from the for-each loop
dbutils.widgets.text("core_feature_id", "1", "Core Feature ID")
core_feature_id = int(dbutils.widgets.get("core_feature_id"))

print(f"Processing core_feature_id: {core_feature_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Feature Data

# COMMAND ----------

# Read the specific row from the table
feature_df = spark.sql(f"""
    SELECT
        core_feature_id,
        car_manufacturer,
        car_name,
        car_model,
        core_feature,
        core_feature_news,
        news_updated
    FROM this_is_for_tahir_only.robocars.feature_comparison
    WHERE core_feature_id = {core_feature_id}
""")

# COMMAND ----------

# Get the feature details
feature_row = feature_df.collect()[0]

car_manufacturer = feature_row.car_manufacturer
car_name = feature_row.car_name
car_model = feature_row.car_model
core_feature = feature_row.core_feature
current_news = feature_row.core_feature_news
current_news_updated = feature_row.news_updated

print(f"Car: {car_name}")
print(f"Model: {car_model}")
print(f"Feature: {core_feature}")
print(f"Current News: {current_news}")
print(f"Last Updated: {current_news_updated}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Claude API with Web Search

# COMMAND ----------

# Get Claude API key from Databricks secret scope
# You'll need to set this up in your Databricks workspace:
# databricks secrets create-scope --scope claude-api
# databricks secrets put --scope claude-api --key api-key
try:
    claude_api_key = dbutils.secrets.get(scope="claude-api", key="api-key")
except Exception as e:
    print(f"Warning: Could not retrieve Claude API key from secrets: {e}")
    print("Please set up the secret scope 'claude-api' with key 'api-key'")
    # For testing, you can use an environment variable
    claude_api_key = os.getenv("ANTHROPIC_API_KEY", "")

# COMMAND ----------

# Initialize Claude client
client = anthropic.Anthropic(api_key=claude_api_key)

# COMMAND ----------

# Create the prompt for Claude
prompt = f"""
Search for the latest news and updates about the {core_feature} feature in the {car_name} ({car_model})
manufactured by {car_manufacturer}.

Current information we have (from {current_news_updated}):
{current_news}

Please search for:
1. Any recent announcements or updates about this specific feature
2. Software updates or improvements
3. New capabilities or enhancements
4. Safety certifications or regulatory approvals
5. Market availability changes

If you find new information that is different from what we currently have, please summarize it concisely in 1-2 sentences.
If there is no new information, please respond with "No new updates found."
"""

# COMMAND ----------

# Call Claude API with web search enabled
print("Querying Claude API with web search...")

try:
    response = client.messages.create(
        model="claude-3-5-sonnet-20241022",
        max_tokens=1024,
        tools=[
            {
                "type": "web_search_20241022",
                "name": "web_search",
                "max_uses": 5
            }
        ],
        messages=[
            {
                "role": "user",
                "content": prompt
            }
        ]
    )

    # Extract the response text
    new_news = None
    for block in response.content:
        if hasattr(block, 'text'):
            new_news = block.text
            break

    if new_news is None:
        new_news = "No response from Claude API"

    print(f"\nClaude Response:\n{new_news}\n")

except Exception as e:
    print(f"Error calling Claude API: {e}")
    new_news = f"Error: {str(e)}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update Table if New News Found

# COMMAND ----------

# Only update if we found new information
if new_news and "No new updates found" not in new_news and "Error:" not in new_news:
    print(f"New information found! Updating table...")

    # Get the Delta table
    delta_table = DeltaTable.forName(spark, "this_is_for_tahir_only.robocars.feature_comparison")

    # Update the row
    delta_table.update(
        condition=f"core_feature_id = {core_feature_id}",
        set={
            "core_feature_news": f"'{new_news.replace(chr(39), chr(39)+chr(39))}'",  # Escape single quotes
            "news_updated": f"cast('{datetime.now().isoformat()}' as timestamp)"
        }
    )

    print(f"Successfully updated feature_id {core_feature_id}")

    # Verify the update
    updated_df = spark.sql(f"""
        SELECT core_feature_id, core_feature, core_feature_news, news_updated
        FROM this_is_for_tahir_only.robocars.feature_comparison
        WHERE core_feature_id = {core_feature_id}
    """)

    display(updated_df)

else:
    print(f"No new information to update for feature_id {core_feature_id}")

# COMMAND ----------

# Set output task value to indicate completion
dbutils.jobs.taskValues.set(
    key=f"processed_feature_{core_feature_id}",
    value={
        "core_feature_id": core_feature_id,
        "processed": True,
        "updated": "No new updates found" not in new_news and "Error:" not in new_news,
        "timestamp": datetime.now().isoformat()
    }
)

print(f"Task completed for feature_id {core_feature_id}")
