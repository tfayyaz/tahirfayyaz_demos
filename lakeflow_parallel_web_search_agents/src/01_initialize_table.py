# Databricks notebook source
# MAGIC %md
# MAGIC # Initialize Feature Comparison Table
# MAGIC
# MAGIC This notebook creates and populates the Unity Catalog table with Change Data Feed enabled.
# MAGIC Table: this_is_for_tahir_only.robocars.feature_comparison

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from datetime import datetime, timedelta
import random

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Database and Table

# COMMAND ----------

# Create catalog and schema if they don't exist
spark.sql("CREATE CATALOG IF NOT EXISTS this_is_for_tahir_only")
spark.sql("CREATE SCHEMA IF NOT EXISTS this_is_for_tahir_only.robocars")

# COMMAND ----------

# Drop table if exists (for clean slate)
spark.sql("DROP TABLE IF EXISTS this_is_for_tahir_only.robocars.feature_comparison")

# COMMAND ----------

# Create table with Change Data Feed enabled
spark.sql("""
CREATE TABLE this_is_for_tahir_only.robocars.feature_comparison (
    core_feature_id INT,
    car_manufacturer STRING,
    car_name STRING,
    car_model STRING,
    core_feature STRING,
    core_feature_news STRING,
    news_updated TIMESTAMP
)
USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Sample Data

# COMMAND ----------

# Define sample data for self-driving cars
manufacturers_data = {
    "Tesla": {
        "models": ["Model S", "Model 3", "Model X", "Model Y"],
        "features": [
            "Autopilot Full Self-Driving",
            "Navigate on Autopilot",
            "Auto Lane Change",
            "Autopark",
            "Summon",
            "Traffic Light Recognition",
            "Stop Sign Recognition",
            "City Street Steering",
            "Smart Summon",
            "Enhanced Autopilot"
        ]
    },
    "Waymo": {
        "models": ["Jaguar I-PACE", "Chrysler Pacifica"],
        "features": [
            "Lidar Sensing System",
            "360-degree Vision",
            "Rider-only Experience",
            "Emergency Stop Protocol",
            "Weather Detection System",
            "Pedestrian Detection",
            "Cyclist Detection",
            "Remote Assistance",
            "Automated Parking",
            "Route Optimization"
        ]
    },
    "Cruise": {
        "models": ["Chevrolet Bolt EV"],
        "features": [
            "Remote Assistance Network",
            "Continuous Learning System",
            "Urban Navigation",
            "Obstacle Avoidance",
            "V2X Communication",
            "Redundant Safety Systems",
            "Lidar Array",
            "Radar System",
            "Camera Vision",
            "Night Vision"
        ]
    },
    "Mercedes-Benz": {
        "models": ["EQS", "S-Class", "E-Class"],
        "features": [
            "Drive Pilot Level 3",
            "Active Distance Assist",
            "Active Steering Assist",
            "Active Lane Change Assist",
            "Active Emergency Stop Assist",
            "PRE-SAFE Impulse",
            "Parking Package",
            "Traffic Sign Assist",
            "Evasive Steering Assist",
            "Active Brake Assist"
        ]
    },
    "BMW": {
        "models": ["iX", "i7", "7 Series", "5 Series"],
        "features": [
            "Highway Assistant",
            "Personal Pilot",
            "Parking Assistant Plus",
            "Extended Traffic Jam Assistant",
            "Lane Change Warning",
            "Evasion Aid",
            "Front Cross-Traffic Alert",
            "Active Cruise Control",
            "Speed Limit Info",
            "Lane Departure Warning"
        ]
    },
    "Audi": {
        "models": ["A8", "e-tron", "Q8"],
        "features": [
            "Traffic Jam Pilot",
            "Adaptive Cruise Assist",
            "Lane Keeping Assist",
            "Park Assist Plus",
            "360-degree Cameras",
            "Cross Traffic Assist",
            "Turn Assist",
            "Exit Warning",
            "Collision Avoidance Assist",
            "Emergency Assist"
        ]
    },
    "Volvo": {
        "models": ["XC90", "XC60", "S90"],
        "features": [
            "Pilot Assist",
            "City Safety",
            "Oncoming Lane Mitigation",
            "Run-off Road Mitigation",
            "Blind Spot Information",
            "Cross Traffic Alert",
            "Park Assist Pilot",
            "360-degree Camera",
            "Distance Alert",
            "Lane Keeping Aid"
        ]
    },
    "Ford": {
        "models": ["Mustang Mach-E", "F-150 Lightning", "Bronco"],
        "features": [
            "BlueCruise Hands-Free",
            "Active Drive Assist",
            "Lane Centering",
            "Traffic Sign Recognition",
            "Evasive Steering Assist",
            "Active Park Assist 2.0",
            "Pre-Collision Assist",
            "Blind Spot Information",
            "Cross-Traffic Alert",
            "Adaptive Cruise Control"
        ]
    },
    "GM (Cadillac)": {
        "models": ["Escalade", "Lyriq", "CT6"],
        "features": [
            "Super Cruise",
            "Enhanced Automatic Parking",
            "Adaptive Cruise Control",
            "Lane Keep Assist",
            "Automatic Emergency Braking",
            "Pedestrian Detection",
            "Following Distance Indicator",
            "IntelliBeam Headlamps",
            "HD Surround Vision",
            "Rear Camera Mirror"
        ]
    },
    "Nissan": {
        "models": ["Ariya", "Leaf", "Rogue"],
        "features": [
            "ProPILOT Assist 2.0",
            "Intelligent Lane Intervention",
            "Blind Spot Warning",
            "Rear Cross Traffic Alert",
            "Intelligent Around View Monitor",
            "ProPILOT Park",
            "Emergency Braking",
            "Pedestrian Detection",
            "High Beam Assist",
            "Traffic Sign Recognition"
        ]
    }
}

# COMMAND ----------

# Generate 100 rows of data
data_rows = []
feature_id = 1

for manufacturer, details in manufacturers_data.items():
    for feature in details["features"]:
        if feature_id > 100:
            break

        model = random.choice(details["models"])
        car_name = f"{manufacturer} {model}"

        # Generate realistic news placeholders
        news_templates = [
            f"Latest update on {feature} shows improved performance in urban environments",
            f"{feature} receives software update version 2.{random.randint(1,9)}",
            f"New safety certification achieved for {feature}",
            f"{feature} now available in {random.randint(10, 50)} additional markets",
            f"Performance improvements in {feature} reduce latency by {random.randint(10, 40)}%"
        ]

        news = random.choice(news_templates)

        # Generate timestamp within the last 30 days
        days_ago = random.randint(0, 30)
        news_timestamp = datetime.now() - timedelta(days=days_ago)

        data_rows.append({
            "core_feature_id": feature_id,
            "car_manufacturer": manufacturer,
            "car_name": car_name,
            "car_model": model,
            "core_feature": feature,
            "core_feature_news": news,
            "news_updated": news_timestamp
        })

        feature_id += 1

    if feature_id > 100:
        break

# COMMAND ----------

# Create DataFrame from the generated data
schema = StructType([
    StructField("core_feature_id", IntegerType(), False),
    StructField("car_manufacturer", StringType(), False),
    StructField("car_name", StringType(), False),
    StructField("car_model", StringType(), False),
    StructField("core_feature", StringType(), False),
    StructField("core_feature_news", StringType(), False),
    StructField("news_updated", TimestampType(), False)
])

df = spark.createDataFrame(data_rows, schema)

# COMMAND ----------

# Insert data into the table
df.write.format("delta").mode("append").saveAsTable("this_is_for_tahir_only.robocars.feature_comparison")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Data

# COMMAND ----------

# Display the table contents
display(spark.sql("SELECT * FROM this_is_for_tahir_only.robocars.feature_comparison ORDER BY core_feature_id"))

# COMMAND ----------

# Show table properties to confirm CDF is enabled
display(spark.sql("DESCRIBE DETAIL this_is_for_tahir_only.robocars.feature_comparison"))

# COMMAND ----------

print(f"Successfully created and populated table with {spark.table('this_is_for_tahir_only.robocars.feature_comparison').count()} rows")
