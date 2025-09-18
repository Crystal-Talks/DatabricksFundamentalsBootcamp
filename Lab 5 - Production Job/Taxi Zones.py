# Databricks notebook source
# MAGIC %md
# MAGIC ###Define Root Path and Catalog
# MAGIC As you have copied earlier in Lab 2.
# MAGIC
# MAGIC 1. If Unity Catalog is not enabled:
# MAGIC     - Use file path: "/FileStore/"
# MAGIC     - Use catalog: hive_metastore
# MAGIC
# MAGIC 2. If Unity Catalog is enabled, use Volume:
# MAGIC     - Use file path - "/Volumes/oreillycatalog/default/datafiles/"
# MAGIC     - Use catalog: oreillycatalog

# COMMAND ----------

#Uncomment line whichever is applicable and execute

# If Unity Catalog is not enabled
#sourceFolderPath = "/FileStore/"

# If Unity Catalog is enabled
sourceFolderPath = "/Volumes/oreillycatalog/default/datafiles/"

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Uncomment line whichever is applicable and execute
# MAGIC
# MAGIC -- If Unity Catalog is not enabled
# MAGIC -- USE CATALOG hive_metastore
# MAGIC
# MAGIC -- If Unity Catalog is enabled
# MAGIC USE CATALOG oreillycatalog

# COMMAND ----------

# MAGIC %md ### A. Define File Path

# COMMAND ----------

# Define input & output paths

taxiZonesInputPath = sourceFolderPath + "Raw/TaxiZones.csv"

taxiZonesOutputPath = sourceFolderPath + "Output/TaxiZones"

# COMMAND ----------

# MAGIC %md ### B. Read File by Applying Schema

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# Create schema for Taxi Zones

taxiZonesSchema = (
                        StructType
                        ([ 
                            StructField("LocationID"             , IntegerType()   , True),
                            StructField("Borough"                , StringType()    , True),
                            StructField("Zone"                   , StringType()    , True),
                            StructField("service_zone"           , StringType()    , True)
                        ])
                   )

# COMMAND ----------

# Create DataFrame by applying the schema

taxiZonesDF = (
                    spark
                        .read
                        .option("header", "true")

                        .schema(taxiZonesSchema)

                        .csv(taxiZonesInputPath)
                )

# Print schema
taxiZonesDF.printSchema()

# COMMAND ----------

# MAGIC %md ### C. Save Data to Data Lake as Spark (Delta) Table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE DATABASE IF NOT EXISTS TaxiDB;

# COMMAND ----------

(
    taxiZonesDF
        .write

        .mode("overwrite")

        .format("delta")
        
        .saveAsTable("taxidb.taxizones")
)