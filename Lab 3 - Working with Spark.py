# Databricks notebook source
# Create a Python list
employees = [
                (1, "John", 10000),
                (2, "Fred", 20000),
                (3, "Anna", 30000),
                (4, "James", 40000),
                (5, "Mohit", 50000)
            ]

employeesDF = spark.createDataFrame(employees)

# COMMAND ----------

employeesDF.show()

# COMMAND ----------

employeesDF = employeesDF.toDF("id", "name", "salary")

employeesDF.show()

# COMMAND ----------

display(employeesDF)

# COMMAND ----------

# Filter data

newdf = (
            employeesDF
                .where("salary > 20000")
                .where("ID = 4")
                
        )

display(newdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Define Root Path
# MAGIC As you have copied earlier in Lab 2.
# MAGIC
# MAGIC 1. If Unity Catalog is not enabled, use DBFS:
# MAGIC     - Use file path: "/FileStore/"
# MAGIC
# MAGIC 2. If Unity Catalog is enabled, use Volume:
# MAGIC     - Use file path: "/Volumes/oreillycatalog/default/datafiles/"

# COMMAND ----------

#Uncomment line whichever is applicable and execute

# If Unity Catalog is not enabled
# sourceFolderPath = "/FileStore/"

# If Unity Catalog is enabled
sourceFolderPath = "/Volumes/oreillycatalog/default/datafiles/"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Databricks Utilities

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

display(dbutils.fs.ls(sourceFolderPath + "Raw/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Working with DataFrames

# COMMAND ----------

# Make sure to set the path correctly >> Use head command to verify if file is accessible

greenTaxisFilePath = sourceFolderPath + "Raw/GreenTaxis_202501.csv"

# COMMAND ----------

# Change the path based on file location in your Data Lake

dbutils.fs.head(greenTaxisFilePath)

# COMMAND ----------

# Read csv file
greenTaxiDF = (
                  spark
                    .read                     
                    .csv(greenTaxisFilePath)
              )

display(greenTaxiDF)

# COMMAND ----------

# Read csv file by setting header as true
greenTaxiDF = (
                  spark
                    .read

                    .option("header", "true")
                    
                    .csv(greenTaxisFilePath)
              )

display(greenTaxiDF)

# COMMAND ----------

# Read csv file by setting header and inferring schema
greenTaxiDF = (
                  spark
                    .read

                    .option("header", "true")
                    .option("inferSchema", "true")
                    
                    .csv(greenTaxisFilePath)
              )

display(greenTaxiDF)

# COMMAND ----------

greenTaxiDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Applying Schemas

# COMMAND ----------

# Create schema for Green Taxi Data

from pyspark.sql.functions import *
from pyspark.sql.types import *
  
greenTaxiSchema = (
            StructType()               
               .add("VendorId", "integer")
               .add("lpep_pickup_datetime", "timestamp")
               .add("lpep_dropoff_datetime", "timestamp")
               .add("store_and_fwd_flag", "string")
               .add("RatecodeID", "integer")
               .add("PULocationID", "integer")
               .add("DOLocationID", "integer")
  
              .add("passenger_count", "integer")
              .add("trip_distance", "double")
              .add("fare_amount", "double")
              .add("extra", "double")
              .add("mta_tax", "double")
              .add("tip_amount", "double")
  
              .add("tolls_amount", "double")
              .add("ehail_fee", "double")
              .add("improvement_surcharge", "double")
              .add("total_amount", "double")
              .add("payment_type", "integer")
              .add("trip_type", "integer")
         )

# COMMAND ----------

# Read csv file by applying schema
greenTaxiDF = (
                  spark
                    .read

                    .option("header", "true")

                    .schema(greenTaxiSchema)
                    
                    .csv(greenTaxisFilePath)
              )

display(greenTaxiDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Analyzing Data

# COMMAND ----------

display(
    greenTaxiDF.describe(
                             "passenger_count",                                     
                             "trip_distance"                                     
                        )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Cleaning Raw Data

# COMMAND ----------

# Count before filtering
print("Before = " + str(greenTaxiDF.count()))

# Filter inaccurate data
greenTaxiDF = (
                  greenTaxiDF
                          .where("passenger_count > 0")
  
                          .filter(col("trip_distance") > 0.0)
)

# Count after filtering
print("After = " + str(greenTaxiDF.count()))

# COMMAND ----------

# Drop rows with nulls
greenTaxiDF = (
                  greenTaxiDF
                          .na.drop('all')
              )

# COMMAND ----------

# Map of default values
defaultValueMap = {'payment_type': 5, 'RateCodeID': 1}

# Replace nulls with default values
greenTaxiDF = (
                  greenTaxiDF
                      .na.fill(defaultValueMap)
              )

# COMMAND ----------

# Drop duplicate rows
greenTaxiDF = (
                  greenTaxiDF
                          .dropDuplicates()
              )

# COMMAND ----------

# Filter incorrect records
greenTaxiDF = (
                  greenTaxiDF
                          .where("lpep_pickup_datetime >= '2025-01-01' AND lpep_dropoff_datetime < '2025-02-01'")
              )

# COMMAND ----------

# Display the final count

print("Final count after cleanup = " + str(greenTaxiDF.count()))

# COMMAND ----------

display(greenTaxiDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Applying Transformations

# COMMAND ----------

greenTaxiDF = (
                  greenTaxiDF

                        # Select only limited columns
                        .select(
                                  col("VendorID"),
                                  col("passenger_count").alias("PassengerCount"),
                                  col("trip_distance").alias("TripDistance"),
                                  col("lpep_pickup_datetime").alias("PickupTime"),                          
                                  col("lpep_dropoff_datetime").alias("DropTime"), 
                                  col("PUlocationID").alias("PickupLocationId"), 
                                  col("DOlocationID").alias("DropLocationId"), 
                                  col("RatecodeID"), 
                                  col("total_amount").alias("TotalAmount"),
                                  col("payment_type").alias("PaymentType")
                               )
              )

greenTaxiDF.printSchema()

# COMMAND ----------

# Create a derived column - Trip time in minutes
greenTaxiDF = (
                  greenTaxiDF
                        .withColumn("TripTimeInMinutes", 
                                        round(
                                                (unix_timestamp(col("DropTime")) - unix_timestamp(col("PickupTime"))) 
                                                    / 60
                                             )
                                   )
              )

greenTaxiDF.printSchema()

# COMMAND ----------

# Create a derived column - Trip type
greenTaxiDF = (
                  greenTaxiDF
                        .withColumn("TripType", 
                                        when(
                                                col("RatecodeID") == 6,
                                                  "SharedTrip"
                                            )
                                        .otherwise("SoloTrip")
                                   )
              )

greenTaxiDF.printSchema()

# COMMAND ----------

# Create derived columns for year, month and day
greenTaxiDF = (
                  greenTaxiDF
                        .withColumn("TripYear", year(col("PickupTime")))
                        .withColumn("TripMonth", month(col("PickupTime")))
                        .withColumn("TripDay", dayofmonth(col("PickupTime")))
              )

greenTaxiDF.printSchema()

# COMMAND ----------

display(greenTaxiDF)

# COMMAND ----------

greenTaxiGroupedDF = (
                          greenTaxiDF
                            .groupBy("TripDay")
                            .agg(sum("TotalAmount").alias("total"))
  
                            .orderBy(col("TripDay").desc())
                     )
    
display(greenTaxiGroupedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Joining with another dataset

# COMMAND ----------

# Make sure to set the path correctly >> Use head command to verify if file is accessible

taxiZonesFilePath = sourceFolderPath + "Raw/TaxiZones.csv"

# COMMAND ----------

dbutils.fs.head(taxiZonesFilePath)

# COMMAND ----------

# Read TaxiZones file
taxiZonesDF = (
                  spark
                      .read
                      .option("header", "true")
                      .option("inferSchema", "true")
                      .csv(taxiZonesFilePath)
              )

display(taxiZonesDF)

# COMMAND ----------

greenTaxiWithZonesDF = (
                          greenTaxiDF.alias("g")
                                     .join(taxiZonesDF.alias("t"),                                               
                                               col("t.LocationId") == col("g.PickupLocationId"),
                                              "inner"
                                          )
                       )

display(greenTaxiWithZonesDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Exercise

# COMMAND ----------

# EXERCISE - JOIN greenTaxiWithZonesDF with TaxiZones on DropLocationId. And group by PickupZone and DropZone, and provide average of TotalAmount.

# COMMAND ----------

# MAGIC %md
# MAGIC ##Working with Spark SQL

# COMMAND ----------

# Create a local temp view
greenTaxiDF.createOrReplaceTempView("GreenTaxiTripData")

# COMMAND ----------

display(
  spark.sql("SELECT PassengerCount, PickupTime FROM GreenTaxiTripData WHERE PickupLocationID = 1")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT PassengerCount, PickupTime 
# MAGIC FROM GreenTaxiTripData 
# MAGIC WHERE PickupLocationID = 1

# COMMAND ----------

# MAGIC %md
# MAGIC ##Reading JSON

# COMMAND ----------

# Make sure to set the path correctly >> Use head command to verify if file is accessible

paymentTypesFilePath = sourceFolderPath + "Raw/PaymentTypes.json"

# COMMAND ----------

paymentTypes = (
                    spark
                        .read
                        .json(paymentTypesFilePath)
)

display(paymentTypes)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Writing Output to Data Lake

# COMMAND ----------

#Define the path

greenTaxisOutputFilePath = sourceFolderPath + "Output/GreenTaxis"

# COMMAND ----------

# Write output as CSV File
(
    greenTaxiDF   
        .write
        .option("header", "true")
        .option("dateFormat", "yyyy-MM-dd HH:mm:ss.S")
        .mode("overwrite")
        .csv(greenTaxisOutputFilePath + ".csv")
)

# COMMAND ----------

# Load the dataframe as parquet to storage
(
    greenTaxiDF  
      .write
      .option("header", "true")
      .option("dateFormat", "yyyy-MM-dd HH:mm:ss.S")
      .mode("overwrite")
      .parquet(greenTaxisOutputFilePath + ".parquet")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Open catalog from left menu, and verify the creation of folders and files in the defined location
# MAGIC
# MAGIC 1. If Unity Catalog is not enabled, check files in DBFS browser
# MAGIC 2. If Unity Catalog is enabled, check files in Volume

# COMMAND ----------

# MAGIC %md
# MAGIC ###Define Root Catalog
# MAGIC
# MAGIC 1. If Unity Catalog is not enabled, use hive_metastore:
# MAGIC     - USE hive_metastore
# MAGIC
# MAGIC 2. If Unity Catalog is enabled, use previously created oreillycatalog:
# MAGIC     - USE oreillycatalog

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

# MAGIC %md
# MAGIC ###Working with Spark SQL and Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE DATABASE IF NOT EXISTS TaxiDB

# COMMAND ----------

# Store data as a Managed Table
(
    greenTaxiDF
      .write
      .mode("overwrite")
      .saveAsTable("TaxiDB.GreenTaxisManaged")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM TaxiDB.GreenTaxisManaged
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE EXTENDED TaxiDB.GreenTaxisManaged

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE TaxiDB.GreenTaxisManaged