-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Create bronze table

-- COMMAND ----------

CREATE MATERIALIZED VIEW YellowTaxis_Bronze
(
    RideId                  INT              COMMENT 'This is the primary key column',
    VendorId                INT,
    PickupTime              TIMESTAMP,
    DropTime                TIMESTAMP,
    PickupLocationId        INT,
    DropLocationId          INT,
    CabNumber               STRING,
    DriverLicenseNumber     STRING,
    PassengerCount          INT,
    TripDistance            DOUBLE,
    RatecodeId              INT,
    PaymentType             INT,
    TotalAmount             DOUBLE,
    FareAmount              DOUBLE,
    Extra                   DOUBLE,
    MtaTax                  DOUBLE,
    TipAmount               DOUBLE,
    TollsAmount             DOUBLE,         
    ImprovementSurcharge    DOUBLE,
    
    FileName                STRING,
    CreatedOn               TIMESTAMP
)

--USING DELTA

PARTITIONED BY (VendorId)

COMMENT "Bronze table for YellowTaxis"

AS

SELECT *

     --, INPUT_FILE_NAME()     AS FileName          -- For Hive metastore
     , _metadata.file_path     AS FileName            -- For Unity Catalog
     , CURRENT_TIMESTAMP()   AS CreatedOn

-- If Unity Catalog is enabled
FROM parquet.`/Volumes/oreillycatalog/default/datafiles/DLT/YellowTaxis/`

-- # If Unity Catalog is not enabled
-- FROM parquet.`/FileStore/Raw/DLT/YellowTaxis/`