-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Create silver table

-- COMMAND ----------

CREATE MATERIALIZED VIEW YellowTaxis_Silver
(
    RideId                  INT               COMMENT 'This is the primary key column',
    VendorId                INT,
    PickupTime              TIMESTAMP,
    DropTime                TIMESTAMP,
    PickupLocationId        INT,
    DropLocationId          INT,    
    TripDistance            DOUBLE,    
    TotalAmount             DOUBLE,
    
    PickupYear              INT              GENERATED ALWAYS AS (YEAR  (PickupTime)),
    PickupMonth             INT              GENERATED ALWAYS AS (MONTH (PickupTime)),
    PickupDay               INT              GENERATED ALWAYS AS (DAY   (PickupTime)),
        
    CreatedOn               TIMESTAMP,
    
    
    -- Define the constraints
    CONSTRAINT Valid_TotalAmount    EXPECT (TotalAmount IS NOT NULL AND TotalAmount > 0) ON VIOLATION DROP ROW,
    
    CONSTRAINT Valid_TripDistance   EXPECT (TripDistance > 0)                            ON VIOLATION DROP ROW,
    
    CONSTRAINT Valid_RideId         EXPECT (RideId IS NOT NULL AND RideId > 0)           ON VIOLATION FAIL UPDATE
)

--USING DELTA

PARTITIONED BY (PickupLocationId)

AS

SELECT RideId
     , VendorId
     , PickupTime
     , DropTime
     , PickupLocationId
     , DropLocationId     
     , TripDistance
     , TotalAmount
     
     , CURRENT_TIMESTAMP()   AS CreatedOn

FROM YellowTaxis_Bronze