-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Create gold table for date-wise summary

-- COMMAND ----------

CREATE MATERIALIZED VIEW YellowTaxis_SummaryByZone_Gold

AS

SELECT Zone, Borough

       , COUNT(RideId)        AS TotalRides
       , SUM(TripDistance)    AS TotalDistance
       , SUM(TotalAmount)     AS TotalAmount

FROM YellowTaxis_Silver yt

    JOIN TaxiZones_Silver tz  ON yt.PickupLocationId = tz.LocationId
    
GROUP BY Zone, Borough