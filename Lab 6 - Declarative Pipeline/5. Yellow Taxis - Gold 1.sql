-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Create gold table for location-wise summary

-- COMMAND ----------

CREATE MATERIALIZED VIEW YellowTaxis_SummaryByLocation_Gold

AS

SELECT PickupLocationId, DropLocationId

       , COUNT(RideId)        AS TotalRides
       , SUM(TripDistance)    AS TotalDistance
       , SUM(TotalAmount)     AS TotalAmount

FROM YellowTaxis_Silver
    
GROUP BY PickupLocationId, DropLocationId