-- Databricks notebook source
CREATE MATERIALIZED VIEW TaxiZones_Bronze

COMMENT "Live Bronze table for Taxi Zones"

AS

SELECT *

-- If Unity Catalog is enabled
FROM parquet.`/Volumes/oreillycatalog/default/datafiles/DLT/TaxiZones.parquet`

-- # If Unity Catalog is not enabled
-- FROM parquet.`/FileStore/Raw/DLT/TaxiZones.parquet`