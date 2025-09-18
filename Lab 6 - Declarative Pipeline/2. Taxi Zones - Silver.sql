-- Databricks notebook source
CREATE MATERIALIZED VIEW TaxiZones_Silver
(
    CONSTRAINT Valid_LocationId EXPECT (LocationId IS NOT NULL AND LocationId > 0) ON VIOLATION DROP ROW
)

COMMENT "Live Bronze table for Taxi Zones"

AS

SELECT *
FROM TaxiZones_Bronze