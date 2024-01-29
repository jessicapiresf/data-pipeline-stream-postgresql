-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC ## Tabela Gold
-- MAGIC
-- MAGIC A tabela gold `onlineshop_gold` conterá um relatório baseado no número do produto, com a o total de quantidade e o valor total referente ao productno.
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE LIVE TABLE onlineshop_gold_sales_report(
  productno long COMMENT "a five or six-digit unique character used to identify a specific product",
  total_quantity_sold long COMMENT "total quantity sold",
  total_sales_value double COMMENT "total sales value"
)
TBLPROPERTIES(pipelines.reset.allowed = false)
COMMENT "Aggregated report by product number"
AS SELECT 
    productno,
    SUM(quantity) as total_quantity_sold,
    SUM(price * quantity) as total_sales_value
FROM LIVE.onlineshop_silver
GROUP BY productno;

