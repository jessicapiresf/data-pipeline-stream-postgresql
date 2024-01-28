# Databricks notebook source
# MAGIC %fs 
# MAGIC ls /mnt/pjstglakehouse/landing-zone/onlineshop

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog `hive_metastore`; select * from `business`.`onlineshop_silver` limit 100;

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog `hive_metastore`; select * from `business`.`onlineshop_gold_sales_report` limit 100;

# COMMAND ----------


