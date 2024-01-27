# Databricks notebook source
# MAGIC %md
# MAGIC ## Entendendo a base de dados
# MAGIC Esse documento tem como objetivo entender a base de dados de trabalho

# COMMAND ----------

df = spark.read.csv("https://www.kaggle.com/datasets/gabrielramos87/an-online-shop-business/download?datasetVersionNumber=7")
df.display()

 = (spark.read
  .format("csv")
  .option("mode", "PERMISSIVE")
  .load("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv")
)

# COMMAND ----------


