-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC ## Materializando a tabela Prata
-- MAGIC
-- MAGIC A tabela silver `onlineshop_silver` conterá a visualização mais atualizada. Será uma réplica da tabela PostgresSQL original.
-- MAGIC
-- MAGIC Para propagar as operações a camada Silver, devemos habilitar explicitamente o recurso no pipeline adicionando e habilitando a configuração applyChanges nas configurações do pipeline DLT
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Camada Prata - Tabela Limpa (Impor Restrições)
CREATE OR REFRESH TEMPORARY STREAMING LIVE TABLE onlineshop_bronze_clean_v(
  id string COMMENT "unique identifier of the record in the table",
  transactionno long COMMENT "a six-digit unique number that defines each transaction",
  date string COMMENT "the date when each transaction was generated",
  productno long COMMENT "a five or six-digit unique character used to identify a specific product",
  productname string COMMENT "product/item name",
  price double COMMENT "the price of each product per unit in pound sterling (£)",
  customerno string COMMENT "the quantity of each product per transaction",
  quantity long COMMENT "a five-digit unique number that defines each customer",
  country string COMMENT "name of the country where the customer resides",
  op string COMMENT "CDC operation",
  operation_date timestamp COMMENT "CDC operation date",
  EventEnqueuedUtcTime string COMMENT "event enqueued utc time",

  CONSTRAINT valid_id EXPECT (id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_transactionno EXPECT (transactionno IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_date EXPECT (date IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_productno EXPECT (productno IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_productname EXPECT (productname IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_price EXPECT (price IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_quantity EXPECT (quantity IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_country EXPECT (country IS NOT NULL)  ON VIOLATION DROP ROW
)
PARTITIONED BY (country)
TBLPROPERTIES ("quality" = "silver")
COMMENT "Cleansed bronze online shop view (i.e. what will become Silver)"
AS SELECT *
FROM STREAM(LIVE.onlineshop_bronze);


-- COMMAND ----------

-- DBTITLE 1,Excluir registros indesejados - Silver Table - DLT SQL 
CREATE OR REFRESH STREAMING LIVE TABLE onlineshop_silver
TBLPROPERTIES ("quality" = "silver")
PARTITIONED BY (country)
COMMENT "Clean, merged onlineshop";

-- COMMAND ----------

APPLY CHANGES INTO LIVE.onlineshop_silver
FROM stream(LIVE.onlineshop_bronze_clean_v)
  KEYS (id)
  APPLY AS DELETE WHEN op = "d"
  SEQUENCE BY operation_date --auto-incremental ID to identity order of events
  COLUMNS * EXCEPT (operation_date, EventEnqueuedUtcTime)
