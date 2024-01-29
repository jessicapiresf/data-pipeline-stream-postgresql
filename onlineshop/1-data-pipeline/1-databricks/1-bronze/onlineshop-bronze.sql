-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC # Implementar CDC no pipeline DLT:
-- MAGIC
-- MAGIC -----------------
-- MAGIC ###### Autor: Jéssica Pires de Freitas
-- MAGIC -----------------
-- MAGIC
-- MAGIC
-- MAGIC <img src="https://github.com/jessicapiresf/data-pipeline-stream-postgresql/blob/main/01-onlineshop/0-resources/arquitetura.png?raw=true"> 
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Importância da captura de dados de alteração (CDC)
-- MAGIC
-- MAGIC Change Data Capture (CDC) é o processo que captura as alterações nos registros feitas em um armazenamento de dados como banco de dados, data warehouse, etc. Essas alterações geralmente se referem a operações como exclusão, adição e atualização de dados.
-- MAGIC
-- MAGIC Uma maneira direta de replicação de dados é pegar um dump de banco de dados que exportará um banco de dados e importá-lo para um LakeHouse/DataWarehouse/Lake, mas esta não é uma abordagem escalonável.
-- MAGIC
-- MAGIC Change Data Capture, captura apenas as alterações feitas no banco de dados e aplica essas alterações ao banco de dados de destino. O CDC reduz a sobrecarga e oferece suporte a análises em tempo real. Ele permite o carregamento incremental e elimina a necessidade de atualização de carga em massa.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Abordagens do CDC 
-- MAGIC
-- MAGIC **1- Desenvolver processo interno de CDC:**
-- MAGIC
-- MAGIC - Tarefa complexa: a replicação de dados do CDC não é uma solução fácil e única. Devido às diferenças entre os provedores de banco de dados, aos vários formatos de registro e à inconveniência de acessar os registros de log, o CDC é um desafio.
-- MAGIC
-- MAGIC - Manutenção Regular: Escrever um script de processo de CDC é apenas o primeiro passo. Você precisa manter uma solução personalizada que possa mapear regularmente as alterações mencionadas. Isso requer muito tempo e recursos.
-- MAGIC
-- MAGIC - Sobrecarga: Os desenvolvedores nas empresas já enfrentam o fardo das consultas públicas. O trabalho adicional para a construção de uma solução CDC personalizada afetará os projetos existentes.
-- MAGIC
-- MAGIC **2- Utilização de ferramentas CDC como Debezium .**
-- MAGIC
-- MAGIC Neste repositório estamos usando dados de CDC provenientes de uma ferramenta do Debezium. Como uma ferramenta CDC lê logs de banco de dados: não dependemos mais da atualização de uma determinada coluna pelos desenvolvedores.
-- MAGIC
-- MAGIC - Uma ferramenta CDC como o Debezium se encarrega de capturar cada linha alterada. Ele registra o histórico de alterações de dados nos logs do Kafka, de onde o aplicativo os consome.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Setup/Requirements:
-- MAGIC
-- MAGIC Antes de executar este notebook como um pipeline, inclua um caminho para o notebook em um pipeline DLT, para permitir que esse notebook seja executado sobre os dados do CDC gerados.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Fluxo CDC com uma ferramenta CDC, autoloader e pipeline DLT:
-- MAGIC
-- MAGIC - Uma ferramenta CDC lê logs de banco de dados, produz mensagens json que incluem as alterações e transmite os registros com a descrição das alterações para Kafka
-- MAGIC - Kafka transmite as mensagens que contêm operações INSERT, UPDATE e DELETE e as armazena no armazenamento de objetos em nuvem (pasta S3, ADLS, etc).
-- MAGIC - Usando o Autoloader, carregamos gradativamente as mensagens do armazenamento de objetos em nuvem e as armazenamos na tabela Bronze, à medida que armazena as mensagens brutas
-- MAGIC - Em seguida, podemos executar APPLY CHANGES INTO na tabela limpa da camada Bronze para propagar os dados mais atualizados para a Silver Table
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ###Como é a saída do Debezium?
-- MAGIC
-- MAGIC Uma mensagem json descrevendo os dados alterados tem campos interessantes semelhantes a a lista abaixo:
-- MAGIC
-- MAGIC - operação: um código de operação (DELETE, APPEND, UPDATE, CREATE)
-- MAGIC - Operation_date: a data e o carimbo de data/hora do registro para cada ação de operação
-- MAGIC
-- MAGIC Alguns outros campos que você pode ver na saída do Debezium são:
-- MAGIC - before: a linha antes da alteração
-- MAGIC - after: a linha após a alteração
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### Carregamento incremental de dados usando o Auto Loader (cloud_files)
-- MAGIC Trabalhar com sistema externo pode ser desafiador devido à atualização do esquema. O banco de dados externo pode ter atualização de esquema, adição ou modificação de colunas, e nosso sistema deve ser robusto contra essas mudanças.
-- MAGIC O Databricks Autoloader (`cloudFiles`) lida com a inferência e a evolução do esquema imediatamente.
-- MAGIC
-- MAGIC O Autoloader nos permite ingerir com eficiência milhões de arquivos de um armazenamento em nuvem e oferecer suporte a inferência de esquema eficiente e evolução em escala. Neste notebook aproveitamos o Autoloader para lidar com dados de streaming (e lote).
-- MAGIC
-- MAGIC Vamos usá-lo para criar nosso pipeline e ingerir os dados JSON brutos entregues pelo Kafka + Debezium.

-- COMMAND ----------

-- DBTITLE 1,Vamos explorar nossos dados recebidos - Tabela Bronze - Autoloader e DLT
SET spark.source;
CREATE OR REFRESH STREAMING LIVE TABLE onlineshop_bronze (
    id string COMMENT "unique identifier of the record in the table",
    transactionno long COMMENT "a six-digit unique number that defines each transaction", 
    date string COMMENT "the date when each transaction was generated",
    productno long COMMENT "a five or six-digit unique character used to identify a specific product",
    productname string COMMENT "product/item name",
    price double COMMENT "the price of each product per unit in pound sterling (£)",
    customerno strinG COMMENT "the quantity of each product per transaction", 
    quantity long COMMENT "a five-digit unique number that defines each customer", 
    country string COMMENT "name of the country where the customer resides",
    op string COMMENT "CDC operation",
    operation_date timestamp COMMENT "CDC operation date",
    EventEnqueuedUtcTime string COMMENT "event enqueued utc time"
)
PARTITIONED BY (country)
TBLPROPERTIES ("quality" = "bronze")
COMMENT "New online shop data incrementally ingested from cloud object storage landing zone"
AS 
SELECT
    CAST(after.id AS string) as id,
    after.transactionno AS transactionno,
    after.date AS date,
    after.productno AS productno,
    after.productname AS productname,
    after.price AS price,
    CAST(after.customerno AS string) AS customerno,
    after.quantity AS quantity,
    after.country AS country,
    op,
    to_utc_timestamp(CAST(source.ts_ms / 1000 AS timestamp), 'UTC') AS operation_date,
    EventEnqueuedUtcTime
FROM
    cloud_files(
        "/mnt/pjstglakehouse/landing-zone/cdc/onlineshop",
        "json",
    map("cloudFiles.inferColumnTypes", "true")
  );

