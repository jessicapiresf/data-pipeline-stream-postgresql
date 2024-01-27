-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC # Implementar CDC no pipeline DLT: alterar captura de dados
-- MAGIC
-- MAGIC -----------------
-- MAGIC ###### Autor: Jéssica Pires de Freitas
-- MAGIC -----------------
-- MAGIC
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
-- MAGIC - Sobrecarga: Os desenvolvedores nas empresas já enfrentam o fardo das consultas públicas. O trabalho adicional para a construção de uma solução CDC personalizada afetará os projetos existentes de geração de receitas.
-- MAGIC
-- MAGIC **2- Utilização de ferramentas CDC como Debezium .**
-- MAGIC
-- MAGIC Neste repositório estamos usando dados de CDC provenientes de uma ferramenta do Debezium. Como uma ferramenta CDC lê logs de banco de dados: não dependemos mais da atualização de uma determinada coluna pelos desenvolvedores.
-- MAGIC
-- MAGIC - Uma ferramenta CDC como o Debezium se encarrega de capturar cada linha alterada. Ele registra o histórico de alterações de dados nos logs do Kafka, de onde seu aplicativo os consome.

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
-- MAGIC - Em seguida, podemos executar APPLY CHANGES INTO na tabela limpa da camada Bronze para propagar os dados mais atualizados downstream para a Silver Table
-- MAGIC
-- MAGIC Aqui está o fluxo que implementaremos, consumindo dados do CDC de um banco de dados externo. Observe que a entrada pode ter qualquer formato, incluindo fila de mensagens como Kafka.
-- MAGIC
-- MAGIC <img src="https://raw.githubusercontent.com/databricks/delta-live-tables-notebooks/main/change-data-capture-example/images/cdc_flow_new.png" alt='Prepare todos os seus dados para BI e ML'/>

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
    id long,
    transactionno long,
    date string,
    productno long,
    productname string,
    price double,
    customerno string, 
    quantity long, 
    country string,
    op string,
    operation_date timestamp,
    EventEnqueuedUtcTime string
)
TBLPROPERTIES ("quality" = "bronze")
COMMENT "Novos dados onlineshop ingeridos de forma incremental em landing-zone"
AS SELECT
    after.id,
    after.transactionno,
    after.date,
    after.productno,
    after.productname,
    after.price,
    CAST(after.customerno AS string) AS customerno,
    after.quantity,
    after.country,
    op,
    to_utc_timestamp(CAST(ts_ms / 1000 AS timestamp), 'UTC') AS operation_date,
    EventEnqueuedUtcTime
FROM
    cloud_files(
        "/mnt/pjstglakehouse/landing-zone/onlineshop_cdc",
        "json",
        map("cloudFiles.inferColumnTypes", "true")
    )
;

-- COMMAND ----------

-- DBTITLE 1,Camada Prata - Tabela Limpa (Impor Restrições)
CREATE OR REFRESH TEMPORARY STREAMING LIVE TABLE onlineshop_bronze_clean_v(
  CONSTRAINT valid_id EXPECT (id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_transactionno EXPECT (transactionno IS NOT NULL),
  CONSTRAINT valid_operation EXPECT (op IS NOT NULL) ON VIOLATION DROP ROW
)
TBLPROPERTIES ("quality" = "silver")
COMMENT "Cleansed bronze customer view (i.e. what will become Silver)"
AS SELECT * 
FROM STREAM(LIVE.onlineshop_bronze);

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Materializando a tabela Prata
-- MAGIC
-- MAGIC A tabela silver `onlineshop_silver` conterá a visualização mais atualizada. Será uma réplica da tabela MYSQL original.
-- MAGIC
-- MAGIC Para propagar as operações a camada Silver, devemos habilitar explicitamente o recurso no pipeline adicionando e habilitando a configuração applyChanges nas configurações do pipeline DLT
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Excluir registros indesejados - Silver Table - DLT SQL 
CREATE OR REFRESH STREAMING LIVE TABLE onlineshop_silver
TBLPROPERTIES ("quality" = "silver")
COMMENT "Clean, merged customers";

-- COMMAND ----------

APPLY CHANGES INTO LIVE.onlineshop_silver
FROM stream(LIVE.onlineshop_bronze_clean_v)
  KEYS (id)
  APPLY AS DELETE WHEN op = "d"
  SEQUENCE BY operation_date --auto-incremental ID to identity order of events
  COLUMNS * EXCEPT (operation_date, EventEnqueuedUtcTime)