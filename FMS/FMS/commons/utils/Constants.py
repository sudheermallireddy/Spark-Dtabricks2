# Databricks notebook source
# MAGIC %run ./TransformationLogger

# COMMAND ----------

# DBTITLE 1,Zone
RAW = "raw"
CURATED = "curated"
REFERENCE = "reference"

# COMMAND ----------

# DBTITLE 1,Format
AVRO = "avro"
PARQUET = "parquet"
CSV = "csv"
TEXT="txt"

# COMMAND ----------

# DBTITLE 1,Storage-Type
ADLS2 = "adls2"
SQL = "sql"

# COMMAND ----------

# DBTITLE 1,Others
FORWARD = "/"
DEBUG_MODE = "debug_mode"
DEBUG_MODE_Y = "y"
DEBUG_MODE_N = "n"
JDBC_URL_KEY = "jdbcUrl"
COMMA = ","
BLANK_SPACE = " "
CONTROLA = "\u0001"
HISTORY = "history"

# COMMAND ----------

# DBTITLE 1,Sites
MOGALAKWENA = "mogalakwena"
LOS_BRONCES = "los_bronces"
SISHEN = "sishen"
MINAS_RIO = "minas_rio"
KOLOMELA = "kolomela"
VENETIA = "venetia"
CAPCOAL = "capcoal"

# COMMAND ----------

# DBTITLE 1,Functional Domain
FMS = "fleet_management"
REFERENCE = "reference"

# COMMAND ----------

# DBTITLE 1,Entity
SHOVELS = "shovels"
TRUCKS = "trucks"

# COMMAND ----------

# DBTITLE 1,Write Modes
OVERWRITE = "overwrite"
APPEND = "append"

# COMMAND ----------

# DBTITLE 1,Adls-Details 
adls2_storage_account_name = get_value_from_KV("p101-mneustordlake001-name")
adls2_storage_account_key = get_value_from_KV("p101-mneustordlake001-key")

# COMMAND ----------

# DBTITLE 1,Db Connection
jdbcHostname = get_value_from_KV("p101-menu-db-shared-001-instance")
jdbcPort = get_value_from_KV("p101-menu-db-shared-001-port")
jdbcDatabase = get_value_from_KV("p101-menu-db-shared-001-dbname")
jdbcUsername = get_value_from_KV("p101-menu-db-shared-001-username")
jdbcPassword = get_value_from_KV("p101-menu-db-shared-001-password")

# COMMAND ----------

# DBTITLE 1,Spark Config
spark.conf.set("fs.azure.account.key.{}.dfs.core.windows.net".format(adls2_storage_account_name),adls2_storage_account_key)

# COMMAND ----------

# DBTITLE 1,Logger Config
debug_mode = dbutils.widgets.get(DEBUG_MODE)
logger = TransformationLogger(debug_mode)
logger.info("Constants:","Logger created with DEBUG_MODE : "+ debug_mode)

# COMMAND ----------

# DBTITLE 1,Application Insight Telemetry Client
telemetryClient = get_value_from_KV("p101-menu-ai-shared-001-telemetry")

# COMMAND ----------

# DBTITLE 1,Data Profiling
WARNING = "Warning"
SUCCESS = "Success"
FAILED = "Failed"