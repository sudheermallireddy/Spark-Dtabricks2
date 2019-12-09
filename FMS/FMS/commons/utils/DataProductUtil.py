# Databricks notebook source
TAG_DPU = "DataProductUtil"

# COMMAND ----------

# DBTITLE 1,Scope name
def get_scope_name():
  return "mneu-akv-da-001"

# COMMAND ----------

# DBTITLE 1,Key vaults
def get_value_from_KV(key):
  return dbutils.secrets.get(scope = get_scope_name(), key = key)

# COMMAND ----------

# MAGIC %run ../utils/TransformationLogger

# COMMAND ----------

# MAGIC %run ../exceptions/DataTransformationException

# COMMAND ----------

# DBTITLE 1,Fms Taxonomy
def get_fms_raw_base_taxonomy(input_config):
  try:
    path = "abfss://{}@{}.dfs.core.windows.net/{}/{}/{}/{}/{}/".format(RAW, \
                                                                                 adls2_storage_account_name, \
                                                                                 input_config.business_unit, \
                                                                                 input_config.site, \
                                                                                 input_config.functional_domain, \
                                                                                 input_config.system, \
                                                                                 input_config.date)
    return path
  except Exception as e:
    message = "Failed to generate the path for zone : {}, site: {}, business_unit: {}, functional_domain: {},\
          system: {}, date: {}".format(RAW, input_config.site, input_config.business_unit, \
                                       input_config.functional_domain, input_config.system, input_config.date)
    logger.error(TAG_DPU,"PathCreationException", message)
    raise DataTransformationException(TAG_DPU,"PathCreationException",message)
    
def get_fms_curated_base_taxonomy(input_config):
  try:
      path = "abfss://{}@{}.dfs.core.windows.net/{}/{}/{}/{}/{}/".format(CURATED, \
                                                                                adls2_storage_account_name, \
                                                                                input_config.business_unit, \
                                                                                input_config.site, \
                                                                                input_config.functional_domain, \
                                                                                input_config.system, \
                                                                                input_config.date)
      return path
  except:
    message = "Failed to generate the path for zone : {}, site: {}, business_unit: {}, functional_domain: {},\
      system: {}, date: {}".format(CURATED, input_config.site, input_config.business_unit, \
                                   input_config.functional_domain, input_config.system, input_config.date)
    logger.error(TAG_DPU,"PathCreationException", message)
    raise DataTransformationException(TAG_DPU,"PathCreationException",message)

# COMMAND ----------

# DBTITLE 1,Connection Details
def get_connection_properties():
  logger.debug("DataProductUtil","Connection Details","Creating the connection properties")
  try:
    jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
    connectionProperties = {
      "user" : jdbcUsername,
      "password" : jdbcPassword,
      "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver",
      "jdbcUrl" : jdbcUrl
    }
    return connectionProperties
  except Exception as e:
    logger.error(TAG_DPU,"ConnectionInstantiationException",str(e))
    raise DataTransformationException(TAG_DPU,"ConnectionInstantiationException","Failed to create the connection properties with " + jdbcHostname + ", "+ jdbcPort + ", "+ jdbcDatabase)

# COMMAND ----------

def get_final_input_path(base_input_path, input_file_name):
  return base_input_path +input_file_name.replace("[","\[").replace("]","\]")

# COMMAND ----------

# DBTITLE 1,File System Operation
def get_list_of_file_path(path):
  logger.debug(TAG_DPU,"File System Operation","Listing the file at path : {}".format(path))
  return dbutils.fs.ls(path)

def move_the_file(old_path, new_path):
  logger.debug(TAG_DPU,"File System Operation","Moving the file from {} to {}".format(old_path, new_path))
  return dbutils.fs.mv(old_path,new_path)

def delete_the_file(file_path):
  logger.debug(TAG_DPU,"File System Operation","Deleting the file : {}".format(file_path))
  return dbutils.fs.rm(file_path)

def delete_path_if_exist(path):
  try:
    list_of_file = dbutils.fs.ls(path)
    #only if the file exist in path
    if(len(list_of_file) > 0):
      #deleting individual files
      for file in list_of_file:
        logger.debug(TAG_DPU,"File System Operation","deleting {}".format(file.path))
        #to exclude the dir
        if(file.size > 0):
          try:
            dbutils.fs.rm(file.path)
          except Exception as e:
            logger.error(TAG_DPU,"File System Operation","Failed while deleting the file from {} with ERROR {}".format(path,str(e)))
            raise DataTransformationException(TAG_DPU,"File System Operation","Failed while deleting the file from : {}".format(path))
  except DataTransformationException as e:
    raise DataTransformationException(TAG_DPU, e.exeptionType, e.message)
  except Exception as e:
    logger.debug(TAG_DPU,"File System Operation","Failed to perform the delete path if exist operation for {}".format(path))

# COMMAND ----------

from pyspark.sql.functions import lit
  
def addSrcApplicationAndSiteNameColumns(dataFrame, inputConfig):
  return dataFrame.withColumn("Site_Name",lit(inputConfig.site.replace("_", " ").upper())) \
                    .withColumn("Src_Application",lit(inputConfig.src_application.replace("_", " ").upper()))

# COMMAND ----------

from pyspark.sql.functions import lit

def addSrcApplicationAndSiteNameColumnsCapcoalFms(dataFrame, inputConfig):
  return dataFrame.withColumn("Site_Name", expr("case when SRC_System = 'CAPCOAL' then 'CAPCOAL' " + "when SRC_System = 'DAWSON' then 'DAWSON' " + " end")).withColumn("Src_Application",lit(inputConfig.src_application.replace("_", " ").upper()))