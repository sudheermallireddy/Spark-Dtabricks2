# Databricks notebook source
# MAGIC %run ../RawTransformationFactory

# COMMAND ----------

# MAGIC %run ../../commons/IOInterfaces

# COMMAND ----------

# MAGIC %run ../../commons/utils/DataProductUtil

# COMMAND ----------

TAG_FMS_KOLOMELA = "FMS_KOLOMELA"

# COMMAND ----------

class SiteKolomelaFms(RawTransformationFactory):
  
    def read_data(self, zone, store_type, file_format, input_path):      
      logger.info(TAG_FMS_KOLOMELA,"Reading data for site Kolomela with file format : {} at {}".format(file_format, input_path))
      return Storage.get_instance(store_type).read_data(file_format,input_path) 
        
    def transform_data(self, input_df, input_config):
      logger.info(TAG_FMS_KOLOMELA,"Transforming data")
      return addSrcApplicationAndSiteNameColumns(input_df,input_config)
        
    def write_data(self, store_type, file_format, output_path, transformed_df):
      logger.info(TAG_FMS_KOLOMELA,"Writing data for site Kolomela in file format :{} at {} with store type : {}".format(file_format,output_path, store_type))
      Storage.get_instance(store_type).write_data(file_format, output_path, transformed_df)
    
    def get_adls2_path(self, zone, input_config):
      logger.debug(TAG_FMS_KOLOMELA,"Input Path","Creating input path for {} Zone".format(zone))
      if zone == RAW:
        try:
          return get_fms_raw_base_taxonomy(input_config)
        except DataTransformationException as e:
          logger.error(TAG_FMS_KOLOMELA,"PathCreationException", e.message)
          raise DataTransformationException(TAG_FMS_KOLOMELA,"PathCreationException",e.message)
          
      elif zone == CURATED:
        try:
          return get_fms_curated_base_taxonomy(input_config)
        except DataTransformationException as e:
          logger.error(TAG_FMS_KOLOMELA,"PathCreationException", e.message)
          raise DataTransformationException(TAG_FMS_KOLOMELA,"PathCreationException",e.message)

      elif zone == REFERENCE:
        try:
          path = "abfss://{}@{}.dfs.core.windows.net/{}/{}/".format(zone, \
                                                                 adls2_storage_account_name, \
                                                                 "equipment","curated")
          return path
        except:
          logger.error(TAG_FMS_KOLOMELA,"PathCreationException","Failed to generate the path for zone: {}".format(zone))
          raise DataTransformationException(TAG_FMS_KOLOMELA,"PathCreationException","Failed to generate the path for zone: {}".format(zone))

# COMMAND ----------

