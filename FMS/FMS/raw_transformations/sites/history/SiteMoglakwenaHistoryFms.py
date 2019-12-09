# Databricks notebook source
# MAGIC %run ../../RawTransformationFactory

# COMMAND ----------

# MAGIC %run ../../../commons/IOInterfaces

# COMMAND ----------

# MAGIC %run ../../../commons/utils/DataProductUtil

# COMMAND ----------

TAG_MOG_FMS = "Mog_History_Fms"

# COMMAND ----------

class SiteMoglakwenaHistoryFms(RawTransformationFactory):
  
    def read_data(self, zone, store_type, file_format, input_path):      
      logger.info(TAG_MOG_FMS,"Reading data for site Moglakwena History with file format : {} at {}".format(file_format, input_path))
      return Storage.get_instance(store_type).read_data(file_format,input_path) 
        
    def transform_data(self, input_df, input_config):
      logger.info(TAG_MOG_FMS,"Transforming data")
      return addSrcApplicationAndSiteNameColumns(input_df,input_config)
        
    def write_data(self, store_type, file_format, output_path, transformed_df):
      logger.info(TAG_MOG_FMS,"Writing data for site Moglakwena History in file format :{} at {} with store type : {}".format(file_format,output_path, store_type))
      Storage.get_instance(store_type).write_data(file_format, output_path, transformed_df)
    
    def get_adls2_path(self, zone, input_config):
      logger.debug(TAG_MOG_FMS,"Input Path","Creating input path for {} Zone".format(zone))
      if zone == RAW:
        try:
          return get_fms_raw_base_taxonomy(input_config)
        except DataTransformationException as e:
          logger.error(TAG_MOG_FMS,"PathCreationException", e.message)
          raise DataTransformationException(TAG_DPU,"PathCreationException",e.message)
          
      elif zone == CURATED:
        try:
          return get_fms_curated_base_taxonomy(input_config)
        except DataTransformationException as e:
          logger.error(TAG_MOG_FMS,"PathCreationException", e.message)
          raise DataTransformationException(TAG_DPU,"PathCreationException",e.message)

      elif zone == REFERENCE:
        try:
          path = "abfss://{}@{}.dfs.core.windows.net/{}/{}/".format(zone, \
                                                                 adls2_storage_account_name, \
                                                                 "equipment", "curated")
          return path
        except DataTransformationException as e:
          logger.error(TAG_MOG_FMS,"PathCreationException","Failed to generate the path for zone: {}".format(zone))
          raise DataTransformationException(TAG_MOG_FMS,"PathCreationException","Failed to generate the path for zone: {}".format(zone))
      

# COMMAND ----------

