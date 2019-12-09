# Databricks notebook source
# MAGIC %run ./Constant

# COMMAND ----------

TAG_CAPCOAL = "CapcoalTransformationsFms" 

# COMMAND ----------

from pyspark.sql.functions import *
import time
import datetime


def cp_transform_tbl(base_input_path, site_executor, input_config, file_name):
  logger.info(TAG_CAPCOAL,"Performing Transformation for FMS Capcoal")
  
  try:
    file_path = get_final_input_path(base_input_path, file_name)
    file_read_df = site_executor.read_data(CURATED, ADLS2, PARQUET, file_path) 
    tbl_df = file_read_df
    tbl_df = tbl_df.replace('', 'NULL')
  except Exception as e:
    logger.error(TAG_CAPCOAL,"ReadException","Failed to read the data from path {}: {}".format(file_path, str(e)))
    raise DataTransformationException(TAG_CAPCOAL,"ReadException","Failed to read the data from path {}".format(file_path))
    
  return tbl_df

# COMMAND ----------

class CapcoalTransformationsFms:
  def execute(self, data_dict, site_executor, input_config):
    logger.info(TAG_CAPCOAL,"Executing for system {}".format(input_config.system))
    error_count = 0

    try:
      base_input_path = site_executor.get_adls2_path(CURATED, input_config)
      logger.info(TAG_CAPCOAL,"Base_input_path : "+base_input_path)
    except DataTransformationException as e:
      logger.error(TAG_CAPCOAL, e.exeptionType, e.message)
      raise SystemExit(e.message)
      
    try:
      tbl_equipment_df = cp_transform_tbl(base_input_path, site_executor,input_config,CP_EQUIPMENT)
      tbl_equipment_df = tbl_equipment_df.where(tbl_equipment_df["deleted_at"].isNull())
      data_dict[STG_EQUIPMENT] = tbl_equipment_df
    except DataTransformationException as e:
      logger.error(TAG_CAPCOAL,"TransformationException","Failed to transform the data for table: {}".format(STG_EQUIPMENT))
      error_count += 1
      
    try:
      tbl_data_extraction_enum_categories_df = cp_transform_tbl(base_input_path, site_executor,input_config,CP_DATA_EXTRACTION_ENUM_CATEGORIES)
      data_dict[STG_DATA_EXTRACTION_ENUM_CATEGORIES] = tbl_data_extraction_enum_categories_df
    except DataTransformationException as e:
      logger.error(TAG_CAPCOAL,"TransformationException","Failed to transform the data for table: {}".format(STG_DATA_EXTRACTION_ENUM_CATEGORIES))
      error_count += 1
      
    try:
      tbl_data_extraction_enum_tables_df = cp_transform_tbl(base_input_path, site_executor,input_config,CP_DATA_EXTRACTION_ENUM_TABLES)
      tbl_data_extraction_enum_tables_df = tbl_data_extraction_enum_tables_df.where(tbl_data_extraction_enum_tables_df["deleted_at"].isNull())
      data_dict[STG_DATA_EXTRACTION_ENUM_TABLES] = tbl_data_extraction_enum_tables_df
    except DataTransformationException as e:
      logger.error(TAG_CAPCOAL,"TransformationException","Failed to transform the data for table: {}".format(STG_DATA_EXTRACTION_ENUM_TABLES))
      error_count += 1
      
    try:
      tbl_data_extraction_grades_df = cp_transform_tbl(base_input_path, site_executor,input_config,CP_DATA_EXTRACTION_GRADES)
      tbl_data_extraction_grades_df = tbl_data_extraction_grades_df.where(tbl_data_extraction_grades_df["deleted_at"].isNull())
      data_dict[STG_DATA_EXTRACTION_GRADES] = tbl_data_extraction_grades_df
    except DataTransformationException as e:
      logger.error(TAG_CAPCOAL,"TransformationException","Failed to transform the data for table: {}".format(STG_DATA_EXTRACTION_GRADES))
      error_count += 1
      
    try:
      tbl_data_extraction_locations_df = cp_transform_tbl(base_input_path, site_executor,input_config,CP_DATA_EXTRACTION_LOCATIONS)
      tbl_data_extraction_locations_df = tbl_data_extraction_locations_df.where(tbl_data_extraction_locations_df["deleted_at"].isNull())
      data_dict[STG_DATA_EXTRACTION_LOCATIONS] = tbl_data_extraction_locations_df
    except DataTransformationException as e:
      logger.error(TAG_CAPCOAL,"TransformationException","Failed to transform the data for table: {}".format(STG_DATA_EXTRACTION_LOCATIONS))
      error_count += 1
      
    try:
      tbl_dim_day_df = cp_transform_tbl(base_input_path, site_executor,input_config,CP_DATA_EXTRACTION_MATERIAL_TONNAGES)
      data_dict[STG_DATA_EXTRACTION_MATERIAL_TONNAGES] = tbl_dim_day_df
    except DataTransformationException as e:
      logger.error(TAG_CAPCOAL,"TransformationException","Failed to transform the data for table: {}".format(STG_DATA_EXTRACTION_MATERIAL_TONNAGES))
      error_count += 1
      
    try:
      tbl_dim_day_df = cp_transform_tbl(base_input_path, site_executor,input_config,CP_DATA_EXTRACTION_SHIFT_BUCKETS)
      tbl_dim_day_df = tbl_dim_day_df.where(tbl_dim_day_df["deleted_at"].isNull())
      data_dict[STG_DATA_EXTRACTION_SHIFT_BUCKETS] = tbl_dim_day_df
    except DataTransformationException as e:
      logger.error(TAG_CAPCOAL,"TransformationException","Failed to transform the data for table: {}".format(STG_DATA_EXTRACTION_SHIFT_BUCKETS))
      error_count += 1
      
    try:
      tbl_dim_day_df = cp_transform_tbl(base_input_path, site_executor,input_config,CP_DATA_EXTRACTION_SHIFT_DUMPS)
      tbl_dim_day_df = tbl_dim_day_df.where(tbl_dim_day_df["deleted_at"].isNull())
      data_dict[STG_DATA_EXTRACTION_SHIFT_DUMPS] = tbl_dim_day_df
    except DataTransformationException as e:
      logger.error(TAG_CAPCOAL,"TransformationException","Failed to transform the data for table: {}".format(STG_DATA_EXTRACTION_SHIFT_DUMPS))
      error_count += 1
    
    try:
      tbl_dim_day_df = cp_transform_tbl(base_input_path, site_executor,input_config,CP_DATA_EXTRACTION_SHIFT_HAULS)
      tbl_dim_day_df = tbl_dim_day_df.where(tbl_dim_day_df["deleted_at"].isNull())
      data_dict[STG_DATA_EXTRACTION_SHIFT_HAULS] = tbl_dim_day_df
    except DataTransformationException as e:
      logger.error(TAG_CAPCOAL,"TransformationException","Failed to transform the data for table: {}".format(STG_DATA_EXTRACTION_SHIFT_HAULS))
      error_count += 1
      
    try:
      tbl_dim_day_df = cp_transform_tbl(base_input_path, site_executor,input_config,CP_DATA_EXTRACTION_SHIFT_LOADS)
      tbl_dim_day_df = tbl_dim_day_df.where(tbl_dim_day_df["deleted_at"].isNull())
      data_dict[STG_DATA_EXTRACTION_SHIFT_LOADS] = tbl_dim_day_df
    except DataTransformationException as e:
      logger.error(TAG_CAPCOAL,"TransformationException","Failed to transform the data for table: {}".format(STG_DATA_EXTRACTION_SHIFT_LOADS))
      error_count += 1
      
    try:
      tbl_dim_day_df = cp_transform_tbl(base_input_path, site_executor,input_config,CP_DATA_EXTRACTION_WORKERS)
      tbl_dim_day_df = tbl_dim_day_df.where(tbl_dim_day_df["deleted_at"].isNull())
      data_dict[STG_DATA_EXTRACTION_WORKERS] = tbl_dim_day_df
    except DataTransformationException as e:
      logger.error(TAG_CAPCOAL,"TransformationException","Failed to transform the data for table: {}".format(STG_DATA_EXTRACTION_WORKERS))
      error_count += 1
      
   
      
    if error_count >0:
      raise SystemExit("Tansformation failed for the Capcoal tables")
    
    return data_dict