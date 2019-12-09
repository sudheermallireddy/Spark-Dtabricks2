# Databricks notebook source
# MAGIC %run ./Constant

# COMMAND ----------

TAG_TV = "VenetiaTransformationsFms"

# COMMAND ----------

# DBTITLE 1,src_application values
SORD = "sord"
WENCO = "wenco"

# COMMAND ----------

# DBTITLE 1,Equipment
from pyspark.sql.functions import lit, upper, trim
from pyspark.sql.types import *

def vn_transform_tbl_Equipment(base_input_path, site_executor, input_config):
  logger.info(TAG_TV,"Performaing Transformation for Equipment")
  
  site_name=input_config.site
  #fetching the reference input path
  reference_input_path = site_executor.get_adls2_path(REFERENCE, None)
  logger.info(TAG_TV,"reference_input_path : "+reference_input_path)
  
  try:
    equip_path = get_final_input_path(base_input_path, EQUIP)
    equip_raw_df = site_executor.read_data(CURATED, ADLS2, PARQUET, equip_path)
    equip_df = equip_raw_df.select("EQUIP_IDENT", "FLEET_IDENT", "ACTIVE", "EQP_TYPE", "EQMODEL_CODE", "TEST").filter(" TEST == 'N' ")
    
  except Exception as e:
    logger.error(TAG_TV,"ReadException","Failed to read the data from path {} : {}".format(equip_path, str(e)))
    raise DataTransformationException(TAG_TV,"ReadException","Failed to read the data from path {}".format(equip_path))
  
  try:
    fleet_path = get_final_input_path(base_input_path, FLEET)
    fleet_row_df = site_executor.read_data(CURATED, ADLS2, PARQUET, fleet_path)
    fleet_df = fleet_row_df.select("FLT_FLEET_IDENT")
    
  except Exception as e:
    logger.error(TAG_TV,"ReadException","Failed to read the data from path {} : {}".format(equip_path, str(e)))
    raise DataTransformationException(TAG_TV,"ReadException","Failed to read the data from path {}".format(equip_path))

  try:
    equip_type_path = get_final_input_path(base_input_path, EQUIP_TYPE)
    equip_type_df = site_executor.read_data(CURATED, ADLS2, PARQUET, equip_type_path).select("EQTYPE_IDENT", "SHORT_NAME", "DESCRIP")
    
  except Exception as e:
    logger.error(TAG_TV,"ReadException","Failed to read the data from path {} : {}".format(equip_type_path, str(e)))
    raise DataTransformationException(TAG_TV,"ReadException","Failed to read the data from path {}".format(equip_type_path))

  try:
    eqmt_model_path = get_final_input_path(base_input_path, EQUIP_MODEL)
    eqmt_model_df = site_executor.read_data(CURATED, ADLS2, PARQUET, eqmt_model_path).withColumnRenamed("EQMODEL_CODE","MODEL_EQMODEL_CODE").select("MODEL_DESC", "MODEL_EQMODEL_CODE")
    
  except Exception as e:
    logger.error(TAG_TV,"ReadException","Failed to read the data from path {} : {}".format(eqmt_model_path, str(e)))
    raise DataTransformationException(TAG_TV,"ReadException","Failed to read the data from path {}".format(eqmt_model_path))

  try:
    catalog_path = get_final_input_path(reference_input_path, EQUIPMENT_CATALOGUE)
    eqmt_catalog_df = site_executor.read_data(REFERENCE, ADLS2,PARQUET,catalog_path) \
 .select("MDM_Global_Site_Code","SRC_System","IsReportable","Plant_No","MDM_Global_Plant_No","Make_Code","Make_Name","Category_Code","Category_Name","Type_Code","Type_Name","Act_Bucket_Bowl_Size_m3","Act_Rated_Bucket_Bowl_Capacity_t","Act_Rated_Suspended_Load_t","Equipment_Model_Code","Equipment_Model_Name","Equipment_Model_ID","Equipment_Variant_Code","Equipment_Variant_Name","Equipment_Variant_ID","Mining_Method","Site_Fleet_Code","Site_Fleet_Description","Site_Equipment_Description","Site_Production_Type_Code","Site_Production_Type_Description","Equipment_Is_Active","Group_Performance_Report","Equipment_Is_Hired","Global_Benchmark_Report","Benchmark_Report_Category","Equipment_Model_Type","Equipment_Fleet_Code","BM_Report_Label")
  except Exception as e:
    logger.error(TAG_TV,"ReadException","Failed to read the data from path {} : {}".format(catalog_path, str(e)))
    raise DataTransformationException(TAG_TV,"ReadException","Failed to read the data from path {}".format(catalog_path))
    

  #performing join based on logical model
  try:
    cleansed_equip_catalogue_df = eqmt_catalog_df.filter((upper(eqmt_catalog_df.SRC_System) == lit(site_name.upper())) & \
                                                   (eqmt_catalog_df.Plant_No.isNotNull())) \
                            .withColumn("MDM_Global_Plant_No", trim(eqmt_catalog_df.MDM_Global_Plant_No)) 

    from pyspark.sql.types import StringType
    tbl_Equipment_df =  equip_df.join(cleansed_equip_catalogue_df, (equip_df.EQUIP_IDENT == trim(cleansed_equip_catalogue_df.Plant_No)), "fullouter")\
                       .join(fleet_df, (equip_df.FLEET_IDENT == fleet_df.FLT_FLEET_IDENT), "left") \
                       .join(equip_type_df, (equip_df.EQP_TYPE == equip_type_df.EQTYPE_IDENT), "left") \
                       .join(eqmt_model_df, (equip_df.EQMODEL_CODE == eqmt_model_df.MODEL_EQMODEL_CODE), "left") \
                       .withColumnRenamed("EQUIP_IDENT", "Field_Id") \
                       .withColumnRenamed("IsReportable","Is_Reportable") \
                       .withColumn("Src_Model_Type_ID", when((col("EQP_TYPE").isNull()), -1).otherwise(col("EQP_TYPE"))) \
                       .withColumn("Src_Model_Type_Code",when((col("SHORT_NAME").isNull()),'Unknown').otherwise(col("SHORT_NAME"))) \
                       .withColumn("Src_Model_Type_Description", when(equip_type_df.DESCRIP.isNull(), 'Unknown').otherwise(equip_type_df.DESCRIP)) \
                       .withColumn("Src_Fleet_Description", when((col("MODEL_DESC").isNull()), -1).otherwise(col("MODEL_DESC"))) \
                       .withColumn("Src_Fleet_ID", when(equip_df.EQMODEL_CODE.isNull(), -1).otherwise(equip_df.EQMODEL_CODE)) \
                       .withColumn("Equipment_Is_Active", when(cleansed_equip_catalogue_df.Equipment_Is_Active.isNotNull(), cleansed_equip_catalogue_df.Equipment_Is_Active).otherwise(equip_df.ACTIVE)) \
                       .withColumn("src_application",lit(input_config.src_application.upper())) \
                       .withColumn("site_Name",lit(site_name.upper())) \
                       .withColumn('equipment_id',lit(None).cast(StringType())) \
                       .select("equipment_id","Field_Id", "Src_Model_Type_Id", "Src_Model_Type_Code","Src_Model_Type_Description", \
					   "Src_Fleet_ID","Src_Fleet_Description","MDM_Global_Site_Code","SRC_System", \
                           "Is_Reportable","Plant_No","MDM_Global_Plant_No","Make_Code", \
                           "Make_Name","Category_Code","Category_Name","Type_Code", \
                           "Type_Name","Act_Bucket_Bowl_Size_m3","Act_Rated_Bucket_Bowl_Capacity_t","Act_Rated_Suspended_Load_t", \
                           "Equipment_Model_Code","Equipment_Model_Name","Equipment_Model_ID","Equipment_Variant_Code", \
                           "Equipment_Variant_Name","Equipment_Variant_ID","Mining_Method","Site_Fleet_Code", \
                           "Site_Fleet_Description","Site_Equipment_Description", \
                           "Site_Production_Type_Code","Site_Production_Type_Description", \
                           "Equipment_Is_Active","Group_Performance_Report","Equipment_Is_Hired","Global_Benchmark_Report", \
                           "Benchmark_Report_Category","Equipment_Model_Type","Equipment_Fleet_Code", \
                           "BM_Report_Label","site_name","src_application")
    
    
  except Exception as e:
    logger.error(TAG_TV,"TransformationException","Failed to perform join operation for table Equipment : {}".format(str(e)))
    raise DataTransformationException(TAG_TV,"TransformationException",\
                                      "Failed to perform join operation for table Equipment")
  return tbl_Equipment_df.distinct()

# COMMAND ----------

# DBTITLE 1,Equipment Status 
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import col, when, lit
from pyspark.sql.types import *

def vn_transform_tbl_equipment_status(base_input_path, site_executor, input_config):
  logger.info(TAG_TV,"Performaing Transformation for Equipment Status")
  
  site_name=input_config.site
  # Getting Equipment Status tables
  eqmt_status_trans_path = get_final_input_path(base_input_path, EQUIPMENT_STATUS_TRANS)
  eqmt_status_trans_df = site_executor.read_data(CURATED, ADLS2, PARQUET, eqmt_status_trans_path)
  
  try:
     transformed_Equipment_Status_df = eqmt_status_trans_df.withColumn('status_start_time',lit(None).cast(StringType())) \
                    .withColumn('shift_id',lit(None).cast(LongType())) \
                    .withColumn('status_start_timestamp',col('START_TIMESTAMP')) \
                    .withColumnRenamed('EQUIP_IDENT','equipment') \
                    .withColumn('status_start_time_seconds',lit(None).cast(IntegerType())) \
                    .withColumnRenamed('STATUS_CODE', 'delay_reason') \
                    .withColumn('delay_category_id',lit(None).cast(StringType())) \
                    .withColumn('flag_codes',lit(None).cast(StringType())) \
                    .withColumn('delay_type',lit(None).cast(StringType())) \
                    .withColumnRenamed('SUB_STATUS_CODE', 'delay_type_code') \
                    .withColumn('comment',lit(None).cast(StringType())) \
                    .withColumn('operator_id',lit(None).cast(StringType())) \
                    .withColumn('is_aux',lit(None).cast(IntegerType())) \
                    .withColumn('source_id',col('EQUIP_STATUS_REC_IDENT').cast(LongType())) \
                    .withColumn('duration_seconds',(unix_timestamp("END_TIMESTAMP") - unix_timestamp("START_TIMESTAMP")).cast(IntegerType()))\
                    .withColumn('shift_date', col('SHIFT_DATE'))\
                    .withColumnRenamed('SHIFT_IDENT','shift_code')\
                    .withColumn('status_end_timestamp', col('END_TIMESTAMP') )
      
  except Exception as e:
    logger.error(TAG_TV,"TransformationException","Failed to perform join operation for table Equipment status : {}".format(str(e)))
    raise DataTransformationException(TAG_TV,"TransformationException",\
                                      "Failed to perform join operation for table Equipment status")

  tbl_Equipment_Status_df = transformed_Equipment_Status_df.select("status_start_time","shift_id","status_start_timestamp", \
                                                                   "duration_seconds","equipment","delay_reason","status_start_time_seconds", \
                                                                   "delay_category_id","flag_codes","delay_type_code", \
                                                                   "delay_type","comment","operator_id","is_aux","site_name", "src_application","status_end_timestamp","shift_code","shift_date","source_id")

  
  return tbl_Equipment_Status_df.distinct().repartition(200)

# COMMAND ----------

# DBTITLE 1,Delay Reasons
# Getting Delay Reasons Dependent Entity Tables
from pyspark.sql.functions import lit, regexp_replace
from pyspark.sql.types import *

def vn_transform_tbl_delay_reasons(base_input_path, site_executor, input_config):
  logger.info(TAG_TV,"Performaing Transformation for Delay Reasons")
  site_name=input_config.site
  
  try:
    dim_downtime_path = get_final_input_path(base_input_path, DIM_DOWNTIME)
    dim_downtime_df = site_executor.read_data(CURATED, ADLS2, PARQUET, dim_downtime_path)
    
    filterd_dim_downtime_df= dim_downtime_df.filter("Level_2 != 'Not Available'").filter("Level_2 != 'Not Applicable'")
    
  except Exception as e:
    logger.error(TAG_TV,"ReadException","Failed to read the data from path {} : {}".format(dim_downtime_path, str(e)))
    raise DataTransformationException(TAG_TV,"ReadException","Failed to read the data from path {}".format(dim_downtime_path))

  try:
    tbl_Delay_Reasons_df = filterd_dim_downtime_df.withColumnRenamed("Level_2", "Delay_Reason_Id") \
                   .withColumn("delay_type_code", when((col("level_3").isNull()), "N/A").otherwise(col("level_3"))) \
                   .withColumn("delay_type", when((col("Reference_C").isNull()), "Unknown").otherwise(col("Reference_C"))) \
                   .withColumnRenamed("Reference_A", "Delay_Category") \
                   .withColumn("Time_Model_Code",regexp_replace('Level_1', '_R&M', ''))\
                   .withColumnRenamed("Level_1", "Delay_Category_ID") \
                   .withColumnRenamed("Reference_B", "Delay_Reason_Description") \
				   .withColumnRenamed("isactive", "Is_Active") \
				   .withColumnRenamed("isdeleted", "Is_Deleted") \
                   .select("site_name","src_application","delay_reason_id","delay_reason_description",\
         "delay_type_code","delay_type","delay_category_id","delay_category","Time_Model_Code","Is_Active","Is_Deleted")
  except Exception as e:
    logger.error(TAG_TV,"TransformationException","Failed to perform join operation for table Delay Reasons: {}".format(str(e)))
    raise DataTransformationException(TAG_TV,"TransformationException",\
                                      "Failed to perform join operation for table Delay Reasons")
    
  return tbl_Delay_Reasons_df.distinct()

# COMMAND ----------

# DBTITLE 1,Material
# Getting Material Entity Table
from pyspark.sql.functions import *
from pyspark.sql.types import *

def vn_transform_tbl_material(base_input_path, site_executor, input_config):
  logger.info(TAG_TV,"Performaing Transformation for Material")
  site_name = input_config.site
  try:
    material_path = get_final_input_path(base_input_path, MATERIAL)
    material_df = site_executor.read_data(CURATED, ADLS2, PARQUET, material_path)
  except Exception as e:
    logger.error(TAG_TV,"ReadException","Failed to read the data from path {} : {}".format(material_path, str(e)))
    raise DataTransformationException(TAG_TV,"ReadException","Failed to read the data from path {}".format(material_path))
    
  try:
    material_type_path = get_final_input_path(base_input_path, MATERIAL_TYPE)
    material_type_df = site_executor.read_data(CURATED, ADLS2, PARQUET, material_type_path).select("DESCRIP", "MAT_TYPE")
  except Exception as e:
    logger.error(TAG_TV,"ReadException","Failed to read the data from path {} : {}".format(material_type_path, str(e)))
    raise DataTransformationException(TAG_TV,"ReadException","Failed to read the data from path {}".format(material_type_path))

# Transforming the Material entity, no such transformations just need to rename and copy as-is
  
  try:
    tbl_Material_df = material_df.join(material_type_df, (material_df.MAT_MATERIAL_GROUP == material_type_df.MAT_TYPE), "left") \
                .withColumn("Material_Id", col("MAT_MATERIAL_IDENT").cast(LongType())) \
				.withColumn("enum_type_id", lit(None).cast(LongType())) \
                .withColumn("idx", lit(None).cast(IntegerType())) \
                .withColumn("Material_Description", col("MAT_DESC")) \
                .withColumnRenamed("MAT_DESC", "Material_Abbreviation") \
                .withColumn("logical_order", lit(None).cast(IntegerType())) \
                .withColumn('Is_Ore', when((col("MAT_ORE_FLAG") == 'Y'), 1).otherwise(0)) \
                .withColumn('Is_Waste', when((col("MAT_ORE_FLAG") == 'N'), 1).otherwise(0)) \
                .withColumn("Load_Group",col("MAT_MATERIAL_GROUP").cast(StringType())) \
                .withColumnRenamed("DESCRIP", "Load_Group_Name") \
                .withColumn("Units", lit(None).cast(StringType())) \
                .withColumn("units_name", lit(None).cast(StringType())) \
                .withColumn("is_active", col("MAT_ACTIVE").cast(StringType())) \
                .select("Material_Id","enum_type_id","Idx", "Material_Description", \
                        "Material_Abbreviation","Logical_Order","Is_Ore", \
                        "Is_Waste","Load_Group","Load_Group_Name","Units", \
                        "Units_Name", "Site_Name","Src_Application","is_active")
    
  except Exception as e:
    logger.error(TAG_TV,"TransformationException","Failed to perform column rename operation for table Material: {}".format(str(e)))
    raise DataTransformationException(TAG_TV,"TransformationException",\
                                      "Failed to perform column rename operation for table Material")
  return tbl_Material_df.distinct()

# COMMAND ----------

# DBTITLE 1,Truck Cycles
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import col, when, lit

# Getting Material Entity Table
def vn_transform_tbl_truck_cycles(base_input_path, site_executor, input_config):
  site_name = input_config.site
 
  try:
    haul_cycle_trans_path = get_final_input_path(base_input_path, HAUL_CYCLE_TRANS)
    haul_cycle_trans_df = site_executor.read_data(CURATED, ADLS2, PARQUET, haul_cycle_trans_path) \
                          .select("START_SHIFT_IDENT","HAUL_CYCLE_REC_IDENT","LOAD_START_SHIFT_DATE","LOAD_START_SHIFT_IDENT","MATERIAL_IDENT", \
                                 "LOAD_START_TIMESTAMP","HAULING_UNIT_BADGE_IDENT","DUMP_LOCATION_SNAME","LOAD_LOCATION_SNAME","HAUL_DISTANCE", \
                                 "EFH_HAUL_DISTANCE","DUMP_END_TIMESTAMP","START_TIMESTAMP","QUANTITY_REPORTING","START_SHIFT_DATE", \
                                 "src_application","site_name","HAULING_UNIT_IDENT","LOADING_UNIT_IDENT")
  except Exception as e:
    logger.error(TAG_TV,"ReadException","Failed to read the data from path {}: {}".format(haul_cycle_trans_path, str(e)))
    raise DataTransformationException(TAG_TV,"ReadException","Failed to read the data from path {}".format(haul_cycle_trans_path))
    
    
  try:
    equipment_trans_path = get_final_input_path(base_input_path, EQUIPMENT_TRANS)
    equipment_trans_df = site_executor.read_data(CURATED, ADLS2, PARQUET, equipment_trans_path) \
                         .select("HAUL_CYCLE_REC_IDENT","EQUIP_IDENT")
  except Exception as e:
    logger.error(TAG_TV,"ReadException","Failed to read the data from path {}: {}".format(equipment_trans_path, str(e)))
    raise DataTransformationException(TAG_TV,"ReadException","Failed to read the data from path {}".format(equipment_trans_path))
  
  try:
    material_path = get_final_input_path(base_input_path, MATERIAL)
    material_df = site_executor.read_data(CURATED, ADLS2, PARQUET, material_path).select("MAT_MATERIAL_IDENT","MAT_ORE_FLAG")
  except Exception as e:
    logger.error(TAG_TV,"ReadException","Failed to read the data from path {} : {}".format(material_path, str(e)))
    raise DataTransformationException(TAG_TV,"ReadException","Failed to read the data from path {}".format(material_path))
  
  try:
    equip_path = get_final_input_path(base_input_path, EQUIP)
    equip_df = site_executor.read_data(CURATED, ADLS2, PARQUET, equip_path).select("DESCRIP","EQUIP_IDENT")
    equip_df = equip_df.alias("EQ")
    equip_df1 = equip_df.alias("LU")
    haul_cycle_trans_df = haul_cycle_trans_df.alias("HCT")
    material_df = material_df.alias("MAT")
  except Exception as e:
    logger.error(TAG_TV,"ReadException","Failed to read the data from path {} : {}".format(equip_path, str(e)))
    raise DataTransformationException(TAG_TV,"ReadException","Failed to read the data from path {}".format(equip_path))
    
  try:
      joined_df = haul_cycle_trans_df.join(equip_df, (col("HCT.HAULING_UNIT_IDENT") == col("EQ.EQUIP_IDENT")),'left')
      joined_dfs1=joined_df.join(equip_df1, (col("HCT.LOADING_UNIT_IDENT") == col("LU.EQUIP_IDENT")), 'left')
      joined_dfs_2 = joined_dfs1.join(material_df,(col("MAT.MAT_MATERIAL_IDENT") == col("HCT.MATERIAL_IDENT")),'left')
                        
  except Exception as e:
    message = "Failed to perform join between the source tables : {}".format(str(e))
    logger.error(TAG_TV,"ReadException",message)
    raise DataTransformationException(TAG_TV,"ReadException",message)
  
  try:
    truck_cycle_df = joined_dfs_2.withColumnRenamed("START_SHIFT_IDENT","shift_id") \
                               .withColumn("source_load_id",haul_cycle_trans_df.HAUL_CYCLE_REC_IDENT) \
                               .withColumnRenamed("LOAD_START_SHIFT_DATE","shift_date") \
                               .withColumnRenamed("LOAD_START_SHIFT_IDENT","load_start_shift_id") \
                               .withColumnRenamed("LOAD_START_TIMESTAMP","load_start_timestamp") \
                               .withColumnRenamed("HAULING_UNIT_BADGE_IDENT","truck_operator_id") \
                               .withColumnRenamed("DUMP_LOCATION_SNAME","dump_location") \
                               .withColumnRenamed("LOAD_LOCATION_SNAME","load_location") \
                               .withColumn("load_travel_distance",col("HAUL_DISTANCE").cast(StringType())) \
                               .withColumn("load_equivalent_flat_haul_distance",col("EFH_HAUL_DISTANCE").cast(StringType())) \
                               .withColumn("dump_full_travel_duration",(unix_timestamp("DUMP_END_TIMESTAMP") - \
                                                                        unix_timestamp("START_TIMESTAMP")).cast(IntegerType())) \
                               .withColumn("is_ore", when(material_df.MAT_ORE_FLAG == 'Y', 1).otherwise(0)) \
                               .withColumn("is_waste", when(material_df.MAT_ORE_FLAG == 'N', 1).otherwise(0)) \
                               .withColumn("material_id",col("MATERIAL_IDENT").cast(LongType())) \
                               .withColumn("equipment", col("LU.EQUIP_IDENT")) \
                               .withColumn("truck",joined_df.EQUIP_IDENT) \
                               .withColumn("load_tonnage",haul_cycle_trans_df.QUANTITY_REPORTING) \
                               .withColumn("load_tons",haul_cycle_trans_df.QUANTITY_REPORTING) \
                               .select("shift_id","source_load_id","shift_date","load_start_shift_id","load_start_timestamp","truck_operator_id", \
                                        "load_equivalent_flat_haul_distance","dump_location","load_tonnage", \
                                       "load_location","dump_full_travel_duration","load_travel_distance","is_ore","is_waste","material_id",\
                                       "equipment", "truck","load_tons","src_application","site_name")        
  except Exception as e:
    logger.error(TAG_TV,"TransformationException","Failed to perform column rename operation for table Truck Cycles: {}".format(str(e)))
    raise DataTransformationException(TAG_TV,"TransformationException","Failed to perform column rename operation for table Truck Cycles")
  
  return truck_cycle_df.distinct()

# COMMAND ----------

# DBTITLE 1,Person
from pyspark.sql.functions import lit
from pyspark.sql.types import *
def vn_transform_tbl_person(base_input_path, site_executor, input_config):
  logger.info(TAG_TV,"Performaing Transformation for person")
  site_name=input_config.site
  try:
    person_path = get_final_input_path(base_input_path, BADGE)
    person_df = site_executor.read_data(CURATED, ADLS2, PARQUET, person_path)
    tbl_person_df = person_df.withColumnRenamed("BADGE_IDENT","Person_Id") \
                             .withColumn("Person_full_Name", lit(concat(when(col("FIRST_NAME").isNotNull(), col("FIRST_NAME")).otherwise(lit("")), lit(" "), when(col("LAST_NAME").isNotNull(), col("LAST_NAME")).otherwise(lit(""))))) \
                             .withColumnRenamed("LAST_NAME","last_name")\
                             .withColumnRenamed("FIRST_NAME","first_name")\
                             .withColumnRenamed("MIDDLE_INITIAL","middle_initial")\
                             .withColumnRenamed("ACTIVE","is_active")\
                             .select("person_id","person_full_name","site_name",\
            "src_application","last_name","first_name","middle_initial","is_active")
                                      
  except Exception as e:
    logger.error(TAG_TV,"ReadException","Failed to read the data from path {}: {}".format(person_path, str(e)))
    raise DataTransformationException(TAG_TV,"ReadException","Failed to read the data from table {}".format(person_path))
    
  return tbl_person_df.distinct()

# COMMAND ----------

class VenetiaTransformationsFms:
  def execute(self, data_dict, site_executor, input_config):
    logger.info(TAG_TV,"Excuting for site {}".format(input_config.site))
    error_count = 0
    
    try:
      base_input_path = site_executor.get_adls2_path(CURATED, input_config)
      logger.info(TAG_CTS,"base_input_path :{} ".format(base_input_path))
    except DataTransformationException as e:
      logger.error(TAG_TV, e.exeptionType, e.message)
      raise SystemExit(e.message)
    
    if(input_config.system == WENCO):
      try:
        data_dict[STG_VN_EQUIPEMENT] = None
        tbl_Equipment_df = vn_transform_tbl_Equipment(base_input_path, site_executor,input_config)
        data_dict[STG_VN_EQUIPEMENT] = tbl_Equipment_df
      except DataTransformationException as e:
        logger.error(TAG_TV,"TransformationException","Failed to transform the data for table: {}".format(STG_VN_EQUIPEMENT))
        error_count += 1
        
      try:
        data_dict[STG_VN_EQUIPEMENT_STATUS] = None
        tbl_Equipment_Status_df = vn_transform_tbl_equipment_status(base_input_path, site_executor, input_config)
        data_dict[STG_VN_EQUIPEMENT_STATUS] = tbl_Equipment_Status_df
      except DataTransformationException as e:
        logger.error(TAG_TV,"TransformationException","Failed to transform the data for \
        table: {}".format(STG_VN_EQUIPEMENT_STATUS))
        error_count += 1
        
      try:
        data_dict[STG_VN_MATERIAL] = None
        tbl_Material_df = vn_transform_tbl_material(base_input_path, site_executor, input_config)
        data_dict[STG_VN_MATERIAL] = tbl_Material_df
      except DataTransformationException as e:
        logger.error(TAG_TV,"TransformationException","Failed to transform the data for table: {}".format(STG_VN_MATERIAL))
        error_count += 1
        
      try:
        data_dict[STG_VN_TRUCK_CYCLES] = None
        truck_cycle_df = vn_transform_tbl_truck_cycles(base_input_path, site_executor, input_config)
        data_dict[STG_VN_TRUCK_CYCLES] = truck_cycle_df
      except DataTransformationException as e:
        logger.error(TAG_TV,"TransformationException","Failed to transform the data for table: {}".format(STG_VN_TRUCK_CYCLES))
        error_count += 1
        
        
      try:
        data_dict[STG_VN_PERSON] = None
        tbl_person_df = vn_transform_tbl_person(base_input_path, site_executor, input_config)
        data_dict[STG_VN_PERSON] = tbl_person_df      
      except DataTransformationException as e:
        logger.error(TAG_TV,"TransformationException","Failed to transform the data for table: {}".format(STG_VN_PERSON) + "\n" + e.message)
        error_count += 1
    
      if error_count == 5:
        raise SystemExit("Tansformation failed for all the table for site Venetia source system WENCO ")
    
    elif(input_config.system == SORD):
      try:
        data_dict[STG_VN_DELAY_REASON] = None
        tbl_Delay_Reasons_df = vn_transform_tbl_delay_reasons(base_input_path, site_executor, input_config)
        data_dict[STG_VN_DELAY_REASON] = tbl_Delay_Reasons_df
      except DataTransformationException as e:
        logger.error(TAG_TV,"TransformationException","Failed to transform the data for table: {}".format(STG_VN_DELAY_REASON))
        error_count += 1
      
      if error_count == 1:
        raise SystemExit("Tansformation failed for all the table for site Venetia source system WENCO ")

      
    
    return data_dict

# COMMAND ----------

