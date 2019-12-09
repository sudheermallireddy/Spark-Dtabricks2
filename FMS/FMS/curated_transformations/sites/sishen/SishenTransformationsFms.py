# Databricks notebook source
# MAGIC %run ./Constant

# COMMAND ----------

TAG_SH = "SishenTransformationsFms"

# COMMAND ----------

# DBTITLE 1,Equipment
from pyspark.sql.functions import lit, trim

def sh_transform_tbl_Equipment(base_input_path, site_executor, input_config):
  logger.info(TAG_SH,"Performaing Transformation for Equipment")
  
  site_name=input_config.site
  #fetching the reference input path
  reference_input_path = site_executor.get_adls2_path(REFERENCE, None)
  logger.info(TAG_SH,"reference_input_path : "+reference_input_path)
  
  try:
    aux_path = get_final_input_path(base_input_path, SH_STD_SHIFT_AUX)
    std_shift_aux_raw_df = site_executor.read_data(CURATED, ADLS2, PARQUET, aux_path)
    std_shift_aux_df = std_shift_aux_raw_df.withColumn("UnitType", std_shift_aux_raw_df.Unit) \
                                           .select("FieldId", "UnitId", "Unit", "UnitType", "EqmttypeId", "Eqmttype")
  except Exception as e:
    logger.error(TAG_SH,"ReadException","Failed to read the data from path {} : {}".format(aux_path, str(e)))
    raise DataTransformationException(TAG_SH,"ReadException","Failed to read the data from path {}".format(aux_path))

  try:
    eqmt_path = get_final_input_path(base_input_path, SH_STD_SHIFT_EQMT)
    std_shift_eqmt_df = site_executor.read_data(CURATED, ADLS2, PARQUET, eqmt_path)\
                                     .select("FieldId", "UnitId", "Unit", "UnitType", "EqmttypeId", "Eqmttype")
  except Exception as e:
    logger.error(TAG_SH,"ReadException","Failed to read the data from path {} : {}".format(eqmt_path, str(e)))
    raise DataTransformationException(TAG_SH,"ReadException","Failed to read the data from path {}".format(eqmt_path))

  #Unifying into single Dataframe
  try:
    std_all_eqmt_df = std_shift_aux_df.union(std_shift_eqmt_df) \
                                      .distinct()
  except Exception as e:
    logger.error(TAG_SH,"TransformationException","Failed to perform union operation for table Equipment : {}".format(str(e)))
    raise DataTransformationException(TAG_SH,"TransformationException",\
                                      "Failed to perform union operation for table Equipment")
    
  # Selecting only Relevant columns to remove the irrelevant data.
  try:
    catalog_path = get_final_input_path(reference_input_path, EQUIPMENT_CATALOGUE)
    std_eqmt_catalog_df = site_executor.read_data(REFERENCE, ADLS2,PARQUET,catalog_path).select("Equipment_ID","MDM_Global_Site_Code","SRC_System","IsReportable","Plant_No","MDM_Global_Plant_No","Make_Code","Make_Name","Category_Code","Category_Name","Type_Code","Type_Name","Act_Bucket_Bowl_Size_m3","Act_Rated_Bucket_Bowl_Capacity_t","Act_Rated_Suspended_Load_t","Equipment_Model_Code","Equipment_Model_Name","Equipment_Model_ID","Equipment_Variant_Code","Equipment_Variant_Name","Equipment_Variant_ID","Mining_Method","Site_Fleet_Code","Site_Fleet_Description","Site_Equipment_Description","Site_Production_Type_Code","Site_Production_Type_Description","Equipment_Is_Active","Group_Performance_Report","Equipment_Is_Hired","Global_Benchmark_Report","Benchmark_Report_Category","Equipment_Model_Type","Equipment_Fleet_Code","BM_Report_Label")
  except Exception as e:
    logger.error(TAG_SH,"ReadException","Failed to read the data from path {} : {}".format(catalog_path, str(e)))
    raise DataTransformationException(TAG_SH,"ReadException","Failed to read the data from path {}".format(catalog_path))
    

  #performing join based on logical model
  try:
    site_equipment_df = std_eqmt_catalog_df.filter((upper(std_eqmt_catalog_df.SRC_System) == lit(site_name.upper())) & \
                                                   (std_eqmt_catalog_df.Plant_No.isNotNull())) \
                                           .withColumn("MDM_Global_Plant_No",trim(std_eqmt_catalog_df.MDM_Global_Plant_No))
    
    #Populating null values for the mentioned columns as the values are being mannully updated from Fms source
    from pyspark.sql.types import StringType
    tbl_Equipment_df =  site_equipment_df.join(std_all_eqmt_df, (std_all_eqmt_df.FieldId == trim(site_equipment_df.Plant_No)), "fullouter") \
                    .withColumnRenamed("FieldId", "Field_Id") \
                    .withColumn("Src_Model_Type_Id",lit(None).cast(StringType())) \
                    .withColumn("Src_Model_Type_Code",lit(None).cast(StringType())) \
                    .withColumn("Src_Model_Type_Description",lit(None).cast(StringType())) \
                    .withColumn( "Src_Fleet_ID",lit(None).cast(StringType())) \
                    .withColumn("Src_Fleet_Description",lit(None).cast(StringType())) \
                    .withColumnRenamed("IsReportable","Is_Reportable") \
                    .withColumn("Src_Application",lit(input_config.src_application.upper())) \
                    .withColumn("Site_Name",lit(site_name.upper())) \
                    .select("Equipment_ID","Field_Id", "Src_Model_Type_Id", "Src_Model_Type_Code","Src_Model_Type_Description", \
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
                           "BM_Report_Label","Site_Name","Src_Application")
                   
  except Exception as e:
    logger.error(TAG_SH,"TransformationException","Failed to perform join operation for table Equipment : {}".format(str(e)))
    raise DataTransformationException(TAG_SH,"TransformationException",\
                                      "Failed to perform join operation for table Equipment")
  return tbl_Equipment_df.distinct()

# COMMAND ----------

# DBTITLE 1,Equipment Status 
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import col, when, lit

def sh_transform_tbl_equipment_status(base_input_path, site_executor, input_config):
  logger.info(TAG_SH,"Performaing Transformation for Equipment Status")
  
  site_name=input_config.site
  # Getting Equipment Status tables
  eqmt_status_path = get_final_input_path(base_input_path, SH_STD_SHIFT_STATE)
  eqmt_status_df = site_executor.read_data(CURATED, ADLS2, PARQUET, eqmt_status_path)
  display(eqmt_status_df)
  shift_info_df = sh_transform_tbl_shift_info(base_input_path, site_executor, input_config).where(col("ShiftId").isNotNull()) .select('ShiftId','Crew').withColumnRenamed('ShiftId', 'ShiftId_df')

  try:
    joined_equip_status_df = eqmt_status_df.join(shift_info_df, eqmt_status_df.ShiftId == shift_info_df.ShiftId_df ,'left') \
                                           .withColumn('Status_Start_Time',from_unixtime(unix_timestamp('FieldId','HH:mm:ss'),'HH:mm:ss')) \
                                           .withColumnRenamed('ShiftId', 'Shift_Id') \
                                           .withColumn('Status_Start_Timestamp', from_unixtime(eqmt_status_df['Timestamp'], 'yyyy-MM-dd HH:mm:ss')) \
                                           .withColumnRenamed('Duration', 'Duration_Seconds') \
                                           .withColumn('Equipment', when((col("Eqmt") != 'null'), col('Eqmt')).otherwise(col('AuxEqmt'))) \
                                           .withColumnRenamed('FieldTime', 'Status_Start_Time_Seconds') \
                                           .withColumnRenamed('FieldReason', 'Delay_Reason') \
                                           .withColumnRenamed('Crew','Crew_Name') \
                                           .withColumnRenamed('TimecatId', 'Delay_Category_ID') \
                                           .withColumnRenamed('FlagCodes', 'Flag_Codes') \
                                           .withColumnRenamed('Status', 'Delay_Type') \
                                           .withColumnRenamed('StatusIdx', 'Delay_Type_Code') \
                                           .withColumnRenamed('FieldComment', 'Comment') \
                                           .withColumnRenamed('OperId', 'Operator_Id') \
                                           .withColumnRenamed('IsAuxil', 'Is_Aux') \
                                           .withColumn("Status_End_Timestamp", lit(None).cast(StringType())) \
                                           .withColumn("Shift_Code", lit(None).cast(StringType())) \
                                           .withColumn("Shift_Date", lit(None).cast(StringType())) \
                                           .withColumn("Source_Id", lit(None).cast(LongType()))
  except Exception as e:
    logger.error(TAG_SH,"TransformationException","Failed to perform join operation for table Equipment status : {}".format(str(e)))
    raise DataTransformationException(TAG_SH,"TransformationException",\
                                      "Failed to perform join operation for table Equipment status")

  tbl_Equipment_Status_df = joined_equip_status_df.select("Status_Start_Time","Shift_Id","Status_Start_Timestamp", \
                                                                   "Duration_Seconds","Equipment","Status_Start_Time_Seconds","Delay_Reason", \
                                                                   "Delay_Category_ID","Flag_Codes","Delay_Type_Code","Crew_Name", \
                                                                   "Delay_Type","Comment","Operator_Id","Is_Aux","Site_Name", "Src_Application","Status_End_Timestamp","Shift_Code","Shift_Date","Source_Id")
  
  return tbl_Equipment_Status_df.distinct()

# COMMAND ----------

# DBTITLE 1,Operating Entity (Site) - (Manually - Single Record Sishen)
# This one is not needed as it has been generated by me itself.
# logger.info("Operating Entity - Already generated manually not needed as of now.")

# INSERT INTO [dbo].[tbl_Operating_Entity]
#           ([MDM_Global_Site_Code]
#           ,[Operating_Entity_Name]
#           ,[Operating_Type]
#           ,[Site]
#           ,[Business_Unit_Name]
#           ,[Business_Unit_Abbreviation]
#           ,[Anglo_Region]
#           ,[Country])
#     VALUES
#           ('AA-ZA-0058','Sishen','Open Cut'
#           ,'Sishen'
#           ,'Platinum'
#           ,'PLAT'
#           ,'EMEA'
#           ,'South Africa');

# COMMAND ----------

# DBTITLE 1,Delay Reasons
# Getting Delay Reasons Dependent Entity Tables
from pyspark.sql.functions import lit

def sh_transform_tbl_delay_reasons(base_input_path, site_executor, input_config):
  logger.info(TAG_SH,"Performaing Transformation for Delay Reasons")
  
  site_name=input_config.site
  try:
    enum_path = get_final_input_path(base_input_path, ENUM)
    enum_df = site_executor.read_data(CURATED, ADLS2, PARQUET, enum_path).filter("EnumTypeId = 19").select("Id", "Description")
    
  except Exception as e:
    logger.error(TAG_SH,"ReadException","Failed to read the data from path {} : {}".format(enum_path, str(e)))
    raise DataTransformationException(TAG_SH,"ReadException","Failed to read the data from path {}".format(enum_path))

  try:
    ext_reasons_path = get_final_input_path(base_input_path, EXT_REASONS)
    ext_reasons_df = site_executor.read_data(CURATED, ADLS2, PARQUET, ext_reasons_path).select("FieldId", "FieldName", "Type", "Category", "CategoryId", "Site_Name","Src_Application")
  except Exception as e:
    logger.error(TAG_SH,"ReadException","Failed to read the data from path {} : {}".format(ext_reasons_path, str(e)))
    raise DataTransformationException(TAG_SH,"ReadException","Failed to read the data from path {}".format(ext_reasons_path))

  try:
    tbl_Delay_Reasons_df = ext_reasons_df.join(enum_df, ext_reasons_df.Type == enum_df.Description, 'left') \
                   .withColumnRenamed("FieldId", "Delay_Reason_Id") \
                   .withColumn("Delay_Type_Code",col("Id").cast(StringType())) \
                   .withColumnRenamed("Type", "Delay_Type") \
                   .withColumnRenamed("Category", "Delay_Category") \
                   .withColumn("Delay_Category_ID",col("CategoryId").cast(StringType())) \
                   .withColumnRenamed("FieldName", "Delay_Reason_Description") \
                   .withColumn("Time_Model_Code", lit(None).cast(StringType())) \
                   .withColumn("Is_Active", lit(None).cast(BooleanType())) \
                   .withColumn("Is_Deleted", lit(None).cast(BooleanType())) \
                   .drop("Description")
  except Exception as e:
    logger.error(TAG_SH,"TransformationException","Failed to perform join operation for table Delay Reasons: {}".format(str(e)))
    raise DataTransformationException(TAG_SH,"TransformationException",\
                                      "Failed to perform join operation for table Delay Reasons")
    
  return tbl_Delay_Reasons_df.distinct()

# COMMAND ----------

# DBTITLE 1,Shovel Buckets
from pyspark.sql.functions import unix_timestamp, lit

# Getting Delay Reasons Dependent Entity Tables
def sh_transform_tbl_shovel_buckets(base_input_path, site_executor, input_config):
  logger.info(TAG_SH,"Performaing Transformation for Shovel Buckets")
  
  site_name=input_config.site
  try:
    shift_load_bucket_pivot_path = get_final_input_path(base_input_path, CUST_SHIFT_LOAD_BKT_DETAIL_PIVOT)
    shift_load_bucket_pivot_df = site_executor.read_data(CURATED, ADLS2, PARQUET, shift_load_bucket_pivot_path)
  except Exception as e:
    logger.error(TAG_SH,"ReadException","Failed to read the data from path {} :{}".format(shift_load_bucket_pivot_path, str(e)))
    raise DataTransformationException(TAG_SH,"ReadException","Failed to read the data from path {}".format(shift_load_bucket_pivot_path))

    
# Transforming the necessay columns and renaming the others as per the EDM. TO ASK Hesten for ShiftStartTimestamp
  try:
    tbl_Shovel_Buckets_df = shift_load_bucket_pivot_df.withColumnRenamed('Id','Load_Id') \
      .withColumnRenamed('Crew','Crew_Name') \
      .withColumnRenamed('ShiftId','Shift_Id') \
      .withColumn('Transaction_Date', shift_load_bucket_pivot_df.ShiftStartTimestamp.cast('date')) \
      .withColumnRenamed('FullShiftName','Full_Shift_Name') \
      .withColumnRenamed('ShiftName','Shift_Name') \
      .withColumnRenamed('Truck','Truck') \
      .withColumnRenamed('Excav','Excavator') \
      .withColumnRenamed('Grade','Grade') \
      .withColumnRenamed('LoadLocation','Load_Location') \
      .withColumnRenamed('Tons','Total_Load_Tons') \
      .withColumnRenamed('TruckOperId','Truck_Operator_Id') \
      .withColumnRenamed('ExcavOperId','Excavator_Operator_Id') \
      .withColumnRenamed('LoadfactorValue','Load_Factor_Value') \
      .withColumnRenamed('Tonnage','Tonnage') \
      .withColumnRenamed('AssignTimestamp','Assign_Timestamp') \
      .withColumnRenamed('ArriveTimestamp','Arrive_Timestamp') \
      .withColumnRenamed('QueueTime','Queue_Time') \
      .withColumnRenamed('SpottingTimestamp','Spotting_Timestamp') \
      .withColumnRenamed('SpotTime','Spot_Time') \
      .withColumnRenamed('LoadingTimestamp','Loading_Timestamp') \
      .withColumnRenamed('LoadingTime','Loading_Time') \
      .withColumnRenamed('FullTimestamp','Full_Timestamp') \
      .withColumnRenamed('MaterialTypeID','Material_Type_ID') \
      .withColumnRenamed('MaterialGroupId','Material_Group_Id') \
      .withColumnRenamed('IsOre','Is_Ore') \
      .withColumnRenamed('IsWaste','Is_Waste') \
      .withColumnRenamed('IsTrammed','Is_Trammed') \
      .withColumnRenamed('IsExtraload','Is_Extraload') \
      .withColumnRenamed('ShovelIdleTime','Shovel_Idle_Time') \
      .withColumnRenamed('TruckIdleTime','Truck_Idle_Time') \
      .withColumnRenamed('Buckettons1','Bucket_Tons_1') \
      .withColumnRenamed('Buckettons2','Bucket_Tons_2') \
      .withColumnRenamed('Buckettons3','Bucket_Tons_3') \
      .withColumnRenamed('Buckettons4','Bucket_Tons_4') \
      .withColumnRenamed('Buckettons5','Bucket_Tons_5') \
      .withColumnRenamed('Buckettons6','Bucket_Tons_6') \
      .withColumnRenamed('Buckettons7','Bucket_Tons_7') \
      .withColumnRenamed('Buckettons8','Bucket_Tons_8') \
      .withColumnRenamed('Buckettons9','Bucket_Tons_9') \
      .withColumnRenamed('Buckettons10','Bucket_Tons_10') \
      .withColumnRenamed('Buckettons11','Bucket_Tons_11') \
      .withColumnRenamed('Buckettons12','Bucket_Tons_12') \
      .withColumnRenamed('Buckettime1','Bucket_Timestamp_1') \
      .withColumnRenamed('Buckettime2','Bucket_Timestamp_2') \
      .withColumnRenamed('Buckettime3','Bucket_Timestamp_3') \
      .withColumnRenamed('Buckettime4','Bucket_Timestamp_4') \
      .withColumnRenamed('Buckettime5','Bucket_Timestamp_5') \
      .withColumnRenamed('Buckettime6','Bucket_Timestamp_6') \
      .withColumnRenamed('Buckettime7','Bucket_Timestamp_7') \
      .withColumnRenamed('Buckettime8','Bucket_Timestamp_8') \
      .withColumnRenamed('Buckettime9','Bucket_Timestamp_9') \
      .withColumnRenamed('Buckettime10','Bucket_Timestamp_10') \
      .withColumnRenamed('Buckettime11','Bucket_Timestamp_11') \
      .withColumnRenamed('Buckettime12','Bucket_Timestamp_12') \
      .select('Load_Id','Crew_Name','Shift_Id','Transaction_Date','Full_Shift_Name', \
              'Shift_Name','Truck','Excavator','Grade','Load_Location', \
              'Total_Load_Tons','Truck_Operator_Id','Excavator_Operator_Id','Load_Factor_Value', \
              'Tonnage','Assign_Timestamp','Arrive_Timestamp','Queue_Time', \
              'Spotting_Timestamp','Spot_Time','Loading_Timestamp','Loading_Time', \
              'Full_Timestamp','Material_Type_ID','Material_Group_Id','Is_Ore', \
              'Is_Waste','Is_Trammed','Is_Extraload','Shovel_Idle_Time', \
              'Truck_Idle_Time','Bucket_Tons_1','Bucket_Tons_2','Bucket_Tons_3', \
              'Bucket_Tons_4','Bucket_Tons_5','Bucket_Tons_6','Bucket_Tons_7', \
              'Bucket_Tons_8','Bucket_Tons_9','Bucket_Tons_10','Bucket_Tons_11', \
              'Bucket_Tons_12','Bucket_Timestamp_1','Bucket_Timestamp_2','Bucket_Timestamp_3', \
              'Bucket_Timestamp_4','Bucket_Timestamp_5','Bucket_Timestamp_6','Bucket_Timestamp_7', \
              'Bucket_Timestamp_8','Bucket_Timestamp_9','Bucket_Timestamp_10','Bucket_Timestamp_11', \
              'Bucket_Timestamp_12',"Site_Name","Src_Application")
  except Exception as e:
    logger.error(TAG_SH,"TransformationException","Failed to perform column rename operation for table Shovel Buckets: {}".format(str(e)))
    raise DataTransformationException(TAG_SH,"TransformationException",\
                                      "Failed to perform column rename operation for table Shovel Buckets")
    

  return tbl_Shovel_Buckets_df.distinct()

# COMMAND ----------

# DBTITLE 1,Material
# Getting Material Entity Table
from pyspark.sql.functions import lit

def sh_transform_tbl_material(base_input_path, site_executor, input_config):
  logger.info(TAG_SH,"Performaing Transformation for Material")
  
  site_name = input_config.site
  try:
    enum_material_path = get_final_input_path(base_input_path, ENUM_MATERIAL)
    enum_material_df = site_executor.read_data(CURATED, ADLS2, PARQUET, enum_material_path)
  except Exception as e:
    logger.error(TAG_SH,"ReadException","Failed to read the data from path {} : {}".format(enum_material_path, str(e)))
    raise DataTransformationException(TAG_SH,"ReadException","Failed to read the data from path {}".format(enum_material_path))

# Transforming the Material entity, no such transformations just need to rename and copy as-is
  
  try:
    tbl_Material_df = enum_material_df.withColumnRenamed("Id", "Material_Id") \
                .withColumnRenamed("EnumTypeId", "Enum_Type_Id") \
                .withColumnRenamed("Description", "Material_Description") \
                .withColumnRenamed("Abbreviation", "Material_Abbreviation") \
                .withColumnRenamed("LogicalOrder", "Logical_Order") \
                .withColumnRenamed("IsOre", "Is_Ore") \
                .withColumnRenamed("IsWaste", "Is_Waste") \
                .withColumn("Load_Group",col("LoadGroup").cast(StringType())) \
                .withColumnRenamed("LoadGroupName", "Load_Group_Name") \
                .withColumnRenamed("Units", "Units") \
                .withColumnRenamed("UnitsName", "Units_Name") \
                .withColumn("is_active", lit(None).cast(StringType())) \
                .select("Material_Id","Enum_Type_Id","Idx", "Material_Description", \
                        "Material_Abbreviation","Logical_Order","Is_Ore", \
                        "Is_Waste","Load_Group","Load_Group_Name","Units", \
                        "Units_Name", "Site_Name","Src_Application","is_active")
  except Exception as e:
    logger.error(TAG_SH,"TransformationException","Failed to perform column rename operation for table Material: {}".format(str(e)))
    raise DataTransformationException(TAG_SH,"TransformationException",\
                                      "Failed to perform column rename operation for table Material")
  return tbl_Material_df.distinct()

# COMMAND ----------

# DBTITLE 1,Truck Loads
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import lit

# Getting Material Entity Table
def sh_transform_tbl_truck_loads(base_input_path, site_executor, input_config):
  logger.info(TAG_SH,"Performaing Transformation for Truck Loads")
  
  site_name = input_config.site
  try:
    shift_info_path = get_final_input_path(base_input_path, STD_SHIFT_INFO)
    shift_info_df = site_executor.read_data(CURATED, ADLS2, PARQUET, shift_info_path) \
                                   .select("ShiftId", "FullShiftSuffix")
  except Exception as e:
    logger.error(TAG_SH,"ReadException","Failed to read the data from path {}: {}".format(shift_info_path, str(e)))
    raise DataTransformationException(TAG_SH,"ReadException","Failed to read the data from path {}".format(shift_info_path))
    
  try:
    shift_loads_path = get_final_input_path(base_input_path, STD_SHIFT_LOADS)
    shift_loads_df = site_executor.read_data(CURATED, ADLS2, PARQUET, shift_loads_path)
  except Exception as e:
    logger.error(TAG_SH,"ReadException","Failed to read the data from path {} : {}".format(shift_loads_path, str(e)))
    raise DataTransformationException(TAG_SH,"ReadException","Failed to read the data from path {}".format(shift_loads_path))

  try:
    joined_dfs = shift_loads_df.join(shift_info_df, shift_loads_df.ShiftId == shift_info_df.ShiftId, 'left')
  except Exception as e:
    message = "Failed to perform join between the source tables : {}".format(str(e))
    logger.error(TAG_SH,"ReadException",message)
    raise DataTransformationException(TAG_SH,"ReadException",message)
  
  try:
    renamed_df = joined_dfs.withColumn("Shift_Id",shift_loads_df.ShiftId) \
                           .withColumn("Load_Id",shift_loads_df.Id) \
                           .withColumn("Distance_Hauled",shift_loads_df.TotalDistance) \
                           .withColumn("Queue_Time",shift_loads_df.QueueTime) \
                           .withColumn("Travel_Distance",shift_loads_df.TotalDistance) \
                           .withColumn("Loading_Queue_Time",shift_loads_df.QueueTime) \
                           .withColumn("Shift_Name",shift_info_df.FullShiftSuffix) \
                           .withColumnRenamed("Crew","Crew_Name") \
                           .withColumnRenamed("PrevDumpId","Previous_Dump_Id") \
                           .withColumnRenamed("NextShiftDumpId","Next_Shift_Dump_Id") \
                           .withColumnRenamed("TruckOperatorName","Truck_Operator_Name") \
                           .withColumnRenamed("TruckOperId","Truck_Operator_Employee_Id") \
                           .withColumnRenamed("Excav","Equipment") \
                           .withColumnRenamed("ExcavOperatorName","Equipment_Operator_Name") \
                           .withColumnRenamed("ExcavOperId","Equipment_Operator_Employee_Id") \
                           .withColumnRenamed("MaterialTypeID","Material_Id") \
                           .withColumnRenamed("MaterialType","Material_Description") \
                           .withColumnRenamed("LoadLocationId","Load_Location_Id") \
                           .withColumnRenamed("LoadLocation","Load_Location") \
                           .withColumnRenamed("LoadLocationUnit","Load_Location_Unit") \
                           .withColumnRenamed("Region","Load_Region") \
                           .withColumnRenamed("Tons","Tons") \
                           .withColumnRenamed("Tonnage","Tonnage") \
                           .withColumnRenamed("TruckSize","Truck_Size") \
                           .withColumnRenamed("FullTimestamp","Full_Timestamp") \
                           .withColumnRenamed("AssignTimestamp","Assign_Timestamp") \
                           .withColumnRenamed("ArriveTimestamp","Arrive_Timestamp") \
                           .withColumnRenamed("SpottingTimestamp","Spotting_Timestamp") \
                           .withColumnRenamed("LoadingTimestamp","Loading_Timestamp") \
                           .withColumnRenamed("EmptyTravelDuration","Empty_Travel_Duration") \
                           .withColumnRenamed("SpotTime","Loading_Spot_Time") \
                           .withColumnRenamed("LoadingTime","Loading_Time") \
                           .withColumnRenamed("ExpectedEmptyTravelDuration","Expected_Empty_Travel_Duration") \
                           .withColumnRenamed("EquivalentFlatHaulDistance","Equivalent_Flat_Haul_Distance") \
                           .withColumnRenamed("TruckIdleTime","Truck_Idle_Time") \
                           .withColumnRenamed("ShovelIdleTime","Shovel_Idle_Time") \
                           .withColumnRenamed("IsOre","Is_Ore") \
                           .withColumnRenamed("IsWaste","Is_Waste") \
                           .withColumnRenamed("IsTrammed","Is_Trammed") \
                           .withColumnRenamed("IsExtraLoad","Is_Extra_Load")
               
  except Exception as e:
    logger.error(TAG_SH,"ReadException","Failed to renamed the column : {}".format(str(e)))
    raise DataTransformationException(TAG_SH,"ReadException","Failed to renamed the column: renamed_df")
  
  try:
    tbl_truck_loads_df = renamed_df.select("Site_Name","Src_Application","Shift_Id","Shift_Name","Load_Id","Previous_Dump_Id", \
                                           "Next_Shift_Dump_Id","Truck","Truck_Operator_Name", \
                                           "Truck_Operator_Employee_Id","Crew_Name", \
                                           "Equipment","Equipment_Operator_Name","Equipment_Operator_Employee_Id","Material_Id", \
                                           "Material_Description","Load_Location_Id","Load_Location","Load_Location_Unit", \
                                           "Distance_Hauled","Tons","Tonnage","Truck_Size","Full_Timestamp", \
                                           "Queue_Time","Assign_Timestamp","Arrive_Timestamp","Spotting_Timestamp","Loading_Timestamp", \
                                           "Travel_Distance","Empty_Travel_Duration","Loading_Queue_Time","Loading_Spot_Time", \
                                           "Loading_Time","Expected_Empty_Travel_Duration","Equivalent_Flat_Haul_Distance","Truck_Idle_Time", \
                                           "Shovel_Idle_Time","Is_Ore","Is_Waste","Is_Trammed","Is_Extra_Load")
  except Exception as e:
    logger.error(TAG_SH,"ReadException","Failed to select the column from {} : {}".format("tbl_truck_loads_df", str(e)))
    raise DataTransformationException(TAG_SH,"ReadException","Failed to select the column from {}".format("tbl_truck_loads_df"))

  return tbl_truck_loads_df.distinct()

# COMMAND ----------

# DBTITLE 1,Truck Dumps
from pyspark.sql.functions import lit

def sh_transform_tbl_truck_dumps(base_input_path, site_executor, input_config):
  logger.info(TAG_SH,"Performaing Transformation for Truck Dumps")
  
  site_name = input_config.site
  try:
    shift_equipment_df = site_executor.read_data(None, SQL, None, STG_SH_EQUIPEMENT) \
                                      .select("Plant_No", "MDM_Global_Plant_No")
  except Exception as e:
    logger.error(TAG_LBT,"ReadException","Failed to read the data from path {}: {}".format(STG_SH_EQUIPEMENT, str(e)))
    raise DataTransformationException(TAG_LBT,"ReadException","Failed to read the data from table {}".format(STG_SH_EQUIPEMENT))
  
  try:
    enum_path = get_final_input_path(base_input_path, "fms.Enum.parquet")
    enum_df = site_executor.read_data(CURATED, ADLS2, PARQUET, enum_path).select("Id", "EnumTypeId","Description","Abbreviation").distinct()
                                      
  except Exception as e:
    logger.error(TAG_SH,"ReadException","Failed to read the data from path {}: {}".format(enum_path, str(e)))
    raise DataTransformationException(TAG_SH,"ReadException","Failed to read the data from table {}".format(enum_path))
    
  try:
    shift_dumps_path = get_final_input_path(base_input_path, SH_STD_SHIFT_DUMPS)
    shift_dumps_df = site_executor.read_data(CURATED, ADLS2, PARQUET, shift_dumps_path)
  except Exception as e:
    logger.error(TAG_SH,"ReadException","Failed to read the data from path {}: {}".format(shift_dumps_path, str(e)))
    raise DataTransformationException(TAG_SH,"ReadException","Failed to read the data from path {}".format(shift_dumps_path))                              
  try:
    shift_info_path = get_final_input_path(base_input_path, SH_STD_SHIFT_INFO)
    shift_info_df = site_executor.read_data(CURATED, ADLS2, PARQUET, shift_info_path) \
                                     .select("ShiftId", "FullShiftSuffix")
  except Exception as e:
    logger.error(TAG_SH,"ReadException","Failed to read the data from path {}: {}".format(shift_info_path, str(e)))
    raise DataTransformationException(TAG_SH,"ReadException","Failed to read the data from path {}".format(shift_info_path))

  try:
    shift_toper_path = get_final_input_path(base_input_path, SH_STD_SHIFT_TOPER)
    shift_toper_df = site_executor.read_data(CURATED, ADLS2, PARQUET, shift_toper_path) \
                                      .select("FieldId", "Id")
  except Exception as e:
    logger.error(TAG_SH,"ReadException","Failed to read the data from path {}: {}".format(shift_toper_path, str(e)))
    raise DataTransformationException(TAG_SH,"ReadException","Failed to read the data from path {}".format(shift_toper_path))

  try:
    shift_locations_path = get_final_input_path(base_input_path, SH_STD_SHIFT_LOCATIONS)
    shift_locations_df = site_executor.read_data(CURATED, ADLS2, PARQUET, shift_locations_path) \
                                          .select("Id", "Region")
  except Exception as e:
    logger.error(TAG_SH,"ReadException","Failed to read the data from path {}: {}".format(shift_locations_path, str(e)))
    raise DataTransformationException(TAG_SH,"ReadException","Failed to read the data from path {}".format(shift_locations_path))

  try:
    joined_dfs = shift_dumps_df.join(shift_info_df, shift_dumps_df.ShiftId == shift_info_df.ShiftId, 'left') \
                               .join(shift_locations_df, ((shift_dumps_df.DumpLocationId == shift_locations_df.Id)), 'left') \
                               .join(shift_toper_df, ((shift_dumps_df.TruckOperatorId == shift_toper_df.Id)), 'left') \
                               .join(shift_equipment_df, ((shift_equipment_df.Plant_No == shift_dumps_df.Truck)), 'left') \
                               .join(enum_df,(shift_dumps_df.LoadtypeId == enum_df.Id), 'inner')
  except Exception as e:
    message = "Failed to perform join between the source tables : {}".format(str(e))
    logger.error(TAG_SH,"ReadException",message)
    raise DataTransformationException(TAG_SH,"ReadException",message)

  try:
    renamed_df = joined_dfs.withColumn("Shift_Id",shift_dumps_df.ShiftId) \
                           .withColumn("Shift_Name",shift_info_df.FullShiftSuffix) \
                           .withColumn("Dump_Id", shift_dumps_df.Id) \
                           .withColumnRenamed("PrevLoadId","Previous_Load_Id") \
                           .withColumnRenamed("NextShiftLoadId","Next_Shift_Load_Id") \
                           .withColumn("Truck_Operator_Name",shift_dumps_df.TruckOperatorName) \
                           .withColumn("Truck_Operator_Id",shift_toper_df.FieldId) \
                           .withColumnRenamed("TruckOperId","Truck_Operator_Employee_Id") \
                           .withColumn("Excavator_Equipment_Id", shift_equipment_df.MDM_Global_Plant_No) \
                           .withColumnRenamed("Excav","Equipment") \
                           .withColumnRenamed("LoadId","Material_Id") \
                           .withColumnRenamed("MaterialType","Material_Description") \
                           .withColumnRenamed("Crew","Crew_Name") \
                           .withColumnRenamed("BlastLocation","Blast_Location") \
                           .withColumnRenamed("BlastLocationId", "Blast_Location_Id") \
                           .withColumnRenamed("DumpLocationId", "Dump_Location_Id") \
                           .withColumnRenamed("DumpLocation","Dump_Location") \
                           .withColumnRenamed("Region","Dump_Region") \
                           .withColumnRenamed("DumpLocationUnit","Dump_Location_Unit") \
                           .withColumnRenamed("DumpLocationUnitType","Dump_Location_Unit_Type") \
                           .withColumn("Distance_Hauled",shift_dumps_df.TotalDistance) \
                           .withColumnRenamed("TruckSize","Truck_Size") \
                           .withColumnRenamed("LoadfactorValue","Load_Factor_Value") \
                           .withColumnRenamed("Tons","Tons_Raw") \
                           .withColumnRenamed("AssignTimestamp","Assign_Timestamp") \
                           .withColumnRenamed("ArriveTimestamp","Arrive_Timestamp") \
                           .withColumnRenamed("DumpingTimestamp","Dumping_Timestamp") \
                           .withColumnRenamed("EmptyTimestamp","Empty_Timestamp") \
                           .withColumn("Travel_Distance",shift_dumps_df.TotalDistance) \
                           .withColumnRenamed("QueueTime","Dumping_Queue_Time") \
                           .withColumnRenamed("SpotTime","Dumping_Spot_Time") \
                           .withColumnRenamed("FullTravelDuration","Full_Travel_Duration") \
                           .withColumnRenamed("DumpingTime","Dumping_Time") \
                           .withColumnRenamed("ExpectedFullTravelDuration","Expected_Full_Travel_Duration") \
                           .withColumnRenamed("EquivalentFlatHaulDistance","Equivalent_Flat_Haul_Distance") \
                           .withColumnRenamed("IsOre","Is_Ore") \
                           .withColumnRenamed("IsWaste","Is_Waste") \
                           .withColumnRenamed("IsTrammed","Is_Trammed") \
                           .withColumnRenamed("IsExtraLoad","Is_Extra_Load") \
                           .withColumn("Load_Type_Id",shift_dumps_df.LoadtypeId) \
                           .withColumn("Load_Type_Description",enum_df.Description)
    
    try:
      tbl_truck_dumps_df = renamed_df.select("Site_Name", "Shift_Id","Shift_Name","Dump_Id", \
                                             "Previous_Load_Id","Next_Shift_Load_Id", \
                                             "Truck","Truck_Operator_Name","Truck_Operator_Id", \
                                             "Truck_Operator_Employee_Id","Excavator_Equipment_Id","Equipment","Material_Id", \
                                             "Material_Description","Crew_Name","Blast_Location_Id", \
                                             "Blast_Location","Dump_Location_Id","Dump_Location", \
                                             "Dump_Region","Dump_Location_Unit","Dump_Location_Unit_Type", \
                                             "Distance_Hauled","Truck_Size","Load_Factor_Value", \
                                             "Tons_Raw","Tonnage","Assign_Timestamp", \
                                             "Arrive_Timestamp","Dumping_Timestamp","Empty_Timestamp", \
                                             "Travel_Distance","Dumping_Queue_Time","Dumping_Spot_Time", \
                                             "Full_Travel_Duration","Dumping_Time", \
                                             "Expected_Full_Travel_Duration","Equivalent_Flat_Haul_Distance","Is_Ore", \
                                             "Is_Waste","Is_Trammed","Is_Extra_Load","Load_Type_Id", "Load_Type_Description","Src_Application")
    except Exception as e:
      logger.error(TAG_SH,"ReadException","Failed to read the data from path {}: {}".format("tbl_truck_dump_df", str(e)))
      raise DataTransformationException(TAG_SH,"ReadException","Failed to read the data from path {}".format("tbl_truck_loads_df"))
      
  except Exception as e:
    message = "Failed to perform either column renamad or new column creation : {}".format(str(e))
    logger.error(TAG_SH,"RenamedException",message)
    raise DataTransformationException(TAG_SH,"RenamedException",message)
  
  return tbl_truck_dumps_df.distinct()

# COMMAND ----------

# DBTITLE 1,Enum
def sh_transform_tbl_enum(base_input_path, site_executor, input_config):
  logger.info(TAG_SH,"Performaing Transformation for Enum")
  
  try:
    enum_path = get_final_input_path(base_input_path, ENUM)
    enum_df = site_executor.read_data(CURATED, ADLS2, PARQUET, enum_path).select("Id", "EnumTypeId","Description","Abbreviation","Site_Name", "Src_Application").distinct()
    tbl_enum_df = enum_df.withColumnRenamed("Id","Enum_Id") \
                         .withColumnRenamed("EnumTypeId","Enum_Type_Id") \
                         .withColumnRenamed("Description","Enum_Description") \
                         .withColumnRenamed("Abbreviation","Enum_Abbreviation") \
                         .select("Enum_Id", "Enum_Type_Id", "Enum_Description", "Enum_Abbreviation","Site_Name","Src_Application")
                                      
  except Exception as e:
    logger.error(TAG_SH,"ReadException","Failed to read the data from path {}: {}".format(enum_path, str(e)))
    raise DataTransformationException(TAG_SH,"ReadException","Failed to read the data from table {}".format(enum_path))
    
  return tbl_enum_df.distinct()

# COMMAND ----------

# DBTITLE 1,Shift Info
def sh_transform_tbl_shift_info(base_input_path, site_executor, input_config):
  logger.info(TAG_SH,"Performaing Transformation for shiftinfo")
  
  try:
    shift_info_path = get_final_input_path(base_input_path, SH_SHIFT_INFO)
    shift_info_df = site_executor.read_data(CURATED, ADLS2, PARQUET, shift_info_path)
  except Exception as e:
    logger.error(TAG_SH,"ReadException","Failed to read the data from path {}: {}".format(shift_info_path, str(e)))
    raise DataTransformationException(TAG_SH,"ReadException","Failed to read the data from table {}".format(shift_info_path))
    
  return shift_info_df.distinct()

# COMMAND ----------

# DBTITLE 1,Person
def sh_transform_tbl_person(base_input_path, site_executor, input_config):
  logger.info(TAG_SH,"Performaing Transformation for person")
  
  try:
    person_path = get_final_input_path(base_input_path, SH_PERSON)
    person_df = site_executor.read_data(CURATED, ADLS2, PARQUET, person_path).select("FieldId","FieldName","Site_Name","Src_Application")
    tbl_person_df = person_df.withColumnRenamed("FieldId","Person_Id") \
                             .withColumnRenamed("FieldName","Person_full_Name")
  except Exception as e:
    logger.error(TAG_SH,"ReadException","Failed to read the data from path {}: {}".format(person_path, str(e)))
    raise DataTransformationException(TAG_SH,"ReadException","Failed to read the data from table {}".format(person_path))
    
  return tbl_person_df.distinct()

# COMMAND ----------

# DBTITLE 1,Enum CREW
def sh_transform_tbl_enum_crew(base_input_path, site_executor, input_config):
  logger.info(TAG_SH,"Performaing transformation for enum crew")
  
  try:
    enum_crew_path = get_final_input_path(base_input_path, SH_ENUM_CREW)
    enum_crew_df = site_executor.read_data(CURATED, ADLS2, PARQUET, enum_crew_path)
    tbl_enum_crew_df = enum_crew_df.withColumnRenamed("Id","id") \
                                    .withColumnRenamed("EnumTypeId","enum_type_id") \
                                    .withColumnRenamed("Idx","idx") \
                                    .withColumnRenamed("Description","description") \
                                    .withColumnRenamed("Abbreviation","abbreviation") \
                                    .withColumnRenamed("Flags","flags") \
                                    .withColumnRenamed("LogicalOrder","logical_order") \
                                    .withColumnRenamed("TxtColor","txt_color") \
                                    .withColumnRenamed("BgColor","bg_color") \
                                    .withColumnRenamed("Site_Name","site_name") \
                                    .withColumnRenamed("Src_Application","src_application") \
                                    .select("id", "enum_type_id", "idx", "description", "abbreviation", "flags", "logical_order", "txt_color", "bg_color", "site_name", "src_application")
                                      
  except Exception as e:
    logger.error(TAG_SH,"ReadException","Failed to read the data from path {}: {}".format(enum_crew_path, str(e)))
    raise DataTransformationException(TAG_SH,"ReadException","Failed to read the data from table {}".format(enum_crew_path))
    
  return tbl_enum_crew_df.distinct()

# COMMAND ----------

class SishenTransformationsFms:
  def execute(self, data_dict, site_executor, input_config):
    logger.info(TAG_SH,"Excuting for site {}".format(input_config.site))
    error_count = 0
    
    try:
      base_input_path = site_executor.get_adls2_path(CURATED, input_config)
      logger.info(TAG_SH,"Base_input_path : "+base_input_path)
    except DataTransformationException as e:
      logger.error(TAG_SH, e.exeptionType, e.message)
      raise SystemExit(e.message)
      
    try:
      data_dict[STG_SH_EQUIPEMENT] = None
      tbl_Equipment_df = sh_transform_tbl_Equipment(base_input_path, site_executor,input_config)
      data_dict[STG_SH_EQUIPEMENT] = tbl_Equipment_df
    except DataTransformationException as e:
      logger.error(TAG_SH,"TransformationException","Failed to transform the data for table: {}".format(STG_SH_EQUIPEMENT))
      error_count += 1
      
    try:
      data_dict[STG_SH_EQUIPEMENT_STATUS] = None
      tbl_Equipment_Status_df = sh_transform_tbl_equipment_status(base_input_path, site_executor, input_config)
      data_dict[STG_SH_EQUIPEMENT_STATUS] = tbl_Equipment_Status_df
    except DataTransformationException as e:
      logger.error(TAG_SH,"TransformationException","Failed to transform the data for \
      table: {}".format(STG_SH_EQUIPEMENT_STATUS))
      error_count += 1
      
    try:
      data_dict[STG_SH_DELAY_REASON] = None
      tbl_Delay_Reasons_df = sh_transform_tbl_delay_reasons(base_input_path, site_executor, input_config)
      data_dict[STG_SH_DELAY_REASON] = tbl_Delay_Reasons_df
    except DataTransformationException as e:
      logger.error(TAG_SH,"TransformationException","Failed to transform the data for table: {}".format(STG_SH_DELAY_REASON))
      error_count += 1
      
    try:
      data_dict[STG_SH_SHOVEL_BUCKETS] = None
      tbl_Shovel_Buckets_df = sh_transform_tbl_shovel_buckets(base_input_path, site_executor, input_config)
      data_dict[STG_SH_SHOVEL_BUCKETS] = tbl_Shovel_Buckets_df
    except DataTransformationException as e:
      logger.error(TAG_SH,"TransformationException","Failed to transform the data for table: {}".format(STG_SH_SHOVEL_BUCKETS))
      error_count += 1
      
    try:
      data_dict[STG_SH_MATERIAL] = None
      tbl_Material_df = sh_transform_tbl_material(base_input_path, site_executor, input_config)
      data_dict[STG_SH_MATERIAL] = tbl_Material_df
    except DataTransformationException as e:
      logger.error(TAG_SH,"TransformationException","Failed to transform the data for table: {}".format(STG_SH_MATERIAL))
      error_count += 1
      
    try:
      data_dict[STG_SH_TRUCK_LOADS] = None
      tbl_Truck_Loads_df = sh_transform_tbl_truck_loads(base_input_path, site_executor, input_config)
      data_dict[STG_SH_TRUCK_LOADS] = tbl_Truck_Loads_df      
    except DataTransformationException as e:
      logger.error(TAG_SH,"TransformationException","Failed to transform the data for table: {}".format(STG_SH_TRUCK_LOADS) + "\n" + e.message)
      error_count += 1
      
    try:
      data_dict[STG_SH_TRUCK_DUMPS] = None
      tbl_Truck_Dumps_df = sh_transform_tbl_truck_dumps(base_input_path, site_executor, input_config)
      data_dict[STG_SH_TRUCK_DUMPS] = tbl_Truck_Dumps_df      
    except DataTransformationException as e:
      logger.error(TAG_SH,"TransformationException","Failed to transform the data for table: {}".format(STG_SH_TRUCK_DUMPS) + "\n" + e.message)
      error_count += 1
      
    try:
      data_dict[STG_SH_ENUM] = None
      tbl_Enum_df = sh_transform_tbl_enum(base_input_path, site_executor, input_config)
      data_dict[STG_SH_ENUM] = tbl_Enum_df      
    except DataTransformationException as e:
      logger.error(TAG_SH,"TransformationException","Failed to transform the data for table: {}".format(STG_SH_TRUCK_DUMPS) + "\n" + e.message)
      error_count += 1
    
    try:
      data_dict[STG_SH_SHIFT_INFO] = None
      tbl_shift_info_df = sh_transform_tbl_shift_info(base_input_path, site_executor, input_config)
      data_dict[STG_SH_SHIFT_INFO] = tbl_shift_info_df      
    except DataTransformationException as e:
      logger.error(TAG_SH,"TransformationException","Failed to transform the data for table: {}".format(STG_SH_SHIFT_INFO) + "\n" + e.message)
      error_count += 1
      
    try:
      data_dict[STG_SH_PERSON] = None
      tbl_person_df = sh_transform_tbl_person(base_input_path, site_executor, input_config)
      data_dict[STG_SH_PERSON] = tbl_person_df      
    except DataTransformationException as e:
      logger.error(TAG_SH,"TransformationException","Failed to transform the data for table: {}".format(STG_SH_PERSON) + "\n" + e.message)
      error_count += 1
      
    try:
      data_dict[STG_SH_ENUM_CREW] = None
      tbl_enum_crew_df = sh_transform_tbl_enum_crew(base_input_path, site_executor, input_config)
      data_dict[STG_SH_ENUM_CREW] = tbl_enum_crew_df      
    except DataTransformationException as e:
      logger.error(TAG_SH,"TransformationException","Failed to transform the data for table: {}".format(STG_SH_ENUM_CREW) + "\n" + e.message)
      error_count += 1
    
    if error_count == 11:
      raise SystemExit("Tansformation failed for all the table for site Sishen")
    
    return data_dict

# COMMAND ----------

