# Databricks notebook source
# DBTITLE 1,Shovels
#source
MR_STD_SHIFT_AUX = "fms.StdShiftaux.parquet"
MR_STD_SHIFT_EQMT = "fms.StdShifteqmt.parquet"
MR_STD_SHIFT_STATE = "fms.StdShiftstate.parquet"
EXT_REASONS = "fms.ExtReasons.parquet"
CUST_SHIFT_LOAD_BKT_DETAIL_PIVOT = "fms.CustShiftloadbktdetailpivot.parquet"
MR_ENUM = "fms.Enum.parquet"
MR_ENUM_MATERIAL = "fms.EnumMaterial.parquet"
MR_SHIFT_INFO = "fms.ShiftInfo.parquet"
MR_PERSON = "fms.StdShiftoper.parquet"
MR_ENUM_CREW = "fms.EnumCREW.parquet"

EQUIPMENT_CATALOGUE = "equipment_catalogue.parquet"



#sink
STG_MR_EQUIPEMENT = "stg_mr_Equipment"
STG_MR_EQUIPEMENT_STATUS = "stg_mr_Equipment_Status"
STG_MR_OPERATING_ENTITY = "stg_mr_Operating_Entity"
STG_MR_DELAY_REASON = "stg_mr_Delay_Reason"
STG_MR_SHOVEL_BUCKETS = "stg_mr_Shovel_Buckets"
STG_MR_MATERIAL = "stg_mr_Material"
STG_MR_ENUM = "stg_mr_Enum"
STG_MR_SHIFT_INFO = "stg_mr_Shift_Info"
STG_MR_PERSON = "stg_mr_person"
STG_MR_ENUM_CREW = "stg_mr_enum_crew"

# COMMAND ----------

# DBTITLE 1,Trucks
#source
MR_STD_SHIFT_DUMPS = "fms.StdShiftDumps.parquet"
MR_STD_SHIFT_INFO = "fms.ShiftInfo.parquet"
MR_STD_SHIFT_LOADS = "fms.StdShiftLoads.parquet"
MR_STD_SHIFT_TOPER = "fms.StdShiftoper.parquet"
MR_STD_SHIFT_LOCATIONS = "fms.StdShiftLocations.parquet"

#sink
STG_MR_TRUCK_LOADS = "stg_mr_Truck_Loads"
STG_MR_TRUCK_DUMPS = "stg_mr_Truck_Dumps"