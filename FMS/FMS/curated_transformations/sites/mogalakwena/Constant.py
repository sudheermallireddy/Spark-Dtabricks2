# Databricks notebook source
# DBTITLE 1,Shovels
#source
STD_SHIFT_AUX = "fms.stdShiftaux.parquet"
STD_SHIFT_EQMT = "fms.stdShifteqmt.parquet"
STD_SHIFT_STATE = "fms.stdShiftstate.parquet"
EXT_REASONS = "fms.ExtReasons.parquet"
CUST_SHIFT_LOAD_BKT_DETAIL_PIVOT = "fms.CustShiftloadbktdetailpivot.parquet"
ENUM = "fms.Enum.parquet"
ENUM_MATERIAL = "fms.EnumMaterial.parquet"
MOG_SHIFT_INFO = "fms.ShiftInfo.parquet"
MOG_PERSON = "fms.StdShiftoper.parquet"
MOG_ENUM_CREW = "fms.EnumCREW.parquet"

EQUIPMENT_CATALOGUE = "equipment_catalogue.parquet"

#sink
STG_MOG_EQUIPEMENT = "stg_mog_Equipment"
STG_MOG_EQUIPEMENT_STATUS = "stg_mog_Equipment_Status"
STG_MOG_OPERATING_ENTITY = "stg_mog_Operating_Entity"
STG_MOG_DELAY_REASON = "stg_mog_Delay_Reason"
STG_MOG_SHOVEL_BUCKETS = "stg_mog_Shovel_Buckets"
STG_MOG_MATERIAL = "stg_mog_Material"
STG_MOG_ENUM = "stg_mog_Enum"
STG_MOG_SHIFT_INFO = "stg_mog_Shift_Info"
STG_MOG_PERSON = "stg_mog_person"
STG_MOG_ENUM_CREW = "stg_mog_enum_crew"

# COMMAND ----------

# DBTITLE 1,Trucks
#source
STD_SHIFT_DUMPS = "fms.StdShiftDumps.parquet"
STD_SHIFT_INFO = "fms.ShiftInfo.parquet"
STD_SHIFT_LOADS = "fms.StdShiftLoads.parquet"
STD_SHIFT_TOPER = "fms.StdShiftoper.parquet"
STD_SHIFT_LOCATIONS = "fms.StdShiftLocations.parquet"

#sink
STG_MOG_TRUCK_LOADS = "stg_mog_Truck_Loads"
STG_MOG_TRUCK_DUMPS = "stg_mog_Truck_Dumps"