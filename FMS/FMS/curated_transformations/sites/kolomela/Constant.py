# Databricks notebook source
# DBTITLE 1,Shovels
#source
KM_STD_SHIFT_AUX = "fms.stdShiftaux.parquet"
KM_STD_SHIFT_STATE = "fms.StdShiftstate.parquet"
KM_STD_SHIFT_EQMT = "fms.stdShifteqmt.parquet"
EXT_REASONS = "fms.ExtReasons.parquet"
CUST_SHIFT_LOAD_BKT_DETAIL_PIVOT = "fms.CustShiftloadbktdetailpivot.parquet"
KM_ENUM = "fms.Enum.parquet"
KM_ENUM_MATERIAL = "fms.EnumMaterial.parquet"
KM_SHIFT_INFO = "fms.ShiftInfo.parquet"
KM_PERSON = "fms.StdShiftoper.parquet"
KM_ENUM_CREW = "fms.EnumCREW.parquet"

EQUIPMENT_CATALOGUE = "equipment_catalogue.parquet"

#sink
STG_KM_EQUIPEMENT = "stg_km_Equipment"
STG_KM_EQUIPEMENT_STATUS = "stg_km_Equipment_Status"
STG_KM_OPERATING_ENTITY = "stg_km_Operating_Entity"
STG_KM_DELAY_REASON = "stg_km_Delay_Reason"
STG_KM_SHOVEL_BUCKETS = "stg_km_Shovel_Buckets"
STG_KM_MATERIAL = "stg_km_Material"
STG_KM_ENUM = "stg_km_Enum"
STG_KM_SHIFT_INFO = "stg_km_Shift_Info"
STG_KM_PERSON = "stg_km_person"
STG_KM_ENUM_CREW = "stg_km_enum_crew"

# COMMAND ----------

# DBTITLE 1,Trucks
#soruce
KM_STD_SHIFT_DUMPS = "fms.StdShiftDumps.parquet"
KM_STD_SHIFT_INFO = "fms.ShiftInfo.parquet"
KM_STD_SHIFT_LOADS = "fms.StdShiftLoads.parquet"
KM_STD_SHIFT_TOPER = "fms.StdShiftoper.parquet"
KM_STD_SHIFT_LOCATIONS = "fms.StdShiftLocations.parquet"

#sink
STG_KM_TRUCK_LOADS = "stg_km_Truck_Loads"
STG_KM_TRUCK_DUMPS = "stg_km_Truck_Dumps"