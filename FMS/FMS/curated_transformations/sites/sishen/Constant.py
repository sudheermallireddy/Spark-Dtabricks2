# Databricks notebook source
# DBTITLE 1,Shovels
#source
SH_STD_SHIFT_AUX = "fms.stdShiftaux.parquet"
SH_STD_SHIFT_EQMT = "fms.stdShifteqmt.parquet"
SH_STD_SHIFT_STATE = "fms.StdShiftstate.parquet"
EXT_REASONS = "fms.ExtReasons.parquet"
CUST_SHIFT_LOAD_BKT_DETAIL_PIVOT = "fms.CustShiftloadbktdetailpivot.parquet"
ENUM = "fms.Enum.parquet"
ENUM_MATERIAL = "fms.EnumMaterial.parquet"
SH_SHIFT_INFO = "fms.ShiftInfo.parquet"
SH_PERSON = "fms.StdShiftoper.parquet"
SH_ENUM_CREW = "fms.EnumCREW.parquet"

EQUIPMENT_CATALOGUE = "equipment_catalogue.parquet"

#sink
STG_SH_EQUIPEMENT = "stg_sh_Equipment"
STG_SH_EQUIPEMENT_STATUS = "stg_sh_Equipment_Status"
STG_SH_OPERATING_ENTITY = "stg_sh_Operating_Entity"
STG_SH_DELAY_REASON = "stg_sh_Delay_Reason"
STG_SH_SHOVEL_BUCKETS = "stg_sh_Shovel_Buckets"
STG_SH_MATERIAL = "stg_sh_Material"
STG_SH_ENUM = "stg_sh_Enum"
STG_SH_SHIFT_INFO = "stg_sh_Shift_Info"
STG_SH_PERSON = "stg_sh_person"
STG_SH_ENUM_CREW = "stg_sh_enum_crew"

# COMMAND ----------

# DBTITLE 1,Trucks
#source
SH_STD_SHIFT_DUMPS = "fms.StdShiftDumps.parquet"
SH_STD_SHIFT_INFO = "fms.ShiftInfo.parquet"
SH_STD_SHIFT_LOADS = "fms.StdShiftLoads.parquet"
SH_STD_SHIFT_TOPER = "fms.StdShiftoper.parquet"
SH_STD_SHIFT_LOCATIONS = "fms.StdShiftLocations.parquet"

#sink
STG_SH_TRUCK_LOADS = "stg_sh_Truck_Loads"
STG_SH_TRUCK_DUMPS = "stg_sh_Truck_Dumps"