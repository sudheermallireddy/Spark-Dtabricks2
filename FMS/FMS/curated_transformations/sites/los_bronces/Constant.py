# Databricks notebook source
# DBTITLE 1,Shovels
#source
LB_STD_SHIFT_AUX = "fms.StdShiftaux.parquet"
LB_STD_SHIFT_STATE = "fms.StdShiftstate.parquet"
LB_STD_SHIFT_EQMT = "fms.StdShifteqmt.parquet"
EXT_REASONS = "fms.ExtReasons.parquet"
CUST_SHIFT_LOAD_BKT_DETAIL_PIVOT = "fms.CustShiftloadbktdetailpivot.parquet"
ENUM = "fms.Enum.parquet"
ENUM_MATERIAL = "fms.EnumMaterial.parquet"
LB_SHIFT_INFO = "fms.ShiftInfo.parquet"
LB_PERSON = "fms.StdShiftoper.parquet"
LB_ENUM_CREW = "fms.EnumCREW.parquet"

EQUIPMENT_CATALOGUE = "equipment_catalogue.parquet"

#sink
STG_LB_EQUIPEMENT = "stg_lb_Equipment"
STG_LB_EQUIPEMENT_STATUS = "stg_lb_Equipment_Status"
STG_LB_OPERATING_ENTITY = "stg_lb_Operating_Entity"
STG_LB_DELAY_REASON = "stg_lb_Delay_Reason"
STG_LB_SHOVEL_BUCKETS = "stg_lb_Shovel_Buckets"
STG_LB_MATERIAL = "stg_lb_Material"
STG_LB_ENUM = "stg_lb_Enum"
STG_LB_SHIFT_INFO = "stg_lb_Shift_Info"
STG_LB_PERSON = "stg_lb_person"
STG_LB_ENUM_CREW = "stg_lb_enum_crew"

# COMMAND ----------

# DBTITLE 1,Trucks
#soruce
STD_SHIFT_DUMPS = "fms.StdShiftDumps.parquet"
STD_SHIFT_INFO = "fms.ShiftInfo.parquet"
STD_SHIFT_LOADS = "fms.StdShiftLoads.parquet"
STD_SHIFT_TOPER = "fms.StdShiftoper.parquet"
STD_SHIFT_LOCATIONS = "fms.StdShiftLocations.parquet"

#sink
STG_LB_TRUCK_LOADS = "stg_lb_Truck_Loads"
STG_LB_TRUCK_DUMPS = "stg_lb_Truck_Dumps"