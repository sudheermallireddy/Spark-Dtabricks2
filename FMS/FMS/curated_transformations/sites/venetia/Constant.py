# Databricks notebook source
# DBTITLE 1,Shovels
#source
DIM_DOWNTIME = "dbo.Dim_Downtime.parquet"
EQUIP = "dbo.EQUIP.parquet"
EQUIP_TYPE = "dbo.EQUIP_TYPE.parquet"
EQUIP_MODEL = "dbo.EQUIP_MODEL.parquet"
MATERIAL_TYPE = "dbo.MATERIAL_TYPE.parquet"
MATERIAL = "dbo.MATERIAL.parquet"
EQUIPMENT_STATUS_TRANS = "dbo.EQUIPMENT_STATUS_TRANS.parquet"
BADGE = "dbo.BADGE.parquet"
FLEET = "dbo.FLEET.parquet"
EQUIPMENT_CATALOGUE = "equipment_catalogue.parquet"
HAUL_CYCLE_TRANS="dbo.HAUL_CYCLE_TRANS.parquet"
VEN_STATUS_ANGLO_TIMEMODEL="dbo.VEN_STATUS_ANGLO_TIMEMODEL.parquet"
HAUL_UNIT_STATUS_TRANS_COL="dbo.HAUL_UNIT_STATUS_TRANS_COL.parquet"
HAUL_CYCLE_BUCKET_TRANS="dbo.HAUL_CYCLE_BUCKET_TRANS.parquet"
EQUIPMENT_TRANS = "dbo.EQUIPMENT_TRANS.parquet"

#sink
STG_VN_EQUIPEMENT = "stg_vn_Equipment"
STG_VN_EQUIPEMENT_STATUS = "stg_vn_Equipment_Status"
STG_VN_OPERATING_ENTITY = "stg_vn_Operating_Entity"
STG_VN_DELAY_REASON = "stg_vn_Delay_Reason"
STG_VN_SHOVEL_BUCKETS = "stg_vn_Shovel_Buckets"
STG_VN_MATERIAL = "stg_vn_Material"
STG_VN_ENUM = "stg_vn_Enum"
STG_VN_SHIFT_INFO = "stg_vn_Shift_Info"
STG_VN_PERSON = "stg_vn_person"
STG_VN_TRUCK_CYCLES="stg_vn_truck_cycles"

# COMMAND ----------

# DBTITLE 1,Trucks
#source
#STD_SHIFT_DUMPS = "fms.StdShiftDumps.parquet"
# STD_SHIFT_INFO = "fms.ShiftInfo.parquet"
# STD_SHIFT_LOADS = "fms.StdShiftLoads.parquet"
# STD_SHIFT_TOPER = "fms.StdShiftoper.parquet"
# STD_SHIFT_LOCATIONS = "fms.StdShiftLocations.parquet"

# #sink
# STG_MOG_TRUCK_LOADS = "stg_mog_Truck_Loads"
# STG_MOG_TRUCK_DUMPS = "stg_mog_Truck_Dumps"