# Databricks notebook source
# DBTITLE 1,CAPCOAL
#source
CP_EQUIPMENT = "leica.data_extraction_equipment.parquet"
CP_DATA_EXTRACTION_ENUM_CATEGORIES = "leica.data_extraction_enum_categories.parquet"
CP_DATA_EXTRACTION_ENUM_TABLES = "leica.data_extraction_enum_tables.parquet"
CP_DATA_EXTRACTION_GRADES = "leica.data_extraction_grades.parquet"
CP_DATA_EXTRACTION_LOCATIONS = "leica.data_extraction_locations.parquet"
CP_DATA_EXTRACTION_MATERIAL_TONNAGES = "leica.data_extraction_material_tonnages.parquet"
CP_DATA_EXTRACTION_SHIFT_BUCKETS = "leica.data_extraction_shift_buckets.parquet"
CP_DATA_EXTRACTION_SHIFT_DUMPS = "leica.data_extraction_shift_dumps.parquet"
CP_DATA_EXTRACTION_SHIFT_HAULS = "leica.data_extraction_shift_hauls.parquet"
CP_DATA_EXTRACTION_SHIFT_LOADS = "leica.data_extraction_shift_loads.parquet"
CP_DATA_EXTRACTION_WORKERS = "leica.data_extraction_workers.parquet"


#sink
STG_EQUIPMENT = "stg_cap_data_extraction_equipment"
STG_DATA_EXTRACTION_ENUM_CATEGORIES = "stg_cap_data_extraction_enum_categories"
STG_DATA_EXTRACTION_ENUM_TABLES = "stg_cap_data_extraction_enum_tables"
STG_DATA_EXTRACTION_GRADES = "stg_cap_data_extraction_grades"
STG_DATA_EXTRACTION_MATERIAL_TONNAGES = "stg_cap_data_extraction_material_tonnages"
STG_DATA_EXTRACTION_LOCATIONS = "stg_cap_data_extraction_locations"
STG_DATA_EXTRACTION_WORKERS = "stg_cap_data_extraction_workers"
STG_DATA_EXTRACTION_SHIFT_BUCKETS = "stg_cap_data_extraction_shift_buckets"
STG_DATA_EXTRACTION_SHIFT_DUMPS = "stg_cap_data_extraction_shift_dumps"
STG_DATA_EXTRACTION_SHIFT_HAULS = "stg_cap_data_extraction_shift_hauls"
STG_DATA_EXTRACTION_SHIFT_LOADS = "stg_cap_data_extraction_shift_loads"