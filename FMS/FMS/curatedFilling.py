# Databricks notebook source
# MAGIC %run ./commons/utils/DataProductUtil

# COMMAND ----------

# MAGIC %run ./commons/utils/DataProductUdfs

# COMMAND ----------

# MAGIC %run ./raw_transformations/sites/VenetiaFms

# COMMAND ----------

# MAGIC %run ./raw_transformations/sites/SiteLosBroncesFms

# COMMAND ----------

# MAGIC %run ./raw_transformations/sites/SiteKolomelaFms

# COMMAND ----------

# MAGIC %run ./raw_transformations/sites/history/SiteMoglakwenaHistoryFms

# COMMAND ----------

# MAGIC %run ./raw_transformations/sites/SiteMoglakwenaFms

# COMMAND ----------

# MAGIC %run ./raw_transformations/sites/SiteSishenFms

# COMMAND ----------

# MAGIC %run ./raw_transformations/sites/SiteMinasRioFms

# COMMAND ----------

# MAGIC %run ./commons/utils/Constants

# COMMAND ----------

# MAGIC %run ./commons/InputConfig1

# COMMAND ----------

TAG_RTC = "mneu-adb-job-raw-to-curated"

# COMMAND ----------

def rename_the_files(base_output_path,input_path):
  logger.debug(TAG_RTC,"rename_the_files","Renaming the files inside path :"+base_output_path)
  for file in get_list_of_file_path(base_output_path):
    logger.debug(TAG_RTC,"rename_the_files","file name :"+ file.name)
    if file.name.startswith("part-"):
      if (input_config.site == MOGALAKWENA and input_config.date == HISTORY):
        logger.debug(TAG_RTC,"converting csv-parquet","Final output path :"+base_output_path+file.name)
        move_the_file(base_output_path+file.name, base_output_path+input_path.name.replace("[","").replace("]","").replace(TEXT,PARQUET))
      else:
        logger.debug(TAG_RTC,"Converting avro-parquet","Final output path :"+base_output_path+file.name)
        move_the_file(base_output_path+file.name, base_output_path+input_path.name.replace("[","").replace("]","").replace(AVRO,PARQUET))
      
    elif file.name.startswith("_committed") or file.name.startswith("_started"):
      delete_the_file(base_output_path+file.name)

# COMMAND ----------

def pipeline(base_input_path, base_output_path, rowTransformer, input_path, input_config):
#adding size condition just to make sure we process files not the dir
  if(input_path.size > 0):
    path = get_final_input_path(base_input_path, input_path.name)
    logger.debug(TAG_RTC,"","Reading data for input path : "+path)
    if (input_config.site == MOGALAKWENA and input_config.date == HISTORY):
      input_df = rowTransformer.read_data(RAW, ADLS2, CSV, path)
    else:
      input_df = rowTransformer.read_data(RAW, ADLS2, AVRO, path)
    transformedDF =  rowTransformer.transform_data(input_df, input_config)
    rowTransformer.write_data(ADLS2, PARQUET, base_output_path, transformedDF)
    rename_the_files(base_output_path,input_path)

# COMMAND ----------

def execute(base_input_path, base_output_path, rowTransformer, list_of_input_path, failed_fiels, input_config):
  if len(list_of_input_path) == 0:
    return failed_fiels
  else:
    try:
      file = list_of_input_path.pop(0)
      pipeline(base_input_path,base_output_path, rowTransformer, file, input_config)
      return execute(base_input_path,base_output_path, rowTransformer, list_of_input_path, failed_fiels, input_config )
    except DataTransformationException as dte:
      logger.error(TAG_RTC, dte.exeptionType, dte.message)
      return execute(base_input_path, base_output_path, rowTransformer,list_of_input_path, failed_fiels+1,input_config)

# COMMAND ----------

def check_basepath_exist(base_path):
  try:
    dbutils.fs.ls(base_path)
    return True
  except Exception as e:
    return False

# COMMAND ----------

site_list = [['kolomela','kumba','fleet_management','sorsd','Modular Dispatch'],
['sishen','kumba','fleet_management','sorsd','Modular Dispatch'],
['venetia','de_beers','fleet_management','sord','SORD'],
['venetia','de_beers','fleet_management','wenco','WENCO'],
['mogalakwena','platinum','fleet_management','sorsd','Modular Dispatch'],
['los_bronces','copper','fleet_management','sorsd','Modular Dispatch'],
['minas_rio','iob','fleet_management','sorsd','Modular Dispatch']]

from datetime import datetime
from datetime import timedelta

today = datetime.strptime(datetime.today().strftime('%Y/%m/%d'),'%Y/%m/%d')

for list in site_list:
  operating_date = datetime.strptime(dbutils.widgets.get("start_date"),'%Y/%m/%d')
  while operating_date <= today :
    #Reading input perameter
    input_config = InputConfig1(list)
    input_config.date = operating_date.strftime('%Y/%m/%d')
    
    operating_date = (operating_date + timedelta(days=1))
    
    rowTransformer = RawTransformationFactory.getInstance(input_config)
    #Genrating the base input and output path
    try:
      base_input_path = rowTransformer.get_adls2_path(RAW,input_config)
      logger.info(TAG_RTC,"base_input_path: "+base_input_path)
    except DataTransformationException as e:
      logger.error(TAG_RTC, e.exeptionType, e.message)
      raise DataTransformationException(TAG_RTC, e.exeptionType, e.message)
    try:
      base_output_path = rowTransformer.get_adls2_path(CURATED, input_config)
      logger.info(TAG_RTC,"base_output_path: "+base_output_path)
    except DataTransformationException as e:
      logger.error(TAG_RTC, e.exeptionType, e.message)
      raise DataTransformationException(TAG_RTC, e.exeptionType, e.message)
    
    try:
      is_raw_present= check_basepath_exist(base_input_path)
    except DataTransformationException as e:
      logger.error(TAG_RTC, e.exeptionType, e.message)
    
    try:
      is_not_curated_present= not check_basepath_exist(base_output_path)
    except DataTransformationException as e:
      logger.error(TAG_RTC, e.exeptionType, e.message)
    
    if  is_raw_present:
      if is_not_curated_present:
        #fetching list of file inside the base input path
        list_of_input_path = get_list_of_file_path(base_input_path)
        count_input_path = len(list_of_input_path)
        logger.debug(TAG_RTC,"","Number of input files : "+str(len(list_of_input_path)))
        failedfiles = execute(base_input_path, base_output_path, rowTransformer, list_of_input_path,0,input_config)
        if count_input_path == failedfiles:
          raise DataTransformationException(TAG_RTC, "FailedForAllFiels", \
                                          "All files are failed to load, Failed file Count \n failedfiles - " + str(failedfiles) \
                                           +" list_of_input_path.length - "+str(count_input_path))
    
    

# COMMAND ----------

site_list = [['kolomela','kumba','fleet_management','sorsd','Modular Dispatch'],
['sishen','kumba','fleet_management','sorsd','Modular Dispatch'],
['venetia','de_beers','fleet_management','sord','SORD'],
['venetia','de_beers','fleet_management','wenco','WENCO'],
['mogalakwena','platinum','fleet_management','sorsd','Modular Dispatch'],
['los_bronces','copper','fleet_management','sorsd','Modular Dispatch'],
['minas_rio','iob','fleet_management','sorsd','Modular Dispatch']]
 
from datetime import datetime
from datetime import timedelta
 
hrs = ['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23','00']
 
today = datetime.strptime(datetime.today().strftime('%Y/%m/%d'),'%Y/%m/%d')
for list in site_list:
  operating_date = datetime.strptime(dbutils.widgets.get("start_date"),'%Y/%m/%d')
  while operating_date <= today :
    
    #Reading input perameter
    input_config = InputConfig1(list)
    input_config.date = operating_date.strftime('%Y/%m/%d')
    
    operating_date = (operating_date + timedelta(days=1))
    
    rowTransformer = RawTransformationFactory.getInstance(input_config)
    for num in hrs:  
      #Genrating the base input and output path
      try:
        base_input_path = rowTransformer.get_adls2_path(RAW,input_config)+ num+"/"
        logger.info(TAG_RTC,"base_input_path: "+base_input_path)
      except DataTransformationException as e:
        logger.error(TAG_RTC, e.exeptionType, e.message)
        raise DataTransformationException(TAG_RTC, e.exeptionType, e.message)
      try:
        base_output_path = rowTransformer.get_adls2_path(CURATED, input_config)+ num+"/"
        logger.info(TAG_RTC,"base_output_path: "+base_output_path)
      except DataTransformationException as e:
        logger.error(TAG_RTC, e.exeptionType, e.message)
        raise DataTransformationException(TAG_RTC, e.exeptionType, e.message)
      try:
        is_raw_present= check_basepath_exist(base_input_path)
      except DataTransformationException as e:
        logger.error(TAG_RTC, e.exeptionType, e.message)
      
      try:
        is_not_curated_present= not check_basepath_exist(base_output_path)
      except DataTransformationException as e:
        logger.error(TAG_RTC, e.exeptionType, e.message)
      
      if  is_raw_present:
        if is_not_curated_present:
          #fetching list of file inside the base input path
          list_of_input_path = get_list_of_file_path(base_input_path)
          count_input_path = len(list_of_input_path)
          logger.debug(TAG_RTC,"","Number of input files : "+str(len(list_of_input_path)))
          failedfiles = execute(base_input_path, base_output_path, rowTransformer, list_of_input_path,0,input_config)
          if count_input_path == failedfiles:
            raise DataTransformationException(TAG_RTC, "FailedForAllFiels", \
                                            "All files are failed to load, Failed file Count \n failedfiles - " + str(failedfiles) \
                                             +" list_of_input_path.length - "+str(count_input_path))
      
      

# COMMAND ----------

