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

# MAGIC %run ./raw_transformations/sites/SiteMoglakwenaFms

# COMMAND ----------

# MAGIC %run ./raw_transformations/sites/history/SiteMoglakwenaHistoryFms

# COMMAND ----------

# MAGIC %run ./raw_transformations/sites/SiteSishenFms

# COMMAND ----------

# MAGIC %run ./raw_transformations/sites/SiteMinasRioFms

# COMMAND ----------

# MAGIC %run ./raw_transformations/sites/SiteCapcoalFms

# COMMAND ----------

# MAGIC %run ./commons/utils/Constants

# COMMAND ----------

# MAGIC %run ./curated_transformations/CuratedTransformationFactory

# COMMAND ----------

# MAGIC %run ./commons/InputConfig

# COMMAND ----------

# MAGIC %run ./data_profiling/SourceValidation

# COMMAND ----------

TAG_RTC = "mneu-adb-job-raw-to-curated"

# COMMAND ----------

input_config = InputConfig()
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

#Reading input perameter
input_config = InputConfig()

rowTransformer = RawTransformationFactory.getInstance(input_config)
#Genrating the base input and output path
try:
  base_input_path = rowTransformer.get_adls2_path(RAW,input_config)
  logger.info(TAG_RTC,"base_input_path: "+base_input_path)
except DataTransformationException as e:
  logger.error(TAG_RTC, e.exeptionType, e.message)
  raise DataTransformationException(TAG_RTC, e.exeptionType, e.message)

try:
  validation=SourceValidation()
  status = validation.isEmpty(input_config, AVRO, base_input_path)
  logger.debug(TAG_RTC, "", "validation.isEmpty() - "+str(status))
  if(status == FAILED):
    raise Exception()
except Exception as e:
  logger.info(TAG_RTC,"Failed at empty file check")
  sys.exit()
  
try:
  base_output_path = rowTransformer.get_adls2_path(CURATED, input_config)
  logger.info(TAG_RTC,"base_output_path: "+base_output_path)
except DataTransformationException as e:
  logger.error(TAG_RTC, e.exeptionType, e.message)
  raise DataTransformationException(TAG_RTC, e.exeptionType, e.message)
#deleting the output path if exist
try:
  logger.debug(TAG_RTC, "", "Deleting file if exist from : {}".format(base_output_path))
  delete_path_if_exist(base_output_path)
except DataTransformationException as e:
  logger.error(TAG_RTC, e.exeptionType, e.message)
  raise DataTransformationException(TAG_RTC, e.exeptionType, e.message)
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

TAG_CTS = "mneu-adb-job-curated-to-sqldb"

# COMMAND ----------

import sys

#Reading input perameter
input_config = InputConfig()


try:
  data_dict = {}
  #getting the intance for the site
  
  rowTransformer = RawTransformationFactory.getInstance(input_config)

  #Creating and calling the transformer based on site and functional_domain
  try:
    curatedTransformer = CuratedTransformationFactory.get_instance(input_config)
  except DataTransformationException as e:
    logger.error(TAG_CTS,"InvalidInputException",e.message)
    raise SystemExit(e)

  try:
    processed_data_dict = curatedTransformer.execute(data_dict, rowTransformer, input_config)
    logger.debug(TAG_CTS, "", "Number of transformation in dict :"+ str(len(processed_data_dict)))
  except SystemExit as e:
    logger.error(TAG_CTS,"TransformationException",str(e))
    raise SystemExit(str(e))

# TODO : Need to optimize the pipeline, process the table parallel 
  #writing data into tables
  error_count = 0
  for table in processed_data_dict:
    try:
      logger.info(TAG_CTS,"Storing data for table :"+table)
      rowTransformer.write_data(SQL, None, table, processed_data_dict[table])
    except DataTransformationException as e:
      logger.error(TAG_CTS,"WriteException", e.message)
      error_count += 1

  if error_count > 0:
    message = "Failed to write the data for all the tables detail: {}".format(input_config)
    logger.error(TAG_CTS,"",message)
    raise  DataTransformationException(TAG_CTS,"DataWriteException", message)

except SystemExit as e:
  logger.error(TAG_CTS,"SystemExitException",str(e))
  sys.exit()

# COMMAND ----------

