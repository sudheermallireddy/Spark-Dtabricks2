# Databricks notebook source
# MAGIC %run ../commons/utils/TransformationLogger

# COMMAND ----------

SV_TAG = 'source_validation'
logger = TransformationLogger(debug_mode)

# COMMAND ----------

import sys
from applicationinsights import TelemetryClient
tc = TelemetryClient(telemetryClient)

# COMMAND ----------

class SourceValidation:

  def isEmpty(self, input_config, file_type, base_input_path):
    '''
    If folder, list files under it. Else - Read file.
    And change STATUS to -
          Failed - All files are empty
          Warning - Few files are empty but not all 
          Success - Not a single file is empty
    Abort job on Failed state, Continue job for Success and Warning.
    Log status in Application Insight - PipelineName, Status(Failed|Warning|Success), FileStatus (FilePath:Status)
    '''
    try:
      status = FAILED
      dict = {}
      
      try:
        list_of_input_path = dbutils.fs.ls(base_input_path)
        logger.info(SV_TAG,'Directory '+base_input_path)
      except Exception as e:
        logger.info(SV_TAG, "Provided input path is not a directory. Processing for a file")  
        
        try:
          new_base_input_path = base_input_path.replace("[","\[").replace("]","\]")
          try:
            df = spark.read.format(file_type).load(new_base_input_path)
          except:
            logger.info("Failed to read file")
            raise Exception
          
          if len(df.head(2)) == 0:
            logger.info(SV_TAG,"Empty File"+base_input_path)
            status = FAILED
            dict[path] = status
          else: 
            logger.info(SV_TAG,"Valid File "+base_input_path)
            status = SUCCESS
            dict[path] = status
          
          if status == FAILED or status == WARNING:
            list = [(k, v) for k, v in dict.items()]
            tc.track_exception(*sys.exc_info(), properties={ 'PipelineName': 'FMS_'+input_config.site, 'Status': status, 'FileStatus': list })
          return status
        
        except Exception as e:
          logger.info(SV_TAG, "Provided input path is neither a file nor a directory")
          print(sys.exc_info()[0],"occured.")
          raise Exception
      
      try:
        if len(list_of_input_path) < 1:
          raise Exception("No files to read under provided path")
      except Exception as nfe:
        raise Exception
      
      try:
        for file in list_of_input_path:
          path = base_input_path + "/" + file.name
          file_name = file.name.replace("[","\[").replace("]","\]")
          new_path = base_input_path + "/" + file_name
          
          try:
            df = spark.read.format(file_type).load(new_path)
          except Exception as e:
            logger.info("Failed to read file")
            raise Exception
          
          if len(df.head(2)) != 0:
            status = SUCCESS
            dict[path] = status
            logger.info(SV_TAG,"Valid File: "+file.name)
            continue
          else:
            status = FAILED
            dict[path] = status
            logger.info(SV_TAG,"Empty File: "+file.name)
      
      except Exception as e:
        print(sys.exc_info()[0],"occured.")
        raise Exception
      
      try:
        if SUCCESS in dict.values() and FAILED in dict.values():
          status = WARNING
        elif FAILED not in dict.values():
          status = SUCCESS
        elif SUCCESS not in dict.values():
          status = FAILED
          
        if status == FAILED or status == WARNING:
          list = [(k, v) for k, v in dict.items()]
          tc.track_exception(*sys.exc_info(), properties={'PipelineName': 'FMS_'+input_config.site, 'Status': status, 'FileStatus': list })
        return status
      
      except Exception as e:
        print(sys.exc_info()[0],"occured.")
        logger.info(SV_TAG,"Failed while tracking exception in Application Insights")
        raise Exception
    
    except Exception as e:
      logger.info(SV_TAG,"Failed at Application Insights telemtry client")
      return FAILED
    
    finally:
      tc.flush()