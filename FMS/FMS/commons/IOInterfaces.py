# Databricks notebook source
TAG_IOI = "IOInterfaces"

# COMMAND ----------

class Storage:
  def get_instance(store_type):
    logger.debug(TAG_IOI,"Storage","Returning store type for "+store_type)
    if store_type == ADLS2:
        return Adls2Storage()
    elif store_type == SQL:
        return SqlStorage()
  

# COMMAND ----------

class Adls2Storage():    
  def read_data(self, file_format, path):
    logger.debug(TAG_IOI,"Adls2Storage","Reading data from ADLS2 with file format: {} and path :{}".format(file_format,path))
    df = None
    try:
      if file_format == AVRO:
          df = spark.read.format("com.databricks.spark.avro").load(path)
      elif file_format == PARQUET:
          df = spark.read.parquet(path)
      elif file_format == CSV:
          df = spark.read.format(CSV).option("delimiter",CONTROLA).load(path, header=True)
    except Exception as e:
      logger.error(TAG_IOI,"FileReadException","Failed to read data from path {}: {}".format(path,str(e)))
      raise DataTransformationException(TAG_IOI,"FileReadException","Failed to read data from path : "+path)
    return df

  def write_data(self, file_format, output_path, transformed_df):
    logger.debug(TAG_IOI,"Adls2Storage","Writing data in ADLS2 with file format: {} at {}".format(file_format, output_path))
    try:
      if file_format == PARQUET:
        transformed_df.coalesce(1).write.mode("append").parquet(output_path)
    except Exception as e:
      logger.error(TAG_IOI,"FileWriteException","Failed while writing data at path : "+output_path)
      raise DataTransformationException(TAG_IOI,"FileWriteException","Failed while writing data at path : "+output_path)

# COMMAND ----------

class SqlStorage():
  
  def read_data(self, file_format, table_name):
    logger.debug(TAG_IOI,"SqlStorage","Reading data from {} in SQL".format(table_name))
    try:
      connection_properties = get_connection_properties()
      return spark.read.jdbc(connection_properties[JDBC_URL_KEY], table_name, properties = connection_properties)
    except Exception as e:
      logger.error(TAG_IOI,"DatabaseReadException","Failed in reading data for {} : {}".format(table_name,str(e)))
      raise DataTransformationException(TAG_IOI,"DatabaseReadException","Failed in reading data for "+table_name)
      
  def write_data(self, file_format, table_name, output_df):
    logger.debug(TAG_IOI,"SqlStorage","Storing data in SQL at {} in {} format".format(table_name, file_format))
    try:
      connection_properties = get_connection_properties()
      logger.debug(TAG_IOI,"SqlStorage","JDBC URL :"+ connection_properties[JDBC_URL_KEY])
      output_df.write.option("truncate","true").mode("overwrite").jdbc(connection_properties[JDBC_URL_KEY], table_name, properties = connection_properties)
    except Exception as e:
      logger.error(TAG_IOI,"DatabaseWriteException","Failed in writing data for "+table_name)
      raise DataTransformationException(TAG_IOI,"DatabaseWriteException","Failed in writing data for "+table_name+ " \n "+str(e))
      
  def write_data_for_sap(self, write_mode, table_name, output_df):
    logger.debug(TAG_IOI,"SqlStorage","Storing data in SQL at {}".format(table_name))
    try:
      connection_properties = get_connection_properties()
      logger.debug(TAG_IOI,"SqlStorage","JDBC URL :"+ connection_properties[JDBC_URL_KEY])
      output_df.write.mode(write_mode).jdbc(connection_properties[JDBC_URL_KEY], table_name, properties = connection_properties)
    except Exception as e:
      logger.error(TAG_IOI,"DatabaseWriteException","Failed in writing data for "+table_name)
      raise DataTransformationException(TAG_IOI,"DatabaseWriteException","Failed in writing data for "+table_name+ " \n "+str(e))
      

# COMMAND ----------

