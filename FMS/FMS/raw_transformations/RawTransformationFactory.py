# Databricks notebook source
TAG_RawTrans = "FMS_RawTransformationFactory"

# COMMAND ----------

class RawTransformationFactory:
  def getInstance(inputConfig):
    logger.debug(TAG_RawTrans,"DataProductFactory","Genrating Instance for site: " +inputConfig.site)
    if(inputConfig.functional_domain == FMS):
      if(inputConfig.site == MOGALAKWENA): 
        return SiteMoglakwenaFms()
      elif(inputConfig.site == LOS_BRONCES):        
        return SiteLosBroncesFms()
      elif(inputConfig.site == CAPCOAL):        
        return SiteCapcoalFms()
      elif(inputConfig.site == SISHEN):        
        return SiteSishenFms()
      elif(inputConfig.site == MINAS_RIO):
        return SiteMinasRioFms()
      elif(inputConfig.site == KOLOMELA):
        return SiteKolomelaFms()
      elif(inputConfig.site == VENETIA):
        return VenetiaFms()
      elif(inputConfig.site == MOGALAKWENA & inputConfig.date == HISTORY):
        return SiteMoglakwenaHistoryFms()
      else:
        return self
      
  def read_data(self, zone, store_type, file_format, path):
    logger.error(TAG_RawTrans,"NotImplementedError","Reading interface has not implemented")
    raise DataTransformationException("DataProductFactory","NotImplementedError","Reading interface has not implemented")
   
      
  def transform_data(self, input_df, input_config):
    logger.error(TAG_RawTrans,"NotImplementedError","Transformation functionality has not implemented")
    raise DataTransformationException("DataProductFactory","NotImplementedError","Transformation functionality has not implemented")
   
      
  def write_data(self, store_type, file_format, output_path, transformed_df):
    logger.error(TAG_RawTrans,"NotImplementedError","Writing interface has not implemented")
    raise DataTransformationException("DataProductFactory","NotImplementedError","Writing interface has not implemented")
   
  def get_adls2_path(self, zone, input_config):
    logger.error(TAG_RawTrans,"NotImplementedError","Path creation interface has not implemented")
    raise DataTransformationException("DataProductFactory","NotImplementedError","Path creation interface has not implemented")

# COMMAND ----------

