# Databricks notebook source
# MAGIC %run ./sites/kolomela/KolomelaTransformationsFms

# COMMAND ----------

# MAGIC %run ./sites/los_bronces/LosBroncesTransformationsFms

# COMMAND ----------

# MAGIC %run ./sites/minas_rio/MinasRioTransformationsFms

# COMMAND ----------

# MAGIC %run ./sites/mogalakwena/MogalakwenaTransformationsFms

# COMMAND ----------

# MAGIC %run ./sites/mogalakwena/MogalakwenaTransformationsHistoryFms

# COMMAND ----------

# MAGIC %run ./sites/venetia/VenetiaTransformationsFms

# COMMAND ----------

# MAGIC %run ./sites/sishen/SishenTransformationsFms

# COMMAND ----------

# MAGIC %run ./sites/capcoal/CapcoalTransformationsFms

# COMMAND ----------

TAG_DPT = "DataProductTransformation"

# COMMAND ----------


class CuratedTransformationFactory:
  def get_instance(inputConfig):
    logger.debug(TAG_DPT,"Transformation","Returning transformer for site {}".format(inputConfig.site))
    try:
      logger.info(TAG_DPT,"Creating transformation object for site: {} \t functional Domain: {} ".format(inputConfig.site,inputConfig.functional_domain))
      if inputConfig.functional_domain == FMS:
        if (inputConfig.site == MOGALAKWENA):
          if(inputConfig.date == HISTORY):
            return MogalakwenaTransformationsHistoryFms()
          else: 
            return MogalakwenaTransformationsFms()
        elif inputConfig.site == LOS_BRONCES:
          return LosBroncesTransformationsFms()
        elif inputConfig.site == SISHEN:
          return SishenTransformationsFms()
        elif inputConfig.site == MINAS_RIO:
          return MinasRioTransformationsFms()
        elif inputConfig.site == KOLOMELA:
          return KolomelaTransformationsFms()
        elif inputConfig.site == VENETIA:
          return VenetiaTransformationsFms()
        elif inputConfig.site == CAPCOAL:
          return CapcoalTransformationsFms()
      else:
          message = "functional domain or site value is empty or invalid"
          logger.error(TAG_DPT,"InvalidInputParameterException", message)
          raise DataTransformationException(TAG_DPT,"InvalidInputParameterException", message) 
    except:
      message = "Failed to return tranformation instance for site: {}".format(inputConfig.site)
      logger.error(TAG_DPT,"InvalidInputParameterException", message)
      raise DataTransformationException(TAG_DPT,"InvalidInputParameterException", message)

# COMMAND ----------

