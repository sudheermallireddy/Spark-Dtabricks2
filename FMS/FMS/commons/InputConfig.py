# Databricks notebook source
class InputConfig:
  site = None
  business_unit = None
  functional_domain = None
  system = None
  date = None
  src_application = None
  
  
  def __init__(self):
    self.site = dbutils.widgets.get("site")
    self.business_unit = dbutils.widgets.get("business_unit")
    self.functional_domain = dbutils.widgets.get("functional_domain")
    self.system = dbutils.widgets.get("system")
    self.date  = dbutils.widgets.get("date")
    self.src_application = dbutils.widgets.get("src_application").upper()
    
  def __repr__(self):
    return "InputConfig()"
  def __str__(self):
    return "Input Perameters \n site : {},\n business unit : {},\n functional domain : {},\n system : {},\ndate: {}, \n src_application: {}\n".format( self.site, self.business_unit, self.functional_domain, self.system, self.date, \
    self.src_application)
  

# COMMAND ----------

