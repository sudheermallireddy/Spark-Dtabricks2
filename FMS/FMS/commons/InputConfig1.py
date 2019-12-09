# Databricks notebook source
class InputConfig1:
  site = None
  business_unit = None
  functional_domain = None
  system = None
  date = None
  src_application = None
  
  
  def __init__(self, list):
    self.site = list[0]
    self.business_unit = list[1]
    self.functional_domain = list[2]
    self.system = list[3]
#     self.date  = list[3]
    self.src_application = list[4]
    
  def __repr__(self):
    return "InputConfig()"
  def __str__(self):
    return "Input Perameters \n site : {},\n business unit : {},\n functional domain : {},\n system : {},\ndate: {}, \n src_application: {}\n".format( self.site, self.business_unit, self.functional_domain, self.system, self.date, \
    self.src_application)
  

# COMMAND ----------



# COMMAND ----------

