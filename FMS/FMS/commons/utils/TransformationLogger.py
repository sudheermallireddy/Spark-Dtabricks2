# Databricks notebook source
class TransformationLogger:
  
  mode = None

  #defining constructor  
  def __init__(self, debug_mode):
    self.mode = debug_mode
      
  # 
  def debug(self, notebook, cell, message):
    if(self.mode == DEBUG_MODE_Y):
      print(str(notebook+": "+cell+": "+message))
  
  def info(self, notebook, message):
    print(str(notebook+": "+message))
    #tc.track_trace(manual_limits_path)
  
  def error(self, className, exeptionType, message):
    print(str("Exception occurred in: "+className+", Exception type: "+exeptionType+", Message: "+message))
  
  def warning(self, statement):
    print(statement)
    
  def verbose(self, statement):
    print(statement)
  