# Databricks notebook source
class DataTransformationException(Exception):
  def __init__(self, className, exeptionType, message):
    self.message = message
    self.className = className
    self.exeptionType = exeptionType
  
  def __str__(self):
    return str("Exception occurred in: "+self.className+", Exception type: "+self.exeptionType+", Message: "+self.message)
