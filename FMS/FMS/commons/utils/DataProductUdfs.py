# Databricks notebook source
def is_numeric(value):
      if value:
        return value.isdigit()
      else:
        return False