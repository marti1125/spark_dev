# Databricks notebook source
from pyspark import SparkConf, SparkContext

# COMMAND ----------

conf = SparkConf().setAppName("Read File")

# COMMAND ----------

sc = SparkContext.getOrCreate(conf)

# COMMAND ----------

text = sc.textFile("sample.txt")

# COMMAND ----------

print(text.collect())
