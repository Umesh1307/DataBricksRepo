# Databricks notebook source
# Mounting ADLS inside databricks
dbutils.fs.mount(
    source = "wasbs://demo-folder@tigerstorageaccountgrp.blob.core.windows.net",
    mount_point = "/mnt/blob",
    extra_configs = {"fs.azure.account.key.tigerstorageaccountgrp.blob.core.windows.net":"qGri6BZi9ROryFPA2FeucrfHGJ7t1m7vjEl4LUP9y0pv+srrQExl4w2kXntTO/IRrQyVdRoIbll++AStBWO+nA=="})

# COMMAND ----------


