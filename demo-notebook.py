# Databricks notebook source
# MAGIC %fs
# MAGIC ls dbfs:/mnt/blob/stt_sample_files.zip/stt_sample_files/knn/

# COMMAND ----------

from pyspark.sql.functions import input_file_name, regexp_extract

# Read CSV files from a directory in dbfs that match the file pattern
df = spark.read.csv("dbfs:/mnt/blob/stt_sample_files.zip/stt_sample_files/knn/TnT_KNDUI*.csv", header=True, inferSchema=True)

# Add a new column "Source_File_Name" dynamically
df = df.withColumn("Source_File_Name", regexp_extract(input_file_name(), r'TnT_KNDUI_(\d{14})\.csv', 0))

# Show the resulting DataFrame
df.write.mode("overwrite").saveAsTable("App_Supplies_Laser_Toner_Track.KN_TEMP_TABLE")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM App_Supplies_Laser_Toner_Track.KN_TEMP_TABLE;

# COMMAND ----------

# MAGIC %sql
# MAGIC --> SET1 TEMP TABLE
# MAGIC CREATE OR REPLACE TABLE SET1 AS (
# MAGIC SELECT
# MAGIC TMP1.PALLET_ID,
# MAGIC TMP1.SERIAL_NUMBER,
# MAGIC MAX(TMP1.HP_SKU) AS HP_SKU,
# MAGIC MAX(TMP1.SOURCE_FILE_NAME) AS SOURCE_FILE_NAME   
# MAGIC FROM App_Supplies_Laser_Toner_Track.KN_TEMP_TABLE TMP1
# MAGIC GROUP BY TMP1.PALLET_ID, TMP1.SERIAL_NUMBER
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC --> SET2 TEMP TABLE
# MAGIC CREATE OR REPLACE TABLE SET2 AS ( 
# MAGIC SELECT 
# MAGIC PALLET_ID, 
# MAGIC COUNT(DISTINCT SERIAL_NUMBER) AS PALLET_SN_COUNT
# MAGIC FROM App_Supplies_Laser_Toner_Track.KN_TEMP_TABLE
# MAGIC GROUP BY PALLET_ID
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC --> KNN FINAL TABLE
# MAGIC CREATE OR REPLACE TABLE App_Supplies_Laser_Toner_Track.HSKU_KN_DATA
# MAGIC SELECT 
# MAGIC set1_temp.PALLET_ID,
# MAGIC set1_temp.SERIAL_NUMBER,
# MAGIC set1_temp.HP_SKU,
# MAGIC set2_temp.PALLET_SN_COUNT,
# MAGIC DIM.PALLET_QUANTITY AS PALLET_EXPECTED_SN_COUNT,
# MAGIC CASE WHEN set2_temp.PALLET_SN_COUNT = DIM.PALLET_QUANTITY THEN 'Y' ELSE 'N' END AS ACTIVE_FLAG,
# MAGIC CURRENT_TIMESTAMP() AS INSERT_TIMESTAMP,
# MAGIC set1_temp.SOURCE_FILE_NAME AS SOURCE_FILE_NAME
# MAGIC FROM SET1 set1_temp
# MAGIC INNER JOIN SET2 set2_temp ON set1_temp.PALLET_ID = set2_temp.PALLET_ID
# MAGIC LEFT OUTER JOIN 
# MAGIC App_Supplies_Laser_Toner_Track.HP_SKU_DIM DIM
# MAGIC ON set1_temp.HP_SKU = DIM.HP_SKU
# MAGIC AND DIM.REGION_BOX_ART = 'EMEA'
# MAGIC WHERE NOT EXISTS (SELECT 1 FROM App_Supplies_Laser_Toner_Track.HSKU_KN_DATA TGT
# MAGIC WHERE set1_temp.PALLET_ID = TGT.PALLET_ID AND set1_temp.SERIAL_NUMBER = TGT.SERIAL_NUMBER)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM App_Supplies_Laser_Toner_Track.HSKU_KN_DATA;
