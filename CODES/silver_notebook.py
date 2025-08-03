# Databricks notebook source
df=spark.read.format("parquet")\
    .option("inferSchema",True)\
    .load('abfss://bronze@dbricksstrgacc.dfs.core.windows.net/raw_data')

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

df=df.withColumn('model_category', F.split(df['Model_ID'],'-')[0])
df.display()

# COMMAND ----------

df= df.withColumn('revenue_per_unit',df['Revenue']/df['Units_Sold'])
df.display()

# COMMAND ----------

from pyspark.sql.functions import sum

df.groupBy('Year', 'BranchName').agg(sum('Units_Sold').alias('Total_Units_Sold')).orderBy('Year', 'Total_Units_Sold', ascending=[True, False]).display()

# COMMAND ----------

df.write.format('delta')\
  .mode('overwrite')\
  .option('overwriteSchema', "true")\
  .option('path','abfss://silver@dbricksstrgacc.dfs.core.windows.net/carsales')\
  .save()


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM DELTA.`abfss://silver@dbricksstrgacc.dfs.core.windows.net/carsales`

# COMMAND ----------
