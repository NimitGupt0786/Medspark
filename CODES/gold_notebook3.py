# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

dbutils.widgets.text('incremental_flag', '0')

# COMMAND ----------

incremental_flag = dbutils.widgets.get('incremental_flag')
print(incremental_flag)

# COMMAND ----------

df_src=spark.sql(""" SELECT DISTINCT Dealer_ID FROM DELTA.`abfss://silver@dbricksstrgacc.dfs.core.windows.net/carsales` """)

df_src.display()

# COMMAND ----------

if not spark.catalog.tableExists("cars_catalog.gold.dim_dealer"):
    df_sink= spark.sql(""" select 1 as dim_dealer_key, Dealer_ID from DELTA.`abfss://silver@dbricksstrgacc.dfs.core.windows.net/carsales` where 1=0""")
else:
    df_sink=spark.sql(""" select dim_dealer_key, Dealer_ID from cars_catalog.gold.dim_dealer """)

df_sink.display()

# COMMAND ----------

df_filter = df_src.join(df_sink, df_src.Dealer_ID  == df_sink.Dealer_ID , 'left').select(df_src.Dealer_ID , df_sink.dim_dealer_key)

df_filter.display()


# COMMAND ----------

df_filter_old = df_filter.filter(df_filter.dim_dealer_key.isNotNull())
df_filter_old.display()

# COMMAND ----------

df_filter_new = df_filter.filter(df_filter.dim_dealer_key.isNull())

df_filter_new.display()

# COMMAND ----------

if incremental_flag == '0': 
    max_value = 0
else:
    if spark.catalog.tableExists('cars_catalog.gold.dim_dealer'):
        max_value_df = spark.sql("SELECT max(dim_dealer_key) FROM cars_catalog.gold.dim_dealer")
        max_value = max_value_df.first()[0]
    else:
        max_value=0
max_value+=1

# COMMAND ----------

df_filter_new=df_filter_new.withColumn('dim_dealer_key',max_value+monotonically_increasing_id())
df_filter_new.display()

# COMMAND ----------

df_final = df_filter_new.union(df_filter_old)
df_final.display()

# COMMAND ----------

from delta.tables import DeltaTable

if spark.catalog.tableExists('cars_catalog.gold.dim_dealer'):
    delta_table = DeltaTable.forPath(spark, "abfss://gold@dbricksstrgacc.dfs.core.windows.net/dim_dealer")
    delta_table.alias("target").merge(df_final.alias("source"), "target.dim_dealer_key = source.dim_dealer_key")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()

else:
    df_final.write.format("delta")\
        .mode("overwrite")\
        .option("path", "abfss://gold@dbricksstrgacc.dfs.core.windows.net/dim_dealer")\
        .saveAsTable("cars_catalog.gold.dim_dealer")

# COMMAND ----------
