
# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

dbutils.widgets.text('incremental_flag', 'o')

# COMMAND ----------

incremental_flag = dbutils.widgets.get('incremental_flag')
print(incremental_flag)

# COMMAND ----------

df_src=spark.sql(""" SELECT * FROM DELTA.`abfss://silver@dbricksstrgacc.dfs.core.windows.net/carsales` """)

# COMMAND ----------

df_src=spark.sql(""" SELECT DISTINCT Branch_ID, BranchName FROM DELTA.`abfss://silver@dbricksstrgacc.dfs.core.windows.net/carsales` """)

df_src.display()

# COMMAND ----------

if not spark.catalog.tableExists('cars_catalog.gold.dim_branch'):
    df_sink=spark.sql("""
        select 1 as dim_branch_key, Branch_ID, BranchName
        from DELTA.`abfss://silver@dbricksstrgacc.dfs.core.windows.net/carsales`
        where 1=0
                      """)
else:
    df_sink=spark.sql(""" 
        select dim_branch_key, Branch_ID, BranchName
        from cars_catalog.gold.dim_branch
                      """)
    
df_sink.display()

# COMMAND ----------

df_filter=df_src.join(df_sink, df_src.Branch_ID==df_sink.Branch_ID,'left').select(df_src.Branch_ID, df_src.BranchName, df_sink.dim_branch_key)
df_filter.display()

# COMMAND ----------

df_filter_old = df_filter.filter(df_filter.dim_branch_key.isNotNull())
df_filter_old.display()

# COMMAND ----------

df_filter_new = df_filter.filter(df_filter.dim_branch_key.isNull()).select('Branch_ID', 'BranchName')
df_filter_new.display()

# COMMAND ----------

if (incremental_flag=='0'):
    max_value=0
else:
    if spark.catalog.tableExists('cars_catalog.gold.dim_branch'):
        max_value_df= spark.sql("select max(dim_branch_key) from cars_catalog.gold.dim_branch")
        max_value=max_value_df.first()[0]
    else:
        max_value=0
max_value+=1

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

df_filter_new=df_filter_new.withColumn("dim_branch_key", max_value+monotonically_increasing_id())
df_filter_new.display()

# COMMAND ----------

if not spark.catalog.tableExists('cars_catalog.gold.dim_branch'):
    df_final=df_filter_new
else:
    df_final=df_filter_new.union(df_filter_old)

df_final.display()

# COMMAND ----------

from delta.tables import DeltaTable

if spark.catalog.tableExists('cars_catalog.gold.dim_branch'):
    delta_table= DeltaTable.forPath(spark,"abfss://gold@dbricksstrgacc.dfs.core.windows.net/dim_branch")
    delta_table.alias('target').merge(df_final.alias('source'), "target.dim_branch_key=source.dim_branch_key").whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()
else:
    df_final.write.format("delta").mode("overwrite")\
            .option("path","abfss://gold@dbricksstrgacc.dfs.core.windows.net/dim_branch")\
            .saveAsTable("cars_catalog.gold.dim_branch")

# COMMAND ----------

# -- # dbutils.fs.rm("abfss://gold@dbricksstrgacc.dfs.core.windows.net/dim_model", recurse=True)

# DROP TABLE IF EXISTS cars_catalog.gold.dim_model

# COMMAND ----------
