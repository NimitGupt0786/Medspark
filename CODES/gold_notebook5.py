# Databricks notebook source
df_silver=spark.sql("""select * from DELTA.`abfss://silver@dbricksstrgacc.dfs.core.windows.net/carsales`""")
df_silver.display()

# COMMAND ----------

df_dealer = spark.sql("SELECT * FROM cars_catalog.gold.dim_dealer")

df_branch = spark.sql("SELECT * FROM cars_catalog.gold.dim_branch")

df_model = spark.sql("SELECT * FROM cars_catalog.gold.dim_model")

df_date = spark.sql("SELECT * FROM cars_catalog.gold.dim_date")

print(df_silver.count())

# COMMAND ----------


df_fact = df_silver.join(df_branch, df_silver.Branch_ID==df_branch.Branch_ID, how='left') \
    .join(df_dealer, df_silver.Dealer_ID==df_dealer.Dealer_ID, how='left') \
    .join(df_model, df_silver.Model_ID==df_model.Model_ID, how='left') \
    .join(df_date, df_silver.Date_ID==df_date.Date_ID, how='left')\
    .select(df_silver.Revenue, df_silver.Units_Sold, df_branch.dim_branch_key, df_dealer.dim_dealer_key, df_model.dim_model_key, df_date.dim_date_key)

df_fact.display()



# COMMAND ----------

from delta.tables import DeltaTable

if spark.catalog.tableExists("cars_catalog.gold.factsales"):
    delta_table=DeltaTable.forPath(spark,"abfss://gold@dbricksstrgacc.dfs.core.windows.net/factsales")

    delta_table.alias("target").merge(df_fact.alias("source"), "target.dim_branch_key = source.dim_branch_key and target.dim_dealer_key = source.dim_dealer_key and target.dim_model_key = source.dim_model_key and target.dim_date_key = source.dim_date_key")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
else:
    df_fact.write.format("delta")\
                .mode("overwrite")\
                .option('path','abfss://gold@dbricksstrgacc.dfs.core.windows.net/factsales')\
                .saveAsTable("cars_catalog.gold.factsales")
                

# COMMAND ----------
