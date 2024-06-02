# Databricks notebook source
dbutils.widgets.text("Environment", "dev", "Set the current environment/catalog name")
dbutils.widgets.text("RunType", "once", "Set once to run as a batch")
dbutils.widgets.text("ProcessingTime", "5 seconds", "Set the microbatch interval")

# COMMAND ----------

env = dbutils.widgets.get("Environment")
once = True if dbutils.widgets.get("RunType")=="once" else False
processing_time = dbutils.widgets.get("ProcessingTime")
if once:
    print(f"Starting processing in batch mode.")
else:
    print(f"Starting processing in stream mode with {processing_time} microbatch.")

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", True)
spark.conf.set("spark.databricks.delta.autoCompact.enabled", True)
spark.conf.set("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")

# COMMAND ----------

# MAGIC %run ./02-setup

# COMMAND ----------

SH = SetupHelper(env)

# COMMAND ----------

SH.cleanup()

# COMMAND ----------

setup_required = spark.sql(f"SHOW DATABASES IN {SH.catalog}").filter(f"databaseName == '{SH.db_name}'").count() != 1
print(setup_required)
if setup_required:
    SH.setup()
    SH.validate()
else:
    spark.sql(f"USE {SH.catalog}.{SH.db_name}")

# COMMAND ----------

# MAGIC
# MAGIC %run ./03-bronze

# COMMAND ----------

BZ = Bronze(env)

# COMMAND ----------

BZ.consume(once, processing_time)

# COMMAND ----------

# MAGIC %run ./04-silver

# COMMAND ----------

SL = Silver(env)

# COMMAND ----------

SL.upsert(once, processing_time)

# COMMAND ----------

# MAGIC  %run ./05-gold

# COMMAND ----------

GL = Gold(env)

# COMMAND ----------

GL.upsert(once, processing_time)

# COMMAND ----------


