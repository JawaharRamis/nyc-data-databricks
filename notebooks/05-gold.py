# Databricks notebook source
class Upserter:
    def __init__(self, merge_query, temp_view_name):
        self.merge_query = merge_query
        self.temp_view_name = temp_view_name 
        
    def upsert(self, df_micro_batch, batch_id):
        df_micro_batch.createOrReplaceTempView(self.temp_view_name)
        df_micro_batch._jdf.sparkSession().sql(self.merge_query)""

# COMMAND ----------

class Gold():
    def __init__(self, env):
        self.Conf = Config() 
        self.landing_zone = self.Conf.unmanaged_loc + "/data-zone" 
        self.checkpoint_base = self.Conf.unmanaged_loc + "/checkpoints"
        self.managed_loc = self.Conf.managed_loc + "/data"
        self.catalog = self.Conf.catalog + '_' + env
        self.db_name = "gold"
        self.maxFilesPerTrigger = self.Conf.maxFilesPerTrigger
        spark.sql(f"USE {self.catalog}.{self.db_name}")

     def upsert_crashes_hourly_summary(self, once=True, processing_time="15 seconds", startingVersion=0):
        from pyspark.sql.functions import udf,concat_ws, col, date_format
        from pyspark.sql.window import Window
        from pyspark.sql import functions as F
        from pyspark.sql.types import StringType

         query = f"""
            MERGE INTO {self.catalog}.{self.db_name}.crashes_sv a
            USING crashes_delta b
            ON a.collision_id=b.collision_id
            WHEN NOT MATCHED THEN INSERT *
            """
        
        data_upserter=Upserter(query, "crashes_delta")

        window = Window.partitionBy(F.col("crash_date"), F.col("crash_hour")).orderBy(F.col("borough_count").desc())
        # window = Window.partitionBy("crash_date", "crash_hour", "borough")

        df_delta = (spark.readStream
                        .option("startingVersion", startingVersion)
                        .option("ignoreDeletes", True)
                        .table(f"{self.catalog}.silver.crashes_sv")
                        .withColumn(
                            "crash_hour",
                            date_format(F.hour("crash_time"), "HH:00:00")
                        )
                        .withColumn(
                            "borough_count",
                            F.count("borough").over(window)  # Count occurrences per borough within the window
                        )
                        # .withColumn(
                        #     "borough_count",
                        #     F.count("borough").over(window)
                        # )
                        .groupBy(
                            "crash_date", "crash_hour" 
                        )
                        .agg(
                            F.count("*").alias("number_of_incidents"),
                            sum("total_injured").alias("total_injuries"),
                            sum("total_killed").alias("total_deaths")
                            F.max("borough_count").alias("most_frequent_borough_count"), 
                            F.first("borough").alias("most_frequent_borough")
                        )
                        )
                        .select(
                            "crash_date", "crash_hour", "number_of_incidents", "total_injuries", "total_deaths"
                        )
                        .dropDuplicates(["unique_id"])
                )

    def upsert_crashes_hourly_summary(self, once=True, processing_time="15 seconds", startingVersion=0):
        from pyspark.sql.functions import udf,concat_ws, col, date_format
        from pyspark.sql.window import Window
        from pyspark.sql import functions as F
        from pyspark.sql.types import StringType

         query = f"""
            MERGE INTO {self.catalog}.{self.db_name}.crashes_sv a
            USING crashes_delta b
            ON a.collision_id=b.collision_id
            WHEN NOT MATCHED THEN INSERT *
            """
        
        data_upserter=Upserter(query, "crashes_delta")

        df_delta = spark.readStream \
                        .option("startingVersion", 0) \
                        .option("ignoreDeletes", True) \
                        .table(f"{self.catalog}.silver.vehicles_sv")

        df_delta = df_delta.groupBy("collision_id") \
                        .agg(
                            F.collect_list(F.struct("*")).alias("vehicles_involved") 
                        )

        attributes = [
            "unique_id",
            "vehicle_id"
            "point_of_impact_v1",
            "pre_crash_v1",
            "driver_license_jurisdiction",
            "driver_license_status",
            "sex",
            "state_registration",
            "travel_direction"
            "vehicle_damages",
            "vehicle_type",
            "vehicle_year",
            "vehicle_occupants"
        ]

        select_exprs = ["collision_id"]
            for i, attr in enumerate(attributes, start=1):
                filtered_vehicles = F.filter(F.col("vehicles_involved"), lambda v: v.get("unique_id") is not None)
                select_exprs.extend([
                    filtered_vehicles.getItem(0).getItem(attr).alias(f"{attr}_v_1"),
                    filtered_vehicles.getItem(1).getItem(attr).alias(f"{attr}_v_2"),
                ])
                select_exprs.extend([
                    col("vehicles_involved")[0].getItem(attr).alias(f"{attr}_v_1"),
                    col("vehicles_involved")[1].getItem(attr).alias(f"{attr}_v_2"),
                ])

        df_delta = df_delta.select(*select_exprs)

# COMMAND ----------

from pyspark.sql import SparkSession, functions as F
from pyspark.sql import functions as F

# Assuming you already have a SparkSession created (replace with your initialization)
spark = SparkSession.builder.appName("Vehicle consolidation").getOrCreate()

# Read stream from vehicles table
df_delta = spark.readStream \
  .option("startingVersion", 0) \
  .option("ignoreDeletes", True) \
  .table(f"nyc_collision_dev.silver.vehicles_sv")

# Group by collision_id and collect all other columns into a struct
df_delta = df_delta.groupBy("collision_id") \
  .agg(
      F.collect_list(F.struct("*")).alias("vehicles_involved") 
  )

df_delta = df_delta.select(
    "collision_id",
    F.col("vehicles_involved")[0].getItem("travel_direction").alias("travel_direction_vehicle_1"),
    F.col("vehicles_involved")[1].getItem("travel_direction").alias("travel_direction_vehicle_2"),
    F.col("vehicles_involved")[0].getItem("vehicle_damages").alias("vehicle_1_damages"),
    F.col("vehicles_involved")[1].getItem("vehicle_damages").alias("vehicle_2_damages")
)



df_delta.printSchema()
# # Write the result to a memory checkpoint or desired sink (replace with your configuration)
# query = df_delta.writeStream \
#   .option("checkpointLocation", "/tmp/checkpoint")  # Replace with your checkpoint location
#   .format("memory")  # Replace with your desired sink format (e.g., parquet, jdbc)
#   .option("table", "gold_table_name")  # Replace with your gold table name
#   .start()

# # Trigger the query execution
# query.awaitTermination()


# COMMAND ----------

from pyspark.sql import SparkSession, functions as F
from pyspark.sql import functions as F

# Assuming you already have a SparkSession created (replace with your initialization)
spark = SparkSession.builder.appName("Vehicle consolidation").getOrCreate()

# Read stream from vehicles table
df_delta = spark.readStream \
  .option("startingVersion", 0) \
  .option("ignoreDeletes", True) \
  .table(f"nyc_collision_dev.silver.vehicles_sv")

# Group by collision_id and collect all other columns into a struct
df_delta = df_delta.groupBy("collision_id") \
  .agg(
      F.collect_list(F.struct("*")).alias("vehicles_involved") 
  )
# List of attributes you want to extract for each vehicle
attributes = [
    "unique_id",
    "vehicle_id"
    "point_of_impact_v1",
    "pre_crash_v1",
    "driver_license_jurisdiction",
    "driver_license_status",
    "sex",
    "state_registration",
    "travel_direction"
    "vehicle_damages",
    "vehicle_type",
    "vehicle_year",
    "vehicle_occupants"
]

select_exprs = ["collision_id"]
for i, attr in enumerate(attributes, start=1):
    select_exprs.extend([
        col("vehicles_involved")[0].getItem(attr).alias(f"{attr}_v_1"),
        col("vehicles_involved")[1].getItem(attr).alias(f"{attr}_v_2"),
    ])

df_delta = df_delta.select(*select_exprs)

df_delta.printSchema()

# COMMAND ----------


