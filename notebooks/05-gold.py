# Databricks notebook source
# MAGIC %run ./01-config

# COMMAND ----------

class Upserter:
    def __init__(self, merge_query, temp_view_name):
        self.merge_query = merge_query
        self.temp_view_name = temp_view_name 
        
    def upsert(self, df_micro_batch, batch_id):
        df_micro_batch.createOrReplaceTempView(self.temp_view_name)
        df_micro_batch._jdf.sparkSession().sql(self.merge_query)

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
            MERGE INTO {self.catalog}.{self.db_name}.crashes_hourly_summary_gd a
            USING crashes_delta b
            ON a.crash_date=b.crash_date AND a.crash_hour=b.crash_hour
            WHEN NOT MATCHED THEN INSERT *
            """
        
        data_upserter=Upserter(query, "crashes_delta")

        window = Window.partitionBy(F.to_timestamp(F.col("crash_date")), F.col("crash_hour"))


        df_delta = (spark.readStream
                        .option("startingVersion", startingVersion)
                        .option("ignoreDeletes", True)
                        .table(f"{self.catalog}.silver.crashes_sv")
                        .withColumn("crash_hour", F.date_format(F.col("crash_time").cast("timestamp"), "HH:00:00"))
                        .groupBy(
                            "crash_date", "crash_hour" 
                        )
                        .agg(
                            F.count("*").alias("number_of_incidents"),
                            F.sum("total_injured").alias("total_injuries"),
                            F.sum("total_killed").alias("total_deaths"),
                        )
                        .select(
                            "crash_date", "crash_hour", "number_of_incidents", "total_injuries", "total_deaths"
                            
                        )
                        .withColumn("last_updated", F.current_timestamp())
                    )
        
        stream_writer = (df_delta.writeStream
                                 .foreachBatch(data_upserter.upsert)
                                 .outputMode("update")
                                 .option("checkpointLocation", f"{self.checkpoint_base}/{self.db_name}/crashes_hourly_summary")
                                 .queryName("crashes_hourly_summary_upsert_stream")
                        )

        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "silver_p2")
        
        if once == True:
            return stream_writer.trigger(availableNow=True).start()
        else:
            return stream_writer.trigger(processingTime=processing_time).start()

    def upsert_vehicle_collisions_details(self, once=True, processing_time="15 seconds", startingVersion=0):
        from pyspark.sql.functions import udf,concat_ws, col, date_format, expr
        from pyspark.sql.window import Window
        from pyspark.sql import functions as F
        from pyspark.sql.types import StringType

        query = f"""
            MERGE INTO {self.catalog}.{self.db_name}.vehicle_collisions_details_gd a
            USING vehicles_delta b 
            ON a.collision_id=b.collision_id
            WHEN NOT MATCHED THEN INSERT *
            """
        
        data_upserter=Upserter(query, "vehicles_delta")

        attributes = [
            "unique_id",
            "vehicle_id",
            "point_of_impact",
            "pre_crash",
            "driver_license_jurisdiction",
            "driver_license_status",
            "sex",
            "state_registration",
            "travel_direction",
            "vehicle_damages",
            "vehicle_type",
            "vehicle_year",
            "vehicle_occupants",
            "crash_date",
            "crash_time"
        ]
        select_exprs = ["collision_id"]

        for attr in attributes:
            if attr in ["crash_date", "crash_time"]:
                select_exprs.append(col("vehicles_involved")[0].getItem(attr).alias(attr))
            else:
                select_exprs.append(col("vehicles_involved")[0].getItem(attr).alias(f"{attr}_v1"))
                select_exprs.append(col("vehicles_involved")[1].getItem(attr).alias(f"{attr}_v2"))

        df_delta = (spark.readStream
                        .option("startingVersion", 0)
                        .option("ignoreDeletes", True)
                        .table(f"{self.catalog}.silver.vehicles_sv")
                        .groupBy("collision_id")
                        .agg(
                            F.collect_list(F.struct("*")).alias("vehicles_involved") 
                        )
                    )

        df_delta = df_delta.select(*select_exprs)

        df_delta.printSchema()

        stream_writer = (df_delta.writeStream
                                 .foreachBatch(data_upserter.upsert)
                                 .outputMode("update")
                                 .option("checkpointLocation", f"{self.checkpoint_base}/{self.db_name}/vehicle_collisions_details")
                                 .queryName("vehicle_collisions_details_upsert_stream")
                        )

        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "silver_p2")
        
        if once == True:
            return stream_writer.trigger(availableNow=True).start()
        else:
            return stream_writer.trigger(processingTime=processing_time).start()

    def _await_queries(self, once):
        if once:
            for stream in spark.streams.active:
                stream.awaitTermination()
                
    def upsert(self, once=True, processing_time="5 seconds"):
        import time
        start = int(time.time())
        print(f"\nExecuting gold layer upsert ...")
        self.upsert_crashes_hourly_summary(once, processing_time)
        self.upsert_vehicle_collisions_details(once, processing_time)
        self._await_queries(once)
        print(f"Completed gold layer upsert {int(time.time()) - start} seconds")
