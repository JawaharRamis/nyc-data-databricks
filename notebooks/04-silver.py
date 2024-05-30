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

class CDCUpserter:
    def __init__(self, merge_query, temp_view_name, id_column, sort_by):
        self.merge_query = merge_query
        self.temp_view_name = temp_view_name 
        self.id_column = id_column
        self.sort_by = sort_by 
        
    def upsert(self, df_micro_batch, batch_id):
        from pyspark.sql.window import Window
        from pyspark.sql import functions as F
        
        window = Window.partitionBy(self.id_column).orderBy(F.col(self.sort_by).desc())
        
        df_micro_batch.filter(F.col("update_type").isin(["new", "update"])) \
                .withColumn("rank", F.rank().over(window)).filter("rank == 1").drop("rank") \
                .createOrReplaceTempView(self.temp_view_name)
        df_micro_batch._jdf.sparkSession().sql(self.merge_query)

# COMMAND ----------

class Silver():
    def __init__(self, env):
        self.Conf = Config() 
        self.landing_zone = self.Conf.unmanaged_loc + "/data-zone" 
        self.checkpoint_base = self.Conf.unmanaged_loc + "/checkpoints"
        self.managed_loc = self.Conf.managed_loc + "/data"
        self.catalog = self.Conf.catalog + '_' + env
        self.db_name = "silver"
        self.maxFilesPerTrigger = self.Conf.maxFilesPerTrigger
        spark.sql(f"USE {self.catalog}.{self.db_name}")
        
    def upsert_crashes(self, once=True, processing_time="15 seconds", startingVersion=0):
        from pyspark.sql.functions import concat_ws, col
        from pyspark.sql import functions as F
        
        query = f"""
            MERGE INTO {self.catalog}.{self.db_name}.crashes_sv a
            USING crashes_delta b
            ON a.collision_id=b.collision_id
            WHEN NOT MATCHED THEN INSERT *
            """
        
        data_upserter=Upserter(query, "crashes_delta")

        contributing_factors_columns = [
            "contributing_factor_vehicle_1", "contributing_factor_vehicle_2",
            "contributing_factor_vehicle_3", "contributing_factor_vehicle_4",
            "contributing_factor_vehicle_5"
        ]

        vehicle_types_columns = [
            "vehicle_type_code_1", "vehicle_type_code_2",
            "vehicle_type_code_3", "vehicle_type_code_4",
            "vehicle_type_code_5"
        ]
        
        df_delta = (spark.readStream
                        .option("startingVersion", startingVersion)
                        .option("ignoreDeletes", True)
                        .table(f"{self.catalog}.bronze.crashes_bz")
                        .withColumn(
                            "contributing_factors",
                            concat_ws(",", *[col(c) for c in contributing_factors_columns])
                        )
                        .withColumn(
                            "vehicle_types",
                            concat_ws(",", *[col(c) for c in vehicle_types_columns])
                        )
                        .withColumn(
                            "total_injured",
                            F.col("number_of_persons_injured") + F.col("number_of_pedestrians_injured") + 
                            F.col("number_of_cyclist_injured") + F.col("number_of_motorist_injured")
                        ) \
                        .withColumn(
                            "total_killed",
                            F.col("number_of_persons_killed") + F.col("number_of_pedestrians_killed") + 
                            F.col("number_of_cyclist_killed") + F.col("number_of_motorist_killed")
                        )
                        .selectExpr("crash_date", "crash_time", "cross_street_name","collision_id","borough",
                                     "zip_code", "latitude", "longitude","load_time", "source_file", "contributing_factors",
                                     "vehicle_types", "total_killed", "total_injured"
                                     )
                        .dropDuplicates(["collision_id"])
                    )
                    
        
        stream_writer = (df_delta.writeStream
                                 .foreachBatch(data_upserter.upsert)
                                 .outputMode("update")
                                 .option("checkpointLocation", f"{self.checkpoint_base}/{self.db_name}/crashes")
                                 .queryName("crashes_upsert_stream")
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
        print(f"\nExecuting silver layer upsert ...")
        self.upsert_crashes(once, processing_time)
        self._await_queries(once)
        print(f"Completed silver layer upsert {int(time.time()) - start} seconds")
        
        
    def assert_count(self, table_name, expected_count, filter="true"):
        print(f"Validating record counts in {table_name}...", end='')
        actual_count = spark.read.table(f"{self.catalog}.{self.db_name}.{table_name}").where(filter).count()
        assert actual_count == expected_count, f"Expected {expected_count:,} records, found {actual_count:,} in {table_name} where {filter}" 
        print(f"Found {actual_count:,} / Expected {expected_count:,} records where {filter}: Success")        
        
    def validate(self, sets):
        import time
        start = int(time.time())
        print(f"\nValidating silver layer records...")
        self.assert_count("users", 5 if sets == 1 else 10)
        self.assert_count("gym_logs", 8 if sets == 1 else 16)
        self.assert_count("user_profile", 5 if sets == 1 else 10)
        self.assert_count("workouts", 16 if sets == 1 else 32)
        self.assert_count("heart_rate", sets * 253801)
        self.assert_count("user_bins", 5 if sets == 1 else 10)
        self.assert_count("completed_workouts", 8 if sets == 1 else 16)
        self.assert_count("workout_bpm", 3968 if sets == 1 else 8192)
        print(f"Silver layer validation completed in {int(time.time()) - start} seconds")     