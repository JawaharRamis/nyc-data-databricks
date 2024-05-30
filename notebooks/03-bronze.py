# Databricks notebook source
# MAGIC %run ./01-config

# COMMAND ----------

class Bronze():
    def __init__(self, env):        
        self.Conf = Config()
        self.landing_zone = self.Conf.unmanaged_loc + "/data-zone" 
        self.checkpoint_base = self.Conf.unmanaged_loc + "/checkpoints"
        self.managed_loc = self.Conf.managed_loc + "/data"
        self.catalog = self.Conf.catalog + '_' + env
        self.db_name = 'bronze'
        spark.sql(f"USE {self.catalog}.{self.db_name}")

        
    def consume_crashes(self, once=True, processing_time="5 seconds"):
        from pyspark.sql import functions as F
        from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType, DoubleType, IntegerType
        schema = StructType([
            StructField("crash_date", DateType(), nullable=True),
            StructField("crash_time", TimestampType(), nullable=True),
            StructField("cross_street_name", StringType(), nullable=True),
            StructField("number_of_persons_injured", IntegerType(), nullable=True),
            StructField("number_of_persons_killed", IntegerType(), nullable=True),
            StructField("number_of_pedestrians_injured", IntegerType(), nullable=True),
            StructField("number_of_pedestrians_killed", IntegerType(), nullable=True),
            StructField("number_of_cyclist_injured", IntegerType(), nullable=True),
            StructField("number_of_cyclist_killed", IntegerType(), nullable=True),
            StructField("number_of_motorist_injured", IntegerType(), nullable=True),
            StructField("number_of_motorist_killed", IntegerType(), nullable=True),
            StructField("contributing_factor_vehicle_1", StringType(), nullable=True),
            StructField("collision_id", StringType(), nullable=False),
            StructField("vehicle_type_code_1", StringType(), nullable=True),
            StructField("on_street_name", StringType(), nullable=True),
            StructField("borough", StringType(), nullable=True),
            StructField("zip_code", StringType(), nullable=True),
            StructField("latitude", DoubleType(), nullable=True),
            StructField("longitude", DoubleType(), nullable=True),
            StructField("location", StringType(), nullable=True),
            StructField("contributing_factor_vehicle_2", StringType(), nullable=True),
            StructField("off_street_name", StringType(), nullable=True),
            StructField("vehicle_type_code_2", StringType(), nullable=True),
            StructField("contributing_factor_vehicle_3", StringType(), nullable=True),
            StructField("contributing_factor_vehicle_4", StringType(), nullable=True),
            StructField("vehicle_type_code_3", StringType(), nullable=True),
            StructField("vehicle_type_code_4", StringType(), nullable=True),         
            StructField("contributing_factor_vehicle_5", StringType(), nullable=True),
            StructField("vehicle_type_code_5", StringType(), nullable=True)
        ])
        
        
        df_stream = (spark.readStream
                        .format("cloudFiles")
                        .schema(schema)
                        .option("maxFilesPerTrigger", 1)
                        .option("cloudFiles.format", "csv")
                        .option("header", "true")
                        .option("mergeSchema", True)
                        .load(self.landing_zone + "/crashes")
                        .withColumn("load_time", F.current_timestamp()) 
                        .withColumn("source_file", F.input_file_name())
                    )
                        
        stream_writer = df_stream.writeStream \
                                 .format("delta") \
                                 .option("checkpointLocation", self.checkpoint_base + "/bronze/crashes") \
                                 .outputMode("append") \
                                 .option("mergeSchema", True) \
                                 .queryName("crashes_bz_ingestion_stream")
        
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "bronze_p2")
        if once == True:
            return stream_writer.trigger(availableNow=True).toTable(f"{self.catalog}.{self.db_name}.crashes_bz")
        else:
            return stream_writer.trigger(processingTime=processing_time).toTable(f"{self.catalog}.{self.db_name}.crashes_bz")
        
    def consume_vehicles(self, once=True, processing_time="5 seconds"):
        from pyspark.sql import functions as F
        from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType, DoubleType, IntegerType

        schema = StructType([
            StructField("crash_date", DateType(), nullable=True),
            StructField("crash_time", TimestampType(), nullable=True),
            StructField("collision_id", StringType(), nullable=False),
            StructField("unique_id", StringType(), nullable=True),
            StructField("point_of_impact", StringType(), nullable=True),
            StructField("precrash", StringType(), nullable=True),
            StructField("public_property_damage", StringType(), nullable=True),
            StructField("public_property_damage_type", StringType(), nullable=True),
            StructField("contributing_factor_1", StringType(), nullable=True),
            StructField("contributing_factor_2", StringType(), nullable=True),
            StructField("driver_license_jurisdiction", StringType(), nullable=True),
            StructField("driver_license_status", StringType(), nullable=True),
            StructField("driver_sex", StringType(), nullable=True),
            StructField("state_registration", StringType(), nullable=True),
            StructField("travel_direction", StringType(), nullable=True),
            StructField("vehicle_damage", StringType(), nullable=True),
            StructField("vehicle_damage_1", StringType(), nullable=True),
            StructField("vehicle_damage_2", StringType(), nullable=True),
            StructField("vehicle_damage_3", StringType(), nullable=True),
            StructField("vehicle_id", StringType(), nullable=True),
            StructField("vehicle_make", StringType(), nullable=True),
            StructField("vehicle_model", StringType(), nullable=True),
            StructField("vehicle_type", StringType(), nullable=True),
            StructField("vehicle_year", IntegerType(), nullable=True),
            StructField("vehicle_occupants", IntegerType(), nullable=True)
        ])

        df_stream = (spark.readStream
                        .format("cloudFiles")
                        .schema(schema)
                        .option("maxFilesPerTrigger", 1)
                        .option("cloudFiles.format", "csv")
                        .option("header", "true")
                        # .option("mergeSchema", True)
                        .load(self.landing_zone + "/vehicle")
                        .withColumn("load_time", F.current_timestamp()) 
                        .withColumn("source_file", F.input_file_name())
                    )
                        
        stream_writer = df_stream.writeStream \
                                 .format("delta") \
                                 .option("checkpointLocation", self.checkpoint_base + "/bronze/vehicles") \
                                 .outputMode("append") \
                                 .option("mergeSchema", True) \
                                 .queryName("vehicles_bz_ingestion_stream")
        
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "bronze_p2")
        if once == True:
            return stream_writer.trigger(availableNow=True).toTable(f"{self.catalog}.{self.db_name}.vehicles_bz")
        else:
            return stream_writer.trigger(processingTime=processing_time).toTable(f"{self.catalog}.{self.db_name}.vehicles_bz")
        
    def consume_persons(self, once=True, processing_time="5 seconds"):
        from pyspark.sql import functions as F
        from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType, DoubleType, IntegerType

        schema = StructType([
            StructField("unique_id", StringType(), nullable=True),
            StructField("collision_id", StringType(), nullable=False),
            StructField("accident_date", DateType(), nullable=True),
            StructField("accident_time", TimestampType(), nullable=True),
            StructField("victim_id", StringType(), nullable=True),
            StructField("victim_type", StringType(), nullable=True),
            StructField("victim_injury", StringType(), nullable=True),
            StructField("vehicle_id", StringType(), nullable=True),
            StructField("victim_age", IntegerType(), nullable=True),
            StructField("ejection", StringType(), nullable=True),
            StructField("emotional_status", StringType(), nullable=True),
            StructField("bodily_injury", StringType(), nullable=True),
            StructField("position_in_vehicle", StringType(), nullable=True),
            StructField("safety_equipment", StringType(), nullable=True),
            StructField("ped_location", StringType(), nullable=True), 
            StructField("ped_action", StringType(), nullable=True),
            StructField("complaint", StringType(), nullable=True),
            StructField("victim_role", StringType(), nullable=True),
            StructField("contributing_factor_1", StringType(), nullable=True),
            StructField("contributing_factor_2", StringType(), nullable=True),
            StructField("victim_sex", StringType(), nullable=True)
        ])

        df_stream = (spark.readStream
                        .format("cloudFiles")
                        .schema(schema)
                        .option("maxFilesPerTrigger", 1)
                        .option("cloudFiles.format", "csv")
                        .option("header", "true")
                        # .option("mergeSchema", True)
                        .load(self.landing_zone + "/peoples")
                        .withColumn("load_time", F.current_timestamp()) 
                        .withColumn("source_file", F.input_file_name())
                    )
                        
        stream_writer = df_stream.writeStream \
                                 .format("delta") \
                                 .option("checkpointLocation", self.checkpoint_base + "/bronze/persons") \
                                 .outputMode("append") \
                                 .option("mergeSchema", True) \
                                 .queryName("persons_bz_ingestion_stream")
        
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "bronze_p2")
        if once == True:
            return stream_writer.trigger(availableNow=True).toTable(f"{self.catalog}.{self.db_name}.persons_bz")
        else:
            return stream_writer.trigger(processingTime=processing_time).toTable(f"{self.catalog}.{self.db_name}.persons_bz")

             
    def consume(self, once=True, processing_time="5 seconds"):
        import time
        start = int(time.time())
        print(f"\nStarting bronze layer consumption ...")
        self.consume_crashes(once, processing_time)
        self.consume_vehicles(once, processing_time)
        self.consume_persons(once, processing_time)
        print("completed crashes consumption ...")
        if once:
            for stream in spark.streams.active:
                stream.awaitTermination()
        print(f"Completed bronze layer consumtion {int(time.time()) - start} seconds")
        
        
    # def assert_count(self, table_name, expected_count, filter="true"):
    #     print(f"Validating record counts in {table_name}...", end='')
    #     actual_count = spark.read.table(f"{self.catalog}.{self.db_name}.{table_name}").where(filter).count()
    #     assert actual_count == expected_count, f"Expected {expected_count:,} records, found {actual_count:,} in {table_name} where {filter}" 
    #     print(f"Found {actual_count:,} / Expected {expected_count:,} records where {filter}: Success")        
        
    # def validate(self, sets):
    #     import time
    #     start = int(time.time())
    #     print(f"\nValidating bronz layer records...")
    #     self.assert_count("registered_users_bz", 5 if sets == 1 else 10)
    #     self.assert_count("gym_logins_bz", 8 if sets == 1 else 16)
    #     self.assert_count("kafka_multiplex_bz", 7 if sets == 1 else 13, "topic='user_info'")
    #     self.assert_count("kafka_multiplex_bz", 16 if sets == 1 else 32, "topic='workout'")
    #     self.assert_count("kafka_multiplex_bz", sets * 253801, "topic='bpm'")
    #     print(f"Bronze layer validation completed in {int(time.time()) - start} seconds")                

# COMMAND ----------


