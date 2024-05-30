# Databricks notebook source
# MAGIC %run ./01-config

# COMMAND ----------

class SetupHelper():   
    def __init__(self, env):
        Conf = Config()
        # self.landing_zone = Conf.base_dir_data + "/raw"
        # self.checkpoint_base = Conf.base_dir_checkpoint + "/checkpoints"     
        self.landing_zone = Conf.unmanaged_loc + "/data-zone"
        self.checkpoint_base = Conf.unmanaged_loc + "/checkpoints"
        self.managed_loc = Conf.managed_loc + "/data"
        self.catalog = Conf.catalog + '_' + env
        self.db_name = Conf.db_name
        self.initialized = False
        
    def create_db(self):
        spark.catalog.clearCache()
        print(f"Creating the database....", end='')
        spark.sql(f"""CREATE SCHEMA IF NOT EXISTS {self.catalog}.bronze
                  MANAGED LOCATION 's3://nyc-opendata-managed/root/'
                  """)
        spark.sql(f"""CREATE SCHEMA IF NOT EXISTS {self.catalog}.silver
                  MANAGED LOCATION 's3://nyc-opendata-managed/root/'
                  """)
        spark.sql(f"""CREATE SCHEMA IF NOT EXISTS {self.catalog}.gold
                  MANAGED LOCATION 's3://nyc-opendata-managed/root/'
                  """)
        spark.sql(f"USE {self.catalog}.bronze")
        self.initialized = True
        print("Created database successfully!!")

    def create_crashes_bz(self):
        if(self.initialized):
            print(f"Creating crashes_bz table...", end='')
            spark.sql(f"""CREATE OR REPLACE TABLE {self.catalog}.bronze.crashes_bz (
                crash_date date,
                crash_time timestamp,
                cross_street_name string,
                number_of_persons_injured integer,
                number_of_persons_killed integer,
                number_of_pedestrians_injured integer,
                number_of_pedestrians_killed integer,
                number_of_cyclist_injured integer,
                number_of_cyclist_killed integer,
                number_of_motorist_injured integer,
                number_of_motorist_killed integer,
                contributing_factor_vehicle_1 string,
                collision_id string,
                vehicle_type_code_1 string,
                on_street_name string,
                borough string,
                zip_code string,
                latitude double,
                longitude double,
                location string,
                contributing_factor_vehicle_2 string,
                off_street_name string,
                vehicle_type_code_2 string,
                contributing_factor_vehicle_3 string,
                contributing_factor_vehicle_4 string,
                vehicle_type_code_3 string,
                vehicle_type_code_4 string,
                contributing_factor_vehicle_5 string,
                vehicle_type_code_5 string,
                load_time timestamp,
                source_file string
                )
                USING DELTA
                LOCATION '{self.managed_loc}/silver/crashes_bz'
                """) 
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")  
    
    def create_vehicles_bz(self):
        if(self.initialized):
            print(f"Creating vehicles_bz table...", end='')
            spark.sql(f"""CREATE OR REPLACE TABLE {self.catalog}.bronze.vehicles_bz(
                    crash_date date,
                    crash_time timestamp,
                    collision_id string,
                    unique_id string,
                    point_of_impact string,
                    precrash string,
                    public_property_damage string,
                    public_property_damage_type string,
                    contributing_factor_1 string,
                    contributing_factor_2 string,
                    driver_license_jurisdiction string,
                    driver_license_status string,
                    driver_sex string,
                    state_registration string,
                    travel_direction string,
                    vehicle_damage string,
                    vehicle_damage_1 string,
                    vehicle_damage_2 string,
                    vehicle_damage_3 string,
                    vehicle_id string,
                    vehicle_make string,
                    vehicle_model string,
                    vehicle_type string,
                    vehicle_year integer,
                    vehicle_occupants integer,
                    load_time timestamp,
                    source_file string
                    )
                USING DELTA
                LOCATION '{self.managed_loc}/bronze/vehicles_bz'
                """)
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
            
    def create_persons_bz(self):
        if(self.initialized):
            print(f"Creating persons_bz table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.bronze.persons_bz(
                    unique_id string,
                    collision_id string,
                    accident_date date,
                    accident_time timestamp,
                    victim_id string,
                    victim_type string,
                    victim_injury string,
                    vehicle_id string,
                    victim_age integer,
                    ejection string,
                    emotional_status string,
                    bodily_injury string,
                    position_in_vehicle string,
                    safety_equipment string,
                    ped_location string,
                    ped_action string,
                    complaint string,
                    victim_role string,
                    contributing_factor_1 string,
                    contributing_factor_2 string,
                    victim_sex string,
                    load_time timestamp,
                    source_file string
                )
                USING DELTA
                LOCATION '{self.managed_loc}/bronze/persons_bz'
                """)
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
    def create_crashes_sv(self):
        if(self.initialized):
            print(f"Creating crashes_sv table...", end='')
            spark.sql(f"""CREATE OR REPLACE TABLE {self.catalog}.silver.crashes_sv (
                crash_date date,
                crash_time timestamp,
                cross_street_name string,
                total_injured integer,
                total_killed integer,
                contributing_factors string,
                collision_id string,
                vehicle_types string,
                borough string,
                zip_code string,
                latitude double,
                longitude double,
                load_time timestamp,
                source_file string
                )
                USING DELTA
                LOCATION '{self.managed_loc}/silver/crashes_sv'
                """) 
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
    
    def create_vehicles_sv(self):
        if(self.initialized):
            print(f"Creating vehicles_sv table...", end='')
            spark.sql(f"""CREATE OR REPLACE TABLE {self.catalog}.silver.vehicles_sv (
                crash_date date,
                crash_time timestamp,
                collision_id string,
                unique_id string,
                point_of_impact string,
                precrash string,
                public_property_damage string,
                public_property_damage_type string,
                contributing_factor string,
                # contributing_factor_2 string,
                driver_license_jurisdiction string,
                driver_license_status string,
                driver_sex string,
                state_registration string,
                travel_direction string,
                vehicle_damage string,
                # # vehicle_damage_1 string,
                # vehicle_damage_2 string,
                # vehicle_damage_3 string,
                vehicle_id string,
                vehicle_make string,
                vehicle_model string,
                vehicle_type string,
                vehicle_year integer,
                vehicle_occupants integer,
                load_time timestamp,
                source_file string
                )
                USING DELTA
                LOCATION '{self.managed_loc}/silver/vehicles_sv'
                """) 
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")

    def setup(self):
        import time
        start = int(time.time())
        print(f"\nStarting setup ...")
        self.create_db()       
        self.create_crashes_bz()
        self.create_vehicles_bz()
        self.create_persons_bz()
        self.create_crashes_sv()
        print(f"Setup completed in {int(time.time()) - start} seconds")
        
    def assert_table(self, table_name, schema):
        assert spark.sql(f"SHOW TABLES IN {self.catalog}.{schema}") \
                   .filter(f"isTemporary == false and tableName == '{table_name}'") \
                   .count() == 1, f"The table {table_name} is missing"
        print(f"Found {table_name} table in {self.catalog}.{self.db_name}: Success")
        
    def validate(self):
        import time
        start = int(time.time())
        print(f"\nStarting setup validation ...")
        schemas = ["bronze", "silver", "gold"]
        for schema in schemas:
            assert spark.sql(f"SHOW DATABASES IN {self.catalog}") \
                    .filter(f"databaseName == '{schema}'") \
                    .count() == 1, f"The database '{self.catalog}.{schema}' is missing"
            print(f"Found database {self.catalog}.{schema}: Success")
        self.assert_table("crashes_bz", "bronze")   
        self.assert_table("vehicles_bz", "bronze")        
        self.assert_table("persons_bz", "bronze")
        self.assert_table("crashes_sv", "silver")
        print(f"Setup validation completed in {int(time.time()) - start} seconds")
        
    def cleanup(self): 
        if spark.sql(f"SHOW DATABASES IN {self.catalog}").count() > 1:
            print(f"Dropping the database {self.catalog} ...", end='')
            spark.sql(f"DROP DATABASE {self.catalog}.bronze CASCADE")
            spark.sql(f"DROP DATABASE {self.catalog}.silver CASCADE")
            spark.sql(f"DROP DATABASE {self.catalog}.gold CASCADE")
            print("Done")
        # print(f"Deleting {self.landing_zone}...", end='')
        # dbutils.fs.rm(self.landing_zone, True)
        # print("Done")
        print(f"Deleting {self.checkpoint_base}...", end='')
        dbutils.fs.rm(self.checkpoint_base, True)
        print("Done") 
