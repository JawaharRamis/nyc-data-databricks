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
                    pre_crash string,
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
                    crash_date date,
                    crash_time timestamp,
                    person_id string,
                    person_type string,
                    person_injury string,
                    vehicle_id string,
                    person_age integer,
                    ped_role string,
                    ejection string,
                    emotional_status string,
                    bodily_injury string,
                    position_in_vehicle string,
                    safety_equipment string,
                    ped_location string,
                    ped_action string,
                    complaint string,
                    contributing_factor_1 string,
                    contributing_factor_2 string,
                    person_sex string,
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
                crash_time string,
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
                crash_time string,
                collision_id string,
                unique_id string,
                point_of_impact string,
                pre_crash string,
                public_property_damage string,
                contributing_factor string,
                driver_license_jurisdiction string,
                driver_license_status string,
                sex string,
                state_registration string,
                travel_direction string,
                vehicle_damages string,
                vehicle_id string,
                vehicle_make string,
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

    def create_persons_sv(self):
        if(self.initialized):
            print(f"Creating persons_sv table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.silver.persons_sv(
                    unique_id string,
                    collision_id string,
                    crash_date date,
                    crash_time string,
                    person_id string,
                    person_type string,
                    person_injury string,
                    vehicle_id string,
                    age integer,
                    bodily_injury string,
                    ped_role string,
                    sex string,
                    load_time timestamp,
                    source_file string
                )
                USING DELTA
                LOCATION '{self.managed_loc}/silver/persons_sv'
                """)
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")

    def create_crashes_hourly_summary_gd(self):
        if(self.initialized):
            print(f"Creating crashes_hourly_summary_gd table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.gold.crashes_hourly_summary_gd(
                    crash_date date,
                    crash_hour string,
                    number_of_incidents string,
                    total_injuries string,
                    total_deaths string,
                    last_updated timestamp
                )
                USING DELTA
                LOCATION '{self.managed_loc}/gold/crashes_hourly_summary_gd'
                """)
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")

    def create_vehicle_collisions_details_gd(self):
        if(self.initialized):
            print(f"Creating vehicle_collisions_details_gd table...", end='')
            spark.sql(f"""CREATE OR REPLACE TABLE {self.catalog}.gold.vehicle_collisions_details_gd(
                collision_id string,
                unique_id_v1 string,
                unique_id_v2 string,
                crash_date date,
                crash_time string,                
                point_of_impact_v1 string,
                point_of_impact_v2 string,
                pre_crash_v1 string,
                pre_crash_v2 string,
                driver_license_jurisdiction_v1 string,
                driver_license_jurisdiction_v2 string,
                driver_license_status_v1 string,
                driver_license_status_v2 string,
                sex_v1 string,
                sex_v2 string,
                state_registration_v1 string,
                state_registration_v2 string,
                travel_direction_v1 string,
                travel_direction_v2 string,
                vehicle_damages_v1 string,
                vehicle_damages_v2 string,
                vehicle_id_v1 string,
                vehicle_id_v2 string,
                vehicle_type_v1 string,
                vehicle_type_v2 string,
                vehicle_year_v1 integer,
                vehicle_year_v2 integer,
                vehicle_occupants_v1 integer,
                vehicle_occupants_v2 integer
                )
                USING DELTA
                LOCATION '{self.managed_loc}/gold/vehicle_collisions_details_gd'
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
        self.create_vehicles_sv()
        self.create_persons_sv()
        self.create_crashes_hourly_summary_gd()
        self.create_vehicle_collisions_details_gd()
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
        self.assert_table("vehicles_sv", "silver")
        self.assert_table("persons_sv", "silver")
        self.assert_table("crashes_hourly_summary_gd", "gold")
        self.assert_table("vehicle_collisions_details_gd", "gold")
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
