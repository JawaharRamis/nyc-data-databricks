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
        self.catalog = env
        self.db_name = Conf.db_name
        self.initialized = False
        
    def create_db(self):
        spark.catalog.clearCache()
        print(f"Creating the database {self.catalog}.{self.db_name}...", end='')
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.catalog}.{self.db_name}")
        spark.sql(f"USE {self.catalog}.{self.db_name}")
        self.initialized = True
        print("Created database successfully!!")

    def create_crashes_bz(self):
        if(self.initialized):
            print(f"Creating crashes_bz table...", end='')
            spark.sql(f"""CREATE OR REPLACE TABLE {self.catalog}.{self.db_name}.crashes_bz (
                collision_id string,
                accident_date date,
                accident_time time,
                borough string,
                zip_code string,
                latitude double,
                longitude double,
                location string,
                on_street_name string,
                cross_street_name string,
                off_street_name string,
                number_of_persons_injured integer,
                number_of_persons_killed integer,
                number_of_pedestrians_injured integer,
                number_of_pedestrians_killed integer,
                number_of_cyclist_injured integer,
                number_of_cyclist_killed integer,
                number_of_motorist_injured integer,
                number_of_motorist_killed integer,
                contributing_factor_vehicle_1 string,
                contributing_factor_vehicle_2 string,
                contributing_factor_vehicle_3 string,
                contributing_factor_vehicle_4 string,
                contributing_factor_vehicle_5 string,
                vehicle_type_code_1 string,
                vehicle_type_code_2 string,
                vehicle_type_code_3 string,
                vehicle_type_code_4 string,
                vehicle_type_code_5 string
                )
                """) 
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
    
    def create_vehicles_bz(self):
        if(self.initialized):
            print(f"Creating vehicles_bz table...", end='')
            spark.sql(f"""CREATE OR REPLACE TABLE {self.catalog}.{self.db_name}.vehicles_bz(
                    crash_date date,
                    crash_time time,
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
                    vehicle_occupants integer    
                    )
                """)
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
            
    def create_persons_bz(self):
        if(self.initialized):
            print(f"Creating persons_bz table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.persons_bz(
                    unique_id string,
                    collision_id string,
                    accident_date date,
                    accident_time time,
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
                    victim_sex string
                """) 
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")       
    
            
    def create_users(self):
        if(self.initialized):
            print(f"Creating users table...", end='')
            spark.sql(f"""CREATE OR REPLACE TABLE {self.catalog}.{self.db_name}.users(
                    user_id bigint, 
                    device_id bigint, 
                    mac_address string,
                    registration_timestamp timestamp
                    )
                  """)  
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")            
    
    def create_gym_logs(self):
        if(self.initialized):
            print(f"Creating gym_logs table...", end='')
            spark.sql(f"""CREATE OR REPLACE TABLE {self.catalog}.{self.db_name}.gym_logs(
                    mac_address string,
                    gym bigint,
                    login timestamp,                      
                    logout timestamp
                    )
                  """) 
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
    def create_user_profile(self):
        if(self.initialized):
            print(f"Creating user_profile table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.user_profile(
                    user_id bigint, 
                    dob DATE, 
                    sex STRING, 
                    gender STRING, 
                    first_name STRING, 
                    last_name STRING, 
                    street_address STRING, 
                    city STRING, 
                    state STRING, 
                    zip INT, 
                    updated TIMESTAMP)
                  """)  
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")

    def create_heart_rate(self):
        if(self.initialized):
            print(f"Creating heart_rate table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.heart_rate(
                    device_id LONG, 
                    time TIMESTAMP, 
                    heartrate DOUBLE, 
                    valid BOOLEAN)
                  """)
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")

            
    def create_user_bins(self):
        if(self.initialized):
            print(f"Creating user_bins table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.user_bins(
                    user_id BIGINT, 
                    age STRING, 
                    gender STRING, 
                    city STRING, 
                    state STRING)
                  """)  
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
                        
            
            
    def create_workout_bpm_summary(self):
        if(self.initialized):
            print(f"Creating workout_bpm_summary table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.workout_bpm_summary(
                    workout_id INT, 
                    session_id INT, 
                    user_id BIGINT, 
                    age STRING, 
                    gender STRING, 
                    city STRING, 
                    state STRING, 
                    min_bpm DOUBLE, 
                    avg_bpm DOUBLE, 
                    max_bpm DOUBLE, 
                    num_recordings BIGINT)
                  """)
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
    def create_gym_summary(self):
        if(self.initialized):
            print(f"Creating gym_summar gold view...", end='')
            spark.sql(f"""CREATE OR REPLACE VIEW {self.catalog}.{self.db_name}.gym_summary AS
                            SELECT to_date(login::timestamp) date,
                            gym, l.mac_address, workout_id, session_id, 
                            round((logout::long - login::long)/60,2) minutes_in_gym,
                            round((end_time::long - start_time::long)/60,2) minutes_exercising
                            FROM gym_logs l 
                            JOIN (
                            SELECT mac_address, workout_id, session_id, start_time, end_time
                            FROM completed_workouts w INNER JOIN users u ON w.user_id = u.user_id) w
                            ON l.mac_address = w.mac_address 
                            AND w. start_time BETWEEN l.login AND l.logout
                            order by date, gym, l.mac_address, session_id
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
        print(f"Setup completed in {int(time.time()) - start} seconds")
        
    def assert_table(self, table_name):
        assert spark.sql(f"SHOW TABLES IN {self.catalog}.{self.db_name}") \
                   .filter(f"isTemporary == false and tableName == '{table_name}'") \
                   .count() == 1, f"The table {table_name} is missing"
        print(f"Found {table_name} table in {self.catalog}.{self.db_name}: Success")
        
    def validate(self):
        import time
        start = int(time.time())
        print(f"\nStarting setup validation ...")
        assert spark.sql(f"SHOW DATABASES IN {self.catalog}") \
                    .filter(f"databaseName == '{self.db_name}'") \
                    .count() == 1, f"The database '{self.catalog}.{self.db_name}' is missing"
        print(f"Found database {self.catalog}.{self.db_name}: Success")
        self.assert_table("crashes_bz")   
        self.assert_table("vehicles_bz")        
        self.assert_table("persons_bz ")
        print(f"Setup validation completed in {int(time.time()) - start} seconds")
        
    def cleanup(self): 
        if spark.sql(f"SHOW DATABASES IN {self.catalog}").filter(f"databaseName == '{self.db_name}'").count() == 1:
            print(f"Dropping the database {self.catalog}.{self.db_name}...", end='')
            spark.sql(f"DROP DATABASE {self.catalog}.{self.db_name} CASCADE")
            print("Done")
        print(f"Deleting {self.landing_zone}...", end='')
        dbutils.fs.rm(self.landing_zone, True)
        print("Done")
        print(f"Deleting {self.checkpoint_base}...", end='')
        dbutils.fs.rm(self.checkpoint_base, True)
        print("Done")    
