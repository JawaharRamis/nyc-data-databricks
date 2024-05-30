# Databricks notebook source
class Config():    
    def __init__(self):      
        # self.base_dir_data = spark.sql("describe external location `data_zone`").select("url").collect()[0][0]
        # self.base_dir_checkpoint = spark.sql("describe external location `checkpoint`").select("url").collect()[0][0]
        self.unmanaged_loc = spark.sql("describe external location `unmanaged-loc`").select("url").collect()[0][0]
        self.db_name = "nyc_collision"
        self.maxFilesPerTrigger = 1000
