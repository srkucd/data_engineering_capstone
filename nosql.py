import configparser
import pandas as pd
import os
import boto3
import uuid
from pyspark.sql import types as T
from time import sleep

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql import SQLContext
from pyspark.sql import types as T
from pyspark.sql.types import *
from pyspark import SparkContext


def spark_generator():
    spark = SparkSession.builder. \
        config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.driver.memory", "15g") \
        .enableHiveSupport().getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")
    return spark


def immigration_data(year, month):
    i94 = pd.read_sas(('i94_' + str(month) + str(year) + '_sub.sas7bdat'), 'sas7bdat',
                      encoding="ISO-8859-1").drop_duplicates()
    i94['id_'] = pd.Series([str(uuid.uuid1()) for each in range(len(i94))])
    i94['arrival_date'] = pd.to_timedelta(i94['arrdate'], unit='D') + pd.Timestamp('1960-1-1')
    spark = spark_generator()
    i94 = spark.createDataFrame(i94)
    i94.createOrReplaceTempView('i94')
    sql = """SELECT i94yr AS year,i94mon AS month,i94cit AS citizenship,
              i94res AS resident,i94port AS port,
              arrival_date,i94mode AS mode,
              i94addr AS us_state,depdate AS depart_date,
              i94bir AS age,i94visa visa_category,
              dtadfile AS date_added,visapost AS visa_issued_by,
              occup AS occupation,entdepa AS arrival_flag,
              entdepd AS depart_flag,entdepu AS update_flag,
              matflag AS match_arrival_depart_flag,
              biryear AS birth_year,dtaddto AS allowed_date,
              gender,insnum AS ins_number,airline,
              admnum AS admission_number,
              fltno AS flight_no,visatype,id_
              FROM i94;
       """
    i94_df = spark.sql(sql)
    i94_df.write.mode('overwrite').partitionBy('month', 'year').parquet('parquet_data/' + str(month) + '_' + str(year))
    print('i94 parquet generation complete.-' + str(month) + '_' + str(year))


def airport():
    airport_codes_url = 's3://srk-data-eng-capstone/airport-codes_csv.csv'
    airport_codes = pd.read_csv(airport_codes_url)
    spark = spark_generator()
    airport_codes = spark.createDataFrame(airport_codes)
    airport_codes.createOrReplaceTempView('airports')
    sql = """SELECT ident, type, name, elevation_ft, continent, 
                iso_country, iso_region, municipality, gps_code, iata_code AS airport_code, coordinates
         FROM airports WHERE iata_code IS NOT NULL
         UNION
         SELECT ident, type, name, elevation_ft, continent,
                iso_country, iso_region, municipality, gps_code, local_code AS airport_code, coordinates
         FROM airports WHERE local_code IS NOT NULL"""
    airports = spark.sql(sql)
    airports.write.mode('overwrite').parquet('parquet_data/airports')
    print('Airport parquet generation complete.')


def us_cities():
    us_city_demographics_url = 's3://srk-data-eng-capstone/us-cities-demographics.csv'
    us_city_demographics = pd.read_csv(us_city_demographics_url, sep=';')
    spark = spark_generator()
    us_city_demographics = spark.createDataFrame(us_city_demographics)
    us_city_demographics.createOrReplaceTempView('us_cities')
    sql = """SELECT city, `Median Age` AS median_age, `Male Population` AS male_population,
              `Female Population` AS female_population, `Total Population` AS population,
              `Number of Veterans` AS num_veterans, `Foreign-born` AS foreign_born, `Average Household Size` AS avg_household_size,
              `State Code` AS state, race, count
       FROM us_cities"""
    us_cities = spark.sql(sql)
    us_cities.write.mode('overwrite').parquet('parquet_data/us_cities')
    print('US cities parquet generation complete.')


def mapping(names):
    origin = open('mappings/{}.txt'.format(names), 'r')
    code = []
    name = []
    for each in origin:
        line = " ".join(each.split())
        try:
            code.append(int(line[:line.index('=')]))
        except:
            code.append(line[1:line.index('=') - 1])
        name.append(line[line.index('=') + 2:-1])
    origin.close()
    col_code = names + '_code'
    col_name = names + '_name'
    df = pd.DataFrame(list(zip(code, name)), columns=[col_code, col_name])
    spark = spark_generator()
    df = spark.createDataFrame(df)
    df.write.mode('overwrite').parquet('parquet_data/' + names)
    print(names + ' parquet generation complete.')


