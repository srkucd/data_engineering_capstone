from pyspark.sql import SparkSession
import pandas as pd
import os

sql = '''SELECT cicid, i94yr, i94mon, i94cit, i94res, i94port,
       arrdate, i94mode, i94addr, depdate, i94bir, i94visa,
       count, dtadfile, visapost, occup, entdepa, entdepd,
       entdepu, matflag, biryear, dtaddto, gender, insnum,
       airline, admnum, fltno, visatype FROM i94'''

spark = SparkSession.builder. \
    config('spark.jars.packages', 'saurfang:spark.sas7bdat:2.0.0-s_2.11'). \
    enableHiveSupport().getOrCreate()


def backup(years, months):
    """
    Create parquet file as local and S3 backup, which can make R & W data easier.

    :param years: which year you decide to choose, in our beta version, only 2016 available.
    :param months: which month you decide to choose
    :return:
    """
    i94_url = 's3://srk-data-eng-capstone/i94/i94_{month}{year}_sub.sas7bdat'.format(month=months, year=str(years))
    csv_filename = '{year}_{month}.csv'.format(year=years, month=months)
    i94 = pd.read_sas(i94_url, 'sas7bdat',
                      encoding="ISO-8859-1").drop_duplicates()
    i94.to_csv(csv_filename)
    df_spark = spark.read.option('header', 'true').csv(csv_filename)
    df_spark.createOrReplaceTempView('i94')
    data = spark.sql(sql)
    data.write.parquet('{year}_{month}.parquet'.format(year=years, month=months), mode='overwrite')
    os.remove('{year}_{month}.csv'.format(year=years, month=months))

