import configparser
import pandas as pd
import os
import uuid
from pyspark.sql import SparkSession


def spark_generator():
    """
    create a spark session
    :return:spark session
    """
    spark = SparkSession.builder. \
        config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.driver.memory", "15g") \
        .enableHiveSupport().getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")
    return spark


def immigration_data(year, month):
    """
    create parquet file of immigration data group by year and month
    :param year: group by which year
    :param month: group by which month (alphabetic abbreviation)
    :return:parquet file of immigration data
    """
    i94 = pd.read_sas(('i94_' + str(month) + str(year) + '_sub.sas7bdat'), 'sas7bdat',
                      encoding="ISO-8859-1").drop_duplicates()
    i94['id_'] = pd.Series([str(uuid.uuid1()) for each in range(len(i94))])
    i94['arrival_date'] = pd.to_timedelta(i94['arrdate'], unit='D') + pd.Timestamp('1960-1-1')
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
    i94_df.write.mode('overwrite') \
        .partitionBy('month', 'year') \
        .format('parquet') \
        .option("compression", "gzip") \
        .save('parquet_data/' + str(month) + '_' + str(year) + '.parquet')
    print('i94 parquet generation complete.-' + str(month) + '_' + str(year))


def airport():
    """
    create parquet file of airport data
    :return: parquet file of airport data
    """
    airport_codes_url = 's3://srk-data-eng-capstone/airport-codes_csv.csv'
    airport_codes = pd.read_csv(airport_codes_url)
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
    airports.write.mode('overwrite') \
        .format('parquet') \
        .option("compression", "gzip") \
        .save('parquet_data/airports.parquet')
    print('Airport parquet generation complete.')


def us_cities():
    """
    create parquet file of  US cities
    :return: parquet file of US cities
    """
    us_city_demographics_url = 's3://srk-data-eng-capstone/us-cities-demographics.csv'
    us_city_demographics = pd.read_csv(us_city_demographics_url, sep=';')
    us_city_demographics = spark.createDataFrame(us_city_demographics)
    us_city_demographics.createOrReplaceTempView('us_cities')
    sql = """SELECT city, `Median Age` AS median_age, `Male Population` AS male_population,
              `Female Population` AS female_population, `Total Population` AS population,
              `Number of Veterans` AS num_veterans, `Foreign-born` AS foreign_born, `Average Household Size` AS avg_household_size,
              `State Code` AS state, race, count
       FROM us_cities"""
    us_cities = spark.sql(sql)
    us_cities.write.mode('overwrite') \
        .format('parquet') \
        .option('compression', 'gzip') \
        .save('parquet_data/us_cities.parquet')
    print('US cities parquet generation complete.')


def mapping(names):
    """
    create parquet files for mapping tables
    :param names:mapping table name
    :return:parquet files of mapping tables
    """
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
    df = spark.createDataFrame(df)
    df.write.mode('overwrite') \
        .format('parquet') \
        .option('compression', 'gzip') \
        .save('parquet_data/' + names + '.parquet')
    print(names + ' parquet generation complete.')


def upload_files(filename):
    """
    upload parquet file to S3
    :param filename: parquet filename
    :return:
    """
    config = configparser.ConfigParser()
    config.read('iam.cfg')
    os.environ['AWS_ACCESS_KEY_ID'] = config['AWS_CREDS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_CREDS']['AWS_SECRET_ACCESS_KEY']

    os.system('aws s3 cp parquet_data/{}.parquet s3://i94-backup --recursive'.format(filename))
    print(filename + ' is uploaded to bucket i94-backup')


def main():
    spark=spark_generator()
    immigration_data(16,'jan')
    airport()
    us_cities()
    mapping_list = ['country','us_states','visacode','mode']
    for each in mapping_list:
        mapping(each)
    uploading_list = ['jan_16','airports','country','mode','us_cities','us_states','visacode']
    for each in uploading_list:
        upload_files(each)


if __name__ == "__main__":
    main()