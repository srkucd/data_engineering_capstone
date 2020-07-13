from pyspark.sql import SparkSession


def create_spark():
    """

    :return:spark session
    """
    spark=SparkSession.builder.\
    config('spark.jars.packages', 'saurfang:spark.sas7bdat:2.0.0-s_2.11').\
    enableHiveSupport().getOrCreate()

    return spark

def read_from_s3(spark,months):
    """
    Read data from Amazon S3

    :param spark: Created spark session
    :param months: Which month of data do you need?
    :return:
    """
    i94_url = 's3://srk-data-eng-capstone/i94/i94_{month}16_sub.sas7bdat'
    i94=pd.read_sas(i94_url, 'sas7bdat',encoding="ISO-8859-1")
    i94.to_csv('test.csv')
    df_spark = spark.read.option('header', 'true').csv('test.csv')

