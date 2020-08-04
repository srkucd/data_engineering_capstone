from pyspark.sql import SparkSession
import pandas as pd
import uuid
import os
from cassandra.cluster import Cluster
from ssl import SSLContext, PROTOCOL_TLSv1, CERT_REQUIRED
from cassandra.auth import PlainTextAuthProvider
from cassandra import ConsistencyLevel

def sparks():
    spark = SparkSession.builder. \
        config('spark.jars.packages', 'saurfang:spark.sas7bdat:2.0.0-s_2.11'). \
        enableHiveSupport().getOrCreate()
    return spark

def load_data(years, months):
    """
    Create parquet file as local and S3 backup, which can make R & W data easier.

    :param years: which year you decide to choose, in our beta version, only 2016 available.
    :param months: which month you decide to choose
    :return:
    """
    i94_url = 's3://srk-data-eng-capstone/i94/i94_{month}{year}_sub.sas7bdat'.format(month=months, year=str(years))
    csv_filename = '{year}_{month}.csv'.format(year=years, month=months)
    parquet_filename = str(years) + '_' + months + '.parquet'
    spark=sparks()

    while True:
        if os.path.isdir(parquet_filename):
            break
        else:
            sql = '''SELECT id_, cicid, i94yr, i94mon, i94cit, i94res, i94port,
                   arrdate, i94mode, i94addr, depdate, i94bir, i94visa,
                   count, dtadfile, visapost, occup, entdepa, entdepd,
                   entdepu, matflag, biryear, dtaddto, gender, insnum,
                   airline, admnum, fltno, visatype FROM i94'''

            i94 = pd.read_sas(i94_url, 'sas7bdat',
                              encoding="ISO-8859-1").drop_duplicates()
            i94['id_'] = pd.Series([uuid.uuid1() for each in range(len(i94))])
            i94.to_csv(csv_filename, index=False)
            df_spark = spark.read.option('header', 'true').csv(csv_filename)
            df_spark.createOrReplaceTempView('i94')
            data = spark.sql(sql)
            data.write.parquet(parquet_filename, mode='overwrite')
            os.system('aws s3 cp {filename} s3://i94-backup --recursive'.format(filename=parquet_filename))
            break

    while True:
        try:
            os.remove('{year}_{month}.csv'.format(year=str(years), month=months))
        except:
            break


def cassandra(years, months):
    ssl_context = SSLContext(PROTOCOL_TLSv1)
    ssl_context.load_verify_locations('AmazonRootCA1.pem')
    ssl_context.verify_mode = CERT_REQUIRED
    auth_provider = PlainTextAuthProvider(username=str(config['APACHE_CASSANDRA_CREDS']['CASSANDRA_USERNAME']),
                                          password=str(config['APACHE_CASSANDRA_CREDS']['CASSANDRA_PASSWORD']))
    cluster = Cluster(['cassandra.eu-west-1.amazonaws.com'], ssl_context=ssl_context, auth_provider=auth_provider,
                      port=9142)

    session = cluster.connect()
    create_keyspace = """CREATE KEYSPACE IF NOT EXISTS "i94"
                       WITH REPLICATION={'class':'SingleRegionStrategy'}"""
    session.execute(create_keyspace)

    create_table = """CREATE TABLE IF NOT EXISTS "i94".i94 (
                                                          cicid DOUBLE,i94yr DOUBLE,i94mon DOUBLE,
                                                          i94cit DOUBLE,i94res DOUBLE,i94port TEXT,
                                                          arrdate DOUBLE,i94mode DOUBLE,i94addr TEXT,
                                                          depdate DOUBLE,i94bir DOUBLE,i94visa DOUBLE,
                                                          count DOUBLE,dtadfile DOUBLE,visapost TEXT,
                                                          occup TEXT,entdepa TEXT,entdepd TEXT,
                                                          entdepu TEXT,matflag TEXT,biryear DOUBLE,
                                                          dtaddto TEXT,gender TEXT,insnum TEXT,
                                                          airline TEXT,admnum DOUBLE,fltno TEXT,
                                                          visatype TEXT,id_ TEXT,
                                                          PRIMARY KEY(id_)
                    ) """
    session.execute(create_table)

    origin_sql = """INSERT INTO "i94".i94 ("cicid","i94yr","i94mon","i94cit","i94res","i94port","arrdate","i94mode","i94addr","depdate",
                                  "i94bir","i94visa","count","dtadfile","visapost","occup","entdepa","entdepd","entdepu","matflag",
                                  "biryear","dtaddto","gender","insnum","airline","admnum","fltno","visatype","id_")
                                  VALUES ({0},{1},{2},{3},{4},'{5}',{6},{7},'{8}',{9},
                                          {10},{11},{12},{13},'{14}','{15}','{16}','{17}','{18}','{19}',
                                          {20},'{21}','{22}','{23}','{24}',{25},'{26}','{27}','{28}')"""

    total_length = len(pd.read_csv('{}_{}.csv').format(year=years, month=months))

    with open('{}_{}.csv').format(year=years, month=months), 'r' as csv:
        # Ignore the first line, which is column head.
        csv = iter(csv)
        next(csv)
        counts = 0
        for each in csv:
            lists = []
            columns = each.split(',')
            cicid = columns[0]
            i94yr = columns[1]
            i94mon = columns[2]
            i94cit = columns[3]
            i94res = columns[4]
            i94port = columns[5]
            arrdate = columns[6]
            i94mode = columns[7]
            i94addr = columns[8]
            depdate = columns[9]
            i94bir = columns[10]
            i94visa = columns[11]
            count = columns[12]
            dtadfile = columns[13]
            visapost = columns[14]
            occup = columns[15]
            entdepa = columns[16]
            entdepd = columns[17]
            entdepu = columns[18]
            matflag = columns[19]
            biryear = columns[20]
            dtaddto = columns[21]
            gender = columns[22]
            insnum = columns[23]
            airline = columns[24]
            admnum = columns[25]
            fltno = columns[26]
            visatype = columns[27]
            id_ = columns[28]

            original_list = [cicid, i94yr, i94mon, i94cit, i94res, i94port, arrdate, i94mode, i94addr, depdate, i94bir,
                             i94visa,
                             count, dtadfile, visapost, occup, entdepa, entdepd, entdepu, matflag, biryear, dtaddto,
                             gender, insnum,
                             airline, admnum, fltno, visatype, id_]

            for each in original_list:
                try:
                    each = float(each)
                    lists.append(each)
                except:
                    if each == '':
                        each = None
                        lists.append(each)
                    else:
                        lists.append(each)
                        continue

            formated_sql = origin_sql.format(lists[0], lists[1], lists[2], lists[3], lists[4], lists[5], lists[6],
                                             lists[7], lists[8], lists[9],
                                             lists[10], lists[11], lists[12], lists[13], lists[14], lists[15],
                                             lists[16], lists[17], lists[18], lists[19],
                                             lists[20], lists[21], lists[22], lists[23], lists[24], lists[25],
                                             lists[26], lists[27], lists[28])

            sql = session.prepare(formated_sql)
            sql.consistency_level = ConsistencyLevel.LOCAL_QUORUM
            session.execute(sql)

            print('Import row{} complete.'.format(counts) + '{} remaining.'.format(total_length - counts))
            counts += 1