import configparser
import os
from nosql import cassandra,sparks

config = configparser.ConfigParser()
config.read('iam.cfg')
os.environ['AWS_ACCESS_KEY_ID']=config['AWS_CREDS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_CREDS']['AWS_SECRET_ACCESS_KEY']

sparks()
cassandra()