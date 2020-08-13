import configparser
import os
from nosql import cassandra,sparks,inserting


def main():
    config = configparser.ConfigParser()
    config.read('iam.cfg')
    os.environ['AWS_ACCESS_KEY_ID'] = config['AWS_CREDS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_CREDS']['AWS_SECRET_ACCESS_KEY']

    sparks()
    print('Spark implemented.')
    username = config['APACHE_CASSANDRA_CREDS']['CASSANDRA_USERNAME']
    password = config['APACHE_CASSANDRA_CREDS']['CASSANDRA_PASSWORD']
    session = cassandra(username,password)
    print('Cassandra Connected.')

    month = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
    for each in month:
        inserting(2016, each, session)
    print('Current existed data imported.')


if __name__ == '__main__':
    main()