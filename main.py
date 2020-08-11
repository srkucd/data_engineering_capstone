import configparser
import os
from nosql import cassandra,sparks


def main():
    config = configparser.ConfigParser()
    config.read('iam.cfg')
    os.environ['AWS_ACCESS_KEY_ID'] = config['AWS_CREDS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_CREDS']['AWS_SECRET_ACCESS_KEY']

    sparks()
    print('Spark implemented.')

    month = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
    for each in month:
        cassandra(2016, each)
    print('Current existed data imported.')


if __name__ == '__main__':
    main()