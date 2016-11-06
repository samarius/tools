import argparse
from datetime import datetime
from dateutil.relativedelta import relativedelta
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import re
import subprocess
import os
import json


def parse_arguments():
    """Parsing command line arguments."""
    parser = argparse.ArgumentParser(
        description="script for downloading and merging log files from S3 for particular time period")
    parser.add_argument("-s", 
                        "--startdate", 
                        help="start date in format YYYYMMDD", 
                        required=True, 
                        type=valid_date)
    parser.add_argument("-e", "--enddate", 
                        help="end date in format YYYYMMDD", 
                        required=True, 
                        type=valid_date)
    parser.add_argument("-f", 
                        "--file", 
                        help="destination file", 
                        required=True)
    parser.add_argument( "-c", "--config",
                        default="/Users/samarius/.get_analytics_log.config.json",
                        help="configuration file path")


    try:
        args = parser.parse_args()
        return args
    except Exception as e:
        print "can't parse command line args: {}".format(repr(e))
        raise


def parse_config(path):
    """Parsing configuration file, if it possible."""
    try:
        confdir = os.path.dirname(path)
        if not os.path.exists(confdir):
            raise ValueError("config path directory does not exist")
        if not os.path.exists(path):
            raise ValueError("config file does not exist")
        if not os.path.isfile(path):
            raise ValueError("config is not regular file")
        if os.path.getsize(path) == 0:
            raise ValueError("config file is empty")
        with open(path, 'rb') as f:
            conf = json.load(f)
        return conf
    except Exception as e:
        print "can't load config file {}: {}".format(path, repr(e))
        raise


def valid_date(s):
    """Check if date  is valid and convert it to  -> date(2016, 12, 13)"""
    try:
        date = datetime.strptime(s, "%Y%m%d")
        return date
    except ValueError:
        msg = "Not a valid date: '{0}'".format(s)
        raise argparse.ArgumentTypeError(msg)


def iterate_months(startdate, enddate):
    """ (2016.7.11, 2016.11.2) -> [2016.7, 2016.8, 2016.9, 2016.10, 2016.11] """
    dirs = []
    while startdate <= enddate:
        dirs.append(datetime.strftime(startdate, '%Y%m'))
        startdate += relativedelta(months=1)
    return dirs


def get_s3dir_filenames(dirname, bucket):
    filenames = []
    for key in bucket.list(dirname):
            filename = key.name.encode('utf-8')
            ### Dirty Hack to skip directory itself
            if not key.name.endswith('/'):
                filenames.append(filename)

    return filenames


def s3_conn(conf):
    conn = S3Connection(conf['access_key'], conf['secret_key'])
    bucket = conn.get_bucket(conf['bucket'])

    return bucket


def parse_dt_from_logfile_name(key):
    """ "worker3-20161213.log" -> date(2016, 12, 13) """
    ### Check file date by regular expression
    keydate = re.search("([0-9]{4}[0-9]{2}[0-9]{2})", key).group(1)
    
    key_dt = datetime.strptime(keydate, '%Y%m%d')
    return key_dt


def check_time_range(file, startdate, enddate):
    """Check if file in time range"""
    key_date = parse_dt_from_logfile_name(file)
    if startdate <= key_date <= enddate:
        return file


def main():
    """Main function."""


    all_files = []
    valid_files = []

    args = parse_arguments()

    config = parse_config(args.config)

    bucket = s3_conn(config['s3'])

    startdate = args.startdate
    enddate = args.enddate
    dst_file = args.file

    s3_dirs = iterate_months(startdate, enddate)

    ### Get all files in all directories
    for directory in s3_dirs:
        files_in_dir = get_s3dir_filenames(directory, bucket)
        all_files.extend(files_in_dir)

    ### Check if file is in time range
    for i in all_files:
        valid_file = check_time_range(i, startdate, enddate)
        valid_files.append(valid_file)

    ### Download files from S3
    for i in valid_files:
        if i:
            download_key = Key(bucket)
            download_key.key = i
            ### remove prefix from filename
            filename = re.sub('.*/', '', i)
            download_key.get_contents_to_filename(filename)
            ### Concantenate files
            with open(dst_file, "a") as outfile:
                subprocess.call(['gunzip', '-c', filename], stdout=outfile)

            subprocess.call(["rm", filename])
    

if __name__ == "__main__":
    main()
