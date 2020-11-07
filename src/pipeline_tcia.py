#!/bin/python
"""
Author: Geng-Yuan Jeng <jengbou@gmail.com>
Minimal Viable Project for Insight 2020C DE Project "BINARY: Brain Image graNARY"
"""
from __future__ import print_function

import os
import argparse
import logging
import glob
import shutil
from zipfile import ZipFile
import boto3
import pydicom
import numpy as np
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from common_tools import norm_2dimg, upload_2dimg, update_db_tcia, get_2dimg_dcm2niix

def parse_input_args():
    """ Parse command line input arguments """
    parser = argparse.ArgumentParser(description="tcia pipeline")
    parser.add_argument('-b', dest='inbucket', default='dataengexpspace',
                        help="input bucket")
    parser.add_argument('-d', dest='outs3subdir', default='data/TCIA_output',
                        help="output s3 subfolder")
    parser.add_argument('-k', dest='ins3key', default='',
                        help="input s3 object key")
    parser.add_argument('-l', dest='logfile', default='pipeline_tcia.log',
                        help="log file")
    parser.add_argument('-s', dest='schema', default='tcia',
                        help="schema for output postgres table")
    args = parser.parse_args()
    return args

def main(args):
    """ main function for processing HCP openaccess data set"""
    opts = {}
    opts['bktname'] = args.inbucket

    conf = SparkConf().setAppName("ETL_TCIAData")
    sctx = SparkContext(conf=conf).getOrCreate()
    sqlctx = SQLContext(sctx)

    # connect to postgres
    opts['dburl'] = "jdbc:postgresql://m5a2x0:1895/mydb"
    opts['dboptions'] = {
        "driver": "org.postgresql.Driver",
        "user": "pg2conn",
        "password": os.getenv('DB_SELECT_PASSWD')
        }
    opts['dbmode'] = "append"
    opts['schema'] = args.schema
    s3client = boto3.client('s3',
                            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))

    opts['tciaobj'] = args.ins3key
    opts['otags'] = '@'.join(opts['tciaobj'].replace("/blob.zip", "").split("/")[3:])
    opts['tmpdir'] = "/tmp/{}".format(opts['otags'])
    try:
        os.makedirs(opts['tmpdir'])
    except OSError:
        pass
    # download blob.zip from s3
    s3client.download_file(opts['bktname'], opts['tciaobj'], "{}/blob.zip".format(opts['tmpdir']))
    # unzip files to a temp dir
    ZipFile("{}/blob.zip".format(opts['tmpdir']), 'r').extractall(opts['tmpdir'])
    # combine and transform the dcm files into a json and a nii.gz
    os.system("dcm2niix -ba n -z y -f %i_3T_%d -o {} {}".format(opts['tmpdir'], opts['tmpdir']))
    niifiles = glob.glob("{}/*.nii.gz".format(opts['tmpdir']))
    logging.info(">>>>>>> nifti files: %s", niifiles)
    opts['jpgkey'] = "{}/{}".format(args.outs3subdir, opts['otags'])
    for niifile in niifiles:
        logging.info("------> processing file: %s", niifile)
        # read the metadata about the MR scans to be processed
        metadata = pydicom.dcmread(glob.glob("{}/*.dcm".format(opts['tmpdir']))[0])
        logging.info(glob.glob("{}/*.dcm".format(opts['tmpdir']))[0])

        # extract and process the images
        images = get_2dimg_dcm2niix(niifile)
        if images is None:
            continue

        logging.info("(slices, length, width): %s", images.shape)
        images = norm_2dimg(images)
        logging.info("Scale range: (%i, %i)", np.min(images), np.max(images))
        tmptag = niifile.split("/")
        opts['outtag'] = tmptag[len(tmptag) - 1].replace(".nii.gz", "")\
          .replace("(", "").replace(")", "")
        # write to db:
        update_db_tcia(sqlctx, metadata, images, opts)
        # upload to s3
        logging.info("======> Upload images to s3://%s/%s/%s_*.jpg",
                     opts['bktname'], opts['jpgkey'], opts['outtag'])
        upload_2dimg(images, opts['bktname'], opts['jpgkey'], opts['outtag'])
    # remove temporary files
    logging.info("+++++++ Remove temp dir: %s", opts['tmpdir'])
    try:
        shutil.rmtree(opts['tmpdir'])
    except OSError as oserr:
        logging.info("Error: %s : %s", opts['tmpdir'], oserr.strerror)

if __name__ == "__main__":
    options = parse_input_args()
    logging.basicConfig(filename=options.logfile, filemode='w', level=logging.INFO)
    main(options)
