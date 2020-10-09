#!/bin/python
"""
Author: Geng-Yuan Jeng <jeengbou@gmail.com>
Minimal Viable Project for Insight 2020C DE Project "BINARY: Brain Image graNARY"
"""
from __future__ import print_function

import os
import argparse
import logging
import numpy as np
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from common_tools import get_s3obj, get_2dimg, norm_2dimg, upload_2dimg

def parse_input_args():
    """ Parse command line input arguments """
    parser = argparse.ArgumentParser(description="main pipeline")
    parser.add_argument('-b', dest='inbucket', default='hcp-openaccess',
                        help="input bucket")
    parser.add_argument('-n', dest='subjectid', default='101006',
                        help="subject ID")
    parser.add_argument('-s', dest='schema', default='hcpopen',
                        help="schema for output postgres table")
    parser.add_argument('-l', dest='logfile', default='pipeline_hcpopen.log',
                        help="log file")
    args = parser.parse_args()
    return args

def main(args):
    """ main function for processing HCP openaccess data set"""
    inbucket = args.inbucket
    subjectid = args.subjectid
    subjtab = "%s.test_subject_%s"%(args.schema, subjectid)
    dbmode = "overwrite"

    conf = SparkConf().setAppName("ETL_HCPdata")
    sctx = SparkContext(conf=conf).getOrCreate()
    # specify which aws credential to use for hcp-openaccess s3 bucket
    sctx._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.getenv('HCP_ACCESS_KEY_ID'))
    sctx._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.getenv('HCP_SECRET_ACCESS_KEY'))
    sqlctx = SQLContext(sctx)

    # connect to postgres
    dburl = "jdbc:postgresql://m5al0:1895/mydb"
    dboptions = {
        "driver": "org.postgresql.Driver",
        "user": "pg2conn",
        "password": os.getenv('DB_SELECT_PASSWD')
        }

    # read the metadata about the MR scans to be processed
    spdf = sqlctx.read.format("csv").options(header='true', delimiter=',', quotechar='"')\
      .load("s3://%s/HCP_1200/%s/unprocessed/3T/%s_3T.csv"%(inbucket, subjectid, subjectid))
    # update PostgreSQL table
    spdf.write.jdbc(url=dburl, table=subjtab, mode=dbmode, properties=dboptions)

    ### process nifti files from hcp open data
    # specify the s3 obj key of hcp-openaccess file to be processed
    hcpobj = "HCP_1200/%s/unprocessed/3T/T1w_MPR1/%s_3T_T1w_MPR1.nii.gz"%(subjectid, subjectid)
    s3obj = get_s3obj(inbucket, hcpobj, "hcpopen")
    images = get_2dimg(s3obj, 1, 135, 185)
    logging.info(images.shape)
    images = norm_2dimg(images)
    logging.info("Scale range: (%i, %i)", np.min(images), np.max(images))
    upload_2dimg(images, "dataengexpspace", "data/HCP_test_output", "hcp_subject_%s"%subjectid)

if __name__ == "__main__":
    options = parse_input_args()
    logging.basicConfig(filename=options.logfile, filemode='w', level=logging.INFO)
    main(options)
