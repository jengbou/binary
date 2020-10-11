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
from common_tools import get_s3obj, get_2dimg_cent, norm_2dimg, update_db_hcp, upload_2dimg

def parse_input_args():
    """ Parse command line input arguments """
    parser = argparse.ArgumentParser(description="main pipeline")
    parser.add_argument('-b', dest='inbucket', default='hcp-openaccess',
                        help="input bucket")
    parser.add_argument('-o', dest='outbucket', default='dataengexpspace',
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
    opts = {}
    opts['bktname'] = args.outbucket
    opts['inbucket'] = args.inbucket
    opts['subjectid'] = args.subjectid
    opts['schema'] = args.schema
    ## subjtab = "{}.test_subject_{}".format(opts['schema'], opts['subjectid'])
    opts['dbmode'] = "overwrite"

    conf = SparkConf().setAppName("ETL_HCPData")
    sctx = SparkContext(conf=conf).getOrCreate()
    # specify which aws credential to use for hcp-openaccess s3 bucket
    sctx._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.getenv('HCP_ACCESS_KEY_ID'))
    sctx._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.getenv('HCP_SECRET_ACCESS_KEY'))
    sqlctx = SQLContext(sctx)

    # connect to postgres
    opts['dburl'] = "jdbc:postgresql://m5a2x0:1895/mydb"
    opts['dboptions'] = {
        "driver": "org.postgresql.Driver",
        "user": "pg2conn",
        "password": os.getenv('DB_SELECT_PASSWD')
        }
    opts['otags'] = "hcp_subject_%s"%opts['subjectid']
    opts['jpgkey'] = "data/HCP_test_output/{}".format(opts['otags'])

    ### process nifti files from hcp open data
    # specify the s3 obj key of hcp-openaccess file to be processed
    hcpobj = "HCP_1200/{}/unprocessed/3T/T1w_MPR1/{}_3T_T1w_MPR1.nii.gz"\
      .format(opts['subjectid'], opts['subjectid'])
    s3obj = get_s3obj(opts['inbucket'], hcpobj, "hcpopen")

    for iaxis, atag in enumerate(["sag", "cor", "ax"]):
        images, ress = get_2dimg_cent(s3obj, iaxis, 50)
        logging.info(images.shape)
        ## opts['dimx'] = images.shape[1]
        ## opts['dimy'] = images.shape[2]
        opts['resx'] = ress[0]
        opts['resy'] = ress[1]
        images = norm_2dimg(images)
        logging.info("Scale range: (%i, %i)", np.min(images), np.max(images))
        opts['outtag'] = "subject{}_3T_T1w_MPR1_{}".format(opts['subjectid'], atag)
        # write metadata to db:
        update_db_hcp(sqlctx, images, opts)
        # upload images to s3
        logging.info("======> Upload images to s3://%s/%s/%s_*.jpg",
                     opts['bktname'], opts['jpgkey'], opts['outtag'])
        upload_2dimg(images, opts['bktname'], opts['jpgkey'], opts['outtag'])

if __name__ == "__main__":
    options = parse_input_args()
    logging.basicConfig(filename=options.logfile, filemode='w', level=logging.INFO)
    main(options)
