#!/bin/python
"""
Author: Geng-Yuan Jeng <jeengbou@gmail.com>
Minimal Viable Project for Insight 2020C DE Project "BINARY: Brain Image graNARY"
Usage: pyspark < run_hcpopen.py
"""
from __future__ import print_function

import sys
import time
import logging
import subprocess
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

if __name__ == "__main__":
    opts = {}
    opts['schema'] = "hcpopen_v1"
    opts['outs3subdir'] = "data/HCP_output_v1"
    opts['filelist'] = "/usr/local/spark-2.4.3-bin-hadoop2.7/datasets/hcp_1200_subjectid.txt"
    opts['master'] = "spark://m5a2x0:7077"
    opts['pyfile'] = "examples/src/main/python/pipeline_hcpopen.py"
    opts['inbktname'] = "hcp-openaccess"
    opts['outbktname'] = "dataengexpspace"
    opts['logfile'] = "run_hcpopen_v1.log"

    logging.basicConfig(filename=opts['logfile'], filemode='w', level=logging.INFO)
    conf = SparkConf().setAppName("run_ETL_HCPData")
    SCTX = SparkContext(conf=conf).getOrCreate()
    sqlctx = SQLContext(SCTX)

    COUNT = 0
    UIPORT = 4050
    subjlist = open(opts['filelist'], 'r').readlines()
    for line in subjlist:
        COUNT += 1
        ## if COUNT <= 200:
        ##     continue
        ## if COUNT == 1000:
        ##     sys.exit()
        SUBJID = "{}".format(int(line))
        SPKCMD = "spark-submit --jars {} --master {} "\
          "--conf spark.ui.port={} "\
          "--total-executor-cores 1 --executor-memory 2G "\
          "{} -b {} -d {} -n {} -o {} -s {} -l {} > {} 2>&1 &"\
          .format("jars/aws-java-sdk-1.7.4.jar,jars/hadoop-aws-2.7.7.jar",
                  opts['master'],
                  UIPORT,
                  opts['pyfile'],
                  opts['inbktname'],
                  opts['outs3subdir'],
                  SUBJID,
                  opts['outbktname'],
                  opts['schema'],
                  "/tmp/pipeline_hcp_{}.log".format(SUBJID),
                  "/tmp/run_hcp_{}.log".format(SUBJID)
                 )
        logging.info("Submit job: %s", SPKCMD)
        p = subprocess.Popen(SPKCMD, shell=True)
        p.wait()
        time.sleep(0.1)
        UIPORT += 1
        if COUNT % 20 == 0:
            time.sleep(1200)
        if COUNT % 200 == 0:
            UIPORT = 4050
            time.sleep(600)
