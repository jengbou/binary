#!/bin/python
"""
Author: Geng-Yuan Jeng <jengbou@gmail.com>
Minimal Viable Project for Insight 2020C DE Project "BINARY: Brain Image graNARY"
Usage: pyspark < run_tcia.py
"""
from __future__ import print_function

import sys
import time
import logging
import subprocess
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

def is_roi(tobj):
    """ Method to select ROI """
    selected = True
    # select only relevant runs
    if tobj.find('SAG') != -1:
        logging.info('>>>>>>> Slices along sagittal direction')
    elif tobj.find('COR') != -1:
        logging.info('>>>>>>> Slices along coronal direction')
    elif tobj.find('AXIAL') != -1 or tobj.find('AX_') != -1:
        logging.info('>>>>>>> Slices along axial direction')
    else:
        selected = False
    return selected

if __name__ == "__main__":
    opts = {}
    opts['schema'] = "tcia_v2"
    opts['outs3subdir'] = "data/TCIA_output_v2"
    opts['filelist'] = "s3://dataengexpspace/data/TCIAData/metadata/filelist_p3.json"
    opts['master'] = "spark://m5a2x0:7077"
    opts['pyfile'] = "src/pipeline_tcia.py"
    opts['bktname'] = "dataengexpspace"
    opts['logfile'] = "run_tcia_v2.log"

    logging.basicConfig(filename=opts['logfile'], filemode='w', level=logging.INFO)
    conf = SparkConf().setAppName("run_ETL_TCIAData")
    SCTX = SparkContext(conf=conf).getOrCreate()
    sqlctx = SQLContext(SCTX)

    tuplelist = [{"instanceuid": x['SeriesInstanceUID'], "s3key": x['S3objkey']}
                 for x in sqlctx.read.json(opts['filelist']).rdd.collect()]

    COUNT = 0
    UIPORT = 4050
    for row in tuplelist:
        tciaobj = row['s3key']
        if not is_roi(tciaobj):
            continue
        COUNT += 1
        ## if COUNT < 200:
        ##     continue
        ## if COUNT == 1000:
        ##     sys.exit()
        SPKCMD = "spark-submit --jars {} --master {} "\
          "--conf spark.ui.port={} "\
          "--total-executor-cores 1 --executor-memory 2G "\
          "{} -b {} -d {} -k {} -s {} -l {} > {} 2>&1 &"\
          .format("jars/aws-java-sdk-1.7.4.jar,jars/hadoop-aws-2.7.7.jar",
                  opts['master'],
                  UIPORT,
                  opts['pyfile'],
                  opts['bktname'],
                  opts['outs3subdir'],
                  tciaobj,
                  opts['schema'],
                  "/tmp/pipeline_tcia_{}.log".format(
                      tciaobj.replace("/blob.zip", "").replace("/", "-")),
                  "/tmp/run_tcia_{}.log".format(
                      tciaobj.replace("/blob.zip", "").replace("/", "-"))
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
