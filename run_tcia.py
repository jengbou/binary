#!/bin/python
"""
Author: Geng-Yuan Jeng <jeengbou@gmail.com>
Minimal Viable Project for Insight 2020C DE Project "BINARY: Brain Image graNARY"
Usage: pyspark < run_tcia.py
"""
from __future__ import print_function

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
    elif tobj.find('AXIAL') != -1 or tobj.find('_AX_') != -1:
        logging.info('>>>>>>> Slices along axial direction')
    else:
        selected = False
    return selected

if __name__ == "__main__":
    LOGFILE = "run_tcia.log"
    SCHEMA = "tciatest"

    logging.basicConfig(filename=LOGFILE, filemode='w', level=logging.INFO)
    DATALIST = "s3://dataengexpspace/data/TCIAData/metadata/filelist_p3.json"
    conf = SparkConf().setAppName("run_ETL_TCIAData")
    SCTX = SparkContext(conf=conf).getOrCreate()
    sqlctx = SQLContext(SCTX)

    tuplelist = [{"instanceuid": x['SeriesInstanceUID'], "s3key": x['S3objkey']}
                 for x in sqlctx.read.json(DATALIST).rdd.collect()]

    UIPORT = 4050
    for row in tuplelist:
        tciaobj = row['s3key']
        if not is_roi(tciaobj):
            continue

        SPKCMD = "spark-submit --jars {} --master {} "\
          "--conf spark.ui.port={} "\
          "--total-executor-cores 1 --executor-memory 1G "\
          "{} -b {} -k {} -s {} -l {} > {} 2>&1 &"\
          .format("jars/aws-java-sdk-1.7.4.jar,jars/hadoop-aws-2.7.7.jar",
                  "spark://m5a2x0:7077",
                  UIPORT,
                  "examples/src/main/python/pipeline_tcia.py",
                  "dataengexpspace",
                  tciaobj,
                  SCHEMA,
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
