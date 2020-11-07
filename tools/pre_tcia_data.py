#!/bin/python
"""
Author: Geng-Yuan Jeng <jengbou@gmail.com>
Minimal Viable Project for Insight 2020C DE Project "BrainScans"
This script produces the SeriesInstanceUID to S3 path (object key) mapping.
Compare with get_tcia_data_onepass.py
Usage: pyspark < pre_tcia_data.py
"""
from __future__ import print_function

import os
import glob
import boto3
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import concat, upper, lit, col, regexp_replace

def main():
    """ mathod to get TCIA data file list"""
    bktname = "dataengexpspace"
    datalist = "s3://dataengexpspace/data/TCIAData/ACRIN-DSC-MR-Brain.json"

    conf = SparkConf().setAppName("Prepare_TCIAData_list")
    sctx = SparkContext(conf=conf).getOrCreate()
    sqlctx = SQLContext(sctx)
    tciadf = sqlctx.read.json(datalist)
    tmpdf = tciadf.na.fill("NULL")
    tmpdf = tmpdf.withColumn('ShortStudyUID', col('StudyInstanceUID').substr(60, 5))
    tmpdf = tmpdf.withColumn('ShortInstanceUID', col('SeriesInstanceUID').substr(60, 5))
    tmpdf = tmpdf.withColumn('SeriesDescription', upper(col('SeriesDescription')))
    tmpdf = tmpdf.withColumn('SeriesDescription', regexp_replace('SeriesDescription',
                                                                 r'[(]', ''))
    tmpdf = tmpdf.withColumn('SeriesDescription', regexp_replace('SeriesDescription',
                                                                 r'[)]', ''))
    tmpdf = tmpdf.withColumn('SeriesDescription', regexp_replace('SeriesDescription',
                                                                 r'[*]', ''))
    tmpdf = tmpdf.withColumn('SeriesDescription', regexp_replace('SeriesDescription',
                                                                 r'[+]', ''))
    tmpdf = tmpdf.withColumn('SeriesDescription', regexp_replace('SeriesDescription',
                                                                 r'\s+', '_'))
    tmpdf = tmpdf.withColumn('SeriesDescription', regexp_replace('SeriesDescription',
                                                                 r'[?]*[/]', 'over'))
    tmpdf = tmpdf.withColumn('SeriesDescription', regexp_replace('SeriesDescription',
                                                                 r'[/]{1}', ''))
    tmpdf = tmpdf.withColumn('SeriesDescription', regexp_replace('SeriesDescription',
                                                                 r'[?]$', ''))
    tmpdf = tmpdf.withColumn('SeriesDescription', regexp_replace('SeriesDescription',
                                                                 r'^[_]', ''))
    tmpdf = tmpdf.withColumn('SeriesDescription', regexp_replace('SeriesDescription',
                                                                 r'[_]$', ''))

    tmpdf = tmpdf.withColumn('S3objkey', concat(lit('data/TCIAData_p3/'),
                                                col('Collection'), lit('/'),
                                                col('PatientID'), lit('/'),
                                                col('SeriesDate'), lit('-'),
                                                col('ShortStudyUID'), lit('/'),
                                                col('SeriesNumber'), lit('-'),
                                                col('SeriesDescription'), lit('-'),
                                                col('ShortInstanceUID'), lit('/blob.zip')
                                               )
                             )
    tmpdf = tmpdf.select('SeriesInstanceUID', 'S3objkey')
    tmpdf.show(5)
    tmpdf.write.save('file:///tmp/tmpdfjson', format='json', mode='overwrite')
    s3client = boto3.client('s3',
                            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))

    tmpname = glob.glob('/tmp/tmpdfjson/*.json')[0]
    print(tmpname)
    response = s3client.upload_file(tmpname, bktname,
                                    "data/TCIAData/metadata/filelist_p3.json")
    if response is not None:
        print(">>>>>>> Upload problem for file: ", tmpname)
    print("remove tmp file: {}".format(tmpname))
    os.remove(tmpname)

if __name__ == "__main__":
    main()
