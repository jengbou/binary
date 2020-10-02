#!/bin/python
"""
Author: Geng-Yuan Jeng <jeengbou@gmail.com>
Minimal Viable Project for Insight 2020C DE Project "BrainScans"
"""
from __future__ import print_function

import os
import re
import urllib
import boto3
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

def main():
    """ mathod to copy TCIA data to S3"""
    bktname = "dataengexpspace"
    objdir = "data/TCIAData"
    datalist = "s3://dataengexpspace/data/TCIAData/ACRIN-DSC-MR-Brain.json"
    conf = SparkConf().setAppName("Move_TCIAData_to_S3")
    sctx = SparkContext(conf=conf).getOrCreate()
    sqlctx = SQLContext(sctx)

    tciadf = sqlctx.read.json(datalist)
    tuplelist = [{"collection": x['Collection'], "subjectid": x['PatientID'],
                  "seriesdate": x['SeriesDate'], "seriesnum": x['SeriesNumber'],
                  "scantype": x['SeriesDescription'],
                  "studyuid": x['StudyInstanceUID'], "instanceuid": x['SeriesInstanceUID']}
                 for x in tciadf.rdd.collect()]

    s3client = boto3.client('s3',
                            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))

    for row in tuplelist:
        apisyntx = "{}/services/v4/TCIA/query/getImage?SeriesInstanceUID={}".format(
            "https://services.cancerimagingarchive.net",
            row['instanceuid'])
        tmpfile = '/tmp/{}.zip'.format(row['instanceuid'])
        print("download file to: {}".format(tmpfile))
        urllib.request.urlretrieve(apisyntx, tmpfile)
        outfile = "{}/{}/{}-{}".format("NULL" if row['collection'] is None else row['collection'],
                                       "NULL" if row['subjectid'] is None else row['subjectid'],
                                       "NULL" if row['seriesdate'] is None else row['seriesdate'],
                                       "NULL" if row['studyuid'] is None else \
                                           row['studyuid'][len(row['studyuid'])-5:])
        scantype = row['scantype']
        if scantype is None:
            scantype = "NULL"
        else:
            scantype = scantype.upper().replace(' ', '_')
            scantype = re.sub(r'[*+]', '', scantype)
            scantype = re.sub(r'C_{1}', '', scantype)
            scantype = re.sub(r'_C{1}', '', scantype)
        outfile = "{}/{}-{}-{}".format(outfile,
                                       "NULL" if row['seriesnum'] is None else row['seriesnum'],
                                       scantype,
                                       "NULL" if row['instanceuid'] is None else \
                                           row['instanceuid'][len(row['studyuid'])-5:])
        outfile = "{}/{}/blob.zip".format(objdir, outfile)
        print("upload to s3://{}/{}".format(bktname, outfile))
        response = s3client.upload_file(tmpfile, bktname, outfile)
        if response is not None:
            print(">>>>>>>>>> Problem seen when uploading: ", outfile)
        print("remove tmp file: {}".format(tmpfile))
        os.remove(tmpfile)

if __name__ == "__main__":
    main()
