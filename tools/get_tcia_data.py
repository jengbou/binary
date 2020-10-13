#!/bin/python
"""
Author: Geng-Yuan Jeng <jeengbou@gmail.com>
Minimal Viable Project for Insight 2020C DE Project "BrainScans"
"""
from __future__ import print_function

import os
import urllib
import boto3
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

def main():
    """ mathod to copy TCIA data to S3"""
    bktname = "dataengexpspace"
    datalist = "s3://dataengexpspace/data/TCIAData/metadata/filelist.json"

    conf = SparkConf().setAppName("Download_TCIAData")
    sctx = SparkContext(conf=conf).getOrCreate()
    sqlctx = SQLContext(sctx)
    tciadf = sqlctx.read.json(datalist)
    tuplelist = [{"instanceuid": x['SeriesInstanceUID'], "s3objkey": x['S3objkey']}
                 for x in tciadf.rdd.collect()]

    s3client = boto3.client('s3',
                            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))
    count = 0
    for row in tuplelist:
        apisyntx = "{}/services/v4/TCIA/query/getImage?SeriesInstanceUID={}".format(
            "https://services.cancerimagingarchive.net",
            row['instanceuid'])
        tmpfile = '/tmp/{}.zip'.format(row['instanceuid'])
        print("download file to: {}".format(tmpfile))
        urllib.request.urlretrieve(apisyntx, tmpfile)
        outfile = row['s3objkey']
        print("upload to s3://{}/{}".format(bktname, outfile))
        response = s3client.upload_file(tmpfile, bktname, outfile)
        if response is not None:
            print(">>>>>>>>>> Problem seen when uploading: ", outfile)
        else:
            count += 1
        print("remove tmp file: {}".format(tmpfile))
        os.remove(tmpfile)
    # check number of files downloaded
    print('Downloaded {} files'.format(count))

if __name__ == "__main__":
    main()
