#!/bin/python
"""
Author: Geng-Yuan Jeng <jeengbou@gmail.com>
Minimal Viable Project for Insight 2020C DE Project "BrainScans"
"""
from __future__ import print_function

import sys
import os
import io
import gzip
import boto3
import matplotlib.pyplot as plt
import nibabel as nib
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: pipeline_hcpopen_mvp <hcp bucket> <subject ID> <schema>",
              file=sys.stderr)
        sys.exit(-1)

    hcpBucket = sys.argv[1]
    subjectID = sys.argv[2]
    subjectTab = "%s.test_subject_%s"%(sys.argv[3], subjectID)

    conf = SparkConf().setAppName("BrainScansMVP")
    SC = SparkContext(conf=conf).getOrCreate()
    # specify which aws credential to use for hcp-openaccess s3 bucket
    SC._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.getenv('HCP_ACCESS_KEY_ID'))
    SC._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.getenv('HCP_SECRET_ACCESS_KEY'))
    sqlContext = SQLContext(SC)

    # connect to postgres
    DBURL = "jdbc:postgresql://m5al0:1895/mydb"
    dboptions = {
        "driver": "org.postgresql.Driver",
        "user": "pg2conn",
        "password": os.getenv('DB_SELECT_PASSWD')
        }

    # read the metadata about the MR scans to be processed
    df = sqlContext.read.format("csv").options(header='true', delimiter=',', quotechar='"')\
      .load("s3://%s/HCP_1200/%s/unprocessed/3T/%s_3T.csv"%(hcpBucket, subjectID, subjectID))

    # update PostgreSQL table
    df.write.jdbc(url=DBURL, table=subjectTab, mode="overwrite", properties=dboptions)

    # specify which aws profile to use for hcp-openaccess s3 bucket
    hcpSession = boto3.Session(profile_name="hcpopen")
    hcpS3 = hcpSession.resource('s3')
    hcpBucket = hcpS3.Bucket(hcpBucket)
    objKey = "HCP_1200/%s/unprocessed/3T/T1w_MPR1/%s_3T_T1w_MPR1.nii.gz"%(subjectID, subjectID)
    obj = hcpBucket.Object(objKey)
    response = obj.get()
    zz = gzip.open(response['Body'])
    rr = zz.read()
    bb = io.BytesIO(rr)
    fh = nib.FileHolder(fileobj=bb)
    img = nib.Nifti1Image.from_file_map({'header': fh, 'image': fh})

    print(img.shape)
    img_data = img.get_data()
    img_data = img_data[:, 135:185, :]

    # get a new handle on s3 using default aws profile
    s3 = boto3.resource('s3')
    # get a handle on the bucket that holds output img files
    bucket = s3.Bucket("dataengexpspace")
    # upload the images
    for i in range(img_data.shape[1]):
        plt.imshow(img_data[:, i, :], cmap="gray", origin="lower")
        tmp = io.BytesIO()
        plt.savefig(tmp)
        tmp.seek(0)
        bucket.put_object(Body=tmp, ContentType='image/png',
                          Key='data/mvp_output/test_subject_%s_%i.png'%(subjectID, i))
