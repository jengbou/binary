#!/bin/python
"""
Author: Geng-Yuan Jeng <jeengbou@gmail.com>
Minimal Viable Project for Insight 2020C DE Project "BrainScans"
"""
from __future__ import print_function

import os
import io
import argparse
import gzip
import boto3
import matplotlib.pyplot as plt
import nibabel as nib
import numpy as np
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

def parse_input_args():
    """ Parse command line input arguments """
    parser = argparse.ArgumentParser(description="main pipeline")
    parser.add_argument('-b', dest='inbucket', default='hcp-openaccess',
                        help="input bucket")
    parser.add_argument('-n', dest='subjectid', default='101006',
                        help="subject ID")
    parser.add_argument('-s', dest='schema', default='hcpopen',
                        help="schema for output postgres table")
    args = parser.parse_args()
    return args

def get_s3obj(bktname, objkey, profile='default'):
    """ helper to get object on aws s3"""
    s3session = boto3.Session(profile_name=profile)
    s3resource = s3session.resource('s3')
    s3bucket = s3resource.Bucket(bktname)
    s3obj = s3bucket.Object(objkey)
    return s3obj

def get_2dimg(s3obj, axis=1, begin=1, end=1, rot=0):
    """ obtain slices of 2D images from nifti data"""
    # get the images from 3s obj
    response = s3obj.get()
    zipfile = gzip.open(response['Body'])
    iobyte = io.BytesIO(zipfile.read())
    fholder = nib.FileHolder(fileobj=iobyte)
    imgs = nib.Nifti1Image.from_file_map({'header': fholder, 'image': fholder})
    imgdata = imgs.get_data()
    print(imgs.shape)

    # extract region of interest; slices from 'begin' to 'end' along 'axis'
    # axis = 0: x-axis, 1: y-axis, 2: z-axis
    if axis == 0:
        imgdata = imgdata[int(begin):int(end), :, :]
    elif axis == 1:
        imgdata = imgdata[:, int(begin):int(end), :]
    elif axis == 2:
        imgdata = imgdata[:, :, int(begin):int(end)]
    imgs = []
    for j in range(imgdata.shape[axis]):
        if axis == 0:
            imgs.append(np.rot90(imgdata[j, :, :], rot))
        elif axis == 1:
            imgs.append(np.rot90(imgdata[:, j, :], rot))
        elif axis == 2:
            imgs.append(np.rot90(imgdata[:, :, j], rot))
    return np.asarray(imgs)

def norm_2dimg(imgs):
    """ normalize 2D images gray scale to be between 0,1"""
    gmax = np.max(imgs)
    gmin = np.min(imgs)
    imgs = (imgs - gmin) / (gmax - gmin)
    return imgs

def upload_2dimg(imgs, bktname, objdir, subid, profile='default'):
    """ upload images to aws s3 """
    s3session = boto3.Session(profile_name=profile)
    s3resource = s3session.resource('s3')
    s3bucket = s3resource.Bucket(bktname)
    for i, val in enumerate(imgs):
        plt.imshow(val, cmap="gray", origin="lower")
        tmp = io.BytesIO()
        plt.savefig(tmp, orientation='portrait', format='jpg')
        tmp.seek(0)
        s3bucket.put_object(Body=tmp, ContentType='image/jpg',
                            Key=objdir+"/test_subject_%s_%i.jpg"%(subid, i),
                            ACL='public-read')

def main(args):
    """ main function for processing HCP openaccess data set"""
    inbucket = args.inbucket
    subjectid = args.subjectid
    subjtab = "%s.test_subject_%s"%(args.schema, subjectid)
    dbmode = "overwrite"

    conf = SparkConf().setAppName("BrainScansMVP")
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
    print(images.shape)
    images = norm_2dimg(images)
    print("Scale range: (", np.min(images), ", ", np.max(images), ")")
    upload_2dimg(images, "dataengexpspace", "data/mvp_output", subjectid)

if __name__ == "__main__":
    main(parse_input_args())
