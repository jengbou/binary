#!/usr/bin/python
"""
Author: Geng-Yuan Jeng <jeengbou@gmail.com>
Minimal Viable Project for Insight 2020C DE Project "BINARY: Brain Image graNARY"
"""
from __future__ import print_function

import io
import re
import logging
import gzip
import urllib.parse
import boto3
import matplotlib.pyplot as plt
import nibabel as nib
import numpy as np

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
    imgdata = imgs.get_fdata()
    logging.info(imgs.shape)

    # extract region of interest; slices from 'begin' to 'end' along 'axis'
    # axis = 0: x-axis (sagittal), 1: y-axis (coronal), 2: z-axis (axial)
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

def get_2dimg_cent(s3obj, axis=1, nslices=-1, rot=0):
    """ obtain central slices of 2D images from nifti data"""
    # get the images from 3s obj
    iobyte = io.BytesIO(gzip.open(s3obj.get()['Body']).read())
    fholder = nib.FileHolder(fileobj=iobyte)
    imgs = nib.Nifti1Image.from_file_map({'header': fholder, 'image': fholder})
    header = imgs.header
    imgdata = imgs.get_fdata()
    logging.info(imgs.shape)

    # extract region of interest; slices from 'begin' to 'end' along 'axis'
    # axis = 0: x-axis (sagittal), 1: y-axis (coronal), 2: z-axis (axial)
    # in header['dim'] 1: x-axis, 2: y- axis, 3: z-axis
    begin = 0
    end = header['dim'][axis + 1] - 1
    if nslices != -1:
        nslices /= 2
        middle = header['dim'][axis + 1] / 2
        begin = middle - nslices
        end = middle + nslices
    if begin < 0:
        begin = 0
    if end >= header['dim'][axis + 1]:
        end = header['dim'][axis + 1] - 1
    resx = header['pixdim'][2]
    resy = header['pixdim'][3]
    ## get the slices
    logging.info("Slice range: %i to %i", begin, end)
    if axis == 0:
        imgdata = imgdata[int(begin):int(end), :, :]
    elif axis == 1:
        imgdata = imgdata[:, int(begin):int(end), :]
        resx = header['pixdim'][1]
        resy = header['pixdim'][3]
    elif axis == 2:
        imgdata = imgdata[:, :, int(begin):int(end)]
        resx = header['pixdim'][1]
        resy = header['pixdim'][2]
    imgs = []
    for j in range(imgdata.shape[axis]):
        if axis == 0:
            imgs.append(np.rot90(imgdata[j, :, :], rot))
        elif axis == 1:
            imgs.append(np.rot90(imgdata[:, j, :], rot))
        elif axis == 2:
            imgs.append(np.rot90(imgdata[:, :, j], rot))
    return np.asarray(imgs), [resx, resy]

def norm_2dimg(imgs):
    """ normalize 2D images gray scale to be between 0,1"""
    gmax = np.max(imgs)
    gmin = np.min(imgs)
    imgs = (imgs - gmin) / (gmax - gmin)
    return imgs

def upload_2dimg(imgs, bktname, objdir, outtag, profile='default'):
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
                            Key=objdir+"/%s_%i.jpg"%(outtag, i),
                            ACL='public-read')
        tmp.close()

def get_2dimg_dcm2niix(filename):
    """
    Obtain slices of 2D images from nii.gz file produced by dcm2niix
    Note: in contrast to nii.gz files from HCP open data,
    TCIA data didn't use consistent approach in choosing slice ordering.
    Some used 'z' as slicing direction independent of scan axis,
    whle others adapt similar convention as HCP did.
    For example, raw data with filename containing "IRPREP" or "SAG_FSPGR"
    use 'x' axis as slice ordering.
    Another example, filenames with "COR_FSPGR_3D" use 'y' axis while
    with "COR_FSPGR" use 'z' axis.
    Fortunately, the number of slices along the scan direction is the minimum one.
    Therefore, the code finds the axis accordingly.
    """
    imgs = nib.load(filename)
    imgdata = imgs.get_fdata()
    logging.info("shape: %s; dim: %i", imgs.shape, len(imgs.shape))
    if len(imgs.shape) != 3:
        logging.info("Skipped %s", filename)
        return None
    imgdata = imgdata[:, :, :]

    # find the axis to extract slices
    nslices = np.min(imgdata.shape)
    axis = 0
    for i in range(3):
        if imgdata.shape[i] == nslices:
            axis = i
            break

    # extract all slices along an axis
    imgs = []
    for j in range(imgdata.shape[axis]):
        if axis == 0:
            imgs.append(imgdata[j, :, :])
        elif axis == 1:
            imgs.append(imgdata[:, j, :])
        elif axis == 2:
            imgs.append(imgdata[:, :, j])

    return np.asarray(imgs)

def update_db_tcia(sqlcontext, metas, imgs, opts):
    """ Method to update db (TCIA)"""
    links = []
    for i, _ in enumerate(imgs):
        links.append('https://dataengexpspace.s3.amazonaws.com/{}'.format(
            urllib.parse.quote("{}/{}_{}.jpg".format(opts['jpgkey'], opts['outtag'], i))))

    scanaxis = ''
    if opts['tciaobj'].upper().find('SAG') != -1:
        scanaxis = 'sag'
    elif opts['tciaobj'].upper().find('COR') != -1:
        scanaxis = 'cor'
    elif opts['tciaobj'].upper().find('AXIAL') != -1 or opts['tciaobj'].upper().find('AX_') != -1:
        scanaxis = 'ax'
    else:
        scanaxis = 'other_{}'.format(re.sub(r'\W', '_', opts['otags'])\
                                         .replace('ACRIN-DSC-MR-Brain-', 'subject'))

    otable = [("{}".format(metas.SeriesInstanceUID),
               "{}".format(metas.SeriesDate),
               "{}".format(scanaxis),
               "{}".format(metas.Manufacturer),
               "{}".format(metas.ManufacturerModelName),
               "{}".format(metas.PixelSpacing[0]),
               "{}".format(metas.PixelSpacing[1]),
               links
              )]

    spdf = sqlcontext.createDataFrame(otable, [
        'series instance uid', 'series date', 'scan axis', 'manufacturer',
        'manufacturer model name', 'pixel spacing-x', 'pixel spacing-y', 'jpgfiles'])

    # update PostgreSQL table
    ## subjtab = "{}.{}".format(opts['schema'], re.sub(r'\W', '_', opts['otags']\
    ##                                             .replace('ACRIN-DSC-MR-Brain-', 'subject')))
    subjtab = "{}.{}".format(opts['schema'], scanaxis)
    logging.info("======> subjtab: %s", subjtab)
    spdf.write.jdbc(url=opts['dburl'], table=subjtab,
                    mode=opts['dbmode'], properties=opts['dboptions'])

def update_db_hcp(sqlcontext, imgs, opts):
    """ Method to update db (HCP openaccess)"""
    links = []
    for i, _ in enumerate(imgs):
        links.append('https://dataengexpspace.s3.amazonaws.com/{}'.format(
            urllib.parse.quote("{}/{}_{}.jpg".format(opts['jpgkey'], opts['outtag'], i))))

    otable = [("{}".format(opts['subjectid']),
               "{}".format('NORECORD'),
               "{}".format(opts['scanaxis']),
               "{}".format('SIEMENS'),
               "{}".format('SKYRA'),
               "{}".format(opts['resx']),
               "{}".format(opts['resy']),
               links
              )]

    spdf = sqlcontext.createDataFrame(otable, [
        'series instance uid', 'series date', 'scan axis', 'manufacturer',
        'manufacturer model name', 'pixel spacing-x', 'pixel spacing-y', 'jpgfiles'])

    # update PostgreSQL table
    subjtab = "{}.{}".format(opts['schema'], opts['scanaxis'])
    logging.info("======> subjtab: %s", subjtab)
    spdf.write.jdbc(url=opts['dburl'], table=subjtab,
                    mode=opts['dbmode'], properties=opts['dboptions'])

if __name__ == '__main__':
    logging.info("[Common tools for the pipeline]")
