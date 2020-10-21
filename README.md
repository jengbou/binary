# **BINARY**: **B**rain **I**mage gra**NARY**

A RESTful API for retrieving brain MRI images.
Created as a minimal viable project for Insight Data Engineering 2020C Program.

[Slidedeck](https://docs.google.com/presentation/d/1ShotzCn2B91CGUAytyrXWHlO4DAl6ki5O5ffQ0Es4KM/edit?usp=sharing)
<!--, [Recorded Dashboard Demo](t.b.a.), and [Dashboard](http://datangexpspace.club/)-->

## Table of Contents
1. [Introduction](README.md#Introduction)
1. [Architecture](README.md#Architecture)
1. [Dataset](README.md#Dataset)
1. [Data processing](README.md#Data-processing)
1. [Setup](README.md#Setup)
    - [Spark](README.md#Spark)
    - [PostgreSQL](README.md#PostgreSQL)
    - [Run the pipelines](README.md#Run-the-pipelines)
    - [Flask RESTful API](README.md#Flask-RESTful-API)
        - [Flask](README.md#Flask)
        - [Nginx](README.md#Nginx)
        - [uWSGI](README.md#uWSGI)
1. [Demo]

## Introduction

There are more than 200,000 brain tumor/metastasis cases per year in the US alone.
Early and precise identification could help come up with better treatment plans and improve living quality while prolonging patient’s lifespan.
It would be beneficial for both health care providers and academic researchers to improve diagnostic and tracking methods to tackle such disease.
There exist numerous studies involving MR imaging from research groups/hospitals around the world, some of these are available in data archives.
However, with a wide variety of conventions adapted independently, external researchers or data scientists would have to spend a lot of their time on finding and cleaning/wrangling such data.
Therefore, the goal of the project is to provide a simple tool to tackle this inconvenience.
The minimal viable solution contains:
* Dedicated pipelines to read and process various neuroimaging formats (e.g., nifti, DICOM, ...)
* Well defined schemas that incorporates forward compatibility
* Computer clusters for batch processing (AWS EC2 instances)
* Dedicated server hosting PostgreSQL database and RESTful API

## Architecture

![arch](https://raw.githubusercontent.com/jengbou/binary/master/images/architecture.png)

## Dataset
[TCIA: ACRIN-DSC-MR-Brain (ACRIN ACRIN 6677/RTOG 0625)](https://wiki.cancerimagingarchive.net/pages/viewpage.action?pageId=50135264#50135264a6b63241cb4843a291c45ef8a62d2073)

[Human Connectome Project (1200 Subjects Data Release)](https://humanconnectome.org/study/hcp-young-adult/document/1200-subjects-data-release)

## Data processing

Copy data from open datasets to S3 (HCP open data is already on S3), batch process them with Spark to produce normalized images (saved on S3) and propagate relevant metadata (e.g., scanner manufacturer, scanner type, scan resolution, links to normalized images on S3,...) to the PostgreSQL database.
User then can retrieve brain MRI images, in jpg format, via RESTful API queries.
The API is built using lightweight Python web framework “Flask” bridged by uWSGI to serve on a dedicated Nginx server

## Setup


### Spark

* Follow the instruction described in this blog: [Simply Install: Spark (Cluster Mode)](https://blog.insightdatascience.com/simply-install-spark-cluster-mode-341843a52b88)

* Start the Spark clusters assuming you install Spark under the directory: `/usr/local/spark`

```bash
cd /usr/local/spark
./sbin/start-all.sh
```
### PostgreSQL

Instructions to set up postgres to be added.

### Run the pipelines

Once your Spark controller and workers are up. Change directory to your work directory (e.g., `/home/ubuntu/workarea`) to which you want to clone the binary code, then do the following (take TCIA pipeline as an example):

```bash
cd /home/ubuntu/workarea
git clone https://github.com/jengbou/binary.git
cd binary
```
* Get TCIA data to S3:
```bash
curl https://services.cancerimagingarchive.net/services/v4/TCIA/query/getSeries?Collection=ACRIN-DSC-MR-Brain\&format=json -o datasets/ACRIN-DSC-MR-Brain.json
```
* Copy this json file 'ACRIN-DSC-MR-Brain.json' to your S3 bucket
```bash
aws s3 cp datasets/ACRIN-DSC-MR-Brain.json s3://<your_bucket>/
```
* point 'datalist' variable in [get_tcia_data_onepass](https://github.com/jengbou/binary/blob/master/tools/get_tcia_data_onepass.py) to the json on S3.
```bash
vi run_tcia.py
```
* modify the following fields in opts to your own setup: schema, outs3subdir, master, bktname, logfile
```bash
pyspark < run_tcia.py >& /tmp/run_tcia_<tag>.log &
```

### Flask RESTful API

To avoid messing up with system's python modules, it's advisable to set up and run in a virtual environment.

#### Flask

```bash
cd /var/www/html/binary-rest
pip3 install virtualenv
virtualenv venv --python=python3.6
source venv/bin/activate
pip install -r requirements.txt 
```

#### Nginx

* To set up Nginx server follow the instruction [here](https://github.com/tecladocode/rest-api-sections/blob/master/section9/lectures/139_setting_up_nginx_and_our_rest_api/commands.md)

* The settings used for this project is in [api/binary-rest.conf](https://github.com/jengbou/binary/blob/master/api/binary-rest.conf)
* When more data points are available, one may need to tune uwsgi_read_timeout and uwsgi_send_timeout to make sure there is enough time for compressing image files for API query download.

#### uWSGI

* To set up uWSGI follow the instruction [here](https://github.com/tecladocode/rest-api-sections/blob/master/section9/lectures/140_setting_up_uWSGI_to_run_our_REST_API/commands.md)

* The setup configurations of this project are in [api/uwsgi_binary_rest.service](https://github.com/jengbou/binary/blob/master/api/uwsgi_binary_rest.service)
(make sure to change the fields enclosed within "<" ">" accordingly).


## Demo

Now you can test the API by the following:

* If you already know what to query
```bash
curl <ip of your Flask web server>/Images/notumor_ax
'Note: this may take upto 90 seconds (depending on your server) before the download starts'
```

* Or you can go the frontpage by typing in ip to web-browser, then use the dropdown menu to query the set of MRI images of interest.


