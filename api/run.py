"""
Author: Geng-Yuan Jeng <jengbou@gmail.com>
Insight 2020C DE Project "BINARY: Brain Image graNARY"
RESTful API app
This is the "app/module" for uwsgi to deploy on AWS EC2
"""

import logging
from app import app as application
from mydb import db

db.init_app(application)

@application.before_first_request
def create_table():
    """
    This create tables to the db that the flask app connects to,
    therefore, it must be called beforee first request.
    """
    db.create_all()

logging.basicConfig(filename='/var/www/html/binary-rest/binary_api.log',
                    filemode='w', level=logging.INFO)
db.init_app(application)
