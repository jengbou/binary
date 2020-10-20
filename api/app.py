"""
Author: Geng-Yuan Jeng <jeengbou@gmail.com>
Insight 2020C DE Project "BINARY: Brain Image graNARY"
RESTful API app
Ref to https://github.com/tecladocode/rest-api-sections/
"""

import os
import logging
from flask import Flask, render_template
from flask_restful import Api
from flask_jwt import JWT
from security import authenticate, identity
from resources.user import UserRegister
from resources.image import Image, ImageList
from resources.store import Store, StoreList
#from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ['DATABASE_URL']
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.secret_key = os.environ['JWT_SECRET']
api = Api(app)

# Note: uncomment for local run
# This block is moved to run.py because:
# 1. uwsgi don't see this as '__main__'
# therefore, db wouldn't be imported before it's called
# 2. recursive import if moved import db before this line

## db = SQLAlchemy()
## @app.before_first_request
## def create_table():
##     db.create_all()
## b = SQLAlchemy(app)

jwt = JWT(app, authenticate, identity)

@app.route('/')
def hello():
    """ Test function for BINARY API """
    return render_template('index.html')

api.add_resource(Store, '/Store/<string:name>')
api.add_resource(StoreList, '/Stores')
api.add_resource(Image, '/Images/<string:name>')
api.add_resource(ImageList, '/ImageList')
api.add_resource(UserRegister, '/register')

if __name__ == '__main__':
    logging.basicConfig(filename='/var/www/html/binary-rest/binary_api.log',
                        filemode='w', level=logging.INFO)
    from mydb import db
    db.init_app(app)
    app.run(port=18002, debug=True)
