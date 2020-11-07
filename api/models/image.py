"""
Author: Geng-Yuan Jeng <jengbou@gmail.com>
Insight 2020C DE Project "BINARY: Brain Image graNARY"
RESTful API app
"""

from flask_jwt import jwt_required
from sqlalchemy.dialects.postgresql import ARRAY
from mydb import db

class ImageModel(db.Model):
    """ Image Model """
    bktname = "dataengexpspace"
    __tablename__ = 'images'
    __table_args__ = {"schema": "api"}
    uid = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(80))
    images = db.Column(ARRAY(db.String))
    ## images = db.Column(ARRAY(db.String, dimensions=50))

    store_id = db.Column(db.Integer, db.ForeignKey('api.store.uid'))
    store = db.relationship('StoreModel')

    def __inti__(self, name, store_id, images):
        """ Initialization """
        self.name = name
        self.store_id = store_id
        self.images = list(enumerate(images))

    def json(self):
        """ Method to return jsonified response """
        return {'name': self.name,
                'images': ["https://{}.s3.amazonaws.com{}".format(
                    self.bktname, img) for img in self.images]}

    @jwt_required
    def save_to_db(self):
        """ Method to save image object to db """
        db.session.add(self)
        db.session.commit()

    @jwt_required
    def delete_from_db(self):
        """ Method to delete image object from db """
        db.session.delete(self)
        db.session.commit()

    @classmethod
    def find_by_name(cls, name):
        """ Method to extract image object in db by name """
        return cls.query.filter_by(name=name).all()
