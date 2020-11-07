"""
Author: Geng-Yuan Jeng <jengbou@gmail.com>
Insight 2020C DE Project "BINARY: Brain Image graNARY"
RESTful API app
"""
from flask_jwt import jwt_required
from mydb import db

class StoreModel(db.Model):
    """ Store Model """
    __tablename__ = 'store'
    __table_args__ = {"schema": "api"}
    uid = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(80))

    images = db.relationship('ImageModel', lazy='dynamic')

    def __inti__(self, name):
        """ Initialization """
        self.name = name

    def json(self):
        """ Method to return jsonified response """
        return {'name': self.name, 'images': [image.json() for image in self.images.all()]}

    @jwt_required
    def save_to_db(self):
        """ Method to save store object to db """
        db.session.add(self)
        db.session.commit()

    @jwt_required
    def delete_from_db(self):
        """ Method to delete store object from db """
        db.session.delete(self)
        db.session.commit()

    @classmethod
    def find_by_name(cls, name):
        """ Method to extract store object in db by name """
        return cls.query.filter_by(name=name).first()
