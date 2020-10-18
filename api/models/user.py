"""
Author: Geng-Yuan Jeng <jeengbou@gmail.com>
Insight 2020C DE Project "BINARY: Brain Image graNARY"
RESTful API app
"""
from flask_jwt import jwt_required
from mydb import db

class UserModel(db.Model):
    """ User Model """
    __tablename__ = 'users'
    __table_args__ = {"schema": "api"}
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80))
    password = db.Column(db.String(80))

    def __init__(self, username, password):
        """ Initialization """
        self.username = username
        self.password = password

    @jwt_required
    def save_to_db(self):
        """ Method to save user object to db """
        db.session.add(self)
        db.session.commit()

    @jwt_required
    def delete_from_db(self):
        """ Method to delete user object from db """
        db.session.delete(self)
        db.session.commit()

    @classmethod
    def find_by_username(cls, username):
        """ Method to extract user object in db by name """
        return cls.query.filter_by(username=username).first()

    @classmethod
    def find_by_id(cls, _id):
        """ Method to extract user object in db by id """
        return cls.query.filter_by(id=_id).first()
