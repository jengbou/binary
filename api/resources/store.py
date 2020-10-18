"""
Author: Geng-Yuan Jeng <jeengbou@gmail.com>
Insight 2020C DE Project "BINARY: Brain Image graNARY"
RESTful API app
"""
from flask_restful import Resource
from flask_jwt import jwt_required
from models.store import StoreModel

class Store(Resource):
    """ Store resource """
    def get(self, name):
        """ API GET """
        store = StoreModel.find_by_name(name)
        if store:
            return store.json()
        return {"message": "Store not found!"}, 404

    @jwt_required
    def post(self, name):
        """ API POST """
        if StoreModel.find_by_name(name):
            return {"message": "A store with name '{}' already exists.".format(name)}, 400
        store = StoreModel(name)
        try:
            store.save_to_db()
        except RuntimeError:
            return {"message": "An error occurred while creating the store."}, 500

        return store.json(), 201

    @jwt_required
    def delete(self, name):
        """ API DELETE """
        store = StoreModel.find_by_name(name)
        if store:
            store.delete_from_db()

        return {"message": "Store deleted."}

class StoreList(Resource):
    """ StoreList resource """
    def get(self):
        """ API GET """
        return {'stores': [store.json() for store in StoreModel.query.all()]}
