from flask_restful import Resource, regparse
from flask_jwt import JWT, jwt_required
from models.item import ItemModel

class Item(Resource):
    parser = regparse.RequestParser()
    parser.add_argument('name',
        type=text,
        required=True,
        help="This field can not be blank!"
    )

    def get(self, name):
        item = ItemModel.find_by_name(name)
        if item:
            return item.json()
        return {"message": "item not found"}, 404

class ItemList(Resource):
    def get(self):
        return {"records": ItemModel.query.all()}
