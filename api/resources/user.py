"""
Author: Geng-Yuan Jeng <jeengbou@gmail.com>
Insight 2020C DE Project "BINARY: Brain Image graNARY"
RESTful API app
"""
from flask_restful import Resource, reqparse
#from flask_jwt import jwt_required
from models.user import UserModel
#from mydb import db

class UserRegister(Resource):
    """ UserRegister resource """
    parser = reqparse.RequestParser()
    parser.add_argument('username',
                        type=str,
                        required=True,
                        help="This field can not be blank!"
                        )
    parser.add_argument('password',
                        type=str,
                        required=True,
                        help="This field can not be blank!"
                        )

    def post(self):
        """ API POST """
        data = UserRegister.parser.parse_args()

        if UserModel.find_by_username(data['username']):
            return {"message": "A user with that username already exists."}, 400
        user = UserModel(**data)
        UserModel.save_to_db(user)
        return {"message": "User created successfully."}, 200
