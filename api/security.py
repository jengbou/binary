"""
Author: Geng-Yuan Jeng <jengbou@gmail.com>
Insight 2020C DE Project "BINARY: Brain Image graNARY"
RESTful API app
"""

from werkzeug.security import safe_str_cmp
from models.user import UserModel

def authenticate(username, password):
    """ Method to authenticate API user """
    user = UserModel.find_by_username(username)
    if user and safe_str_cmp(user.password, password):
        return user
    return None

def identity(payload):
    """ Method to get id of API user for JWT """
    user_id = payload['identity']
    return UserModel.find_by_id(user_id)
