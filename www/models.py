import time, uuid
from www.orm import Model, StringField, BooleanField, FloatField, TextField


def next_id():
    return '%015d%s000' % (int(time.time() * 1000), uuid.uuid4().hex)

class User(Model):
    __table__="users"

class Blog(Model):
    __table__="blogs"

class Comment(Model):
    __table__="comments"
