import datetime
import os
import pymongo
import pprint

from bson.objectid import ObjectId
from dotenv import load_dotenv, find_dotenv
from pymongo import MongoClient

printer = pprint.PrettyPrinter()

def create_document():
    first_name = ["ハーシュ", "カナク", "サンカ", "にシャント"]
    age = [21, 22, 23, 24]
    
    docs = []
    
    for first_name, age in zip(first_name, age):
        doc = {"first_name": first_name, "age": age}
        # person_collection.insert_one(doc)                 # arther than inserthing one at a time, can make a list and insert many
        docs.append(doc)
    
    person_collection.insert_many(docs)


def find_document():
    persons = person_collection.find()           # this is an iterator (cursor), this will return -- <pymongo.cursor.Cursor object at 0x00000202F37592D0>
    
    for person in persons:
        printer.pprint(person)

def find_user():
    user = person_collection.find_one({"first_name":"ハーシュ"})
    printer.pprint(user)

def get_person_by_id(person_id):
    
    _id = ObjectId(person_id)
    person = person_collection.find_one({"_id": _id})
    
    printer.pprint(person)

def get_age_range(min_age, max_age):
    query = {
            "$and": [
                {"age": {"$gte": min_age}},
                {"age": {"$lte": max_age}}
            ]}
    
    users = person_collection.find(query).sort("age")
    print(users)
    for user in users:
        printer.pprint(user)

def project_columns():
    columns = {"_id": 0, "first_name": 1}
    users = person_collection.find({}, columns)
    for user in users:
        printer.pprint(user)

def update_field_by_id(user_id):
    _id = ObjectId(user_id)
    
    # updates = {
    #     "$set": {'new_field': True},            # this will override if the new_field was already present, else will set a new field.
    #     "$inc": {"age": 1},                     # this will inclrement the age f someone with 1
    # }
    
    # person_collection.update_one({"_id": _id}, updates)
    person_collection.update_one({"_id": _id}, {"$unset": {"new_field":""}})

def replace_one(user_id):
    # we have to keep the ID same hence we replace...
    _id = ObjectId(user_id)
    
    new_doc = {
        "first_name": "Sankalp",
        "age":25,
    }
    
    person_collection.replace_one({"_id": _id}, new_doc)

def delete_doc_by_id(user_id):
    _id = ObjectId(user_id)
    
    person_collection.delete_one({"_id":_id})

def add_address_embed(user_id, address):
    _id = ObjectId(user_id)
    
    person_collection.update_one(
        {"_id":_id}, {"$addToSet":{"address":address}}
    )

def add_address_relation(user_id, address):
    _id = ObjectId(user_id)
    
    # because we dont want to mutate the input array
    address = address.copy()
    address["owner_id"] = user_id
    
    # we will need seperate collection for address and we relate them together
    address_collection = production.address
    
    address_collection.insert_one(address)

if __name__ == '__main__':
    load_dotenv(find_dotenv())

    username = os.environ.get("MONGODB_UN")
    password = os.environ.get("MONGODB_PW")

    connection_string = f"mongodb+srv://{username}:{password}@trialcluster.tyatfcm.mongodb.net/test"

    cluster = MongoClient(connection_string)
    db = cluster["test"]                    # can also be written as db = cluster.test
    collection = db["test"]

    post = {"author": "Mike",
            "text": "My first blog post!",
            "tags": ["mongodb", "python", "pymongo"],
            "date": datetime.datetime.utcnow()}

    # posts = db.post       # creating the new collection
    # collection.insert_one(post)         # add the post (entry) in the collection (i.e., test) -- [.inserted_id] returns the id

    new_posts = [{"author": "Mike",
                "text": "Another post!",
                "tags": ["bulk", "insert"],
                "date": datetime.datetime(2009, 11, 12, 11, 14)},
                {"author": "Eliot",
                "title": "MongoDB is fun",
                "text": "and pretty easy too!",
                "date": datetime.datetime(2009, 11, 10, 10, 45)}]

    # collection.insert_many(new_posts)

    production = cluster.production
    person_collection = production.person_collection
    
    address = {
        "address1": "Room No. 307",
        "address2": "laxmi Villa Boys Hostel",
        "city": "Pune",
        "state": "Maharashtra",
        "contry": "India",
        "code":"410507"
    }
    
    # call a function
