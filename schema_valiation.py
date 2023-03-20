import os
import pprint

from datetime import datetime as dt
from bson.objectid import ObjectId
from dotenv import load_dotenv, find_dotenv
from pymongo import MongoClient

load_dotenv(find_dotenv())
printer = pprint.PrettyPrinter()

def create_patient_first_hand_collection():
    patient_validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["first_name", "last_name", "gender", "dob", "alergies", "bloodgroup", "family_doctor"],
            "properties": {
                "first_name": {
                    "bsonType": "string",
                    "description": "must be a string and required"
                },
                "last_name": {
                    "bsonType": "string",
                    "description": "must be a string and required"
                }, 
                "gender": {
                    "enum": ["Male", "Female"],
                    "description": "can only be one of the enum values and is required"
                },
                "dob": {
                    "bsonType": "date",
                    "description":  "must be a date and is required"
                }, 
                "alergies": {
                    "bsonType": "array",
                    "items": {
                        "bsonType": "string",
                        "description": "must be a string and is required"
                    }
                },
                "bloodgroup": {
                    "enum": ["A+", "A-", "B+", "B-", "O+", "O-", "AB+", "AB-"],
                    "description": "can be one of enum values and is required"
                },
                "family_doctor": {
                    "bsonType": "objectId",
                    "description": "must be an object id and required"
                },
            }
        }
    }
    
    try:
        production.create_collection("patient_first_hand")
    except Exception as e:
        print(e)
    
    production.command("collMod", "patient_first_hand", validator=patient_validator)

def create_family_doctor():
    family_doctor_validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["first_name", "last_name", "age", "hospital_name"],
            "properties": {
                "first_name": {
                    "bsonType": "string",
                    "description": "must be a string and required"
                },
                "last_name": {
                    "bsonType": "string",
                    "description": "must be a string and required"
                },
                "age": {
                    "bsonType": "int",
                    "description": "must be an interger and required",
                },
                "hospital_name": {
                    "bsonType": "string",
                    "description": "must be a string and required"
                },
            }
        }
    }
    
    try:
        production.create_collection("family_doctor")
    except Exception as e:
        print(e)
    
    production.command("collMod", "family_doctor", validator=family_doctor_validator)

def create_data():
    family_doctors = family_doctors = [
        {
            "first_name": "Doctor", 
            "last_name": "1",
            "age": 32, 
            "hospital_name": "Clinic 1"
        },
        {
            "first_name": "Doctor", 
            "last_name": "2",
            "age": 45, 
            "hospital_name": "Clinic 2"
        },
        {
            "first_name": "Doctor", 
            "last_name": "3",
            "age": 55, 
            "hospital_name": "Clinic 1"
        },
        {
            "first_name": "Doctor", 
            "last_name": "4",
            "age": 48, 
            "hospital_name": "Clinic 1"
        },
        {
            "first_name": "Doctor", 
            "last_name": "5",
            "age": 39, 
            "hospital_name": "Clinic 1"
        },
    ]
    
    doctor_collection = production.family_doctor
    doctors_ids = doctor_collection.insert_many(family_doctors).inserted_ids
    
    patients = patients = [
        {
            "first_name": "Patient",
            "last_name": "1",
            "gender": "Female", 
            "dob": dt(2001, 7, 23),
            "alergies": ["Balsam of Peru", "Buckwheat"], 
            "bloodgroup": "O+",
            "family_doctor":doctors_ids[0],
        },
        {
            "first_name": "Patient",
            "last_name": "2",
            "gender": "Female", 
            "dob": dt(2001, 8, 11),
            "alergies": ["Sulfonamides", "Pollen"], 
            "bloodgroup": "B+",
            "family_doctor":doctors_ids[2],
        },
        {
            "first_name": "Patient",
            "last_name": "3",
            "gender": "Female", 
            "dob": dt(2000, 11, 5),
            "alergies": ["Sulfites", "Dilantin"], 
            "bloodgroup": "AB+",
            "family_doctor":doctors_ids[4],
        },
        {
            "first_name": "Patient",
            "last_name": "4",
            "gender": "Male", 
            "dob": dt(1999, 8, 23),
            "alergies": ["Shellfish", "Soy"], 
            "bloodgroup": "B-",
            "family_doctor":doctors_ids[3],
        },
        {
            "first_name": "Patient",
            "last_name": "5",
            "gender": "Female", 
            "dob": dt(2006, 8, 19),
            "alergies": ["Mustard", "Sesame"], 
            "bloodgroup": "B+",
            "family_doctor":doctors_ids[1],
        },
        {
            "first_name": "Patient",
            "last_name": "6",
            "gender": "Female", 
            "dob": dt(2008, 9, 20),
            "alergies": ["Oats", "Milk"], 
            "bloodgroup": "O+",
            "family_doctor":doctors_ids[2],
        },
        {
            "first_name": "Patient",
            "last_name": "7",
            "gender": "Female", 
            "dob": dt(2016, 7, 23),
            "alergies": ["Fish", "Stone fruits"], 
            "bloodgroup": "O-",
            "family_doctor":doctors_ids[4],
        },
        {
            "first_name": "Patient",
            "last_name": "8",
            "gender": "Male", 
            "dob": dt(2010, 1, 2),
            "alergies": ["Celery", "Egg"], 
            "bloodgroup": "AB-",
            "family_doctor":doctors_ids[0],
        },
    ]
    
    patient_collection = production.patient_first_hand
    patient_collection.insert_many(patients)

if __name__ == '__main__':

    username = os.environ.get("MONGODB_UN")
    password = os.environ.get("MONGODB_PW")

    connection_string = f"mongodb+srv://{username}:{password}@trialcluster.tyatfcm.mongodb.net/test&authSource=admin"
    cluster = MongoClient(connection_string)
    
    production = cluster.production
    
    # create_patient_first_hand_collection()
    # create_family_doctor()
    # create_data()
    
    # ------------------------------------------------------------------
    doctors_patient_count = production.family_doctor.aggregate([
        {
            "$lookup": {
                "from": "patient_first_hand",
                "localField": "_id",
                "foreignField": "family_doctor",
                "as": "patient",
            }
        },
        {
            "$addFields": {
                "total_patients": {"$size": "$patient"}
            }
        },
        {
            "$project": {"first_name":1, "last_name":1, "total_patients":1, "_id": 0},
        }
    ])
    
    # printer.pprint(list(doctors_patient_count))
    