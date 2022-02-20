import hug
import pymongo
from pymongo import MongoClient
from bson.son import SON
from bson.objectid import ObjectId


def get_db(db_name = 'fliq_data'):
    client = MongoClient('localhost', 27017)
    return client[db_name]


@hug.get('/food/search')
def search_food_names(food_prefix: hug.types.text):
    db = get_db()
    table_food = db["Food"]
    matches = []
    for row in table_food.find():
        if row["food"].startswith(food_prefix):
            matches.append(row["food"])
    return matches


@hug.get('/food/list')
def list_top_n_foods(user_id: hug.types.text, n: int):
    db = get_db()
    table_eatinglog = db["EatingLog"]
    
    pipeline = [
        {"$match": { "who": ObjectId(user_id) }},
        {"$group": {"_id": "$food", "count": {"$sum": 1}}},
        {"$sort": SON([("count", -1), ("_id", -1)])}
    ]
    return list(table_eatinglog.aggregate(pipeline))[:n]


@hug.get('/symptom/list')
def list_symptoms_and_scales(user_id: hug.types.text):
    db = get_db()
    table_y = db["Y"]
    table_scales = db["Scales"]
    
    symptom_dicts = list(table_y.find({"who" : ObjectId(user_id)}))
    
    new_symptom_dicts = []
    for symptom_dict in symptom_dicts:
        print([type(v) for k,v in dict(symptom_dict).items()])
        new_symptom_dict = {k:v for k,v in symptom_dict.items() if type(v) != ObjectId}
        print(new_symptom_dict)
        
        scale_id = symptom_dict["scale"]
        tmp = table_scales.find_one({"_id" : scale_id})
        new_symptom_dict["scale_obj"] = {k:v for k,v in tmp.items() if type(v) != ObjectId}
        new_symptom_dicts.append(new_symptom_dict)
        
    return new_symptom_dicts