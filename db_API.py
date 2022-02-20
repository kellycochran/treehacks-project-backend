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
        if row["food"].lower().startswith(food_prefix.lower()):
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
        new_symptom_dict["scale_obj"]["scale_id"] = str(scale_id)
        new_symptom_dicts.append(new_symptom_dict)
        
    return new_symptom_dicts


@hug.post('/add/user')
def add_user_to_db(args):
    db = get_db()
    table_user = db["User"]
    table_user_keys = ["name", "alwaysHideCalories", "showNutritionGuidelines",
                       "hideBiologicalSex", "alwaysHideWeight"]
    user_dict = {k:v for k,v in args if k in table_user_keys}
    table_user.insert_one(user_dict)
    return 0


@hug.post('/add/Y')
def add_y_to_db(args):
    db = get_db()
    table_y = db["Y"]
    table_y_keys = ["who", "scale", "name"]
    y_dict = {k:v for k,v in args if k in table_y_keys}
    y_dict["who"] = ObjectId(y_dict["who"])
    y_dict["scale"] = ObjectId(y_dict["scale"])
    table_y.insert_one(y_dict)
    return 0


@hug.post('/log/eating')
def add_y_to_db(args):
    db = get_db()
    table_eatinglog = db["EatingLog"]
    table_eatinglog_keys = ["who", "food", "datetime"]
    eatinglog_dict = {k:v for k,v in args if k in table_eatinglog_keys}
    eatinglog_dict["who"] = ObjectId(eatinglog_dict["who"])
    eatinglog_dict["food"] = ObjectId(eatinglog_dict["food"])
    table_eatinglog.insert_one(eatinglog_dict)
    return 0


@hug.post('/log/Y')
def add_y_to_db(args):
    db = get_db()
    table_ylog = db["YLog"]
    table_ylog_keys = ["who", "y", "datetime", "value"]
    ylog_dict = {k:v for k,v in args if k in table_ylog_keys}
    ylog_dict["who"] = ObjectId(ylog_dict["who"])
    ylog_dict["y"] = ObjectId(ylog_dict["y"])
    table_ylog.insert_one(ylog_dict)
    return 0