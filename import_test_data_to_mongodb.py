import pymongo
from pymongo import MongoClient
from datetime import datetime
from random import sample
import pandas as pd


# read in data from API export

df_path = "test_data.df.gz"
df = pd.read_csv(df_path, header=0)


def food_str_to_nutrition_num(food_str, nutrient_name):
    if nutrient_name not in food_str:
        return 0.0
    return float(food_str.split(nutrient_name + "': ")[1].split(",")[0].replace("}", ""))

def add_nutrition_info_to_df(df):
    nutrients = ["calories", "carbohydrates", "sodium", "fat", "sugar", "protein"]
    for nutrient in nutrients:
        df[nutrient] = df["Food_Raw"].map(lambda food : food_str_to_nutrition_num(food, nutrient))
    return df

def process_food_info_in_df(df):
    df = df.rename(columns={"Food":"Food_Raw"})
    df["food"] = df["Food_Raw"].map(lambda x : x.split(",")[0])
    df = add_nutrition_info_to_df(df)
    return df


meals_to_hours = {"Breakfast" : 9,
                  "Lunch" : 13,
                  "Dinner" : 18,
                  "Morning" : 11,
                  "Afternoon" : 15,
                  "Evening" : 21}

def date_and_meal_to_datetime(date, meal):
    year, month, day = [int(num) for num in date.split("-")]
    meal_hour = meals_to_hours[meal]
    return datetime(year, month, day, meal_hour)

def process_dates_and_meals_to_datetimes(df):
    df["datetime"] = df.apply(lambda row: date_and_meal_to_datetime(row["Date"], row["Meal"]),
                              axis=1)
    return df
    
df = process_food_info_in_df(df)
df = process_dates_and_meals_to_datetimes(df)
df = df.drop(["Food_Raw", "Date", "Meal"], axis=1)



# create database

client = MongoClient('localhost', 27017)
db = client['fliq_data']


# create table for users, insert fliq's data

table_user = db['User']

user_dict = {"name" : "Steven",
                  "alwaysHideCalories" : True,
                  "showNutritionGuidelines" : True,
                  "hideBiologicalSex" : False,
                  "alwaysHideWeight" : True}

user_id = table_user.insert_one(user_dict).inserted_id


# create tables for foods, eating log

table_eatinglog = db['EatingLog']
table_food = db['Food']

# insert all of the data from the pandas dataframe into the food table

eatinglog_dicts = []
food_dicts = []
food_dict_foods = set()

for _, row in df.iterrows():
    row_dict = row.to_dict()
    
    eatinglog_dict = {k:v for k,v in row_dict.items() if k in ["datetime", "food"]}
    eatinglog_dict["who"] = user_id
    
    food_dict = {k:v for k,v in row_dict.items() if k not in ["datetime"]}
    print(food_dict)
    
    eatinglog_dicts.append(eatinglog_dict)
    if food_dict["food"] not in food_dict_foods:
        food_dicts.append(food_dict)
        food_dict_foods.add(food_dict["food"])
    
table_eatinglog.insert_many(eatinglog_dicts)
table_food.insert_many(food_dicts)


# create tables for user-created Ys and scales, insert 3 example Ys and scales

table_scales = db['Scales']

scale_dicts = [{"who" : user_id,
                "min" : 1,
                "max" : 5,
                "baseline" : 1,
                "isUnordered" : False},
               {"who" : user_id,
                "min" : 0,
                "max" : 1,
                "baseline" : 0,
                "isUnordered" : True},
               {"who" : user_id,
                "min" : 1,
                "max" : 10,
                "baseline" : 5,
                "isUnordered" : False}]

table_scales.insert_many(scale_dicts)


table_y = db['Y']

scale_names = ["Headache Frequency", "Stomach Cramps Binary", "Mood 1 to 10"]

y_dicts = []
for scale_dict, scale_name in zip(table_scales.find(), scale_names):
    y_dict = {"who" : user_id}
    y_dict["scale"] = scale_dict["_id"]
    y_dict["name"] = scale_name
    y_dicts.append(y_dict)
    
table_y.insert_many(y_dicts)


# create table for recording Y values over time

table_ylog = db["YLog"]


# generate some fake Y data to log in YLog table

all_dates_with_data = set(df["datetime"].map(lambda x : x.date()))

dates_for_headaches = sample(all_dates_with_data, 40)
dates_for_cramps = sample(all_dates_with_data, 20)
dates_for_moods = sample(all_dates_with_data, 60)


headache_dicts = []
for date in dates_for_headaches:
    headache_scale = sample(range(1, 6), 1)[0]
    had_coffee = table_eatinglog.count_documents({"food" : "Black coffee",
                                                  "datetime" : datetime(date.year, 
                                                                        date.month,
                                                                        date.day, 9)}) > 0
    if had_coffee:
        headache_scale += sample([1,2],1)[0]
        headache_scale = min(5, headache_scale)
        
    y_id = table_y.find_one({"name" : "Headache Frequency"})["_id"]
    headache_dict = {"who" : user_id,
                     "y" : y_id,
                     "datetime" : datetime(date.year, date.month, date.day, 20),
                     "value" : headache_scale}
    headache_dicts.append(headache_dict)
table_ylog.insert_many(headache_dicts)


cramps_dicts = []
for date in dates_for_cramps:
    cramp_scale = sample([0,0,0,0,1], 1)[0]
        
    y_id = table_y.find_one({"name" : "Stomach Cramps Binary"})["_id"]
    cramps_dict = {"who" : user_id,
                     "y" : y_id,
                     "datetime" : datetime(date.year, date.month, date.day, 20),
                     "value" : cramp_scale}
    cramps_dicts.append(cramps_dict)
table_ylog.insert_many(cramps_dicts)


mood_dicts = []
for date in dates_for_moods:
    mood_scale = sample(range(1, 11), 1)[0]
        
    y_id = table_y.find_one({"name" : "Mood 1 to 10"})["_id"]
    
    calories_readings = []
    for hour in range(24):
        day_foods = [x["food"] for x in table_eatinglog.find({"datetime" : datetime(date.year, 
                                                                        date.month,
                                                                        date.day, hour)})]
        calories_of_foods = [table_food.find_one({"food" : food})["calories"] for food in day_foods]
        calories_readings.extend(calories_of_foods)
        
    calories_sum = sum(calories_readings)
    if calories_sum < 500:
        mood_scale -= 3
    elif calories_sum < 1000:
        mood_scale -= sample([1,2],1)[0]
    mood_scale = max(mood_scale, 1)
    
    mood_dict = {"who" : user_id,
                     "y" : y_id,
                     "datetime" : datetime(date.year, date.month, date.day, 20),
                     "value" : mood_scale}
    mood_dicts.append(mood_dict)
table_ylog.insert_many(mood_dicts)