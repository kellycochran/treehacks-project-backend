import hug
import pymongo
from pymongo import MongoClient

import datetime
from dateutil.rrule import rrule, MONTHLY

import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.dates as pltdates
import io

COLOR_DARK_BLUE = "#117095"

mpl.rcParams['text.color'] = COLOR_DARK_BLUE
mpl.rcParams['axes.labelcolor'] = COLOR_DARK_BLUE
mpl.rcParams['xtick.color'] = COLOR_DARK_BLUE
mpl.rcParams['ytick.color'] = COLOR_DARK_BLUE


MONTHS = ["Jan", "Feb", "Mar", "Apr", "May",
          "Jun", "Jul", "Aug", "Sep", "Oct",
          "Nov", "Dec"]


def get_db(db_name = 'test_data'):
    client = MongoClient('localhost', 27017)
    return client[db_name]


@hug.get('/reports/symptom')
def plot_symptom_over_time(symptom_name): 
    db = get_db()
    user_id = db["User"].find_one()["_id"]
    symptom = db["Y"].find_one({"who" : user_id, "name" : symptom_name})
    symptom_name = symptom["name"]
    symptom_scale = db["Scales"].find_one({"_id" : symptom["scale"]})
    symptom_min = symptom_scale["min"]
    symptom_max = symptom_scale["max"]
    symptom_base = symptom_scale["baseline"]
    isUnordered = symptom_scale["isUnordered"]
    
    symptom_dates = [log["datetime"] for log in db["YLog"].find({"who" : user_id,
                                                                 "y" : symptom["_id"]})]

    symptom_vals = [log["value"] for log in db["YLog"].find({"who" : user_id,
                                                             "y" : symptom["_id"]})]
    
    data_zip = zip(symptom_dates, symptom_vals)
    
    # filter for only data within the last year
    cutoff_date = datetime.datetime.now() - datetime.timedelta(days=365)
    data_zip = [(d,v) for d,v in data_zip if d > cutoff_date]
    
    dates = [tup[0] for tup in data_zip]
    dates_to_plot = [pltdates.date2num(d) for d in dates]
    vals_to_plot = [tup[1] for tup in data_zip]
    
    if (symptom_min, symptom_max) == (0, 1):
        plt.figure(figsize=(12, 3))
    else:
        plt.figure(figsize=(12, 5))

    plt.scatter(dates_to_plot, vals_to_plot,
                c = dates_to_plot, cmap = "winter",
                alpha = 0.5,
                edgecolor = 'k', linewidth = 0.5)

    for direction in ["left", "bottom"]:
        plt.gca().spines[direction].set_color(COLOR_DARK_BLUE)
    for direction in ["top", "right"]:
        plt.gca().spines[direction].set_visible(False)

    xtick_dates = [d for d in rrule(MONTHLY, dtstart=min(dates), until=max(dates))]
    plt.xticks([pltdates.date2num(dt) for dt in xtick_dates],
               labels=[MONTHS[dt.month - 1] for dt in xtick_dates])

    plt.yticks(range(symptom_min, symptom_max + 1))
    y_lim_pad = (symptom_max - symptom_min) / 10
    plt.ylim(min(vals_to_plot) - y_lim_pad, max(vals_to_plot) + y_lim_pad)
    
    plt.title(symptom_name, fontsize = 16, y = 1.05)
    
    f = io.BytesIO()
    plt.savefig(f, format = "svg")

    return f.getvalue()
