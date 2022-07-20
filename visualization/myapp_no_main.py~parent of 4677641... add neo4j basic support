from flask import Flask, render_template, request
import pymongo
import sys
from visualization.item_history_bfs import get_item_history
from visualization.record_history import get_record_operation
import time
app = Flask(__name__)
@app.route("/")
def hello():
    return render_template('index.html')
@app.route("/index2.html/", methods=['GET', 'POST'])
def item_history():
    index = request.args['index']
    value = request.args['value']
    feature = request.args['feature']
    if request.args['button']=='item':
        # get item_history
        json_edge, json_data = get_item_history(value, feature, index, activities, relations, derivations, entities)
        return render_template('index2.html', json_data=json_data, json_edge=json_edge)
    else:
        # get record_history
        json_edge, json_data = get_record_operation(value, feature, index, activities, relations, derivations, entities)
        return render_template('index2.html', json_data=json_data, json_edge=json_edge)

def main(dbname):
    client = pymongo.MongoClient('localhost', 27017)
    db = client[dbname]
    global entities
    entities = db.entities
    global activities
    activities = db.activities
    global relations
    relations = db.relations
    global derivations
    derivations = db.derivations
    app.run()

