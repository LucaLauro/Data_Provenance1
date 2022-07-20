from flask import Flask, render_template, request, jsonify
import pymongo
import sys
from visualization.item_history_bfs import get_item_history
from visualization.item_history_neo4j import get_item_history_neo4j

from visualization.record_history import get_record_operation
import time
from neo4j_query import get_graph


app = Flask(__name__)
import time

@app.route("/data_entities")
def data_entities():
    entities_count = entities.count_documents({})
    timestamp=time.time()*1000
    annotations=dashboard.find({})
    annotations=list(map(lambda x:{'time':x['time'],'function_name':x['function_name'],'entities':x['entities']},list(annotations)))
    arr = [[{'data': [{'x': timestamp, 'y': int(entities_count)}]}],annotations]

    #return jsonify([{'data':[int(total_count)]}])
    return jsonify(arr)

@app.route("/data_relations")
def data_relations():
    relations_count= int(relations.count_documents({})) + int(derivations.count_documents({}))
    timestamp1=time.time()*1000
    annotations=dashboard.find({})
    annotations=list(map(lambda x:{'time':x['time'],'function_name':x['function_name'],'relations':x['relations']},list(annotations)))
    arr1 = [[{'data': [{'x': timestamp1, 'y': relations_count}]}],annotations]
    #return jsonify([{'data':[int(total_count)]}])
    return jsonify(arr1)
@app.route("/data_table")
def data_table():
    annotations=dashboard.find({})
    annotations=list(map(lambda x:{'time':x['time'],'function_name':x['function_name'],'relations':x['relations']},list(annotations)))
    annotations=list(sorted(annotations,key=lambda x: x['time']))
    print(annotations)
    #for ann in annotations:



@app.route("/")
def hello():
    return render_template('index.html')
@app.route("/index")
def index():
    return render_template('index.html')
@app.route("/home")
def home():
    return render_template('home.html')
@app.route("/index3.html/", methods=['GET', 'POST'])
def item_history():
    index = request.args['index']
    value = request.args['value']
    feature = request.args['feature']
    if request.args['button']=='item':
        # get item_history
        #json_edge, json_data = get_item_history(value, feature, index, activities, relations, derivations, entities)
        json_edge, json_data = get_item_history_neo4j(value, feature, index)

        return render_template('index3.html', json_data=json_data, json_edge=json_edge)
    else:
        # get record_history
        json_edge, json_data = get_record_operation(value, feature, index, activities, relations, derivations, entities)
        return render_template('index2.html', json_data=json_data, json_edge=json_edge)

def main(dbname):
    """
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
    global dashboard
    dashboard= db.dashboard
    """
    app.run(port=5002)

