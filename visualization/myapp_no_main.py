from flask import Flask, render_template, request, jsonify
#import pymongo
import sys
from visualization.item_history_bfs import get_item_history
from visualization.item_history_neo4j import get_item_history_neo4j
from visualization.get_df_metadata import get_df_meta_neo4j
from visualization.get_feature_metadata import get_feature_metadata_neo4j
from visualization.record_history import get_record_operation
from visualization.get_feature_operations import get_feature_operations
from visualization.get_item_history_from_feature_page import get_item_history_neo4j as get_item
import time
from neo4j_query import get_graph
import random
import json
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



@app.route("/old")
def hello():
    return render_template('index.html')
@app.route("/index")
def index():
    return render_template('index.html')
@app.route("/home")
def home1():
    return render_template('home.html')
@app.route("/df_meta_compare", methods=['GET', 'POST'])
def meta_compare():
    print(request.form)
    print(request.args)
    print(request)
    op_index_1 = request.args['op_index_1']
    op_index_2 = request.args['op_index_2']
    json_edge, json_data,df_meta = get_df_meta_neo4j()
    print(len(json_data),len(json_edge))
    print(df_meta)
    last_feature,df_meta=get_last_feature()
    df_op=sorted(list(df_meta.keys()))
    print(df_op)
    feature_op = get_feature_operations()
    return render_template('compare_dataframe.html', json_data=json_data, json_edge=json_edge,df_meta=df_meta,op_index_1=op_index_1,op_index_2=op_index_2,op_index='df_1',last_feature=last_feature,df_meta_operations=df_op,feature_meta_operations=feature_op)
@app.route("/")
def home():

    json_edge, json_data,df_meta = get_df_meta_neo4j()
    print(len(json_data),len(json_edge))
    print(df_meta)
    last_op='df_'+str(sorted(list(map(int,list(map(lambda x : x.split('_')[1],df_meta.keys())))))[-1])
    last_feature,df_meta=get_last_feature()
    df_op=sorted(list(df_meta.keys()))
    print(df_op)
    feature_op = get_feature_operations()

    return render_template('test_meta_dash.html', json_data=json_data, json_edge=json_edge,df_meta=df_meta,op_index=last_op,last_feature=last_feature,df_meta_operations=df_op,feature_meta_operations=feature_op)

@app.route("/meta")
def meta():

    json_edge, json_data,df_meta = get_df_meta_neo4j()
    print(len(json_data),len(json_edge))
    print(df_meta)
    last_op='df_'+str(sorted(list(map(int,list(map(lambda x : x.split('_')[1],df_meta.keys())))))[-1])
    last_feature,df_meta=get_last_feature()
    df_op=sorted(list(df_meta.keys()))
    print(df_op)
    feature_op = get_feature_operations()

    return render_template('test_meta_dash.html', json_data=json_data, json_edge=json_edge,df_meta=df_meta,op_index=last_op,last_feature=last_feature,df_meta_operations=df_op,feature_meta_operations=feature_op)
def get_last_feature():
    json_edge, json_data,df_meta = get_df_meta_neo4j()
    last_op='df_'+str(sorted(list(map(int,list(map(lambda x : x.split('_')[1],df_meta.keys())))))[-1])
    last_features=list(df_meta[last_op]['Columns'])
    return last_features,df_meta


@app.route("/feature_meta_compare", methods=['GET', 'POST'])
def feature_meta_compare():
    op_index_1 = request.args['op_index_1']
    op_index_2 = request.args['op_index_2']
    if len(op_index_1.split('_'))>2:
        feature_name_1='_'.join(op_index_1.split('_')[:-1])
        json_edge_1, json_data_1,feature_meta_1 = get_feature_metadata_neo4j(feature_name_1)

    else:
        json_edge_1, json_data_1,feature_meta_1 = get_feature_metadata_neo4j(op_index_1.split('_')[0])
    if len(op_index_2.split('_')) > 2:
        feature_name_2='_'.join(op_index_2.split('_')[:-1])
        json_edge_2, json_data_2,feature_meta_2 = get_feature_metadata_neo4j(feature_name_2)
    else:
        json_edge_2, json_data_2,feature_meta_2 = get_feature_metadata_neo4j(op_index_2.split('_')[0])

    feature_op=get_feature_operations()
    last_feature,df_meta=get_last_feature()
    #print(feature_meta)
    df_op=sorted(list(df_meta.keys()))
    #print(df_op)
    #print(feature_meta_1)
    return render_template('compare_feature.html', json_data_1=json_data_1, json_edge_1=json_edge_1,feature_meta_1=feature_meta_1,json_data_2=json_data_2, json_edge_2=json_edge_2,feature_meta_2=feature_meta_2,op_index_1=op_index_1,op_index_2=op_index_2,last_feature=last_feature,df_meta_operations=df_op,feature_meta_operations=feature_op)

@app.route("/featuremeta")
def feature_meta():

    feature_meta_id = request.args['feature']
    json_edge, json_data,feature_meta = get_feature_metadata_neo4j(feature_meta_id)

    last_op=str(sorted(list(map(int,list(map(lambda x : x.split('_')[-1],feature_meta.keys())))))[-1])
    last_op=[s for s in feature_meta.keys() if last_op in s][0]
    last_feature,df_meta=get_last_feature()
    df_op=sorted(list(df_meta.keys()))
    #print(df_op)
    #last_feature, df_meta = get_last_feature()
    #print(feature_meta)
    feature_op = get_feature_operations()
    return render_template('feature_meta.html', json_data=json_data, json_edge=json_edge,feature_meta=feature_meta,op_index=last_op,last_feature=last_feature,df_meta_operations=df_op,feature_meta_operations=feature_op)
@app.route("/single_entity")
def single_entity():
    value = request.args['value']
    feature_name=request.args['feature_name']
    entity_partial_id=value+'^^'+feature_name

    json_edge, json_data,df_meta = get_df_meta_neo4j()
    print(len(json_data),len(json_edge))
    print(df_meta)
    last_op='df_'+str(sorted(list(map(int,list(map(lambda x : x.split('_')[1],df_meta.keys())))))[-1])
    last_feature,df_meta=get_last_feature()
    df_op=sorted(list(df_meta.keys()))
    print(df_op)
    feature_op = get_feature_operations()
    if 'op_index' in request.args:
        op_index = request.args['op_index'].split('_')[-1]
        json_edge, json_data = get_item(entity_partial_id, op_index)
    elif 'index' in request.args:
        index= request.args['index']
        json_edge, json_data = get_item_history_neo4j(value, feature_name, index)

    return render_template('single_entity.html', json_data=json_data, json_edge=json_edge,df_meta=df_meta,op_index=last_op,last_feature=last_feature,df_meta_operations=df_op,feature_meta_operations=feature_op)

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

#match (n:Entity) WHERE n.EntityId STARTS WITH 'K4^^key2^^' AND n.EntityId ENDS WITH '1' return n LIMIT 1