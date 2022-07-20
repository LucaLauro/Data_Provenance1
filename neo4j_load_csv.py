import os
import json
import time
query = '''
USING PERIODIC COMMIT 
LOAD CSV FROM 'file:///path/to/myfile.csv' AS row FIELDTERMINATOR ';'
MERGE (:Person {name: row[1], age: row[2]})
'''

#graph.cypher.execute(query)

from neo4j import GraphDatabase

uri = "neo4j://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "admin"))
#path= '/Users/luca/PycharmProjects/dataProvenance2/prov_acquisition/real_world_pipeline/prov_results/census_test_csv'
#path='/Users/luca/PycharmProjects/dataProvenance2/prov_acquisition/real_world_pipeline/prov_results/demo_csv'
def delete_all(tx):
    tx.run("MATCH (n) DETACH DELETE n;")
def create_entities(tx, file_path):
    tx.run(f'LOAD CSV WITH HEADERS FROM "file:///{file_path}" AS row CREATE (e:Entity {{EntityId: row.id, record_id: row.record_id}})')
def create_df_metadata(tx,file_path):
    tx.run(f'LOAD CSV WITH HEADERS FROM "file:///{file_path}" AS row CREATE (d:DfMeta {{DFMetaId: row.id, N_Column: row.n_col,N_Index:row.n_index,NaNPercent:row.list_percent_column_nan,Columns:row.columns,Dtype_percent:row.list_percent_dtypes,CorrMatrix:row.corr_matrix}})')

def create_df_meta_from_relation(tx,file_path):
    tx.run(f'LOAD CSV WITH HEADERS FROM "file:///{file_path}" AS row MATCH (a:Activity {{identifier: row.gen}}) MATCH (df_in:DfMeta {{DFMetaId: row.used}}) MERGE (df_in)-[:META_DERIVATION]->(a)')
def create_df_meta_to_relation(tx,file_path):
    tx.run(f'LOAD CSV WITH HEADERS FROM "file:///{file_path}" AS row MATCH (df_out:DfMeta {{DFMetaId: row.gen}}) MATCH (a:Activity {{identifier: row.used}}) MERGE (a)-[:META_DERIVATION]->(df_out)')
def create_feature_metadata(tx,file_path):
    tx.run(f'LOAD CSV WITH HEADERS FROM "file:///{file_path}" AS row CREATE (f:FeatureMeta {{FeatureMetaId: row.id, feature_name: row.feature_name,index_len:row.index_len,dtype:row.dtype,distinct_values:row.distinct_values,describe_list:row.describe_list,dist_plot:row.dist_plot}})')

def create_feature_meta_from_relation(tx,file_path):
    tx.run(f'LOAD CSV WITH HEADERS FROM "file:///{file_path}" AS row MATCH (a:Activity {{identifier: row.gen}}) MATCH (feature_in:FeatureMeta {{FeatureMetaId: row.used}}) MERGE (feature_in)-[:FEATURE_META_DERIVATION]->(a)')
def create_feature_meta_to_relation(tx,file_path):
    tx.run(f'LOAD CSV WITH HEADERS FROM "file:///{file_path}" AS row MATCH (feature_out:FeatureMeta {{FeatureMetaId: row.gen}}) MATCH (a:Activity {{identifier: row.used}}) MERGE (a)-[:FEATURE_META_DERIVATION]->(feature_out)')
def create_feature_meta_derivation(tx,file_path):
    tx.run(f'LOAD CSV WITH HEADERS FROM "file:///{file_path}" AS row MATCH (feature_out:FeatureMeta {{FeatureMetaId: row.gen}}) MATCH (feature_in:FeatureMeta {{FeatureMetaId: row.used}}) MERGE (feature_in)-[:FEATURE_META_DERIVATION]->(feature_out)')

def create_derivations(tx, file_path):
    tx.run(f'LOAD CSV WITH HEADERS FROM "file:///{file_path}" AS row MATCH (e_out:Entity {{EntityId: row.gen}}) MATCH (e_in:Entity {{EntityId: row.used}}) MERGE (e_out)-[:WAS_DERIVED_FROM]->(e_in)')
def create_generated(tx, file_path, act_id):
    tx.run(f'LOAD CSV  FROM "file:///{file_path}" AS row MATCH (e:Entity {{EntityId: row[0]}}) MATCH (a:Activity {{identifier: $act_id}}) MERGE (e)-[:WAS_GENERATED_BY]->(a)')
def create_used(tx, file_path, act_id):
    tx.run(f'LOAD CSV  FROM "file:///{file_path}" AS row MATCH (e:Entity {{EntityId: row[0]}}) MATCH (a:Activity {{identifier: $act_id}}) MERGE (a)-[:USED]->(e)')
def create_invalidated(tx, file_path, act_id):
    query=f'LOAD CSV  FROM "file:///{file_path}" AS row MATCH (e:Entity {{EntityId: row[0]}}) MATCH (a:Activity {{identifier: $act_id}}) MERGE (e)-[:WAS_INVALIDATED_BY]->(e)'

    tx.run(f'LOAD CSV  FROM "file:///{file_path}" AS row MATCH (e:Entity {{EntityId: row[0]}}) MATCH (a:Activity {{identifier: $act_id}}) MERGE (e)-[:WAS_INVALIDATED_BY]->(e)')
def load_graph(path):
    s=time.time()
    with driver.session() as session:
        session.write_transaction(delete_all)
    #make nodes
    #make nodes
    for folder in os.listdir(path):
        if os.path.isdir(os.path.join(path, folder)):
            for file in os.listdir(os.path.join(path, folder)):
                file_path = os.path.join(path, folder, file)
                if file=='df_metadata.csv':
                    with driver.session() as session:
                        t=time.time()
                        session.write_transaction(create_df_metadata, file_path)
                        print(f'imported DF Meta{time.time()-t}')
                if file.startswith('feature_metadata') and file.endswith('.csv'):
                    if 'derivations' in file or 'relations' in file:
                        continue
                    with driver.session() as session:
                        t=time.time()
                        session.write_transaction(create_feature_metadata, file_path)
                        print(f'imported Feature Meta{time.time()-t}')
                        print('Imported Feature Meta: ' + file_path)
                if file.startswith('entities') and file.endswith('.csv'):
                    with driver.session() as session:
                        t=time.time()
                        session.write_transaction(create_entities, file_path)
                        print(f'imported entities{time.time()-t}')
                        print('Imported entities: ' + file_path)
                if file.startswith('activities') and file.endswith('.json'):
                    with open(file_path) as f:
                        print(file_path)

                        file_data = json.load(f)
                        #for act in file_data:

                        query = "UNWIND $nodes as data CREATE (n:Activity) SET n = data;"
                        t=time.time()

                        result = session.run(query, nodes=file_data)
                        print(f'imported activity{time.time()-t}')

                        print('Imported activities: ' + file_path)
    print(f'imported nodes{time.time() - s}')

    query_index="CREATE INDEX node_index IF NOT EXISTS FOR (e:Entity) ON (e.EntityId)"
    result = session.run(query_index)
    query_index='CREATE INDEX act_index IF NOT EXISTS FOR (a:Activity) ON (a.identifier)'
    result = session.run(query_index)
    print(f'Created index{time.time() - s}')
    time.sleep(4)

    #make relations
    for folder in os.listdir(path):
        if os.path.isdir(os.path.join(path, folder)):
            with driver.session() as session:
                if folder.startswith('output'):
                    """
                    act_path = os.path.join(path, folder, 'activities.json')
                    with open(act_path) as f:
                        file_data = json.load(f)
                        act_id=file_data[0]['identifier']
                    """
                for file in os.listdir(os.path.join(path, folder)):
                    file_path = os.path.join(path, folder, file)

                    if file.startswith('generated') and file.endswith('.csv'):
                        act_id=[x for x in file.split('_') if x.startswith('activity')][0][:-4]
                        print(act_id)
                        with open(file_path) as f:
                            print(file_path)
                            query_generated=f'LOAD CSV  FROM "file:///{file_path}" AS row MATCH (e:Entity {{EntityId: row[0]}}) MATCH (a:Activity {{identifier: $act_id}}) MERGE (e)-[:WAS_GENERATED_BY]->(a)'
                            t=time.time()
                            result = session.run(query_generated, act_id=act_id)
                            print(f'imported generated{time.time()-t}')

                            #session.write_transaction(create_generated, file_path, act_id)
                            print('Imported generated: ' + file_path)
                    if file.startswith('used') and file.endswith('.csv'):
                        act_id=[x for x in file.split('_') if x.startswith('activity')][0][:-4]
                        with open(file_path) as f:
                            print(file_path)
                            query_used=f'LOAD CSV  FROM "file:///{file_path}" AS row MATCH (e:Entity {{EntityId: row[0]}}) MATCH (a:Activity {{identifier: $act_id}}) MERGE (a)-[:USED]->(e)'
                            t=time.time()
                            result = session.run(query_used, act_id=act_id)
                            print(f'imported used{time.time()-t}')

                            #session.write_transaction(create_used, file_path, act_id)
                            print('Imported used: ' + file_path)
                    if file.startswith('invalidated') and file.endswith('.csv'):
                        act_id=[x for x in file.split('_') if x.startswith('activity')][0][:-4]
                        with open(file_path) as f:
                            print(file_path)
                            query_invalidated = f'LOAD CSV  FROM "file:///{file_path}" AS row MATCH (e:Entity {{EntityId: row[0]}}) MATCH (a:Activity {{identifier: $act_id}}) MERGE (e)-[:WAS_INVALIDATED_BY]->(a)'
                            t=time.time()

                            result = session.run(query_invalidated, act_id=act_id)
                            print(f'imported invalidated{time.time()-t}')

                            #session.write_transaction(create_invalidated, file_path, act_id)
                            print('Imported invalidated: ' + file_path)
                    if file.startswith('derivations') and file.endswith('.csv'):
                        with driver.session() as session:
                            t=time.time()
                            session.write_transaction(create_derivations, file_path)
                            print(f'imported derivations{time.time()-t}')

                            print('Imported derivations: ' + file_path)
                    if file.startswith('df_metadata_from_relations') and file.endswith('.csv'):
                        with driver.session() as session:
                            t=time.time()
                            session.write_transaction(create_df_meta_from_relation, file_path)
                            print(f'imported meta from derivations{time.time()-t}')

                            print('Imported meta from derivations: ' + file_path)
                    if file.startswith('df_metadata_to_relations') and file.endswith('.csv'):
                        with driver.session() as session:
                            t=time.time()
                            session.write_transaction(create_df_meta_to_relation, file_path)
                            print(f'imported meta to derivations{time.time()-t}')

                            print('Imported meta to derivations: ' + file_path)
                    if file.startswith('feature_metadata') and file.endswith('from_relations.csv'):
                        with driver.session() as session:
                            t=time.time()
                            session.write_transaction(create_feature_meta_from_relation, file_path)
                            print(f'imported meta from derivations{time.time()-t}')

                            print('Imported meta from derivations: ' + file_path)
                    if file.startswith('feature_metadata') and file.endswith('to_relations.csv'):
                        with driver.session() as session:
                            t=time.time()
                            session.write_transaction(create_feature_meta_to_relation, file_path)
                            print(f'imported meta to derivations{time.time()-t}')

                            print('Imported meta to derivations: ' + file_path)
                    if file.startswith('feature_metadata') and file.endswith('derivations.csv'):
                        with driver.session() as session:
                            t=time.time()
                            session.write_transaction(create_feature_meta_derivation, file_path)
                            print(f'imported meta to derivations{time.time()-t}')

                            print('Imported meta to derivations: ' + file_path)
    print(f'finish import{time.time()-s}')

    driver.close()
