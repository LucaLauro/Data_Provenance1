from neo4j import GraphDatabase, graph
import time
import json
import pprint
uri = "neo4j://localhost:7687"
SEPARATOR = '^^'

driver = GraphDatabase.driver(uri, auth=("neo4j", "admin"))

def get_graph(tx):
    #query_generated1 = f'match (e:Entity {{EntityId:"K0^^key1^^0^^12"}})-[r:WAS_DERIVED_FROM*0..]->(m:Entity) match (m:Entity)-[w]-(a:Activity)  return e,r,m,a,w'
    result1=tx.run(f'MATCH (n:FeatureMeta) RETURN n.FeatureMetaId ORDER BY n.FeatureMetaId')
    #result1=tx.run(query_generated1)
    return result1
def get_feature_operations():
    df_meta=dict()
    with driver.session() as session:
        query_generated = f'MATCH (n:FeatureMeta) RETURN n.FeatureMetaId ORDER BY n.FeatureMetaId'

        result = session.run(query_generated)
        result1=session.write_transaction(get_graph)

    #print(result)
    result=result.values()
    #pprint.pprint(result)
    feature_operations=[ x[0] for x in result]

    return feature_operations
