from neo4j import GraphDatabase, graph
import time
import json
import pprint
uri = "neo4j://localhost:7687"
SEPARATOR = '^^'

driver = GraphDatabase.driver(uri, auth=("neo4j", "admin"))
def create_used(tx):
    result1=tx.run(f'match (e:Entity {{EntityId:"K0^^key1^^0^^12"}})-[r:WAS_DERIVED_FROM*0..]->(m:Entity) match (m:Entity)-[w]-(a:Activity)  return e,r,m,a,w')
    return result1
def get_graph():
    query_generated=f'match (e:Entity {{EntityId:"K0^^key1^^0^^12"}})-[r:WAS_DERIVED_FROM*0..]->(m:Entity) match (m:Entity)-[w]-(a:Activity)  return e,r,m,a,w'
    with driver.session() as session:
        result = session.run(query_generated)
        t=time.time()
        result1=session.write_transaction(create_used)
    print(f'imported generated{time.time()-t}')
    #print(result)
    result=result.values()
    #pprint.pprint(result)

    nodes=dict()
    rels=[]
    for path in result:
        for el in path:
            if el.__class__==graph.Node:
                node = dict()
                label, = el.labels
                id=el.id
                if label=='Entity':
                    ent_id=el.get('EntityId')
                    record_id=el.get('record_id')
                    value, feature, index, op_num = ent_id.split(SEPARATOR)
                    node['class'] = 'entity'
                    node['label'] = feature + ': \\' + 'n' + value
                    description = f'<b>Value</b>: {value} <br/><b>Feature</b>: {feature} <br/><b>Index</b>: {index} <br/><b>Operation Number</b>: {op_num} <br/><b>Id</b>: {ent_id} <br/><b>Record Id</b>: {record_id}'
                    node['description'] = description
                elif label=='Activity':
                    #node['id']=id
                    if 'description' in el.keys():
                        node['label'] = el.get('description')
                    else:
                        node['label'] = el.get('function_name')
                    node['class'] = 'activity'
                    node['shape'] = 'ellipse'
                    description = ''
                    for k, v in el.items():
                        if k == 'attributes':
                            for k, v in el['attributes'].items():
                                description = description + f'<b>{k}</b>: {v} <br/> '
                        elif k != '_id':
                            description = description + f'<b>{k}</b>: {v} <br/> '
                    node['description'] = description
                nodes[id] = node
            else:
                #relazione
                #print(el)
                if type(el)==list:
                    for rel in el:
                        #print(rel.start_node.id, rel.end_node.id, rel.type)
                        rels.append([rel.start_node.id, rel.end_node.id, rel.type])

                else:
                    #print(el.start_node.id,el.end_node.id,el.type)
                    rels.append([el.start_node.id, el.end_node.id, el.type])

    #pprint.pprint(nodes)
    #pprint.pprint(nodes.values())
    #print(len(rels))
    #pprint.pprint(rels)
    rels = set(tuple(x) for x in rels)
    rels = [ list(x) for x in rels ]
    #print(len(rels))
    #pprint.pprint(rels)
    toJSON = list(nodes.values())
    #print(toJSON)
    print(len(nodes.keys()))
    return rels,nodes
#get_graph()