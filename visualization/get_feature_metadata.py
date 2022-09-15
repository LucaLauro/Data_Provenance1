from neo4j import GraphDatabase, graph
import time
import json
import pprint
uri = "neo4j://localhost:7687"
SEPARATOR = '^^'

driver = GraphDatabase.driver(uri, auth=("neo4j", "admin"))

def get_graph(tx):
    #query_generated1 = f'match (e:Entity {{EntityId:"K0^^key1^^0^^12"}})-[r:WAS_DERIVED_FROM*0..]->(m:Entity) match (m:Entity)-[w]-(a:Activity)  return e,r,m,a,w'
    result1=tx.run(f'MATCH (c:FeatureMeta)-[r:FEATURE_META_DERIVATION*0..]->(b:FeatureMeta{{feature_name:"A_x_6"}}) match (c:FeatureMeta)-[w]-(a:Activity)RETURN c,r,b,w,a')
    #result1=tx.run(query_generated1)
    return result1
def get_feature_metadata_neo4j(feature_meta_id):
    feature_metadata=dict()
    with driver.session() as session:
        query_generated = f'MATCH (c:FeatureMeta)-[r:FEATURE_META_DERIVATION*0..]->(b:FeatureMeta{{feature_name:"{str(feature_meta_id)}"}}) match (c:FeatureMeta)-[w]-(a:Activity)RETURN c,r,b,w,a'

        result = session.run(query_generated)
        result1=session.write_transaction(get_graph)

    #print(result)
    result=result.values()
    pprint.pprint(result)

    nodes=dict()
    rels=[]
    for path in result:
        for el in path:
            if el.__class__==graph.Node:
                node = dict()
                label, = el.labels
                id=el.id
                if label=='FeatureMeta':
                    ent_id=el.get('FeatureMetaId')
                    node['class'] = 'featuremeta'
                    node['label'] = ent_id
                    feature_metadata[ent_id]=dict()
                    for k, v in el.items():
                        if k != 'FeatureMetaId' and k != 'feature_name' and k != 'dtype' and k!='CodeLine' and k!='Code':
                            if 'nan,' in v or 'nan]' in v:
                                v=v.replace('nan','\'nan\'')
                            print(k,v)
                            feature_metadata[ent_id][k]=eval(v)
                        if k == 'feature_name' or k == 'dtype' or k=='CodeLine' or k=='Code':
                            feature_metadata[ent_id][k]=v

                    #description = f'<b>Value</b>: {value} <br/><b>Feature</b>: {feature} <br/><b>Index</b>: {index} <br/><b>Operation Number</b>: {op_num} <br/><b>Id</b>: {ent_id} <br/><b>Record Id</b>: {record_id}'
                    #node['description'] = description
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
    #print(len(nodes.keys()))
    #print(nodes)
    #print(rels)
    dict_relations=dict.fromkeys(nodes.keys())
    for el in rels:
        if dict_relations[el[0]]==None:
            dict_relations[el[0]]=[el[2]]
        else:
            dict_relations[el[0]].append(el[2])
        if dict_relations[el[1]] == None:
            dict_relations[el[1]] = [el[2]]
        else:
            dict_relations[el[1]].append(el[2])
    #print(dict_relations)
    dict_relations=[k for k,v in dict_relations.items() if v==['USED']]

    print(len(rels),len(nodes.keys()))

    for el in dict_relations:
        print(el)
        del nodes[el]

    rels=[x for x in rels if x[0] not in dict_relations]
    #print(len(rels),len(nodes.keys()))
    print(feature_metadata)
    print(len(feature_metadata))
    return rels,nodes,feature_metadata
