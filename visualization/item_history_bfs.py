
SEPARATOR='^^'
def search_activity_with_feature(feature,act):

    """

    :param feature: features
    :param act: all the activities
    :return: activities that used or generated the feature or activities that hasn't used or generated features(instance generation)
    """
    # cerca tutte le attività che hanno usato o generato quella colonna
    act_with_feature=[]
    for a in act:
        attributes_act = a['attributes'].keys()
        if 'used_features' in attributes_act:
            used_features = a['attributes']['used_features']
            if type(used_features) != list:
                used_features = [used_features]
            if feature in used_features:
                act_with_feature.append((a['identifier'], int(a['attributes']['operation_number']),a,feature))
        else:
            # instance generation
            act_with_feature.append((a['identifier'], int(a['attributes']['operation_number']), a,[]))
        if 'generated_features' in attributes_act:
            generated_features = a['attributes']['generated_features']
            if feature in generated_features:
                act_with_feature.append((a['identifier'], int(a['attributes']['operation_number']),a,feature))
    # devo eliminare le attività meno specifiche con lo stesso num op. nel caso della instance generation aggiungo anche le attività senza used_features
    # questo mi porta dei problemi. Nella ricerca dell'id della prima entità considererei sia l'attività che ha come used feature quella che cerco
    # sia quella che non ha used feature. Entrambe le attività hanno lo stesso num op e quindi durante la ricerca dell entità troverei un numero n di entità tutte uguali pari al numero delle attività che ho considerato
    # quindi devo considerare solo la più specifica( cioè quella con la used feature uguale a quella che cerco) ed eliminare quelle senza used feature
    # Es. act1(instance generation,used feature =[], num_op=3), act2(instance generation,used feature =[quella che cerco], num_op=3)
    # non considero la act1.
    correct_act_with_feature =[]
    for act in act_with_feature:
        num_op=act[1]
        same_num_op=[ac for ac in act_with_feature if ac[1]==num_op]
        if len(same_num_op)==1:
            correct_act_with_feature.append(act)
        else:
            for acts in same_num_op:
                if len(acts[3])>0 and acts not in correct_act_with_feature:
                    correct_act_with_feature.append(acts)
    return correct_act_with_feature

def node_list_to_json(node_list,activities,entities):
    """
    function to convert the node list in a json with information for the layout of the graph
    :return: json of the node list with all the information for the creation of the graph
    """
    json_nodes=[]
    for node in node_list:
        dict_node=dict()
        #if the node is an activity
        if node.startswith('activity'):
            invalidation=False
            if node.endswith('^^inv'):
                invalidation=True
                node=node[:-5]
            activity=activities.find({'identifier':node})
            activity=list(activity)[0]
            if invalidation:
                dict_node['label']='Invalidation'
            elif 'description' in activity['attributes'].keys():
                dict_node['label']=activity['attributes']['description']
            else:
                dict_node['label']=activity['attributes']['function_name']
            dict_node['class'] = 'activity'
            dict_node['shape']='ellipse'
            description=''
            for k,v in activity.items():
                if k=='attributes':
                    for k,v in activity['attributes'].items():
                         description = description + f'<b>{k}</b>: {v} <br/> '
                elif k!='_id':
                    description = description + f'<b>{k}</b>: {v} <br/> '
            dict_node['description']=description
        elif node.startswith('Imputation'):
            dict_node['label'] = 'Used Columns'
            dict_node['class'] = 'columns'
            description = f'<b>Used Columns</b>: {node[11:]} <br/> Columns used to impute the missing values'
            dict_node['description'] = description
        elif node.startswith('Instance'):
            dict_node['label'] = 'Used Columns'
            dict_node['class'] = 'columns'
            description = f'<b>Used Columns</b>: {node[8:]} <br/> Columns used to do instance generation the missing values'
            dict_node['description'] = description

        else:
            #if the node is an entity
            entity = entities.find({'id': node})
            entity = list(entity)[0]
            value, feature, index, op_num = node.split(SEPARATOR)
            record_id = entity['record_id']
            dict_node['class'] = 'entity'
            dict_node['label'] = feature + ': \\' + 'n' + value
            description = f'<b>Value</b>: {value} <br/><b>Feature</b>: {feature} <br/><b>Index</b>: {index} <br/><b>Operation Number</b>: {op_num} <br/><b>Id</b>: {node} <br/><b>Record Id</b>: {record_id}'
            dict_node['description'] = description

        json_nodes.append(dict_node)
    return json_nodes


#vado a eseguire la ricerca di generazione solo nel caso in cui non ho corrispondenze nella derivazione, e cerco solo nelle attività che operano su quella colonna
def get_item_history(value,feature,index,activities,relations,derivations,entities):
    """

    :param value: value of the entity of which i want to know the history
    :param feature: feature of the entity
    :param index: index of the entity
    :param activities: db of activities
    :param relations: db of relations
    :param derivations: db of derivations
    :param entities: db of entities
    :return: history of the entity

    return the history of the searched entity
    the history is composed of 2 lists:
        node_list: list of the nodes of the graph to pass to graph.js
        edge_list: list of the edges of the graph to pass to graph.js
    use a bfs algorithm, searching derivation to build the history of an entity is like exploring a tree
    """
    # Get activities related to entity_ids:
    print(index)
    act = list(activities.find({}))
    act_with_feature = search_activity_with_feature(feature,act)
    # ordino le attività che hanno usato o generato quella feature in ordine decrescente, in questo modo posso andaare a a ritroso
    act_with_feature.sort(key=lambda tupl: tupl[1], reverse=True)
    edge_list = []
    node_list = []
    # cerco la prima derivazione, devo provare con tutti gli operation number delle attività che hanno usato
    # o generato quella colonna, perchè da interfaccia inserisco solo indice valore e feature
    # in caso di problemi con entità duplicate sostituire con un set
    # queue for the bfs algorithm
    queue = []
    # finding the entity of the input value
    # use all the activities that used or generated the input value
    for activity in act_with_feature:
        entity_id = value + SEPARATOR + feature + SEPARATOR + index + SEPARATOR + str(activity[1])
        # search for invalidation
        act_rel = relations.find(
            {'$and': [
                {'id': str(activity[0])},
                {'invalidated': entity_id}
            ]}, {'_id': 0,'id':1})
        inv_act=list(act_rel)
        if len(inv_act)>0:
            node_list.append(entity_id)
            node_list.append(inv_act[0]['id']+'^^inv')
            edge_list.append([0,1])
        # search for derivations
        used_id = derivations.find({'gen': entity_id}, {'_id': 0, 'used': 1})
        # generated_id=derivations.find({'used':entity_id},{'_id':0,'gen':1})
        used_node = list(used_id)
        print(f'used {used_node}')
        #print(used_node)
        # if there is a derivation
        if len(used_node) > 0:
            #maybe i can avoid this
            # first step of derivation
            for node in used_node:
                node_id=node['used']
                op_num_gen = str(activity[1])
                feature_used = node_id.split(SEPARATOR)[1]
                # cerco l'attività che ha usato used_id
                # searching the activity that used or generated the uses_id
                #print(node_id)
                used_act = activities.find({'$and': [{'$or': [{'attributes.used_features': str(feature_used)},
                                                              {'attributes.generated_features': str(feature_used)}]},
                                                     {'attributes.operation_number': str(op_num_gen)}
                                                     ]},
                                           {'_id': 0, 'identifier': 1,'attributes':1})
                used_act=list(used_act)[0]
                #print(used_act)
                used_act_id = used_act['identifier']
                # se non l'ho aggiunto prima con l'invalidation
                #if i haven't add the first node with the invalidation i put it in the node list
                if len(node_list) == 0:
                    node_list.append(entity_id)
                # append the activity node and edge and the derivation node
                node_list.append(used_act_id)
                edge_list.append([len(node_list) - 1, 0])
                node_list.append(node_id)
                # aggiungo nella coda il nodo da espandere con il relativo indice nella lista di nodi
                #insert the node in the queue for the next iteration
                queue.append((node_id,node_list.index(node_id)))
                edge_list.append([len(node_list) - 1, len(node_list) - 2])
                # imputation node for used columns
                if used_act['attributes']['function_name'] == 'Imputation':
                    node_list.append(f'Imputation {used_act["attributes"]["used_features"]}')
                    edge_list.append([len(node_list) - 1, node_list.index(used_act_id)])
            break

    #print(node_list)
    # se l'entità non ha mai subito operazioni provo solo a cercarla nelle entità

    # if the node hasn't derivations or invalidations
    if len(node_list)==0:
        for activity in act_with_feature:
            entity_id = value + SEPARATOR + feature + SEPARATOR + index + SEPARATOR + str(activity[1])
            entity=entities.find({'id':entity_id})
            nodes=list(entity)
            if len(nodes)>0:
                node=nodes[0]['id']
                node_list.append(node)
                if activity[2]['attributes']['function_name'] == 'Instance Generation':
                    node_list.append(activity[2]['identifier'])
                    edge_list.append((1,0))
                    if 'used_features' in activity[2]['attributes']:
                        node_list.append(f'Instance {activity[2]["attributes"]["used_features"]}')
                        edge_list.append([len(node_list) - 1, node_list.index(activity[2]['identifier'])])
        #print(node_list)
        #if the value has not been changed
        if len(node_list)==0:
            entity_id = value + SEPARATOR + feature + SEPARATOR + index + SEPARATOR + '-1'
            entity = entities.find({'id': entity_id})
            nodes = list(entity)
            if len(nodes) > 0:
                node = nodes[0]['id']
                node_list.append(node)
    print(f'primo giro {queue}')
    #next iterations of the bfs
    #same logic of the first iteration
    while len(queue)>0:
        print(queue)
        entity_id,index_node=queue.pop(0)
        feature=entity_id.split(SEPARATOR)[1]
        num_op=entity_id.split(SEPARATOR)[3]
        act = list(activities.find({}))
        act_with_feature = search_activity_with_feature(feature, act)
        act_with_feature.sort(key=lambda tupl: tupl[1], reverse=True)
        for activity in act_with_feature:
            act_rel = relations.find(
                {'$and': [
                    {'id': str(activity[0])},
                    {'invalidated': entity_id}
                ]}, {'_id': 0,'id':1})
            inv_act = list(act_rel)
            if len(inv_act) > 0:
                if inv_act[0]['id'] != node_list[-1]:
                    node_list.append(inv_act[0]['id']+'^^inv')
                    edge_list.append([index_node, len(node_list)-1])
                break

        used_id = derivations.find({'gen': entity_id}, {'_id': 0, 'used': 1})
        used_node = list(used_id)
        if len(used_node) > 0:
            print(used_node)
            for node in used_node:
                node_id = node['used']
                op_num_gen = entity_id.split(SEPARATOR)[3]
                feature_used = node_id.split(SEPARATOR)[1]
                # cerco l'attività che ha usato used_id

                used_act = activities.find({'$and': [{'$or': [{'attributes.used_features': str(feature_used)},
                                                              {'attributes.generated_features': str(feature_used)}]},
                                                     {'attributes.operation_number': str(op_num_gen)}
                                                     ]},
                                           {'_id': 0, 'identifier': 1,'attributes':1})
                used_act = list(used_act)[0]
                used_act_id = used_act['identifier']
                # se non l'ho aggiunto prima con l'invalidation
                if len(node_list) == 0:
                    node_list.append(entity_id)
                node_list.append(used_act_id)
                edge_list.append([len(node_list) - 1,index_node])
                node_list.append(node_id)
                # aggiungo nella coda il nodo da espandere con il relativo indice nella lista di nodi
                queue.append((node_id, node_list.index(node_id)))
                edge_list.append([len(node_list) - 1, len(node_list) - 2])
                print(used_act['attributes']['function_name'])
                if used_act['attributes']['function_name'] == 'Imputation':
                    node_list.append(f'Imputation {used_act["attributes"]["used_features"]}')
                    edge_list.append([len(node_list) - 1,node_list.index(used_act_id)])

        else:
            # if the node was generated by an activity, create the node of the activity (not first entity of a df or secord df for a join)
            # only for instance generation,join(NaNs generated in left, right and outer join), space transformation
            instance_gen_act = activities.find({'attributes.operation_number': str(num_op)},
                                       {'_id': 0, 'identifier': 1, 'attributes': 1})
            instance_gen_act = list(instance_gen_act)
            for gen_act in instance_gen_act:
                act_rel = relations.find(
                    {'$and': [
                        {'id': gen_act['identifier']},
                        {'generated': entity_id}
                    ]}, {'_id': 0, 'id': 1})
                gen_rel = list(act_rel)
                if len(gen_rel) > 0:
                    if gen_act['attributes']['function_name'] == 'Instance Generation':
                        node_list.append(gen_act['identifier'])
                        edge_list.append((len(node_list) - 1, node_list.index(entity_id)))
                        if 'used_features' in gen_act['attributes']:
                            node_list.append(f'Instance {gen_act["attributes"]["used_features"]}')
                            edge_list.append([len(node_list) - 1, node_list.index(gen_act['identifier'])])
                    if gen_act['attributes']['function_name'] == 'Join':
                        node_list.append(gen_act['identifier'])
                        edge_list.append((len(node_list) - 1, node_list.index(entity_id)))
                    if gen_act['attributes']['function_name'] == 'Space Transformation':
                        node_list.append(gen_act['identifier'])
                        edge_list.append((len(node_list) - 1, node_list.index(entity_id)))
                    break
    print('Nodes:')
    print(node_list)
    print('Edges')
    print(edge_list)
    #print(node_list_to_json(node_list,activities,entities))
    return edge_list,node_list_to_json(node_list,activities,entities)
