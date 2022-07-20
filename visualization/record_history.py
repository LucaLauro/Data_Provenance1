import sys
sys.path.append('../')

from visualization.get_item_history_for_record_history import get_item_history, node_list_to_json, search_activity_with_feature

SEPARATOR = '^^'


def compact_nodes(node_list, edge_list, entities, activities):
    """

    :param node_list: list of list, each list is the history of an entity
    :param edge_list: list of list of the edges
    :param entities: db entities
    :param activities: db activities
    :return:
        json_nodes: flatten list of the nodes
        flat_edges: flatten list of the edges
    """

    index_used = []
    nodes_result = []
    edge_result = []

    # scansiono tutte le liste delle storie e le comparo con le seguenti
    # se trovo un nodo in comune devo prendere i nodi successivi(che nella lista sono i precedenti perchè è in ordine invertito) e li concateno alla
    # lista che sto esaminando. di conconseguenza devo prendere anche gli archi relativi a quei nodi, concatenarli alla lista di archi dell'altra lista
    # e li scalo di una quantità pari alla lista in esame. inoltre devo collegare l'ultimo elemento che concatieno(che visto l'ordine inverso è il nodo
    # immediatamente successivo al nodo in comune)

    # scan the histories and compare each history with the following
    # if there is a common node compare the rest of the history(after the common node,rebember that the history has inverted order,higher index are older entities in the history)
    # if two partial histories are equal, append the entities previous the common node to the actual history
    # the index of the compacted history is put in a used_list so it won't be executed
    for i in range(len(node_list)):
        # if not already compacted
        if i not in index_used:
            temp_node = node_list[i]
            temp_edge = edge_list[i]
            for index, node in enumerate(node_list[i]):
                for relative_index, other_list in enumerate(node_list[i + 1:]):
                    other_index = relative_index + i + 1
                    if other_index not in index_used:
                        if node in other_list:
                            # common node
                            index_item = other_list.index(node)
                            if node_list[i][index:] == other_list[index_item:]:
                                # same partial history
                                # print(f'elementi comuni{node_list[i][index]}')
                                # se ci sono più unità invalidate dalla stessa operazione che si trova in ultimo posto nelle liste non le considero
                                if len(node_list[i][index:]) > 0:
                                    # invalidation activity in the last position makes problem
                                    if not node_list[i][index:][0].endswith('^^inv'):
                                        len_temp = len(temp_node)
                                        temp_node = temp_node + other_list[:index_item]
                                        edges = edge_list[other_index][:index_item]
                                        # aumento i valori degli archi della lunghezza precedente all'unione delle liste dei nodi in esame
                                        if len(edges) > 0:
                                            # compact the edges
                                            edges = [(x + len_temp, y + len_temp) for (x, y) in edges]
                                            # collego l'ultimo nodo con quello in comune
                                            # connect the last node with the common node
                                            edges = edges[:-1] + [(index, edges[-1][1])]

                                        temp_edge = temp_edge + edges
                                        index_used.append(other_index)
            nodes_result.append(temp_node)
            edge_result.append(temp_edge)

    flat_json_nodes = []
    flat_edges = []
    # la lista di liste di archi e di nodi devono diventare delle liste per poter essere eseguite correttamente dal front end.
    # bisogna far scalare tutti gli archi
    # flat the list of nodes and edges( not all the history are compacted, not all have element in common)

    for i in range(len(nodes_result)):
        if i > 0:
            if len(edge_list[i]) > 0:
                for ed in edge_result[i]:
                    new_edge = (ed[0] + len(flat_json_nodes), ed[1] + len(flat_json_nodes))
                    flat_edges.append(new_edge)
                for el in nodes_result[i]:
                    flat_json_nodes.append(el)
            else:
                for el in nodes_result[i]:
                    flat_json_nodes.append(el)
        else:
            for el in nodes_result[i]:
                flat_json_nodes.append(el)
            for ed in edge_result[i]:
                flat_edges.append(ed)
    json_nodes = node_list_to_json(flat_json_nodes, entities, activities)
    # for i in range(len(nodes_result)):
    #    print(json_nodes[i])
    #    print(edge_result[i])
    # print(flat_json_nodes)
    # print(flat_edges)
    return json_nodes, flat_edges


def get_record_id(value, feature, index, entities, activities, derivations, relations):
    """
    :return: record_id of the searched entity
    """
    act = list(activities.find({}))
    act_with_feature = search_activity_with_feature(feature, act)
    act_with_feature.sort(key=lambda tupl: tupl[1], reverse=True)
    entity_id = str()
    for activity in act_with_feature:
        entity_id = value + SEPARATOR + feature + SEPARATOR + index + SEPARATOR + str(activity[1])
        act_rel = relations.find(
            {'$and': [
                {'id': str(activity[0])},
                {'invalidated': entity_id}
            ]}, {'_id': 0, 'id': 1})
        inv_act = list(act_rel)
        if len(inv_act) > 0:
            break
        used_id = derivations.find({'gen': entity_id}, {'_id': 0, 'used': 1})
        # generated_id=derivations.find({'used':entity_id},{'_id':0,'gen':1})
        used_node = list(used_id)
        if len(used_node) > 0:
            break
    # print(entity_id)
    rec = entities.find({'id': entity_id}, {'_id': 0, 'record_id': 1})
    record = list(rec)
    print('Record Id:')
    print(record)
    record_id = record[0]['record_id']
    return record_id


def get_record_operation(value, feature, index, activities, relations, derivations, entities):
    """

    :param value: value of an entity in the record
    :param feature: feature of an entity in the record
    :param index: index of an entity in the record
    :param activities: db activities
    :param relations: db relations
    :param derivations: db derivations
    :param entities: db entities
    :return: flatten list of the history of all the entities in the record
    """
    # Get the entities of the record:
    record_id = get_record_id(value, feature, index, entities, activities, derivations, relations)
    # Get all the entities with the same the record_id
    entities_id = entities.find({'record_id': record_id}, {'id': 1, '_id': 0}).distinct('id')

    id_list = list(entities_id)
    # list of tuple [(entity_id, num_op),...] sorted in decrescent num op
    entity_op_tuple = [(x, int(x.split(SEPARATOR)[3])) for x in id_list]
    entity_op_tuple.sort(key=lambda tupl: tupl[1], reverse=True)
    # print(entity_op_tuple)

    # Get list of activities id related to the record:
    temp = []
    node_list = []
    edge_list = []
    # eseguo la ricerca della storia solo per gli ultimi elementi del record(num op più alto)
    # tutti i nodi che sono all'interno della storia di un altro non sono rieseguiti

    # seach the history of the entity with the bigger number of the operation
    # in each iteration if other nodes are in the history of the precessing entity, they are inserted in a temp list
    # nodes in the temp list are already in an history so they are not executed in get_item_history
    for ent in entity_op_tuple:
        if ent not in temp:
            item_nodes, nodes, edges = get_item_history(ent[0], activities, relations, derivations, entities)
            node_list.append(nodes)
            edge_list.append(edges)
            # print(item_nodes)
            for en in item_nodes:
                if (en, int(en.split(SEPARATOR)[3])) not in temp:
                    temp.append((en, int(en.split(SEPARATOR)[3])))
    # dopo che ho ottenuto la storia di ogni elemento del record bisogna compattarla per collegare le storie con dei nodi in comune(vedi one hot encoding)
    # compact the list of list of nodes and edges in a flatten history
    # the compacted histories are merged if have same partial history (like one hot encoded variables, all the n entities generated derived from only one entity,but there are n histories)
    node_list, edge_list = compact_nodes(node_list, edge_list, entities, activities)

    return edge_list, node_list
