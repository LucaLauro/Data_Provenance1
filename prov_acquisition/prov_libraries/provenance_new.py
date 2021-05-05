import numpy as np
import pandas as pd
import uuid
import os
import time
import json
from multiprocessing import Process, Queue

# global variables for multiprocessing operations
global current_relation
global new_entities
global current_derivations
current_relation = []
new_entities = []
current_derivations = []


class Provenance:
    # Constants:
    NAMESPACE_FUNC = 'activity:'
    NAMESPACE_ENTITY = 'entity:'
    INPUT = 'input'
    OUTPUT = 'output'
    LIST_REL_SIZE=25000
    CHUNK_SIZE = 15000
    CHUNK_INDEX_SIZE = 1000
    # PROV-N objects
    ENTITY = 'prov:entity'
    GENERATED_ENTITY = 'prov:generatedEntity'
    USED_ENTITY = 'prov:usedEntity'
    ACTIVITY = 'prov:activity'

    # PROV-N relations
    GENERATION = 'gen'
    USE = 'used'
    DERIVATION = 'wasDerivedFrom'
    INVALIDATION = 'wasInvalidatedBy'

    DEFAULT_PATH = 'prov_results/'

    SEPARATOR = '^^'

    def __init__(self, df, results_path=None):
        # Inizialize provenance activities, relations and new entities
        self.current_act = []
        self.current_relations = []
        self.new_entities = []
        self.current_derivations=[]
        # Initialize operation number:
        self.operation_number = -1
        self.instance = self.OUTPUT + str(self.operation_number)

        # Set input dataframe parameters:
        self.current_m, self.current_n = df.shape
        self.current_columns = df.columns
        self.current_index = df.index
        self.df = pd.DataFrame.copy(df)
        # Set results path:
        self.results_path = self.DEFAULT_PATH + time.strftime('%Y%m%d-%H%M%S') if results_path is None else results_path

        # Create provenance entities of the input dataframe:
        self.current_ent = self.create_prov_entities(df, self.INPUT)
        # inizialize second dataframe
        self.second_df = []
        self.current_second_ent = []
        # Save input provenance document
        # self.save_json_prov(os.path.join(self.results_path, self.INPUT))

    def timing(f):
        def wrap(*args):
            # Get timing of provenance function:
            time1 = time.time()
            ret = f(*args)
            time2 = time.time()
            # text = '{:s} function took {:.3f} ms'.format(f.__name__, (time2-time1)*1000.0)
            text = '{:s} function took {:.3f} sec.'.format(f.__name__, (time2 - time1))
            print(text)

            self = args[0]

            # Get new folder size
            # new_folder_name = self.INPUT if self.operation_number == -1 else self.OUTPUT + str(self.operation_number)
            # new_folder_path = os.path.join(self.results_path, new_folder_name)
            # total = 0
            # for file in os.scandir(new_folder_path):
            #    if file.is_file():
            #        total += file.stat().st_size

            # size = get_size_format(total)

            # Create folder if not exists
            nameFile = os.path.join(self.results_path, self.INPUT)
            if not os.path.exists(nameFile):
                os.makedirs(nameFile)

            # Save infos in log file
            pipeline_path = os.path.join(self.results_path, 'log_file.txt')
            with open(pipeline_path, 'a+') as log_file:
                log_file.write('[' + time.strftime("%d/%m-%H:%M:%S") + ']' + text + '\n')
                # log_file.write(new_folder_name +' folder size: ' + str(size) + '\n')

            # duration = time2 - time1
            # print(f.__name__
            #      + ' finished in '
            #      + time.strftime('%H:%M:%S', time.gmtime(duration)))

            return ret

        return wrap

    def create_entity(self, record_id, value, feature_name, index, instance):
        """Create a provenance entity.
        Return a dictionary with the id and the record_id of the entity."""
        # Get attributes:
        id=value+self.SEPARATOR+feature_name+self.SEPARATOR+str(index)+self.SEPARATOR+str(instance)
        # Add entity to new numpy array entities:
        ent = {'id': id, 'record_id': record_id}
        self.new_entities.append(ent)
        new_entities.append(ent)
        return ent

    def create_activity(self, function_name, features_name=None, description=None, other_attributes=None,generated_features=None,deleted_used_features=None, deleted_records=None):
        """Create a provenance activity and add to the current activities array.
        Return the id of the new prov activity."""
        # Get default activity attributes:
        attributes = {}
        attributes['function_name'] = function_name
        if features_name is not None:
            attributes['used_features'] = features_name
        if description is not None:
            attributes['description'] = description
        if generated_features is not None:
            attributes['generated_features'] = generated_features
        if deleted_used_features is not None:
            attributes['deleted_used_features'] = deleted_used_features
        if deleted_records is not None:
            attributes['deleted_records'] = deleted_records
        attributes['operation_number'] = str(self.operation_number)

        # Join default and extra attributes:
        if other_attributes is not None:
            attributes.update(other_attributes)

        act_id = self.NAMESPACE_FUNC + str(uuid.uuid4())

        # Add activity to current provenance document:
        act = {'identifier': act_id, 'attributes': attributes}
        self.current_act.append(act)

        return act_id


    def create_relation(self, act_id, generated=None, used=None, invalidated=None):
        """Add a relation to the current relations array.
        Return the new relation."""
        if generated is None:
            generated = []
        if used is None:
            used = []
        if invalidated is None:
            invalidated = []
        max_length=max(len(generated),len(used),len(invalidated))
        if max_length > self.LIST_REL_SIZE:
            for i in range(0, max_length, self.LIST_REL_SIZE):
                # split in multiple activity if the list of generated, used or invalidated are too big
                partial_rel = dict()
                partial_rel['id'] = act_id
                if len(generated)>0:
                    if len(generated)>i :
                        partial_rel['generated'] = generated[i:i+self.LIST_REL_SIZE]
                if len(used) > 0:
                    if len(used) > i:
                        partial_rel['used'] = used[i:i+self.LIST_REL_SIZE]
                if len(invalidated) > 0:
                    if len(invalidated) > i:
                        partial_rel['invalidated'] = invalidated[i:i+self.LIST_REL_SIZE]
                self.current_relations.append(partial_rel)
                current_relation.append(partial_rel)
        else:
            relation = dict()
            relation['id'] = act_id
            if len(generated) > 0:
                relation['generated'] = generated
            if len(used) > 0:
                relation['used'] = used
            if len(invalidated) > 0 :
                relation['invalidated'] = invalidated
            self.current_relations.append(relation)
            current_relation.append(relation)

    def create_derivation(self,used_ent, gen_ent):
        """
        Add a derivation to the current relations array.
        """
        derivation={'gen':gen_ent,'used':used_ent}
        self.current_derivations.append(derivation)
        current_derivations.append(derivation)

    def save_entities_multiproc1(self, entities, ents_path, ind):
        """
            save entities json
        """
        output_name = ents_path + '.json' if ind // self.CHUNK_SIZE == 0 else ents_path + '_' + str(
            ind // self.CHUNK_SIZE) + '.json'
        with open(output_name, 'w', encoding='utf-8') as ents_file:
            ents = entities[ind:ind + self.CHUNK_SIZE]
            json.dump(ents, ents_file, ensure_ascii=False, indent=4)

    def save_entities_multiproc(self, entities, ents_path):
        """
            split the entities list in n list of chunck size lenght and launch a process for each
        """
        print(len(entities))
        process_list = []
        for ind in range(0, len(entities), self.CHUNK_SIZE):
            p = Process(target=self.save_entities_multiproc1, args=(entities, ents_path, ind))
            process_list.append(p)
        cpu_num = os.cpu_count()
        num_proc_run = 0
        index_proc = 0
        for p in process_list:
            if num_proc_run == cpu_num:
                num_proc_run = 0
                for process_running in process_list[index_proc:index_proc + cpu_num]:
                    process_running.join()
                index_proc += cpu_num
            num_proc_run += 1
            p.start()

    def save_deriv_multiproc1(self, deriv_path, ind):
        """
            save derivations json
        """
        output_name = deriv_path + '.json' if ind // self.CHUNK_SIZE == 0 else deriv_path + '_' + str(
            ind // self.CHUNK_SIZE) + '.json'
        with open(output_name, 'w', encoding='utf-8') as deriv_file:
            deriv = self.current_derivations[ind:ind + self.CHUNK_SIZE]
            json.dump(deriv, deriv_file, ensure_ascii=False, indent=4)

    def save_deriv_multiproc(self,deriv_path):
        """
            split the derivations list in n list of chunck size lenght and launch a process for each
        """
        process_list = []
        for ind in range(0, len(self.current_derivations), self.CHUNK_SIZE):
            p = Process(target=self.save_deriv_multiproc1, args=(deriv_path, ind))
            process_list.append(p)
        cpu_num = os.cpu_count()
        num_proc_run = 0
        index_proc = 0
        for p in process_list:
            if num_proc_run == cpu_num:
                num_proc_run = 0
                for process_running in process_list[index_proc:index_proc + cpu_num]:
                    process_running.join()
                index_proc += cpu_num
            num_proc_run += 1
            p.start()
    @timing
    def create_prov_entities(self, dataframe, instance=None):
        """Return a numpy array of new provenance entities related to the dataframe."""
        instance = self.instance if instance is None else instance
        columns = dataframe.columns
        indexes = dataframe.index
        tot_ent = len(columns) * len(indexes)
        if self.operation_number > -1:
            # adding a second dataframe to inputs, join in progress
            nameFile = os.path.join(self.results_path, self.INPUT + '_' + str(self.operation_number))
        else:
            nameFile = os.path.join(self.results_path, self.INPUT)
        """Save provenance in json file."""

        if not os.path.exists(nameFile):
            os.makedirs(nameFile)

        ents_path = os.path.join(nameFile, 'entities')
        values = dataframe.values
        # Create output array of entities:
        from_ent = 0
        num_ent = 0
        entities = np.empty(dataframe.shape, dtype=object)
        for i in range(dataframe.shape[0]):
            record_id = str(uuid.uuid4())
            for j in range(dataframe.shape[1]):
                value = str(values[i, j])
                # Add entity to current provenance document:
                entities[i][j] = self.create_entity( record_id, value, columns[j], indexes[i],
                                                    self.operation_number)

        # Save input entities in json files
        self.save_entities_multiproc(self.new_entities, ents_path)

        return entities

    def set_current_values(self, dataframe, entities_out):
        """Update values of current entities after every operation."""
        # Set output dataframe entities:
        self.current_m, self.current_n = dataframe.shape
        self.current_columns = dataframe.columns
        self.current_index = dataframe.index
        self.current_ent = entities_out
        self.df = pd.DataFrame.copy(dataframe)

    def initialize(self):
        """initialize provenance parameter"""
        self.current_act = []
        self.current_relations = []
        self.new_entities = []
        self.current_derivations = []

        # Increment operation number:
        self.operation_number += 1
        self.instance = self.OUTPUT + str(self.operation_number)

    def save_rel_multiproc1(self, rel_path, j):
        """
            save relations json
        """
        output_name = rel_path + '.json' if j // 3 == 0 else rel_path + '_' + str(
            j // 3) + '.json'
        with open(output_name, 'w', encoding='utf-8') as rel_file:
            rels = self.current_relations[j:j + 3]
            json.dump(rels, rel_file, ensure_ascii=False, indent=4)

    def save_rel_multiproc(self, rel_path):
        """split the derivations list in n list of chunck size lenght and launch a process for each"""
        process_list1 = []
        for j in range(0, len(self.current_relations), 3):
            p_rel = Process(target=self.save_rel_multiproc1, args=(rel_path, j))
            process_list1.append(p_rel)
        cpu_num = os.cpu_count()
        num_proc_run1 = 0
        index_proc1 = 0
        for proc in process_list1:
            if num_proc_run1 == cpu_num:
                num_proc_run1 = 0
                for process_running1 in process_list1[index_proc1:index_proc1 + cpu_num]:
                    process_running1.join()
                index_proc1 += cpu_num
            num_proc_run1 += 1
            proc.start()



    def save_json_prov(self, nameFile):
        """Save provenance in json file."""
        if not os.path.exists(nameFile):
            os.makedirs(nameFile)

        ents_path = os.path.join(nameFile, 'entities')
        acts_path = os.path.join(nameFile, 'activities.json')
        rel_path = os.path.join(nameFile, 'relations')
        deriv_path=os.path.join(nameFile, 'derivations')

        # Save entities:
        # entities = list(self.current_ent.flatten())
        entities = self.new_entities

        if entities:
            #print(len(entities))
            p1 = Process(target=self.save_entities_multiproc, args=(entities, ents_path))

        # Save activities:
        if self.current_act:
            with open(acts_path, 'w', encoding='utf-8') as acts_file:
                json.dump(self.current_act, acts_file, ensure_ascii=False, indent=4)

        # Save all relations:
        if self.current_relations:

            p2 = Process(target=self.save_rel_multiproc, args=(rel_path,))
        if self.current_relations:
            p3 = Process(target=self.save_deriv_multiproc, args=(deriv_path,))

        if entities:
            p1.start()
        ## da sistemare multi proc, se non esiste relazione o entities -> error, già così miglioramento 20/30% a operazione di salvataggio, nel complesso 10/15%
        p2.start()
        if self.current_derivations:
            p3.start()
        if entities:
            p1.join()
        p2.join()
        if self.current_derivations:
            p3.join()

    def add_second_df(self, second_dataframe):
        self.second_df = second_dataframe
        self.initialize()
        self.current_second_ent = self.create_prov_entities(second_dataframe, self.INPUT)
        print('created second df entity')

    ###
    ###  PROVENANCE METHODS
    ###

    @timing
    def get_prov_feature_transformation(self, df_out, columnsName, description=None):
        """Return provenance document related to features trasformation function.

        Keyword argument:
        df_out -- the output dataframe
        columnsName -- list of transformed columns name
        """
        function_name = 'Feature Transformation'
        self.initialize()
        # Get current values:
        entities_in = self.current_ent

        # Output values:
        columns_out = df_out.columns
        indexes_out = df_out.index
        values = df_out.values
        for col_name in columnsName:
            act_id = self.create_activity(function_name, col_name, description)
            col_index = columns_out.get_loc(col_name)
            generated=[]
            used=[]
            invalidated=[]
            for i in range(self.current_m):
                e_in = entities_in[i][col_index]
                e_in_identifier = e_in['id']
                record_id = e_in['record_id']
                value = str(values[i, col_index])
                # Create a new entity with new value:
                e_out = self.create_entity(record_id, value, col_name, indexes_out[i], self.operation_number)
                e_out_identifier = e_out['id']
                self.create_derivation(e_in_identifier, e_out_identifier)
                generated.append(e_out_identifier)
                used.append(e_in_identifier)
                invalidated.append(e_in_identifier)
                entities_in[i][col_index] = e_out
            self.create_relation(act_id, generated=generated, used=used, invalidated=invalidated)

        # Update current values:
        self.set_current_values(df_out, entities_in)

        # Save provenance document in json file:
        self.save_json_prov(os.path.join(self.results_path, self.instance))

        return self

    def space_transformation_multiprocess(self, process_num, start, stop, entities_in, indexes_new, df_out,
                                          entities_out, act_id, indexes, columns_in, queue, values):
        generated = []
        used = []
        invalidated = []
        for i in range(start, stop):
            first_ent = entities_in[i][0]
            record_id = first_ent['record_id']
            not_inserted=True
            for j in indexes_new:
                value = str(values[i, j])
                e_out = self.create_entity( record_id, value, df_out.columns[j], df_out.index[i],
                                           self.operation_number)
                e_out_identifier = e_out['id']
                entities_out[i][j] = e_out
                generated.append(e_out_identifier)
                for index in indexes:
                    e_in = entities_in[i][index]
                    e_in_identifier = e_in['id']
                    self.create_derivation(e_in_identifier, e_out_identifier)
                    if not_inserted:
                        used.append(e_in_identifier)
                        if columns_in[index] not in df_out.columns:
                            invalidated.append(e_in_identifier)
                not_inserted=False
            if i == len(df_out.index) - 1:
                stop = i + 1
                break
        queue.put((process_num, start, stop, entities_out[start:stop], current_derivations, new_entities, generated, used, invalidated))

    @timing
    def get_prov_space_transformation(self, df_out, columnsName, description):
        """Return provenance document related to space trasformation function.

        Keyword argument:
        df_out -- the output dataframe
        columnsName -- list of columns name joined to create the new column
        """
        function_name = 'Space Transformation'
        self.initialize()
        global new_entities
        new_entities = []
        global current_relation
        current_relation = []
        global current_derivations
        current_derivations = []
        generated=[]
        used=[]
        invalidated=[]
        # Get current values:
        entities_in = self.current_ent
        m, n = self.current_m, self.current_n
        columns_in = self.current_columns
        # Output values:
        m_new, n_new = df_out.shape
        columns_out = df_out.columns
        indexes_out = df_out.index
        # Create entities of the output dataframe:
        entities_out = np.empty(df_out.shape, dtype=object)
        values = df_out.values
        # Get feature indexes used for space transformation:
        indexes = []
        for feature in columnsName:
            indexes.append(columns_in.get_loc(feature))

        # Get feature indexes generated by space transformation:
        indexes_new = []
        for feature in columns_out:
            if feature not in columns_in:
                indexes_new.append(columns_out.get_loc(feature))
        drop_columns = [col for col in columnsName if col not in columns_out]

        generated_features=[col for col in columns_out if col not in columns_in]
        # Create space transformation activity:
        act_id = self.create_activity(function_name, columnsName, description,generated_features=generated_features,deleted_used_features=len(drop_columns)>0)

        time_start = time.time()
        # Get provenance related to the new column:
        cpu_num = os.cpu_count()
        process_list = []
        process_num = 0
        queue = Queue()
        # split the rows in chunks
        if m / self.CHUNK_INDEX_SIZE > cpu_num:
            chunk_size = int(m / cpu_num)
            for i in range(0, m, chunk_size):
                process_num += 1
                p = Process(target=self.space_transformation_multiprocess, args=(
                process_num, i, i + chunk_size, entities_in, indexes_new, df_out, entities_out, act_id, indexes,
                columns_in, queue, values))
                process_list.append(p)
        else:
            for i in range(0, m, self.CHUNK_INDEX_SIZE):
                process_num += 1
                p = Process(target=self.space_transformation_multiprocess, args=(
                process_num, i, i + self.CHUNK_INDEX_SIZE, entities_in, indexes_new, df_out, entities_out, act_id,
                indexes, columns_in, queue, values))
                process_list.append(p)
        for process in process_list:
            process.start()
        results = [queue.get() for p in process_list]
        for process in process_list:
            process.join()
        results.sort(key=lambda tup: tup[0])
        for part in results:
            start = part[1]
            stop = part[2]
            entities_out[start:stop, :] = part[3]
            self.current_derivations = self.current_derivations + part[4]
            self.new_entities = self.new_entities + part[5]
            generated = generated + part[6]
            used = used + part[7]
            invalidated = invalidated + part[8]
        self.create_relation(act_id, generated=generated, used=used, invalidated=invalidated)

        print('scansione: ', time.time() - time_start)
        time_start3= time.time()
        # Rearrange unchanged columns:
        for col_name in columns_out:
            if col_name in columns_in:
                old_j = columns_in.get_loc(col_name)
                new_j = columns_out.get_loc(col_name)
                entities_out[:, new_j] = entities_in[:, old_j]
        # print(entities_out)
        # print(type(entities_out))
        print(f'rearrange{time.time()-time_start3}')
        # Update current values:
        self.set_current_values(df_out, entities_out)
        print(f'dopo set {time.time()-time_start3}')
        time_start2 = time.time()
        # Save provenance document in json file:
        self.save_json_prov(os.path.join(self.results_path, self.instance))
        print('salvataggio: ', time.time() - time_start2)
        return self

    @timing
    def get_prov_dim_reduction(self, df_out, description=None):
        """Return provenance document related to selection or projection."""
        # don't use this, is more faster but doesn't work with reindexing operation
        function_name = 'Dimensionality reduction'
        self.initialize()
        # Get current values:
        entities_in = self.current_ent
        columns_in = self.current_columns
        index_in = self.current_index
        m, n = self.current_m, self.current_n

        # Output values:
        columns_out = df_out.columns
        index_out = df_out.index
        m_new, n_new = df_out.shape
        print(index_out)
        delColumnsName = set(columns_in) - set(columns_out)  # List of deleted columns
        delIndex = set(index_in) - set(index_out)  # List of deleted index

        lista = list(delIndex)
        list_ort = sorted(lista)
        print(len(delIndex), list_ort)

        # Create selection activity:
        act_id = self.create_activity(function_name, ', '.join(delColumnsName), description)

        for i in delIndex:
            for j in range(n):
                e_in = entities_in[i][j]
                e_in_identifier = e_in['identifier']
                self.create_relation(self.INVALIDATION, a=e_in_identifier, b=act_id)

        delColumns = []
        for colName in delColumnsName:
            j = columns_in.get_loc(colName)
            delColumns.append(j)
            for i in range(m):
                e_in = entities_in[i][j]
                e_in_identifier = e_in['identifier']
                self.create_relation(self.INVALIDATION, a=e_in_identifier, b=act_id)

        entities_in = np.delete(entities_in, list(delIndex), axis=0)
        entities_out = np.delete(entities_in, delColumns, axis=1)

        # Update current values:
        self.set_current_values(df_out, entities_out)

        # Save provenance document in json file:
        self.save_json_prov(os.path.join(self.results_path, self.instance))

        return self

    @timing
    def get_prov_dim_reduction_hash(self, df_out, description):
        """Return provenance document related to selection or projection."""
        # Use this method!
        # works with reindex
        function_name = 'Dimensionality reduction'
        self.initialize()
        # Get current values:
        entities_in = self.current_ent
        columns_in = self.current_columns
        index_in = self.current_index
        m, n = self.current_m, self.current_n

        # Output values:
        columns_out = df_out.columns
        index_out = df_out.index
        m_new, n_new = df_out.shape

        delColumnsName = set(columns_in) - set(columns_out)  # List of deleted columns
        delIndex = []
        # use df hashes to find deleted rows
        hash_df = pd.util.hash_pandas_object(self.df, index=False)
        value_in = hash_df.values
        hash_out_df = pd.util.hash_pandas_object(df_out, index=False)
        value_out = hash_out_df.values
        deleted_items = 0
        for el in range(len(index_out)):
            hash_in = value_in[el + deleted_items]
            hash_out = value_in[el]
            while hash_in != hash_out:
                delIndex.append(el + deleted_items)
                deleted_items += 1
                # for index deletex consecutively
                hash_in = value_in[el + deleted_items]
                hash_out = value_out[el]
        #print(deleted_items, delIndex)
        # Create selection activity:
        if len(delIndex)>0:
            act_id = self.create_activity(function_name, columns_in, description, deleted_records = True)
        elif len(delColumnsName)>0:
            act_id = self.create_activity(function_name, list(delColumnsName), description, deleted_used_features = True)
        invalidated=[]
        for i in delIndex:
            for j in range(n):
                e_in = entities_in[i][j]
                e_in_identifier = e_in['id']
                invalidated.append(e_in_identifier)

        delColumns = []
        for colName in delColumnsName:
            j = columns_in.get_loc(colName)
            delColumns.append(j)
            for i in range(m):
                e_in = entities_in[i][j]
                e_in_identifier = e_in['id']
                invalidated.append(e_in_identifier)
        self.create_relation(act_id, invalidated=invalidated)

        entities_in = np.delete(entities_in, list(delIndex), axis=0)
        entities_out = np.delete(entities_in, delColumns, axis=1)

        # Update current values:
        self.set_current_values(df_out, entities_out)

        # Save provenance document in json file:
        self.save_json_prov(os.path.join(self.results_path, self.instance))

        return self

    @timing
    def get_prov_instance_generation(self, df_out, columnsName, description=None):
        """Return provenance document related to instance generation function."""
        # compatta tutto, ogni colonna di derivazione un attività
        function_name = 'Instance Generation'
        self.initialize()
        # Get current values:
        entities_in = self.current_ent
        m, n = self.current_m, self.current_n

        # Output values:
        columns_out = df_out.columns
        indexes_out = df_out.index
        m_new, n_new = df_out.shape

        # Create numpy array of new entities:
        new_entities = np.empty((m_new - m, n), dtype=object)
        values = df_out.values
        #acts = {}
        #used=dict()
        used=[]
        #generated=dict()
        generated=[]
        # Provenance of existent data
        """
        for col_name in columnsName:
            act_id = self.create_activity(function_name, col_name, description)
            acts[col_name] = act_id
            col_index = columns_out.get_loc(col_name)
            for i in range(m):
                #print(used)
                e_in = entities_in[i][col_index]
                ent_id = e_in['id']
                if col_name in used:
                    used[col_name]=used[col_name]+[ent_id]
                else:
                    used[col_name]=[ent_id]
        """
        act_id_col_used = self.create_activity(function_name, columnsName, description)

        for col_name in columnsName:

            col_index = columns_out.get_loc(col_name)

            for i in range(m):
                #print(used)
                e_in = entities_in[i][col_index]
                ent_id = e_in['id']
                used.append(ent_id)

        columnsName_out = set(columns_out) - set(columnsName)  # List of non selected columns
        if columnsName_out:
            # element not derivated from a column
            defaultAct_id = self.create_activity(function_name, None, description)
        # works only if the new data is appended to the df
        # Provenance of new data

        default_generated=[]
        for j in range(n):
            for i in range(m, m_new):
                #print(new_entities.shape, i,j)
                if j > 0:
                    record_id = new_entities[i-m][0]['record_id']
                else:
                    record_id = str(uuid.uuid4())
                col_name = columns_out[j]
                #act_id = acts[col_name] if col_name in acts else defaultAct_id
                act_id = act_id_col_used if col_name in columnsName else defaultAct_id
                value = str(values[i, j])
                e_out = self.create_entity(record_id, value, col_name, indexes_out[i], self.operation_number)
                e_out_identifier = e_out['id']
                new_entities[i - m][j] = e_out
                if col_name in columnsName:
                    #if col_name in generated:
                    #    generated[col_name]=generated[col_name]+[e_out_identifier]
                    #else:
                    #    generated[col_name]=[e_out_identifier]
                    generated.append(e_out_identifier)
                else:
                    default_generated.append(e_out_identifier)
            #if columns_out[j] in acts:
            #    self.create_relation(act_id, generated=generated[col_name], used=used[col_name])
        self.create_relation(defaultAct_id, generated=default_generated)
        self.create_relation(act_id_col_used, generated=generated, used=used)

        entities_out = np.concatenate((entities_in, new_entities), axis=0)

        # Update current values:
        self.set_current_values(df_out, entities_out)

        # Save provenance document in json file:
        self.save_json_prov(os.path.join(self.results_path, self.instance))

        return self

    @timing
    def get_prov_value_transformation(self, df_out, columnsName, description=None):
        """Return provenance document related to value transformation function.
        Used when a value inside the dataframe is replaced.

        Keyword argument:
        df_out -- the output dataframe
        columnsName -- list of column names where the value transformation is applied
        """
        function_name = 'Value Transformation'
        self.initialize()
        # Get current values:
        entities_in = self.current_ent

        # Output values:
        columns_out = df_out.columns
        indexes_out = df_out.index
        values = df_out.values
        for col_name in columnsName:
            add_act = True
            col_index = columns_out.get_loc(col_name)
            generated = []
            used = []
            invalidated = []
            for i in range(self.current_m):
                e_in = entities_in[i][col_index]
                e_in_identifier = e_in['id']
                record_id = e_in['record_id']
                val_in = e_in_identifier.split(self.SEPARATOR)[0]
                value = str(values[i, col_index])

                # Check if the input value is the replaced value
                if str(val_in) != str(value):
                    if add_act:
                        # Create value transformation activity:
                        act_id = self.create_activity(function_name, col_name, description)
                        add_act = False
                    # Create new entity with the new value
                    e_out = self.create_entity(record_id, value, col_name, indexes_out[i],
                                               self.operation_number)
                    e_out_identifier = e_out['id']
                    self.create_derivation(e_in_identifier, e_out_identifier)
                    generated.append(e_out_identifier)
                    used.append(e_in_identifier)
                    invalidated.append(e_in_identifier)
                    entities_in[i][col_index] = e_out
            self.create_relation(act_id, generated, used, invalidated)


        # Update current values:
        self.set_current_values(df_out, entities_in)

        # Save provenance document in json file:
        self.save_json_prov(os.path.join(self.results_path, self.instance))

        return self

    @timing
    def get_prov_imputation(self, df_out, columnsName, description=None):
        """Return provenance document related to imputation function.

        Keyword argument:
        df_out -- the output dataframe
        columnsName -- list of column names where the imputation is applied
        """
        function_name = 'Imputation'
        self.initialize()
        # Get current values:
        entities_in = self.current_ent
        # Output values:
        columns_out = df_out.columns
        indexes_out = df_out.index
        values = df_out.values
        t=time.time()
        for col_name in columnsName:
            act_id = self.create_activity(function_name, col_name, description)
            col_index = columns_out.get_loc(col_name)
            generated = []
            used = []
            invalidated = []
            for i in range(self.current_m):
                value = str(values[i, col_index])

                e_in = entities_in[i][col_index]
                e_in_identifier = e_in['id']
                record_id = e_in['record_id']
                val_in = e_in_identifier.split(self.SEPARATOR)[0]

                if val_in == 'nan':
                    # Create new entity with the new value
                    e_out = self.create_entity(record_id, value, col_name, indexes_out[i],
                                               self.operation_number)
                    e_out_identifier = e_out['id']
                    self.create_derivation(e_in_identifier, e_out_identifier)
                    generated.append(e_out_identifier)
                    invalidated.append(e_in_identifier)

                    entities_in[i][col_index] = e_out
                else:
                    used.append(e_in_identifier)
            self.create_relation(act_id, generated, used, invalidated)
        print(f'dopo ciclo {time.time()-t}')
        # Save provenance document in json file:
        self.save_json_prov(os.path.join(self.results_path, self.instance))
        print(f'save {time.time()-t}')
        # Update current values:
        self.set_current_values(df_out, entities_in)
        print(f'set {time.time()-t}')


        return self

    @timing
    def get_prov_union(self, df_out, axis, description=None):
        """Return provenance document related to union function.

        Keyword argument:
        df_out -- the output dataframe
        axis -- axis of the union

        """
        function_name = 'Union'
        self.initialize()
        # Get current values:
        entities_in = self.current_ent
        entities_in_second_df = self.current_second_ent
        column_second_df = self.second_df.columns
        index_second_df = self.second_df.index
        list_columns=list(self.current_columns)
        list_second_columns=list(column_second_df)
        used_features=list(set(list_columns+list_second_columns))
        # Output values:
        columns_out = df_out.columns
        indexes_out = df_out.index
        values = df_out.values
        entities_out = np.empty(df_out.shape, dtype=object)
        act_id = self.create_activity(function_name, features_name=used_features, description=description)
        generated = []
        used = []
        invalidated = []
        if axis == 0:
            # faccio append su colonne (verso il basso)
            # column append
            for i in range(len(indexes_out)):
                record_id = str(uuid.uuid4())
                for j in range(len(columns_out)):
                    value = str(values[i, j])
                    col_name = columns_out[j]
                    e_out = self.create_entity(record_id, value, col_name, indexes_out[i],
                                               self.operation_number)
                    e_out_identifier = e_out['id']
                    if i < self.current_m:
                        if j < len(self.current_columns):
                            e_in = entities_in[i][j]
                            e_in_identifier = e_in['id']
                            self.create_derivation(e_in_identifier, e_out_identifier)
                            generated.append(e_out_identifier)
                            invalidated.append(e_in_identifier)
                            used.append(e_in_identifier)

                        else:
                            # nan, no entity in
                            generated.append(e_out_identifier)
                            #self.create_derivation(act_id, e_out_identifier)
                    else:
                        # from second df
                        if columns_out[j] in column_second_df:
                            index_column = list(column_second_df).index(columns_out[j])
                            e_in = entities_in_second_df[i - len(self.current_index)][index_column]
                            e_in_identifier = e_in['id']
                            self.create_derivation(e_in_identifier, e_out_identifier)
                            generated.append(e_out_identifier)
                            invalidated.append(e_in_identifier)
                            used.append(e_in_identifier)

                        else:
                            # nan, no entity in
                            generated.append(e_out_identifier)
                            #self.create_derivation(act_id, e_out_identifier)
                    entities_out[i][j]=e_out
        elif axis == 1:
            # colonne aggiunte e faccio append sull'indice(verso destra)
            # columns append
            for j in range(len(columns_out)):
                for i in range(len(indexes_out)):
                    value = str(df_out.iat[i, j])
                    if j>0:
                        record_id=entities_out[i][0]['record_id']
                    else:
                        record_id = str(uuid.uuid4())
                    col_name = columns_out[j]
                    e_out = self.create_entity(record_id, value, col_name, indexes_out[i],
                                               self.operation_number)
                    e_out_identifier = e_out['id']
                    if j < len(self.current_columns):
                        if i in self.current_index:
                            # errore se metto i devo prima trovarlo
                            index_index = list(self.current_index).index(indexes_out[i])
                            e_in = entities_in[index_index][j]
                            e_in_identifier = e_in['id']
                            self.create_derivation(e_in_identifier, e_out_identifier)
                            generated.append(e_out_identifier)
                            invalidated.append(e_in_identifier)
                            used.append(e_in_identifier)
                        else:
                            # nan, no entity in
                            generated.append(e_out_identifier)
                            #self.create_derivation(act_id, e_out_identifier)
                    else:
                        if indexes_out[i] in index_second_df:
                            index_index = list(index_second_df).index(indexes_out[i])
                            e_in = entities_in_second_df[index_index][j - len(self.current_columns)]
                            e_in_identifier = e_in['id']
                            self.create_derivation(e_in_identifier, e_out_identifier)
                            generated.append(e_out_identifier)
                            invalidated.append(e_in_identifier)
                            used.append(e_in_identifier)
                        else:
                            # nan, no entity in
                            generated.append(e_out_identifier)
                           # self.create_derivation(act_id, e_out_identifier)
                    entities_out[i][j]=e_out
        else:
            print('wrong axis')
        self.create_relation(act_id, generated, used, invalidated)
        # Save provenance document in json file:
        self.save_json_prov(os.path.join(self.results_path, self.instance))
        # Update current values:
        self.set_current_values(df_out, entities_out)

        return self

    def prov_join_multiprocess(self,process_num, start, stop, indexes_out, df_out,indexes_of_on_column, filler, list_columns, list_second_columns, values_right, values_left,values, columns_out, on, column_second_df, entities_in, entities_out, entities_in_second_df,queue):
        generated=[]
        invalidated=[]
        used=[]
        for i in range(start, stop):
            if i % 1000 == 0:
                print(i)
            if i<df_out.shape[0]:
                # verifico se chiave/i esistono in tutti e 2 i df, se in uno non ci sta ai nan gli metto solo generation
                # prendo la porzione di riga con le colonne di un df alla volta e creo le relazioni
                # per creare le relazioni devo prima trovare gli indici dei valori nei df originali
                # gli indici si resettano sempre con join
                record_id = str(uuid.uuid4())
                in_right = True
                in_left = True
                key_identifier = {}
                row_out_on_values = df_out.iloc[i, indexes_of_on_column].fillna(filler)
                row_out = df_out.iloc[i, :].fillna(filler)

                # search all rows in starting df with the on keys
                row_left = 0
                row_right = 0
                for index, value in row_out_on_values.items():

                    index_left = list_columns.index(index)
                    index_right = list_second_columns.index(index)
                    if type(row_right) == int:
                        row_right = values_right[
                            (values_right[:, index_right] == value)]
                    else:
                        row_right = row_right[
                            (row_right[:, index_right] == value)]

                    if type(row_left) == int:
                        row_left = values_left[
                            (values_left[:, index_left] == value)]
                    else:
                        row_left = row_left[
                            (row_left[:, index_left] == value)]
                    # ora
                if row_right.size == 0:
                    in_right = False
                if row_left.size == 0:
                    in_left = False
                if in_left:
                    # search for the right row to create the provenance
                    left_col_list = list(self.current_columns)
                    out_col_left = list(columns_out)
                    # remove the on columns
                    for key in on:
                        left_col_list.remove(key)
                        out_col_left.remove(key)
                    # remove the columns from second df
                    del_index = []
                    for k in range(len(out_col_left)):
                        if out_col_left[k].endswith('_x') and out_col_left[k][:-2] in self.current_columns:
                            pass
                        elif out_col_left[k] not in self.current_columns:
                            del_index.append(k)
                    # remove using inverted sort index preventing errors by reindexing
                    for index in sorted(del_index, reverse=True):
                        del out_col_left[index]

                    for j in range(len(left_col_list)):
                        # ho le due liste di colonne cruciali per capire la provenienza, devo solo prendere i valori di quella in esame e cercare in tmp con i nomi delle colonne originali
                        col_name = out_col_left[j]
                        if col_name[-2:] == '_x' and col_name[:-2] in self.current_columns:
                            col_name = col_name[:-2]
                        index_col_name = list_columns.index(col_name)
                        # print(row_out[out_col_left[j]])
                        # row_left = row_left[(row_left[col_name] == row_out[out_col_left[j]]) | (pd.isna(row_left[col_name]) & pd.isna(row_out[out_col_left[j]]))]
                        row_left = row_left[(row_left[:, index_col_name] == row_out[out_col_left[j]])]

                    if len(row_left) > 1:
                        row_left = row_left[0]
                    index_index = np.where(np.all(values_left == row_left, axis=1))[0][0]
                    # index_index=self.df[self.df==row_left].index[0]
                    for z in range(len(columns_out)):
                        col_name = columns_out[z]
                        # saerch the index of the left df to create provenance
                        if col_name.endswith('_x') and col_name[:-2] in self.current_columns:
                            col_name = col_name[:-2]

                        if col_name in list_columns:
                            # initialize the entity
                            value = str(values[i, z])
                            e_out = self.create_entity(record_id, value, columns_out[z], indexes_out[i],
                                                       self.operation_number)
                            e_out_identifier = e_out['id']
                            if col_name in on:
                                key_identifier[col_name] = (e_out_identifier, e_out)
                            # col_index = row_left.columns.get_loc(col_name)
                            col_index = list_columns.index(col_name)
                            # index_index = self.current_index.get_loc(row_left.index.values[0])
                            e_in = entities_in[index_index][col_index]
                            e_in_identifier = e_in['id']
                            self.create_derivation(e_in_identifier, e_out_identifier)
                            generated.append(e_out_identifier)
                            invalidated.append(e_in_identifier)
                            used.append(e_in_identifier)
                            entities_out[i][z] = e_out

                else:
                    # outer join or right outer join case, nan value, only generation relation
                    # search the columns that aren't in right df
                    for z in range(len(columns_out)):
                        col_name = columns_out[z]
                        if col_name.endswith('_y') and col_name[:-2] in column_second_df:
                            col_name = col_name[:-2]
                        if col_name not in column_second_df:
                            value = str(values[i, z])
                            e_out = self.create_entity(record_id, value, columns_out[z], indexes_out[i],
                                                       self.operation_number)
                            e_out_identifier = e_out['id']
                            generated.append(e_out_identifier)
                            entities_out[i][z] = e_out

                            # self.create_derivation(act_id, e_out_identifier)
                if in_right:

                    # search for the right row to create the provenance
                    right_col_list = list(column_second_df)
                    out_col_right = list(columns_out)
                    # remove the on columns
                    for key in on:
                        right_col_list.remove(key)
                        out_col_right.remove(key)
                    # remove the columns from second df
                    del_index = []
                    for k in range(len(out_col_right)):
                        if out_col_right[k].endswith('_y') and out_col_right[k][:-2] in column_second_df:
                            pass
                        elif out_col_right[k] not in column_second_df:
                            del_index.append(k)
                    # remove using inverted sort index preventing errors by reindexing
                    for index in sorted(del_index, reverse=True):
                        del out_col_right[index]

                    for j in range(len(right_col_list)):
                        # ho le due liste di colonne cruciali per capire la provenienza, devo solo prendere i valori di quella in esame e cercare in tmp con i nomi delle colonne originali
                        col_name = out_col_right[j]
                        if col_name.endswith('_y') and col_name[:-2] in column_second_df:
                            col_name = col_name[:-2]
                        # print(row_out[out_col_right[j]])
                        index_col_name = list_second_columns.index(col_name)
                        # print(row_out[out_col_right[j]],type(row_out[out_col_right[j]]),pd.isna(row_out[out_col_right[j]]))
                        # row_right = row_right[(row_right[col_name] == row_out[out_col_right[j]]) | (pd.isna(row_right[col_name]) & pd.isna(row_out[out_col_right[j]]))]
                        row_right = row_right[(row_right[:, index_col_name] == row_out[out_col_right[j]])]
                    if len(row_right) > 1:
                        row_right = row_right[0]
                    index_index = np.where(np.all(values_right == row_right, axis=1))[0][0]
                    for w in range(len(columns_out)):

                        col_name = columns_out[w]
                        if col_name.endswith('_y') and col_name[:-2] in column_second_df:
                            col_name = col_name[:-2]

                        # search the index of the right df to create provenance

                        if col_name in list_second_columns:
                            if col_name in on and len(key_identifier) == 0:
                                # if the key entity was created before, don't create another one but use the identifier in the dictionary
                                # initialize the entity
                                value = str(values[i, w])

                                e_out = self.create_entity(record_id, value, columns_out[w], indexes_out[i],
                                                           self.operation_number)
                                e_out_identifier = e_out['id']
                            elif col_name not in on:
                                # if col is not a key column create the entity
                                value = str(values[i, w])

                                e_out = self.create_entity(record_id, value, columns_out[w], indexes_out[i],
                                                           self.operation_number)
                                e_out_identifier = e_out['id']
                            # col_index = row_right.columns.get_loc(col_name)
                            col_index = list_second_columns.index(col_name)

                            # index_index = index_second_df.get_loc(row_right.index.values[0])
                            e_in = entities_in_second_df[index_index][col_index]
                            e_in_identifier = e_in['id']
                            if col_name in on and len(key_identifier) > 0:
                                e_out_identifier = key_identifier[col_name][0]
                                e_out = key_identifier[col_name][1]
                                generated.append(e_out_identifier)
                            self.create_derivation(e_in_identifier, e_out_identifier)
                            invalidated.append(e_in_identifier)
                            used.append(e_in_identifier)
                            entities_out[i][w] = e_out

                else:
                    # if the keys don't exist in right df create the nan's entity with only the generation relation
                    for z in range(len(columns_out)):
                        col_name = columns_out[z]
                        if col_name.endswith('_x') and col_name[:-2] in self.current_columns:
                            # column in left df but with suffix
                            col_name = col_name[:-2]
                        if col_name not in self.current_columns:
                            # take only the columns of right df without the keys
                            value = str(values[i, z])
                            e_out = self.create_entity(record_id, value, columns_out[z], indexes_out[i],
                                                       self.operation_number)
                            e_out_identifier = e_out['id']
                            generated.append(e_out_identifier)
                            entities_out[i][z] = e_out
                            # self.create_derivation(act_id, e_out_identifier)
        queue.put((process_num, start, stop, entities_out[start:stop], current_derivations, new_entities, generated, used, invalidated))


    @timing
    def get_prov_join(self, df_out, on, description=None):
        """Return provenance document related to join function.

                Keyword argument:
                df_out -- the output dataframe
                on -- list of columns to join on or single column

                """
        # funziona con nan su colonne diverse da on
        function_name = 'Join'
        self.initialize()
        global new_entities
        new_entities = []
        global current_relation
        current_relation = []
        global current_derivations
        current_derivations = []
        # Get current values:
        entities_in = self.current_ent
        entities_in_second_df = self.current_second_ent
        column_second_df = self.second_df.columns
        index_second_df = self.second_df.index
        # print(on)
        if type(on) != list:
            on = [on]
        # print(on)
        list_columns = list(self.current_columns)
        list_second_columns = list(column_second_df)
        used_features = list(set(list_columns + list_second_columns))
        # Output values:
        columns_out = df_out.columns
        indexes_out = df_out.index
        filler = 'nan'
        values = df_out.fillna(filler).values
        values_left = self.df.fillna(filler).values
        values_right = self.second_df.fillna(filler).values

        act_id = self.create_activity(function_name, features_name=used_features, description=description)
        entities_out = np.empty(df_out.shape, dtype=object)
        indexes_of_on_column = []
        generated = []
        used = []
        invalidated = []
        for col in on:
            on_col_index = df_out.columns.get_loc(col)
            indexes_of_on_column.append(on_col_index)
        cpu_num = os.cpu_count()
        process_list = []
        process_num = 0
        queue = Queue()
        m=df_out.shape[0]
        if m / self.CHUNK_INDEX_SIZE > cpu_num:
            chunk_size = int(m / cpu_num)
            for i in range(0, m, chunk_size):
                print(i, m)
                process_num += 1
                p = Process(target=self.prov_join_multiprocess, args=(
                    process_num, i, i + chunk_size, indexes_out, df_out,indexes_of_on_column, filler, list_columns, list_second_columns, values_right, values_left,values, columns_out, on, column_second_df, entities_in, entities_out, entities_in_second_df, queue))
                process_list.append(p)
        else:
            for i in range(0, m, self.CHUNK_INDEX_SIZE):
                process_num += 1
                p = Process(target=self.prov_join_multiprocess, args=(
                    process_num, i, i + self.CHUNK_INDEX_SIZE, indexes_out, df_out,indexes_of_on_column, filler, list_columns, list_second_columns, values_right, values_left,values, columns_out, on, column_second_df, entities_in, entities_out, entities_in_second_df, queue))
                process_list.append(p)
        for process in process_list:
            process.start()
        results = [queue.get() for p in process_list]
        for process in process_list:
            process.join()

        results.sort(key=lambda tup: tup[0])
        for part in results:
            start = part[1]
            stop = part[2]
            entities_out[start:stop, :] = part[3]
            self.current_derivations = self.current_derivations + part[4]
            self.new_entities = self.new_entities + part[5]
            generated = generated + part[6]
            used = used + part[7]
            invalidated = invalidated + part[8]
        print('creo relazione')
        self.create_relation(act_id, generated, used, invalidated)
        # Save provenance document in json file:
        print('salvataggio')
        self.save_json_prov(os.path.join(self.results_path, self.instance))
        # Update current values:
        print('setting')
        self.set_current_values(df_out, entities_out)
        # print(entities_out)

        return self

    @timing
    def get_prov_join1(self, df_out, on, description=None):
        """Return provenance document related to join function.

        Keyword argument:
        df_out -- the output dataframe
        on -- list of columns to join on or single column

        """
        # funziona con nan su colonne diverse da on
        function_name = 'Join'
        self.initialize()
        # Get current values:
        entities_in = self.current_ent
        entities_in_second_df = self.current_second_ent
        column_second_df = self.second_df.columns
        index_second_df = self.second_df.index
        # print(on)
        if type(on) != list:
            on = [on]
        # print(on)
        list_columns = list(self.current_columns)
        list_second_columns = list(column_second_df)
        used_features = list(set(list_columns + list_second_columns))
        # Output values:
        columns_out = df_out.columns
        indexes_out = df_out.index
        filler = 'nan'
        values = df_out.fillna(filler).values
        values_left = self.df.fillna(filler).values
        values_right = self.second_df.fillna(filler).values


        act_id = self.create_activity(function_name, features_name=used_features, description=description)
        entities_out = np.empty(df_out.shape, dtype=object)
        indexes_of_on_column = []
        generated = []
        used = []
        invalidated = []
        for col in on:
            on_col_index = df_out.columns.get_loc(col)
            indexes_of_on_column.append(on_col_index)
        for i in range(len(indexes_out)):
            if i % 1000 == 0:
                print(i)
            # verifico se chiave/i esistono in tutti e 2 i df, se in uno non ci sta ai nan gli metto solo generation
            # prendo la porzione di riga con le colonne di un df alla volta e creo le relazioni
            # per creare le relazioni devo prima trovare gli indici dei valori nei df originali
            # gli indici si resettano sempre con join
            record_id = str(uuid.uuid4())
            in_right = True
            in_left = True
            key_identifier = {}
            t = time.time()
            row_out_on_values = df_out.iloc[i, indexes_of_on_column].fillna(filler)
            row_out = df_out.iloc[i, :].fillna(filler)
            # print(row_out_on_values)
            # to search the provenance row of the two df initialize the variable coping the two start df
            # row_left = self.df  # inutile
            # row_right = self.second_df  # inutile
            # values_left=self.df.values
            # values_right=self.second_df.values
            # search all rows in starting df with the on keys
            s = time.time()
            """
            for index, value in row_out_on_values.items():
                row_left = row_left.loc[(row_left[index] == value) | (pd.isna(row_left[index]) & pd.isna(value))]
                row_right = row_right.loc[(row_right[index] == value) | pd.isna(row_right[index]) & pd.isna(value)]
            print('prima numpy'+str(time.time()-s))
            """
            row_left = 0
            row_right = 0
            p = time.time()
            for index, value in row_out_on_values.items():

                index_left = list_columns.index(index)
                index_right=list_second_columns.index(index)
                if type(row_right) == int:
                    row_right = values_right[
                        (values_right[:, index_right] == value) ]
                else:
                    row_right = row_right[
                        (row_right[:, index_right] == value) ]

                if type(row_left) == int:
                    row_left = values_left[
                        (values_left[:, index_left] == value) ]
                else:
                    row_left = row_left[
                        (row_left[:, index_left] == value) ]
                # ora
            if row_right.size == 0:
                in_right = False
            if row_left.size == 0:
                in_left = False
            if in_left:
                # search for the right row to create the provenance
                left_col_list = list(self.current_columns)
                out_col_left = list(columns_out)
                # remove the on columns
                for key in on:
                    left_col_list.remove(key)
                    out_col_left.remove(key)
                # remove the columns from second df
                del_index = []
                for k in range(len(out_col_left)):
                    if out_col_left[k].endswith('_x') and out_col_left[k][:-2] in self.current_columns:
                        pass
                    elif out_col_left[k] not in self.current_columns:
                        del_index.append(k)
                # remove using inverted sort index preventing errors by reindexing
                for index in sorted(del_index, reverse=True):
                    del out_col_left[index]

                for j in range(len(left_col_list)):
                    # ho le due liste di colonne cruciali per capire la provenienza, devo solo prendere i valori di quella in esame e cercare in tmp con i nomi delle colonne originali
                    col_name = out_col_left[j]
                    if col_name[-2:] == '_x' and col_name[:-2] in self.current_columns:
                        col_name = col_name[:-2]
                    index_col_name = list_columns.index(col_name)
                    # print(row_out[out_col_left[j]])
                    # row_left = row_left[(row_left[col_name] == row_out[out_col_left[j]]) | (pd.isna(row_left[col_name]) & pd.isna(row_out[out_col_left[j]]))]
                    row_left = row_left[(row_left[:, index_col_name] == row_out[out_col_left[j]]) ]

                if len(row_left) > 1:
                    row_left = row_left[0]
                row_left[pd.isna(row_left)] = False
                index_index = np.where(np.all(values_left == row_left, axis=1))[0][0]
                # index_index=self.df[self.df==row_left].index[0]
                for z in range(len(columns_out)):
                    col_name = columns_out[z]
                    # saerch the index of the left df to create provenance
                    if col_name.endswith('_x') and col_name[:-2] in self.current_columns:
                        col_name = col_name[:-2]

                    if col_name in list_columns:
                        # initialize the entity
                        value = str(values[i, z])
                        e_out = self.create_entity(record_id, value, columns_out[z], indexes_out[i],
                                                   self.operation_number)
                        e_out_identifier = e_out['id']
                        if col_name in on:
                            key_identifier[col_name] = (e_out_identifier, e_out)
                        # col_index = row_left.columns.get_loc(col_name)
                        col_index = list_columns.index(col_name)
                        # index_index = self.current_index.get_loc(row_left.index.values[0])
                        e_in = entities_in[index_index][col_index]
                        e_in_identifier = e_in['id']
                        self.create_derivation(e_in_identifier, e_out_identifier)
                        generated.append(e_out_identifier)
                        invalidated.append(e_in_identifier)
                        used.append(e_in_identifier)
                        entities_out[i][z] = e_out

            else:
                # outer join or right outer join case, nan value, only generation relation
                # search the columns that aren't in right df
                for z in range(len(columns_out)):
                    col_name = columns_out[z]
                    if col_name.endswith('_y') and col_name[:-2] in list_second_columns:
                        col_name = col_name[:-2]
                    if col_name not in list_second_columns:
                        value = str(values[i, z])
                        e_out = self.create_entity(record_id, value, columns_out[z], indexes_out[i],
                                                   self.operation_number)
                        e_out_identifier = e_out['id']
                        generated.append(e_out_identifier)
                        entities_out[i][z] = e_out

                        # self.create_derivation(act_id, e_out_identifier)
            if in_right:

                # search for the right row to create the provenance
                right_col_list = list(column_second_df)
                out_col_right = list(columns_out)
                # remove the on columns
                for key in on:
                    right_col_list.remove(key)
                    out_col_right.remove(key)
                # remove the columns from second df
                del_index = []
                for k in range(len(out_col_right)):
                    if out_col_right[k].endswith('_y') and out_col_right[k][:-2] in column_second_df:
                        pass
                    elif out_col_right[k] not in column_second_df:
                        del_index.append(k)
                # remove using inverted sort index preventing errors by reindexing
                for index in sorted(del_index, reverse=True):
                    del out_col_right[index]

                for j in range(len(right_col_list)):
                    # ho le due liste di colonne cruciali per capire la provenienza, devo solo prendere i valori di quella in esame e cercare in tmp con i nomi delle colonne originali
                    col_name = out_col_right[j]
                    if col_name.endswith('_y') and col_name[:-2] in column_second_df:
                        col_name = col_name[:-2]
                    # print(row_out[out_col_right[j]])
                    index_col_name = list_second_columns.index(col_name)
                    # print(row_out[out_col_right[j]],type(row_out[out_col_right[j]]),pd.isna(row_out[out_col_right[j]]))
                    # row_right = row_right[(row_right[col_name] == row_out[out_col_right[j]]) | (pd.isna(row_right[col_name]) & pd.isna(row_out[out_col_right[j]]))]
                    row_right = row_right[(row_right[:, index_col_name] == row_out[out_col_right[j]]) ]
                index_index = np.where(np.all(values_right == row_right, axis=1))[0][0]
                for w in range(len(columns_out)):

                    col_name = columns_out[w]
                    if col_name.endswith('_y') and col_name[:-2] in column_second_df:
                        col_name = col_name[:-2]

                    # search the index of the right df to create provenance

                    if col_name in list_second_columns:
                        if col_name in on and len(key_identifier) == 0:
                            # if the key entity was created before, don't create another one but use the identifier in the dictionary
                            # initialize the entity
                            value = str(values[i, w])

                            e_out = self.create_entity(record_id, value, columns_out[w], indexes_out[i],
                                                       self.operation_number)
                            e_out_identifier = e_out['id']
                        elif col_name not in on:
                            # if col is not a key column create the entity
                            value = str(values[i, w])

                            e_out = self.create_entity(record_id, value, columns_out[w], indexes_out[i],
                                                       self.operation_number)
                            e_out_identifier = e_out['id']
                        #col_index = row_right.columns.get_loc(col_name)
                        col_index = list_second_columns.index(col_name)

                        # index_index = index_second_df.get_loc(row_right.index.values[0])
                        e_in = entities_in_second_df[index_index][col_index]
                        e_in_identifier = e_in['id']
                        if col_name in on and len(key_identifier) > 0:
                            e_out_identifier = key_identifier[col_name][0]
                            e_out = key_identifier[col_name][1]
                            generated.append(e_out_identifier)
                        self.create_derivation(e_in_identifier, e_out_identifier)
                        invalidated.append(e_in_identifier)
                        used.append(e_in_identifier)
                        entities_out[i][w] = e_out

            else:
                # if the keys don't exist in right df create the nan's entity with only the generation relation
                for z in range(len(columns_out)):
                    col_name = columns_out[z]
                    if col_name.endswith('_x') and col_name[:-2] in self.current_columns:
                        # column in left df but with suffix
                        col_name = col_name[:-2]
                    if col_name not in self.current_columns:
                        # take only the columns of right df without the keys
                        value = str(values[i, z])
                        e_out = self.create_entity(record_id, value, columns_out[z], indexes_out[i],
                                                   self.operation_number)
                        e_out_identifier = e_out['id']
                        generated.append(e_out_identifier)
                        entities_out[i][z] = e_out
                        # self.create_derivation(act_id, e_out_identifier)

        self.create_relation(act_id, generated, used, invalidated)
        # Save provenance document in json file:
        self.save_json_prov(os.path.join(self.results_path, self.instance))
        # Update current values:
        self.set_current_values(df_out, entities_out)
        # print(entities_out)

        return self


def get_size_format(b, factor=1024, suffix='B'):
    for unit in ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']:
        if b < 1024:
            return f'{b:.2f}{unit}{suffix}'
        b /= 1024
    return f'{b:.2f}Y{suffix}'