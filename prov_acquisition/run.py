from new_queries import create_mongo_pandas
from visualization import myapp_no_main
from neo4j_load_csv import load_graph
def run(dbname, filepath):
    print('run')
    create_mongo_pandas.main(dbname, filepath)
    myapp_no_main.main(dbname)

def run2(dbname,filepath):
    print('run',dbname)
    load_graph(filepath)
    myapp_no_main.main(dbname)