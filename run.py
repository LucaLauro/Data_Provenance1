from new_queries import create_mongo_pandas
from visualization import myapp_no_main

def run(dbname, filepath):
    print('run')
    create_mongo_pandas.main(dbname, filepath)
    myapp_no_main.main(dbname)

