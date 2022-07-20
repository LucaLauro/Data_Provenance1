# DataProvenance 
App for capture and visualize data provenance 


## Needed Libraries
* [flask](https://flask.palletsprojects.com/en/1.1.x/)
* [pymongo](https://pymongo.readthedocs.io/en/stable/)
* use python3 3.6+
* pandas
* numpy
## Used Libraries for visualization
* dagre
* dagre-d3
## Execution


1. To execute the demo pipeline:
    * In terminal go in the folder *prov_acquisition/*
    * Execute the file with the command `python3 real_world_pipeline/demo_shell.py` followed by the name of the collection to be created in Mongo DB `dbname`.
    Example:, **python3 real_world_pipeline/demo_shell.py demo**
    
   
The result will be stored in the folder *prov_acquisition/prov_results/<dataset_name>* and loaded in Mongo.

After the result has been uploded in MongoDB the visualization app will available at the address **localhost:5000**



## Execute only the visualization app

From terminal, in the folder of the app launch `python3 myapp.py db_name`

In the browser the app can be accessed on the address `localhost:5000`

In the 3 text input (Value, Index, Feature) write the relative information of the entity (an element of a df)
 
From the GUI is possible to execute 2 queries:
* Item history
* Record history

Item History displays all the values(and other details) that had the searched entity after each preprocessing operation

Record history displays all the histories of each element in the same row of the searched entity

