# DataProvenance 
App for capture and visualize data provenance and metadata of a preprocessing Pipeline.


## Needed Libraries
* [flask](https://flask.palletsprojects.com/en/1.1.x/)
* [neo4j](https://github.com/neo4j/neo4j-python-driver)
* use python3 3.6+
* pandas
* numpy
## Used Libraries for visualization
* dagre
* dagre-d3
##Tested DB
* Neo4j Desktop 4.3.6
## Execution

1. Set Up the Neo4j DB and comment the row `dbms.directories.import=import`. Use the default credentials

2. To execute the demo pipeline:
    * In terminal go in the folder *prov_acquisition/*
    * Execute the file with the command `python3 real_world_pipeline/demo_shell.py` .
    
   
The result will be stored in the folder *prov_acquisition/prov_results/<dataset_name>* and loaded in Neo4j.

After the result has been uploded in Neo4j the visualization app will available at the address **localhost:5000**



## Execute only the visualization app

From terminal, in the folder of *prov_acquisition/real_world_pipeline* launch `python3 run_app.py `

In the browser the app can be accessed on the address `localhost:5000`

The tab Dataframe Meta describe all the operation and the elements at a dataset level

The tab Feature Meta describe all the operation and the elements at a feature level

Is possible to compare Dataframe meta and Feature Meta using the button Compare Dataset or Compare feature.

From the Feature Meta is also possible to show the history of a single value of the feature