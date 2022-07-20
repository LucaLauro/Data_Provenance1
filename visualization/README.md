# DataProvenance Visualization
App for visualize the data provenance 


## Needed Libraries
* [flask](https://flask.palletsprojects.com/en/1.1.x/)
* [pymongo](https://pymongo.readthedocs.io/en/stable/)
* use python3
## Used Libraries for visualization
* dagre
* dagre-d3
## Execution

From terminal, in the folder of the app launch `python3 myapp.py db_name`

In the browser the app can be accessed on the address `localhost:5000`

In the 3 text input (Value, Index, Feature) write the relative information of the entity (an element of a df)
 
From the GUI is possible to execute 2 queries:
* Item history
* Record history

Item History displays all the values(and other details) that had the searched entity after each preprocessing operation

Record history displays all the histories of each element in the same row of the searched entity

