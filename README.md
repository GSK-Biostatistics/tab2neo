# tab2neo- backend classes
High-level Python classes to load, model and reshape tabular data imported into Neo4j database  
IMPORTANT NOTE: tested on **versions 4.3.6 and 4.4.11 of Neo4j**

## Installation

`pip install tab2neo`

## Modules

DATA LOADERS - modules allowing to read data from various formats and write it to neo4j
- FileDataLoader -  Load data into Neo4j, with support the following input formats: sas7bdat, xpt, rda, xls, xlsx, csv See [details](data_loaders/README.md)   

MODEL APPLIERS
- ModelApplier - Class to restructure data in Neo4j database using Class-Relationship model 
(which as well resides in Neo4j). 
See [details](model_appliers/README.md)

DATA PROVIDERS
- DataProvider - To fetch the data already in the database (in particular, the way the data after the 
transformations with ModelApplier in mode='schema_PROPERTY', or any linked data in Neo4j in mode = 'noschema')
See [details](data_providers/README.md)      

MODEL MANAGERS
- ModelManager - Class to manage metadata nodes (Class-Relationship model)
    
QUERY BUILDERS
- QueryBuilder - Class to support creation of cypher queries to work with data in Neo4j  

Dependencies:
- https://github.com/GSK-Biostatistics/neointerface
