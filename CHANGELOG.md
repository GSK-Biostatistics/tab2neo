Change Log
All notable changes to the tab2neo package will be documented in this file.
[2.0.3.0]

## Added

- Enabled provision of conditions on SUBCLASS via ModelManager

[2.0.2.0]

## Added

- Excluded the labels marked with !! from the WITH and RETURN parts of the query generated for get_data_generic()

[2.0.1.0]

## Added 

- Extended list_where_rel_conditions_per_dict function in Query Builder
  
[2.0.0.0]

## Added

- Support Neo4j version 5.x

[1.3.11.1]

Bugfix:Changed the method name in json for test cases to run

[1.3.11.0]

Added indexes in data_load

[1.3.10.1]

Migrated delete_method() from cldbe MethoApplier to tab2neo DerivationMethod

[1.3.9.0]

Added support of parquet file format in data_loaders/file_data_loader.py

[1.3.8.0]

Added a method in Model Manager to delete the derived schema from graph database. 

[1.3.7.1]

Added delete_subclass method to delete subclass and the propagated terms and rels

[1.3.6.1]

Fix get_subclass() to return value as required by cld_schema_api

[1.3.6.0]

Enabled decode derivation method to keep unmapped values without change

[1.3.5.0]

Include filename in meta in data loaders

[1.3.4.0]

Added functionality in RunCypher action to force rename columns in dataframe if parameter "remove_col_prefixes" is set to true

[1.3.3.0]

Extended build_uri action with additional property "store_on_existing_nodes" for storing the uri on the node

[1.3.2.0]

Added functions to create subclass, get subclass and propagate rels and terms in model manager

[1.3.1.0]

Allowed for import/export schema to/from linkml structures