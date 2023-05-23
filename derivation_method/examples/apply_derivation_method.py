"""
Example script for applying a Derivation Method.

In order to connect to your database, either supply the following environment variables:
    - NEO4J_HOST
    - NEO4J_USER
    - NEO4J_PASSWORD
or give the parameters in the DataProvider class below.

--- THIS SCRIPT WILL EMPTY THE CONNECTED DATABASE WHEN RUN ---

This script uses an example derivation method and data supplied in the derivation_method/examples folder 
that this script should also be present in. It can also be supplied with custom derivations when given 
the correct path to the file and relevant data.
"""

# import required modules and packages
import os
from data_providers.data_provider import DataProvider
from derivation_method.derivation_method import derivation_method_factory
from derivation_method.examples.load_data import load_data

# used to interact with your neo4j database
interface = DataProvider(
    # host=bolt,
    # credentials=(user, pwd)
    verbose=False
)

DERIVATION_NAME = "example_derivation_method"
DERIVATION_FILEPATH = os.path.join('derivation_method', 'examples', f'{DERIVATION_NAME}.json')
# A nice way of looking at derivation_method json files is to load them into the webapp called `arrows.app`
# Just copy the json file contents and paste it directly into the arrows.app interface!

# load some example data into your database - THIS WILL EMPTY THE CONNECTED DATABASE
load_data()

# create a method object that represents the method loaded into the database. 
# It is loaded from file/dict into the database at this stage.
method = derivation_method_factory(
    interface=interface, 
    data=DERIVATION_FILEPATH,  # path to file (json), derivation name already in database, json dict
    study='Example Study',  # provide if no study is present in the database
    overwrite_db=True  # to overwrite any derivation with the same name already present in the database
    )

# this will provide an object to interact will all the derivations in the database
online = derivation_method_factory(interface=interface, data='online')
# e.g.
# print(online.available_methods)
# print(online.applied_methods)

# Now we can apply this method using two ways
#   First - which applies all actions in one process
method.apply()

#   Second - which applies actions step by step allowing you to look at the dataframe for each action:
# df = None
# for action in method.actions:
#     df = action.apply(df)
#     print(df)
#     input("Press Enter to continue...")

# Now the links have been created in the database we can query them out
res_df, query, params = interface.get_data_cld(labels=['Subject', 'TEST_DATA', 'Age', 'TestValue', 'NewValue'], return_nodeid=False)
print('Result DataFrame:')
print(res_df)
