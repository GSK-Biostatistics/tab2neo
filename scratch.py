from query_builders.query_builder import QueryBuilder
from data_providers.data_provider import DataProvider
import pandas as pd
import pytest
from data_providers import data_provider
from query_builders import query_builder
from unittest.mock import patch 
import pytest_mock
import os
import json
from data_providers.data_provider import DataProvider

dp = DataProvider (
    host = "bolt://10.40.225.48:37687/",
    credentials=("neo4j", "admin"),
    debug=True
)

filepath = os.path.dirname(__file__)

dp.clean_slate()
# loading metadata (Class/Property from json created in arrows.app)
with open(os.path.join(filepath, 'tests/test_data_providers/data', 'test_get_data_generic3.json')) as jsonfile:
    dct = json.load(jsonfile)
    dp.load_arrows_dict(dct)

df = dp.get_data_generic(
labels = ['Visit', 'Visit Category'],
infer_rels=True,
labels_to_pack={'Visit': 'Visit Category'},    
use_shortlabel=True,    
return_propname=False,
only_props=['rdfs:label'],    
limit=10,
return_q_and_dict=True,
return_nodeid=False,
pivot=True #TODO: fix for labels_to_pack != None
)[0]

expected_df = pd.DataFrame([{'VISITCAT':'VISIT 1'}])
assert df.equals(expected_df)

