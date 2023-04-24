import json
import os
import pandas as pd

filepath = os.path.dirname(__file__)
import pytest
from data_providers import data_provider

# Provide a DataProvider object (which contains a database connection)
# that can be used by the various tests that need it
@pytest.fixture(scope="module")
def dp():
    dp = data_provider.DataProvider(verbose=False)
    yield dp

def test_get_data_generic_pivot(dp):
    dp.clean_slate()
    # loading metadata (Class/Property from json created in arrows.app)
    with open(os.path.join(filepath, 'data', 'test_get_data_generic3.json')) as jsonfile:
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
