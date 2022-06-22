import pytest
from data_providers import data_provider
import pandas as pd


# Provide a DataProvider object (which contains a database connection)
# that can be used by the various tests that need it
@pytest.fixture(scope="module")
def dp():
    dp = data_provider.DataProvider(mode='noschema')
    dp.clean_slate()
    yield dp


def test_get_data_noschema(dp):
    dp.clean_slate()
    q = """
    UNWIND $data AS row
    CALL apoc.create.node([row['label']], row['properties']) YIELD node 
    RETURN id(node)
    """
    params = {'data': [
        {'label': 'Z', 'properties': {'z':1, 'zz':2.1}},
        {'label': 'B', 'properties': {'b b':'xYz'}},
        {'label': 'C', 'properties': {}},
        {'label': 'D', 'properties': {'d':3}},
        {'label': 'E', 'properties': {'e':4}},
        {'label': 'Z', 'properties': {'z':2}},
        {'label': 'B', 'properties': {'b b': 'AbC'}},
        {'label': 'C', 'properties': {}},
        {'label': 'D', 'properties': {'d': 33}},
    ]}
    nodeids = [res['id(node)'] for res in dp.query(q, params)]
    dp.link_nodes_by_ids(nodeids[0], nodeids[1], 'REL1')
    dp.link_nodes_by_ids(nodeids[2], nodeids[1], 'REL2')
    dp.link_nodes_by_ids(nodeids[3], nodeids[1], 'REL3')
    dp.link_nodes_by_ids(nodeids[5], nodeids[6], 'REL1')
    dp.link_nodes_by_ids(nodeids[7], nodeids[6], 'REL2')
    dp.link_nodes_by_ids(nodeids[8], nodeids[6], 'REL3')
    result = dp.get_data_generic(classes=['Z', 'B', 'C', 'D', 'E'], return_nodeid=False, prefix_keys_with_label=False)
    expected_result =  pd.DataFrame([
        {'zz':2.1, 'z':1, 'd':3, 'b b':'xYz','e':4},
        {'z': 2, 'd': 33, 'b b': 'AbC', 'e': 4},
    ])
    assert result.equals(expected_result)


def test_get_data_noschema_optional_class(dp):
    dp.clean_slate()
    ia = dp.create_node_by_label_and_dict('Strain', {'type':'apple'})
    io = dp.create_node_by_label_and_dict('Strain', {'type': 'orange'})
    if_ = dp.create_node_by_label_and_dict('Fruit', {'n_types':9999})
    iarel = dp.create_node_by_label_and_dict('AdditionalInfo', {'desc': 'blah-blah'})
    dp.link_nodes_by_ids(ia,if_,'IS_A')
    dp.link_nodes_by_ids(io, if_, 'IS_A')
    dp.link_nodes_by_ids(ia, iarel, 'HAS')
    #1
    result = dp.get_data_generic(
        classes = ['Strain', 'Fruit', 'AdditionalInfo'],
        prefix_keys_with_label = True, return_nodeid=False
    )
    assert len(result) == 1
    # print(result)
    #2
    result = dp.get_data_generic(
        classes=['Strain', 'Fruit', 'AdditionalInfo**'],
        prefix_keys_with_label=True, return_nodeid=False
    )
    assert len(result) == 2
    # print(result)

