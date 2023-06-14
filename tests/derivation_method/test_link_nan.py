import pytest
import pandas as pd
from data_providers import DataProvider
import os
import json
from derivation_method import derivation_method_factory
from derivation_method import action as Action
import numpy as np


# TODO: relocate test and test data when derivation_method tests are merged

filepath = os.path.dirname(__file__)
study = 'test_study'


@pytest.fixture
def interface():
    yield DataProvider(rdf=True)


def test_link_action_existing(interface):
    # Test a link method. PNG of method can be found in data/method png
    # Perform existing link tests to ensure they are unaffected

    # -------------------- derive_test_link.json -----------------------
    # loading test data and Class-Relionship schema
    interface.clean_slate()
    with open(os.path.join(filepath, 'data', 'test_data_single.json')) as jsonfile:
        dct = json.load(jsonfile)
    interface.load_arrows_dict(dct)

    # loading test Method metadata
    with open(os.path.join(filepath, 'data', 'raw', 'derive_test_link.json')) as jsonfile:
        inline = json.load(jsonfile)
    method = derivation_method_factory(data=inline, interface=interface, study=study)
    df = pd.DataFrame
    for action in method.actions:
        df = action.apply(df)

    result = interface.query(
        """
        MATCH (ts:`Test Name`)-[:`HAS NUMERIC RESULT`]->(nr:`Numeric Result`)
        RETURN ts, nr
        """
    )

    expected = [{'ts': {'rdfs:label': 'Weight'}, 'nr': {'rdfs:label': '100'}}]

    for i in result:
        assert i in expected

    assert all(i in df.columns for i in ["NR", "_id_NR", "VS", "_id_VS", "TS", "_id_TS"])
    assert df.at[0, "NR"] == '100' and df.at[0, "TS"] == "Weight"

    # -------------------- derive_test_link_from_value -----------------------
    interface.clean_slate()
    # loading test data and Class-Relationship schema
    with open(os.path.join(filepath, 'data', 'test_data_multiple.json')) as jsonfile:
        dct = json.load(jsonfile)
    interface.load_arrows_dict(dct)

    # loading test Method metadata
    with open(os.path.join(filepath, 'data', 'raw', 'derive_test_link_from_value.json')) as jsonfile:
        inline = json.load(jsonfile)
    method = derivation_method_factory(data=inline, interface=interface, study=study)
    df = pd.DataFrame
    for action in method.actions:
        df = action.apply(df)

    result = interface.query(
        """
        MATCH (ts:`Test Name`)-[:`Subject`]->(s:`Subject`)
        RETURN ts, s
        """
    )

    expected = [{'ts': {'rdfs:label': 'Weight'}, 's': {'rdfs:label': '0001'}},
                {'ts': {'rdfs:label': 'Weight'}, 's': {'rdfs:label': '0002'}}]
    for i in result:
        assert i in expected

    # -------------------- derive_test_link_to_value -----------------------
    interface.clean_slate()
    # loading test data and Class-Relationship schema
    with open(os.path.join(filepath, 'data', 'test_data_multiple.json')) as jsonfile:
        dct = json.load(jsonfile)
    interface.load_arrows_dict(dct)
    # loading test Method metadata
    with open(os.path.join(filepath, 'data', 'raw', 'derive_test_link_to_value.json')) as jsonfile:
        inline = json.load(jsonfile)
    method = derivation_method_factory(data=inline, interface=interface, study=study)
    df = pd.DataFrame
    for action in method.actions:
        df = action.apply(df)

    result = interface.query(
        """
        MATCH (ts:`Test Name`)<-[:`HAS TEST NAME`]-(s:`Subject`)
        RETURN ts, s
        """
    )

    expected = [{'ts': {'rdfs:label': 'Weight'}, 's': {'rdfs:label': '0001'}},
                {'ts': {'rdfs:label': 'Weight'}, 's': {'rdfs:label': '0002'}}]
    for i in result:
        assert i in expected

    # -------------------- derive_test_link_to_from_value -----------------------
    interface.clean_slate()
    # loading test data and Class-Relationship schema
    with open(os.path.join(filepath, 'data', 'test_data_multiple.json')) as jsonfile:
        dct = json.load(jsonfile)
    interface.load_arrows_dict(dct)
    # loading test Method metadata
    with open(os.path.join(filepath, 'data', 'raw', 'derive_test_link_to_from_value.json')) as jsonfile:
        inline = json.load(jsonfile)
    method = derivation_method_factory(data=inline, interface=interface, study=study)
    df = pd.DataFrame
    for action in method.actions:
        df = action.apply(df)

    result = interface.query(
        """
        MATCH (ts:`Test Name`)-[:`Subject`]->(s:`Subject`)
        RETURN ts, s
        """
    )

    expected = [{'ts': {'rdfs:label': 'Weight'}, 's': {'rdfs:label': '0001'}}]

    assert result == expected
    interface.clean_slate()


def test_link_nan(interface):

    # --------------------- test_link_nan ------------------------------
    # loading test data and Class-Relationship schema
    interface.clean_slate()
    with open(os.path.join(filepath, 'data', 'test_data_nan.json')) as jsonfile:
        dct = json.load(jsonfile)
    interface.load_arrows_dict(dct)

    with open(os.path.join(filepath, 'data', 'raw', 'derive_test_link_nan.json')) as jsonfile:
        inline = json.load(jsonfile)
    method = derivation_method_factory(data=inline, interface=interface, study=study)
    df = pd.DataFrame
    for action in method.actions:
        # Extend Dataframe to simulate creation of new data with NaNs to be merged by link
        if type(action) == Action.CallAPI:
            df['NR1'] = [100, 100, 23, None, pd.NaT, np.NaN]
        else:
            df = action.apply(df)

    result = interface.query(
        """
        MATCH(:`Vital Signs`)-[:test_rel]->(n:NR1) 
        RETURN properties(n) as props
        """
    )

    expected = [{'props': {'rdfs:label': 100.0}},
                {'props': {'rdfs:label': 100.0}},
                {'props': {'rdfs:label': 23.0}},
                {'props': {}},
                {'props': {}},
                {'props': {}}]

    assert result == expected, 'Action failed to create expected none types'

    detached_nodes_dct = []
    for action in method.actions:
        detached_nodes_dct = action.rollback(detached_nodes_dct)

    rollback_result = interface.query(
        """
        MATCH(n:NR1) 
        RETURN n as remaining_nodes
        """
    )

    assert len(detached_nodes_dct[0]['deleted_nan_nodes']) == 1, 'Link rollback failed to cleanup NaN nodes'
    assert len(rollback_result) == 2, 'Link rollback failed to cleanup NaN nodes'
