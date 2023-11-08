import json
import os
import pandas as pd

filepath = os.path.dirname(__file__)
import pytest
from data_providers import data_provider
from model_managers.model_manager import ModelManager


# Provide a DataProvider object (which contains a database connection)
# that can be used by the various tests that need it
@pytest.fixture(scope="module")
def dp():
    dp = data_provider.DataProvider(verbose=False)
    yield dp

@pytest.fixture(scope="module")
def mm():
    mm = ModelManager(verbose=False, debug=True)
    yield mm

def test_get_data_generic1(dp):
    dp.clean_slate()
    # loading metadata (Class/Property from json created in arrows.app)
    with open(os.path.join(filepath, 'data', 'test_get_data_generic1.json')) as jsonfile:
        dct = json.load(jsonfile)
    dp.load_arrows_dict(dct)

    res = dp.get_data_generic(labels=[], rels=[{'from': 'Visit', 'to': 'Visit Category', 'type': 'HAS CATEGORY'}],
                              return_q_and_dict=True, return_nodeid=False, use_shortlabel=False, return_propname=True)
    df = res[0]
    df = df[sorted(df.columns)]
    q = res[1]
    params = res[2]
    expected_df = pd.DataFrame([{'Visit Category.rdfs:label': 'SCHEDULED', 'Visit.rdfs:label': 'VISIT 1'}])
    print(df)
    print(expected_df)
    assert df.equals(expected_df)
    assert q.startswith(
        'MATCH (`Visit`:`Visit`),\n(`Visit Category`:`Visit Category`),\n(`Visit`)-[`Visit_HAS CATEGORY_Visit Category`:`HAS CATEGORY`]->(`Visit Category`)')


def test_get_data_generic_nopropname(dp):
    dp.clean_slate()
    # loading metadata (Class/Property from json created in arrows.app)
    with open(os.path.join(filepath, 'data', 'test_get_data_generic1.json')) as jsonfile:
        dct = json.load(jsonfile)
    dp.load_arrows_dict(dct)

    df = dp.get_data_generic(labels=[], rels=[{'from': 'Visit', 'to': 'Visit Category', 'type': 'HAS CATEGORY'}],
                             return_q_and_dict=False, return_nodeid=False, return_propname=False)
    df = df[sorted(df.columns)]
    expected_df = pd.DataFrame([{'Visit': 'VISIT 1', 'Visit Category': 'SCHEDULED'}])
    assert df.equals(expected_df)


def test_get_data_generic_shortlabel(dp):
    dp.clean_slate()
    # loading metadata (Class/Property from json created in arrows.app)
    with open(os.path.join(filepath, 'data', 'test_get_data_generic1.json')) as jsonfile:
        dct = json.load(jsonfile)
    dp.load_arrows_dict(dct)

    df, q, params = dp.get_data_generic(
        labels=[],
        rels=[{'from': 'Visit', 'to': 'Visit Category', 'type': 'HAS CATEGORY'}],
        where_map={'Visit': {'rdfs:label': 'VISIT 1'}},
        return_q_and_dict=True,
        return_nodeid=True,
        return_propname=False,
        use_shortlabel=True
    )
    df = df[sorted(df.columns)]
    expected_cols = ['VISIT', 'VISITCAT', dp.qb.gen_id_col_name("VISIT"), dp.qb.gen_id_col_name("VISITCAT")]
    df = df[expected_cols[:-2]]
    expected_df = pd.DataFrame([{'VISIT': 'VISIT 1', 'VISITCAT': 'SCHEDULED'}])
    assert df.equals(expected_df)
    assert q.startswith(
        "MATCH (`VISIT`:`Visit`),\n(`VISITCAT`:`Visit Category`),\n(`VISIT`)-[`VISIT_HAS CATEGORY_VISITCAT`:`HAS CATEGORY`]->(`VISITCAT`)\nWHERE `VISIT`.`rdfs:label` = $par_1")
    assert params == {'par_1': 'VISIT 1'}


def test_get_data_generic_shortlabel_inferrels(dp):
    dp.clean_slate()
    # loading metadata (Class/Property from json created in arrows.app)
    with open(os.path.join(filepath, 'data', 'test_get_data_generic1.json')) as jsonfile:
        dct = json.load(jsonfile)
    dp.load_arrows_dict(dct)

    df, q, params = dp.get_data_generic(
        labels=['Visit', 'Visit Category'],
        infer_rels=True,
        where_map={'Visit': {'rdfs:label': 'VISIT 1'}},
        return_q_and_dict=True,
        return_nodeid=True,
        return_propname=False,
        use_shortlabel=True
    )
    df = df[sorted(df.columns)]
    expected_cols = ['VISIT', 'VISITCAT', dp.qb.gen_id_col_name("VISIT"), dp.qb.gen_id_col_name("VISITCAT")]
    df = df[expected_cols[:-2]]
    expected_df = pd.DataFrame([{'VISIT': 'VISIT 1', 'VISITCAT': 'SCHEDULED'}])
    assert df.equals(expected_df)
    assert q.startswith(
        "MATCH (`VISIT`:`Visit`),\n(`VISITCAT`:`Visit Category`),\n(`VISIT`)-[`VISIT_HAS CATEGORY_VISITCAT`:`HAS CATEGORY`]->(`VISITCAT`)\nWHERE `VISIT`.`rdfs:label` = $par_1")
    assert params == {'par_1': 'VISIT 1'}


def test_get_data_generic_excllabel(dp):
    dp.clean_slate()
    # loading metadata (Class/Property from json created in arrows.app)
    with open(os.path.join(filepath, 'data', 'test_get_data_generic1.json')) as jsonfile:
        dct = json.load(jsonfile)
    dp.load_arrows_dict(dct)

    df, q, params = dp.get_data_generic(
        labels=[f'Visit{dp.ECLASS_MARKER}', 'Visit Category'],
        infer_rels=True,
        where_map={'Visit': {'rdfs:label': 'VISIT 1'}},        
        return_q_and_dict=True,
        return_propname=False,
        return_nodeid=False,
        use_shortlabel=True
    )
    df = df[sorted(df.columns)]
    expected_cols = ['VISITCAT']    
    expected_df = pd.DataFrame([{'VISITCAT': 'SCHEDULED'}])
    print(df)
    assert df.equals(expected_df)    
    assert q.startswith(
        "MATCH (`VISIT`:`Visit`),\n(`VISITCAT`:`Visit Category`),\n(`VISIT`)-[`VISIT_HAS CATEGORY_VISITCAT`:`HAS CATEGORY`]->(`VISITCAT`)\nWHERE `VISIT`.`rdfs:label` = $par_1")
    assert params == {'par_1': 'VISIT 1'}

def test_get_data(dp):
    # Preparing data : create some `Class` nodes, and some "CLASS_RELATES_TO" relationships between them
    dp.clean_slate()
    my_classes = dp.mm.create_related_classes_from_list([['Subject', 'Treatment']])
    assert my_classes == ['Subject', 'Treatment']

    dp.mm.set_short_label('Subject', 'USUBJID')
    # This time, we have 2 Properties for Treatment
    dp.mm.set_short_label('Treatment', 'TRT01A')
    cypher = """
    CREATE (s:Subject {`rdfs:label`:'1234'})-[:`Treatment`]->(t:Treatment {`rdfs:label`:'Placebo', ord: 1.0}) 
    RETURN id(s) as subject, id(t) as treatment
    """
    cq_res = dp.query(cypher)

    df = dp.get_data_generic(labels=my_classes, infer_rels=True)
    for col in my_classes:
        id_col_name = dp.qb.gen_id_col_name(col)
        assert id_col_name in df.columns, f"Missing column {col} in the returned df"  # node id columns
        assert col + ".rdfs:label" in df.columns, f"Missing column {col + '.rdfs:label'} in the returned df"  # data columns
    expected_columns = ['Subject.rdfs:label', 'Treatment.rdfs:label', 'Treatment.ord'] + \
                       [dp.qb.gen_id_col_name(label) for label in ['Subject', 'Treatment']]
    expected_df = pd.DataFrame([['1234', 'Placebo', 1.0, cq_res[0]['subject'], cq_res[0]['treatment']]],
                               columns=expected_columns)
    assert df[expected_columns].equals(expected_df)

    # --------- Add a 2nd data point, with the same schema --------------
    cypher = """
    CREATE (s:Subject {`rdfs:label`:'9876'})-[:`Treatment`]->(t:Treatment {`rdfs:label`:'Active', ord: 2.0})
    RETURN id(s) as subject, id(t) as treatment
    """
    cq_res = dp.query(cypher)
    df = dp.get_data_generic(labels=my_classes, infer_rels=True)
    assert len(df) == 2

    expected_df.loc[1] = ["9876", "Active", 2.0, cq_res[0]['subject'], cq_res[0]['treatment']]
    assert df[expected_columns].equals(expected_df)  # Compare regardless of column order

    # --------  Now retrieve the same data, but with conditions ------

    df = dp.get_data_generic(labels=my_classes, infer_rels=True, where_map={'Treatment': {'rdfs:label': 'Active'}})
    assert len(df) == 1

    # Add a 3rd data point, with the same schema
    cypher = """
    CREATE (s:Subject {`rdfs:label`:'555'})-[:`Treatment`]->(t:Treatment {`rdfs:label`:'Active', ord:1.0})
    RETURN id(s) as subject, id(t) as treatment
    """
    cq_res = dp.query(cypher)
    df = dp.get_data_generic(labels=my_classes, infer_rels=True)
    assert len(df) == 3

    df = dp.get_data_generic(labels=my_classes, infer_rels=True,
                             where_map={'Treatment': {'rdfs:label': 'Active'}, 'Subject': {'rdfs:label': '9876'}})
    expected_df = expected_df[expected_df['Subject.rdfs:label'] == '9876'].reset_index(drop=True)
    assert len(df) == 1
    assert df[expected_columns].equals(expected_df)


def test_get_data_generic_only_props(dp):
    dp.clean_slate()
    # loading metadata (Class/Property from json created in arrows.app)
    with open(os.path.join(filepath, 'data', 'test_get_data_generic2.json')) as jsonfile:
        dct = json.load(jsonfile)
    dp.load_arrows_dict(dct)

    df = dp.get_data_generic(
        labels=[],
        rels=[{'from': 'Visit', 'to': 'Visit Category', 'type': 'HAS CATEGORY'}],
        return_nodeid=False,
        return_propname=True,
        use_shortlabel=True
    )
    assert "VISIT.rdfs:label" in df.columns
    assert "VISIT.another_prop" in df.columns
    assert "VISIT.another_prop2" in df.columns

    df = dp.get_data_generic(
        labels=[],
        rels=[{'from': 'Visit', 'to': 'Visit Category', 'type': 'HAS CATEGORY'}],
        return_nodeid=False,
        return_propname=True,
        only_props="rdfs:label",
        use_shortlabel=True
    )
    assert "VISIT.rdfs:label" in df.columns
    assert "VISIT.another_prop" not in df.columns
    assert "VISIT.another_prop2" not in df.columns

def test_get_data_virtual(dp, mm):
    dp.clean_slate()

    q = """
    MERGE (a:Class{label:"class1", short_label:"C1"})
    MERGE (z:Class {label: "Apple"})-[:HAS_CONTROLLED_TERM]->(t1:Term {`Codelist Code`: 'term1c', `Term Code`: 'term1t', `Order`:2})
    MERGE (b:Class{label:"class2", short_label:"C2"})
    MERGE (c:Class{label:"class3", short_label:"C3"})
    MERGE (d:Class{label:"class4", short_label:"C4"})
    MERGE (b)-[:HAS_CONTROLLED_TERM]->(t2:Term {`Codelist Code`: 'term2c', `Term Code`: 'term2t', `Order`:2})
    """
    dp.query(q)
    cond  =  [{
            'EXISTS>': {'include': [{'Test' : {'uri': ['neo4j://graph.schema#Term/S980028/S91301']}}]},
            'NOT EXISTS': {'exclude': [{'Ser': {'rdfs:label': ['Y']}}, 'Pop', 'Asta']}
        }]
    res6 = mm.create_subclass([['class1', 'class3', cond]])

    res = dp.get_data_virtual(labels=['class3'])
    assert res