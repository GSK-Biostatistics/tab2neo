import re
import pytest
from query_builders.query_builder import QueryBuilder


# Provide QueryBuilder object that can be used by the various tests that need it
# (inside, it includes a database connection)
@pytest.fixture(scope="module")
def qbr():
    qbr = QueryBuilder()
    yield qbr

def test_generate_1match(qbr: QueryBuilder):
    res = qbr.generate_1match(label='Study Subject')
    assert res == "(`Study Subject`:`Study Subject`)"

    res = qbr.generate_1match(label={'label': 'Study Subject', 'short_label': 'n1'})
    assert res == "(`n1`:`Study Subject`)"


def test_generate_query_body1(qbr: QueryBuilder):
    q, params = qbr.generate_query_body(
        labels=[],
        rels=[{'to': 'Exposure', 'from': 'Subject', 'type': 'HAS'}],
        where_map={'Subject': {'id': '001', 'name': 'Bob'}}
    )
    assert q == "MATCH (`Subject`)-[`Subject_HAS_Exposure`:`HAS`]->(`Exposure`)\nWHERE `Subject`.`id` = $par_1 AND `Subject`.`name` = $par_2"
    assert params == {'par_1': '001', 'par_2': 'Bob'}


def test_generate_query_body2(qbr: QueryBuilder):
    q, params = qbr.generate_query_body(
        labels=['Subject', 'Exposure'],
        rels=[{'to': 'Exposure', 'from': 'Subject', 'type': 'HAS'}],
        where_map={'Subject': {'id': '001', 'name': 'Bob'}}
    )
    assert q == "MATCH (`Subject`:`Subject`),\n(`Exposure`:`Exposure`),\n(`Subject`)-[`Subject_HAS_Exposure`:`HAS`]->(`Exposure`)\nWHERE `Subject`.`id` = $par_1 AND `Subject`.`name` = $par_2"
    assert params == {'par_1': '001', 'par_2': 'Bob'}


# def test_generate_schema_check_query_body(qbr: QueryBuilder):
#     q, params = qbr.generate_schema_check_query_body(
#         labels=[],
#         rels=[{'to': 'Exposure', 'from': 'Subject', 'type': 'HAS'}]
#     )
#     assert q == "MATCH (`subject`:`Class`{label:'Subject'}),\n(`exposure`:`Class`{label:'Exposure'}),\n(`subject`)<-[:FROM]-(`st_has_ee`:Relationship)-[:TO]->(`exposure`)\nWHERE `st_has_ee`.`type` = $par_1"
#     assert params == {'par_1': 'HAS'}


def test_split_out_optional(qbr: QueryBuilder):
    # #Test case #1
    labels = ['Subject', 'Sex', 'Exposure**', 'Exposure Unit**', 'Exposure Laterality**']
    rels = [
        {'from': pair[0], 'to': pair[1], 'optional': i > 0}
        for i, pair in enumerate([
            ['Subject', 'Sex'],
            ['Subject', 'Exposure'],
            ['Subject', 'Exposure Unit'],
            ['Subject', 'Exposure Laterality']
        ])
    ]
    res = qbr.split_out_optional(
        labels=labels,
        rels=rels,
        oclass_marker='**')
    # print(res)
    expected_res = [
        (
            ['Subject', 'Sex'],
            [{'from': 'Subject', 'to': 'Sex', 'optional': False}]
        ),
        (
            ['Exposure'],
            [{'from': 'Subject', 'to': 'Exposure', 'optional': True}]
        ),
        (
            ['Exposure Unit'],
            [{'from': 'Subject', 'to': 'Exposure Unit', 'optional': True}]
        ),
        (
            ['Exposure Laterality'],
            [{'from': 'Subject', 'to': 'Exposure Laterality', 'optional': True}]
        )
    ]
    assert res == expected_res

    # #Test case #2
    rels = [
        {'from': pair[0], 'to': pair[1], 'optional': i > 0}
        for i, pair in enumerate([
            ['Subject', 'Sex'],
            ['Subject', 'Exposure'],
            ['Exposure', 'Exposure Unit'],
            ['Exposure', 'Exposure Laterality']
        ])
    ]
    res = qbr.split_out_optional(
        labels=labels,
        rels=rels,
        oclass_marker='**')
    expected_res = [
        (
            ['Subject', 'Sex'],
            [{'from': 'Subject', 'to': 'Sex', 'optional': False}]
        ),
        (
            ['Exposure', 'Exposure Unit', 'Exposure Laterality'],
            [
                {'from': 'Subject', 'to': 'Exposure', 'optional': True},
                {'from': 'Exposure', 'to': 'Exposure Unit', 'optional': True},
                {'from': 'Exposure', 'to': 'Exposure Laterality', 'optional': True}]
        )
    ]
    assert res == expected_res

    # #Test case #3
    labels = ['Subject', 'Sex', 'Exposure**', 'Exposure Unit**', 'Visit**', 'Vitals**', 'Vitals Unit**']
    rels = [
        {'from': pair[0], 'to': pair[1], 'optional': i > 0}
        for i, pair in enumerate([
            ['Subject', 'Sex'],
            ['Subject', 'Exposure'],
            ['Exposure', 'Exposure Unit'],
            ['Exposure', 'Visit'],
            ['Subject', 'Vitals'],
            ['Vitals', 'Vitals Unit'],
            ['Vitals', 'Visit'],
        ])
    ]
    res = qbr.split_out_optional(
        labels=labels,
        rels=rels,
        oclass_marker='**')
    expected_res = [
        (
            ['Subject', 'Sex'],
            [
                {'from': 'Subject', 'to': 'Sex', 'optional': False}
            ]
        ),
        (
            ['Exposure', 'Exposure Unit', 'Visit'],
            [
                {'from': 'Subject', 'to': 'Exposure', 'optional': True},
                {'from': 'Exposure', 'to': 'Exposure Unit', 'optional': True},
                {'from': 'Exposure', 'to': 'Visit', 'optional': True}]
        ),
        (
            ['Vitals', 'Visit', 'Vitals Unit'],
            [
                {'from': 'Subject', 'to': 'Vitals', 'optional': True},
                {'from': 'Vitals', 'to': 'Vitals Unit', 'optional': True},
                {'from': 'Vitals', 'to': 'Visit', 'optional': True},
                {'from': 'Exposure', 'to': 'Visit', 'optional': True}
            ]
        )
    ]
    assert res == expected_res


def test_check_connectedness(qbr: QueryBuilder):
    # 1 label test
    res = qbr.check_connectedness(["1Label"], [])
    assert res

    # 1 label test (2)
    res = qbr.check_connectedness(["1Label"], [{'from': '', 'to': ''}, {'from': '', 'to': ''}])
    assert res

    # connected
    res = qbr.check_connectedness(
        [],
        [
            {'from': 'Subject', 'to': 'Sex'},
            {'from': 'Subject', 'to': 'Exposure'},
            {'from': 'Exposure Dose', 'to': 'Exposure Dose Unit'},
            {'from': 'Exposure', 'to': 'Exposure Dose'},
        ])
    assert res

    # not connected
    res = qbr.check_connectedness(
        [],
        [
            {'from': 'Subject', 'to': 'Sex'},
            {'from': 'Subject', 'to': 'Exposure'},
            #{'from': 'Exposure', 'to': 'Exposure Dose'},
            {'from': 'Exposure Dose', 'to': 'Exposure Dose Unit'}
        ])
    assert not res

def test_enrich_labels_from_rels(qbr: QueryBuilder):
    res = qbr.enrich_labels_from_rels(
        labels = ['Study', 'Domain**'],
        rels = [
            {'from': 'Subject', 'to': 'Sex'},
            {'from': 'Subject', 'to': 'Exposure', 'optional': True},
            {'from': 'Exposure', 'to': 'Exposure Dose', 'optional': False},
            {'from': 'Exposure Dose', 'to': 'Exposure Dose Unit', 'optional': True}
        ],
        oclass_marker = '**'
    )
    assert res == ['Study', 'Domain**', 'Subject', 'Sex', 'Exposure', 'Exposure Dose', 'Exposure Dose Unit**']


def test_where_conditions_ranges(qbr):

    #all True
    test_map = {"Age": {"AGE": {"min": 18, "max": 65, "max_include": True, "min_include": True, "incl_null": True}}}

    (Cypher_list, data_binding_dict) = qbr.list_where_conditions_per_dict(mp=test_map)
    expected = ['(`Age`.`AGE` IS NULL OR ($par_1 <= `Age`.`AGE` <= $par_2))']
    # print(Cypher_list)
    # print(expected)
    assert Cypher_list == expected
    assert data_binding_dict == {'par_1': 18, 'par_2': 65}

    #one False, incl_null == None
    test_map = {"Age": {"AGE": {"min": 18, "max": 65, "max_include": False, "min_include": True}}}

    (Cypher_list, data_binding_dict) = qbr.list_where_conditions_per_dict(mp=test_map)
    assert Cypher_list == ['($par_1 <= `Age`.`AGE` < $par_2)']
    assert data_binding_dict == {'par_1': 18, 'par_2': 65}

    #borders False, incl_null == None
    test_map = {"Age": {"AGE": {"min": 18, "max": 65, "max_include": False, "min_include": False}}}

    (Cypher_list, data_binding_dict) = qbr.list_where_conditions_per_dict(mp=test_map)
    assert Cypher_list == ['($par_1 < `Age`.`AGE` < $par_2)']
    assert data_binding_dict == {'par_1': 18, 'par_2': 65}

    #all False
    test_map = {"Age": {"AGE": {"min": 18, "max": 65, "max_include": False, "min_include": False, "incl_null": False}}}

    (Cypher_list, data_binding_dict) = qbr.list_where_conditions_per_dict(mp=test_map)
    assert Cypher_list == ['(`Age`.`AGE` IS NOT NULL AND ($par_1 < `Age`.`AGE` < $par_2))']
    assert data_binding_dict == {'par_1': 18, 'par_2': 65}

    #incl_null == False alone
    test_map = {"Age": {"AGE": {"incl_null": False}}}

    (Cypher_list, data_binding_dict) = qbr.list_where_conditions_per_dict(mp=test_map)
    assert Cypher_list == ['`Age`.`AGE` IS NOT NULL']
    assert data_binding_dict == {'par_1': None, 'par_2': None}


def test_where_rel_1(qbr):
    test_map = {
        'nobs': {
            'EXISTS': {'include': ['Ser', 'Pop', 'Asta']},
            'NOT EXISTS': {'exclude_matched': ['Ser', 'Pop', 'Asta']}
        }
    }
    (Cypher_list, data_binding_dict) = qbr.list_where_rel_conditions_per_dict(mp=test_map)
    expected_cypher_list = [
        "EXISTS {MATCH (`nobs`)-[]-(x) WHERE (x:`Ser` OR x:`Pop` OR x:`Asta`)}",
        "NOT EXISTS {MATCH (`nobs`)-[]-(x) WHERE NOT (x in [`Ser`, `Pop`, `Asta`])}"
    ]
    # print("\n")
    # print(Cypher_list)
    # print(expected_cypher_list)
    assert Cypher_list == expected_cypher_list
    assert data_binding_dict == {}

def test_where_rel_2(qbr):
    test_map = {
        'nobs': {
            'EXISTS>': {'include': ['Ser', 'Pop', 'Asta']},
            'NOT EXISTS<': {'exclude_matched': ['Ser', 'Pop', 'Asta']}
        }
    }
    (Cypher_list, data_binding_dict) = qbr.list_where_rel_conditions_per_dict(mp=test_map)
    expected_cypher_list = [
        "EXISTS {MATCH (`nobs`)-[]->(x) WHERE (x:`Ser` OR x:`Pop` OR x:`Asta`)}",
        "NOT EXISTS {MATCH (`nobs`)<-[]-(x) WHERE NOT (x in [`Ser`, `Pop`, `Asta`])}"
    ]
    # print("\n")
    # print(Cypher_list)
    # print(expected_cypher_list)
    assert Cypher_list == expected_cypher_list
    assert data_binding_dict == {}


def test_generate_query_body(qbr: QueryBuilder):
    q, params = qbr.generate_query_body(
        labels=[],
        rels=[{'to': 'Exposure', 'from': 'Subject', 'type': 'HAS'}],
        where_rel_map={
            'Subject': {
                'EXISTS': {'exclude': ['Adverse Events']}
            }
        }
    )
    expected_q = "MATCH (`Subject`)-[`Subject_HAS_Exposure`:`HAS`]->(`Exposure`)\nWHERE EXISTS {MATCH (`Subject`)-[]-(x) WHERE NOT (x:`Adverse Events`)}"
    assert q == expected_q
    assert params == {}
