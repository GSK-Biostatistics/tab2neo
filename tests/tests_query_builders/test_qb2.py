import pytest
from query_builders.query_builder import QueryBuilder


# Provide QueryBuilder object that can be used by the various tests that need it
# (inside, it includes a database connection)
@pytest.fixture(scope="module")
def qbr():
    qbr = QueryBuilder()
    yield qbr

def test_generate_1match(qbr: QueryBuilder):
    res = qbr.generate_1match(label='Study Subject', tag='S S')
    assert res == "(`S S`:`Study Subject`)"
    
    res = qbr.generate_1match(label='Study Subject')
    assert res == "(`Study Subject`:`Study Subject`)"

    res = qbr.generate_1match(label={'label': 'Study Subject', 'short_label': 'n1'})
    assert res == "(`n1`:`Study Subject`)"


class TestGenerateQueryBody:

    def test_rel(self, qbr: QueryBuilder):
        q, params = qbr.generate_query_body(
            labels=[],
            rels=[{'to': 'Exposure', 'from': 'Subject', 'type': 'HAS'}],
            where_map={'Subject': {'id': '001', 'name': 'Bob'}}
        )
        assert q == "MATCH (`Subject`)-[`Subject_HAS_Exposure`:`HAS`]->(`Exposure`)\nWHERE `Subject`.`id` = $par_1 AND `Subject`.`name` = $par_2"
        assert params == {'par_1': '001', 'par_2': 'Bob'}

    def test_multi_rel(self, qbr: QueryBuilder):
        q, params = qbr.generate_query_body(
            labels=[],
            rels=[{'to': 'Exposure', 'from': 'Subject', 'type': 'HAS'}, {'from': 'Exposure', 'to': 'Parameter', 'type': 'HAS'}],
            where_map={'Subject': {'id': '001', 'name': 'Bob'}}
        )
        assert q == "MATCH (`Subject`)-[`Subject_HAS_Exposure`:`HAS`]->(`Exposure`),\n(`Exposure`)-[`Exposure_HAS_Parameter`:`HAS`]->(`Parameter`)\nWHERE `Subject`.`id` = $par_1 AND `Subject`.`name` = $par_2"
        assert params == {'par_1': '001', 'par_2': 'Bob'}

    def test_multi_label_and_rel(self, qbr: QueryBuilder):
        q, params = qbr.generate_query_body(
            labels=['Subject', 'Exposure', 'Parameter'],
            rels=[{'to': 'Exposure', 'from': 'Subject', 'type': 'HAS'}, {'from': 'Exposure', 'to': 'Parameter', 'type': 'HAS'}],
            where_map={'Subject': {'id': '001', 'name': 'Bob'}}
        )
        assert q == "MATCH (`Subject`:`Subject`),\n(`Exposure`:`Exposure`),\n(`Parameter`:`Parameter`),\n(`Subject`)-[`Subject_HAS_Exposure`:`HAS`]->(`Exposure`),\n(`Exposure`)-[`Exposure_HAS_Parameter`:`HAS`]->(`Parameter`)\nWHERE `Subject`.`id` = $par_1 AND `Subject`.`name` = $par_2"
        assert params == {'par_1': '001', 'par_2': 'Bob'}
        
    def test_multi_label_and_tags_and_rel(self, qbr: QueryBuilder):
        q, params = qbr.generate_query_body(
            labels=['Subject', 'Exposure', 'Parameter'],
            tags=['USUBJID', None, 'PARAM'],
            rels=[{'to': 'Exposure', 'from': 'Subject', 'type': 'HAS'}, {'from': 'Exposure', 'to': 'Parameter', 'type': 'HAS'}],
            where_map={'Subject': {'id': '001', 'name': 'Bob'}}
        )        
        assert q == "MATCH (`USUBJID`:`Subject`),\n(`Exposure`:`Exposure`),\n(`PARAM`:`Parameter`),\n(`USUBJID`)-[`USUBJID_HAS_Exposure`:`HAS`]->(`Exposure`),\n(`Exposure`)-[`Exposure_HAS_PARAM`:`HAS`]->(`PARAM`)\nWHERE `USUBJID`.`id` = $par_1 AND `USUBJID`.`name` = $par_2"
        assert params == {'par_1': '001', 'par_2': 'Bob'}

    def test_label_and_rel(self, qbr: QueryBuilder):
        q, params = qbr.generate_query_body(
            labels=['Subject', 'Exposure'],
            rels=[{'to': 'Exposure', 'from': 'Subject', 'type': 'HAS'}],
            where_map={'Subject': {'id': '001', 'name': 'Bob'}}
        )
        assert q == "MATCH (`Subject`:`Subject`),\n(`Exposure`:`Exposure`),\n(`Subject`)-[`Subject_HAS_Exposure`:`HAS`]->(`Exposure`)\nWHERE `Subject`.`id` = $par_1 AND `Subject`.`name` = $par_2"
        assert params == {'par_1': '001', 'par_2': 'Bob'}

    def test_label_and_optional_rel(self, qbr: QueryBuilder):
        q, params = qbr.generate_query_body(
            labels=['Subject', 'Exposure'],
            rels=[{'to': 'Exposure', 'from': 'Subject', 'type': 'HAS', 'optional': 'true'}],
            where_map={'Subject': {'id': '001', 'name': 'Bob'}}
        )
        assert q == "MATCH (`Subject`:`Subject`),\n(`Exposure`:`Exposure`)\nOPTIONAL MATCH (`Subject`)-[`Subject_HAS_Exposure`:`HAS`]->(`Exposure`)\nWHERE `Subject`.`id` = $par_1 AND `Subject`.`name` = $par_2"
        assert params == {'par_1': '001', 'par_2': 'Bob'}

    def test_multi_label_and_one_optional_rel(self, qbr: QueryBuilder):
        q, params = qbr.generate_query_body(
            labels=['Subject', 'Exposure', 'Parameter'],
            rels=[{'to': 'Exposure', 'from': 'Subject', 'type': 'HAS', 'optional': 'true'}, {'from': 'Exposure', 'to': 'Parameter', 'type': 'HAS'}],
            where_map={'Subject': {'id': '001', 'name': 'Bob'}}
        )
        assert q == "MATCH (`Subject`:`Subject`),\n(`Exposure`:`Exposure`),\n(`Parameter`:`Parameter`),\n(`Exposure`)-[`Exposure_HAS_Parameter`:`HAS`]->(`Parameter`)\nOPTIONAL MATCH (`Subject`)-[`Subject_HAS_Exposure`:`HAS`]->(`Exposure`)\nWHERE `Subject`.`id` = $par_1 AND `Subject`.`name` = $par_2"
        assert params == {'par_1': '001', 'par_2': 'Bob'}

    def test_multi_label_and_all_optional_rel(self, qbr: QueryBuilder):
        q, params = qbr.generate_query_body(
            labels=['Subject', 'Exposure', 'Parameter'],
            rels=[{'to': 'Exposure', 'from': 'Subject', 'type': 'HAS', 'optional': 'true'}, {'from': 'Exposure', 'to': 'Parameter', 'type': 'HAS', 'optional': 'true'}],
            where_map={'Subject': {'id': '001', 'name': 'Bob'}}
        )
        assert q == "MATCH (`Subject`:`Subject`),\n(`Exposure`:`Exposure`),\n(`Parameter`:`Parameter`)\nOPTIONAL MATCH (`Subject`)-[`Subject_HAS_Exposure`:`HAS`]->(`Exposure`),\n(`Exposure`)-[`Exposure_HAS_Parameter`:`HAS`]->(`Parameter`)\nWHERE `Subject`.`id` = $par_1 AND `Subject`.`name` = $par_2"
        assert params == {'par_1': '001', 'par_2': 'Bob'}

    def test_optional_label_and_optional_rel(self, qbr: QueryBuilder):
        q, params = qbr.generate_query_body(
            labels=['Subject', 'Exposure'],
            rels=[{'to': 'Exposure', 'from': 'Subject', 'type': 'HAS', 'optional': 'true'}],
            match='OPTIONAL MATCH',
            where_map={'Subject': {'id': '001', 'name': 'Bob'}}
        )
        assert q == "OPTIONAL MATCH (`Subject`:`Subject`),\n(`Exposure`:`Exposure`),\n(`Subject`)-[`Subject_HAS_Exposure`:`HAS`]->(`Exposure`)\nWHERE `Subject`.`id` = $par_1 AND `Subject`.`name` = $par_2"
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

    res = qbr.check_connectedness(
        ['Subject', 'Demographics', 'Sex'],
        [
            {'from': 'Subject', 'to': 'Demographics'},
            {'from': 'Demographics', 'to': 'Sex'}
        ])
    assert res
    # not connected
    res = qbr.check_connectedness(
        ['Subject', 'Demographics', 'Sex'],
        [
            {'from': 'Subject', 'to': 'Demographics'}
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


def test_where_not_in(qbr):
    # single not in value
    test_map = {'DOMAIN': {'rdfs:label': {'not_in': 'LB'}}}
    (Cypher_list, data_binding_dict) = qbr.list_where_conditions_per_dict(mp=test_map)
    expected = ['NOT (`DOMAIN`.`rdfs:label` = $par_1)']
    assert Cypher_list == expected
    assert data_binding_dict == {'par_1': 'LB'}
    # multiple not in values
    test_map = {'DOMAIN': {'rdfs:label': [{'not_in': 'DS'}, {'not_in': 'LB'}]}}
    (Cypher_list, data_binding_dict) = qbr.list_where_conditions_per_dict(mp=test_map)
    expected = ['NOT (`DOMAIN`.`rdfs:label` in $par_1)']
    assert Cypher_list == expected
    assert data_binding_dict == {'par_1': ['DS', 'LB']}


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


def test_generate_call(qbr: QueryBuilder):

    # no call query section as no labels_to_pack
    q1 = qbr.generate_call(
        labels=['Subject', 'Exposure'],
        rels=[{'to': 'Exposure', 'from': 'Subject', 'type': 'Exposure'}],
        labels_to_pack={},
        only_props=['rdfs:label']
    )
    expected_q1 = ""
    assert q1 == expected_q1

    q2 = qbr.generate_call(
        labels=['Exposure', 'Exposure Definition'],
        rels=[{'to': 'Exposure Definition', 'from': 'Exposure', 'type': 'Exposure_Definition_Exposure Definition'}],
        labels_to_pack={'Exposure Definition': ['Exposure']},
        only_props=['rdfs:label']
    )
    expected_q2 = 'CALL apoc.path.subgraphNodes(`Exposure`, {relationshipFilter: "Definition", optional:true, minLevel: 1, maxLevel: 1}) YIELD node AS `Exposure Definition_coll`'
    assert q2 == expected_q2


def test_generate_with(qbr: QueryBuilder):

    q1 = qbr.generate_with(
        labels=['Subject', 'Population'],
        labels_to_pack={},
        only_props=['rdfs:label']
    )
    expected_q1 = '''WITH `Subject`
, `Population`'''
    assert q1 == expected_q1

    q2 = qbr.generate_with(
        labels=['Subject', 'Population'],
        labels_to_pack={'Population': ['Safety Population', 'Completers Population']},
        only_props=['rdfs:label']
    )
    expected_q2 = '''WITH `Subject`
, collect(distinct `Population_coll`.`rdfs:label`) as `Population_coll`'''
    assert q2 == expected_q2

    q3 = qbr.generate_with(
        labels=['Subject', 'Age Group'],
        labels_to_pack={'Subject': 'Age Group'},
        only_props=['rdfs:label']
    )
    # With string as value in labels_to_pack (i.e. when we have classes that need to be combined into one column
    # such as with PREXGR1 and PREXGR2
    expected_q3 = '''WITH 
                    apoc.map.fromPairs(collect([CASE
                        WHEN `Age Group`.`Term Code` IS NOT NULL
                        THEN `Age Group`.`Term Code`
                        ELSE `Age Group`.`Short Label`
                        END, `Subject`.`rdfs:label`])) as `Subject_coll`'''
    assert q3 == expected_q3


def test_generate_return(qbr: QueryBuilder):
    q1 = qbr.generate_return(
        labels=['Subject', 'Population'],
        labels_to_pack={},
        return_termorder=True
    )
    expected_q1 = '''RETURN apoc.map.mergeList([CASE 
                                    WHEN `Subject`.Order IS NULL THEN {} 
                                    ELSE {`Subject (N)`:`Subject`.Order} 
                            END
, apoc.map.fromPairs([key in keys(CASE WHEN `Subject`{.*} IS NULL THEN {} ELSE `Subject`{.*} END) | ["Subject" + "." + key, CASE WHEN `Subject`{.*} IS NULL THEN {} ELSE `Subject`{.*} END[key]]])
, CASE 
                                    WHEN `Population`.Order IS NULL THEN {} 
                                    ELSE {`Population (N)`:`Population`.Order} 
                            END
, apoc.map.fromPairs([key in keys(CASE WHEN `Population`{.*} IS NULL THEN {} ELSE `Population`{.*} END) | ["Population" + "." + key, CASE WHEN `Population`{.*} IS NULL THEN {} ELSE `Population`{.*} END[key]]])]) as all'''
    assert q1 == expected_q1

    q2 = qbr.generate_return(
        labels=['Exposure', 'Exposure Definition'],
        labels_to_pack={'Exposure Definition': ['Exposure']},
        return_termorder=True
    )
    expected_q2 = '''RETURN apoc.map.mergeList([CASE 
                                    WHEN `Exposure`.Order IS NULL THEN {} 
                                    ELSE {`Exposure (N)`:`Exposure`.Order} 
                            END
, apoc.map.fromPairs([key in keys(CASE WHEN `Exposure`{.*} IS NULL THEN {} ELSE `Exposure`{.*} END) | ["Exposure" + "." + key, CASE WHEN `Exposure`{.*} IS NULL THEN {} ELSE `Exposure`{.*} END[key]]])
, CASE 
                                    WHEN `Exposure Definition`.Order IS NULL THEN {} 
                                    ELSE {`Exposure Definition (N)`:`Exposure Definition`.Order} 
                            END
, apoc.map.fromPairs([key in keys(CASE WHEN `Exposure Definition`{.*} IS NULL THEN {} ELSE `Exposure Definition`{.*} END) | ["Exposure Definition" + "." + key, CASE WHEN `Exposure Definition`{.*} IS NULL THEN {} ELSE `Exposure Definition`{.*} END[key]]])]) as all'''
    assert q2 == expected_q2

    q3 = qbr.generate_return(
        labels=['Subject', 'Population'],
        labels_to_pack={'Population': ['Safety Population', 'Completers Population']},
        return_termorder=True
    )
    expected_q3 = '''RETURN apoc.map.mergeList([CASE 
                                    WHEN `Subject`.Order IS NULL THEN {} 
                                    ELSE {`Subject (N)`:`Subject`.Order} 
                            END
, apoc.map.fromPairs([key in keys(CASE WHEN `Subject`{.*} IS NULL THEN {} ELSE `Subject`{.*} END) | ["Subject" + "." + key, CASE WHEN `Subject`{.*} IS NULL THEN {} ELSE `Subject`{.*} END[key]]])
, CASE 
                                    WHEN `Population`.Order IS NULL THEN {} 
                                    ELSE {`Population (N)`:`Population`.Order} 
                            END
, apoc.map.fromPairs([key in keys(CASE WHEN `Population`{.*} IS NULL THEN {} ELSE `Population`{.*} END) | ["Population" + "." + key, CASE WHEN `Population`{.*} IS NULL THEN {} ELSE `Population`{.*} END[key]]])]) as all'''
    assert q3 == expected_q3

    q4 = qbr.generate_return(
        labels=['Subject', 'No. of Exacerb in Last Year Group 1'],
        labels_to_pack={'No. of Exacerb in Last Year Group 1': '<=2'},
        return_termorder=True
    )
    expected_q4 = '''RETURN apoc.map.mergeList([CASE 
                                    WHEN `Subject`.Order IS NULL THEN {} 
                                    ELSE {`Subject (N)`:`Subject`.Order} 
                            END
, apoc.map.fromPairs([key in keys(CASE WHEN `Subject`{.*} IS NULL THEN {} ELSE `Subject`{.*} END) | ["Subject" + "." + key, CASE WHEN `Subject`{.*} IS NULL THEN {} ELSE `Subject`{.*} END[key]]])
, CASE 
                                    WHEN `No. of Exacerb in Last Year Group 1`.Order IS NULL THEN {} 
                                    ELSE {`No. of Exacerb in Last Year Group 1 (N)`:`No. of Exacerb in Last Year Group 1`.Order} 
                            END
, CASE WHEN `No. of Exacerb in Last Year Group 1`{.*} IS NULL THEN {} ELSE `No. of Exacerb in Last Year Group 1`{.*} END]) as all'''
    assert q4 == expected_q4
