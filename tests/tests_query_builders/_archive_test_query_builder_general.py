import pytest
import re
from query_builders.query_builder import QueryBuilder


# Provide QueryBuilder object that can be used by the various tests that need it
# (inside, it includes a database connection)
@pytest.fixture(scope="module")
def qbr():
    qbr = QueryBuilder()
    #qbr.clean_slate() #nothing is written/read to/from the database in these tests
    yield qbr


def test_list_data_labels(qbr):
    test_classes = ['Class 1', '$$$']
    generated_list = qbr.list_data_labels(classes=test_classes)
    assert generated_list  == ['(`class 1`:`Class 1`)', '(`$$$`:`$$$`)']


def test_list_where_conditions_per_dict(qbr):
    test_map = {
                    'CAR': {
                        'year': 2021
                    },
                    'BOAT': {
                        'make': 'Jeanneau'
                    }
                }
    (Cypher_list, data_binding_dict) = qbr.list_where_conditions_per_dict(mp=test_map)
    assert Cypher_list == ["`car`.`year` = $par_1", "`boat`.`make` = $par_2"]
    assert data_binding_dict == {"par_1": 2021, "par_2": 'Jeanneau'}
    # Notice that 2021 is an integer, not a string


    test_map = {
        'SUBJECT': {
            'USUBJID': '01-001',
            'SUBJID': '001'
        },
        'SEX': {
            'ASEX': 'Male'
        }
    }
    (Cypher_list, data_binding_dict) = qbr.list_where_conditions_per_dict(mp=test_map)
    assert Cypher_list == ["`subject`.`USUBJID` = $par_1", "`subject`.`SUBJID` = $par_2", "`sex`.`ASEX` = $par_3"]
    assert data_binding_dict == {"par_1": '01-001', "par_2": '001', "par_3": 'Male'}

    # test negation
    test_map = {"Sex": {"SEX": "M"}, "!Race": {"RACE": "WHITE"}}

    (Cypher_list, data_binding_dict) = qbr.list_where_conditions_per_dict(mp=test_map)
    assert Cypher_list == ['`sex`.`SEX` = $par_1', 'NOT (`race`.`RACE` = $par_2)']
    assert data_binding_dict == {'par_1': 'M', 'par_2': 'WHITE'}

    # Test list inclusion
    test_map = {
                    'CAR': {
                        'year': 2021
                    },
                    'BOAT': {
                        'make': ['Jeanneau', 'C&C']     # value is a list
                    }
                }
    (Cypher_list, data_binding_dict) = qbr.list_where_conditions_per_dict(mp=test_map)
    assert Cypher_list == ["`car`.`year` = $par_1", "`boat`.`make` in $par_2"]
    assert data_binding_dict == {"par_1": 2021, "par_2": ['Jeanneau', 'C&C']}


    # Now some pathological cases
    test_map = {
        'EMPTY': {
        }
    }
    (Cypher_list, data_binding_dict) = qbr.list_where_conditions_per_dict(mp=test_map)
    assert Cypher_list == []
    assert data_binding_dict == {}


    test_map = {
    }
    (Cypher_list, data_binding_dict) = qbr.list_where_conditions_per_dict(mp=test_map)
    assert Cypher_list == []
    assert data_binding_dict == {}


    test_map = {
            'POTENTIAL TROUBLE': {
                'My value has a single quote': "It's tricky"
            },
    }
    (Cypher_list, data_binding_dict) = qbr.list_where_conditions_per_dict(mp=test_map)
    assert Cypher_list == ["`potential trouble`.`My value has a single quote` = $par_1"]
    assert data_binding_dict == {"par_1" : "It's tricky"}


def test_where_conditions_ranges(qbr):

    #all True
    test_map = {"Age": {"AGE": {"min": 18, "max": 65, "max_include": True, "min_include": True, "incl_null": True}}}

    (Cypher_list, data_binding_dict) = qbr.list_where_conditions_per_dict(mp=test_map)
    expected = ['(`age`.`AGE` IS NULL OR ($par_1 <= `age`.`AGE` <= $par_2))']
    # print(Cypher_list)
    # print(expected)
    assert Cypher_list == expected
    assert data_binding_dict == {'par_1': 18, 'par_2': 65}

    #one False, incl_null == None
    test_map = {"Age": {"AGE": {"min": 18, "max": 65, "max_include": False, "min_include": True}}}

    (Cypher_list, data_binding_dict) = qbr.list_where_conditions_per_dict(mp=test_map)
    assert Cypher_list == ['($par_1 <= `age`.`AGE` < $par_2)']
    assert data_binding_dict == {'par_1': 18, 'par_2': 65}

    #borders False, incl_null == None
    test_map = {"Age": {"AGE": {"min": 18, "max": 65, "max_include": False, "min_include": False}}}

    (Cypher_list, data_binding_dict) = qbr.list_where_conditions_per_dict(mp=test_map)
    assert Cypher_list == ['($par_1 < `age`.`AGE` < $par_2)']
    assert data_binding_dict == {'par_1': 18, 'par_2': 65}

    #all False
    test_map = {"Age": {"AGE": {"min": 18, "max": 65, "max_include": False, "min_include": False, "incl_null": False}}}

    (Cypher_list, data_binding_dict) = qbr.list_where_conditions_per_dict(mp=test_map)
    assert Cypher_list == ['(`age`.`AGE` IS NOT NULL AND ($par_1 < `age`.`AGE` < $par_2))']
    assert data_binding_dict == {'par_1': 18, 'par_2': 65}

    #incl_null == False alone
    test_map = {"Age": {"AGE": {"incl_null": False}}}

    (Cypher_list, data_binding_dict) = qbr.list_where_conditions_per_dict(mp=test_map)
    assert Cypher_list == ['`age`.`AGE` IS NOT NULL']
    assert data_binding_dict == {'par_1': None, 'par_2': None}


def test_generate_return(qbr):
    # Note: the return_properties argument to qb_generate_return() is not implemented

    generated_string, data_dict = qbr.generate_return(['Single_class'])
    #assert generated_string == "RETURN apoc.map.mergeList([`single_class`{.*}]) as all"
    assert generated_string == "RETURN apoc.map.mergeList([CASE WHEN `single_class`{.*} IS NULL THEN {} ELSE `single_class`{.*} END]) as all"

    generated_string, data_dict = qbr.generate_return(['Study', 'Site', 'Subject', 'Parameter Category', 'Parameter'])
    #assert generated_string == "RETURN apoc.map.mergeList([`study`{.*}, `site`{.*}, `subject`{.*}, `parameter category`{.*}, `parameter`{.*}]) as all"
    assert generated_string.replace("\n","") == "RETURN apoc.map.mergeList([CASE WHEN `study`{.*} IS NULL THEN {} ELSE `study`{.*} END, CASE WHEN `site`{.*} IS NULL THEN {} ELSE `site`{.*} END, CASE WHEN `subject`{.*} IS NULL THEN {} ELSE `subject`{.*} END, CASE WHEN `parameter category`{.*} IS NULL THEN {} ELSE `parameter category`{.*} END, CASE WHEN `parameter`{.*} IS NULL THEN {} ELSE `parameter`{.*} END]) as all"

    generated_string, data_dict = qbr.generate_return(['Study', 'Site'],
                                           return_nodeid = True)
    #assert generated_string == "RETURN apoc.map.mergeList([{`Study`:id(`study`)}, `study`{.*}, {`Site`:id(`site`)}, `site`{.*}]) as all"
    assert generated_string.replace("\n","") == "RETURN apoc.map.mergeList([{`Study`:id(`study`)}, CASE WHEN `study`{.*} IS NULL THEN {} ELSE `study`{.*} END, {`Site`:id(`site`)}, CASE WHEN `site`{.*} IS NULL THEN {} ELSE `site`{.*} END]) as all"


    generated_string, data_dict = qbr.generate_return(['Study', 'Site'],
                                           return_disjoint = True)
    #assert generated_string == "RETURN collect(distinct apoc.map.mergeList([`study`{.*}])) as `Study`, collect(distinct apoc.map.mergeList([`site`{.*}])) as `Site`"
    assert generated_string.replace("\n","") == "RETURN collect(distinct apoc.map.mergeList([CASE WHEN `study`{.*} IS NULL THEN {} ELSE `study`{.*} END])) as `Study`, collect(distinct apoc.map.mergeList([CASE WHEN `site`{.*} IS NULL THEN {} ELSE `site`{.*} END])) as `Site`"

    # testing renaming to rdfs:label (when qbr.mode = 'schema_CLASS')
    qbr.mode = 'schema_CLASS'
    generated_string, data_dict = qbr.generate_return(['Study', 'Site'])
    crop_generate_string = re.sub(r'\s+', ' ', generated_string.replace("\n", ""))
    expected = "RETURN apoc.map.mergeList([ apoc.map.fromPairs([key in keys(CASE WHEN `study`{.*} IS NULL THEN {} ELSE `study`{.*} END) | [ CASE WHEN key = 'rdfs:label' THEN $rename_keys['Study'] ELSE key END, CASE WHEN `study`{.*} IS NULL THEN {} ELSE `study`{.*} END[key]]]) , apoc.map.fromPairs([key in keys(CASE WHEN `site`{.*} IS NULL THEN {} ELSE `site`{.*} END) | [ CASE WHEN key = 'rdfs:label' THEN $rename_keys['Site'] ELSE key END, CASE WHEN `site`{.*} IS NULL THEN {} ELSE `site`{.*} END[key]]]) ]) as all"
    assert crop_generate_string == expected

    # testing renaming to rdfs:label (when qbr.mode = 'schema_CLASS')
    qbr.mode = 'schema_CLASS'
    generated_string, data_dict = qbr.generate_return(['Study', 'Site'], prefix_keys_with_label=True)
    crop_generate_string = re.sub(r'\s+', ' ', generated_string.replace("\n", ""))
    expected = "RETURN apoc.map.mergeList([ apoc.map.fromPairs([key in keys(CASE WHEN `study`{.*} IS NULL THEN {} ELSE `study`{.*} END) | [ CASE WHEN key = 'rdfs:label' THEN $rename_keys['Study'] ELSE key END, CASE WHEN `study`{.*} IS NULL THEN {} ELSE `study`{.*} END[key]]]) , apoc.map.fromPairs([key in keys(CASE WHEN `site`{.*} IS NULL THEN {} ELSE `site`{.*} END) | [ CASE WHEN key = 'rdfs:label' THEN $rename_keys['Site'] ELSE key END, CASE WHEN `site`{.*} IS NULL THEN {} ELSE `site`{.*} END[key]]]) ]) as all"
    print(crop_generate_string)
    print(expected)
    #assert crop_generate_string == expected
    qbr.mode = 'schema_PROPERTY'  # returning back to 'schema_PROPERTY' mode

    # Verify that an exception is raised when given an empty list of classes
    with pytest.raises(Exception):
        assert qbr.generate_return([])     # an empty list is expected to raise an exception
        assert qbr.generate_return('where is the list??')
        assert qbr.generate_return(666)
        assert qbr.generate_return((1, 2))
        assert qbr.generate_return({})


def test_generate_return_return_properties(qbr):
    generated_string, data_dict = qbr.generate_return(['Study', 'Site'],
                                           return_disjoint = True, return_nodeid = True)
    #assert generated_string == "RETURN collect(distinct apoc.map.mergeList([{`Study`:id(`study`)}, `study`{.*}])) as `Study`, collect(distinct apoc.map.mergeList([{`Site`:id(`site`)}, `site`{.*}])) as `Site`"
    assert generated_string.replace("\n","") == "RETURN collect(distinct apoc.map.mergeList([{`Study`:id(`study`)}, CASE WHEN `study`{.*} IS NULL THEN {} ELSE `study`{.*} END])) as `Study`, collect(distinct apoc.map.mergeList([{`Site`:id(`site`)}, CASE WHEN `site`{.*} IS NULL THEN {} ELSE `site`{.*} END])) as `Site`"

    generated_string, data_dict = qbr.generate_return(['Study', 'Site'],
                                           return_disjoint = True, return_properties=["STUDYID", "SITEID"])
    assert generated_string.replace("\n","") == "RETURN collect(distinct apoc.map.mergeList([apoc.map.clean(apoc.map.submap(" \
                               "CASE WHEN `study`{.*} IS NULL THEN {} ELSE `study`{.*} END, " \
                               "['STUDYID', 'SITEID'], NULL, False),[],[NULL])])) as `Study`, " \
                               "collect(distinct apoc.map.mergeList([apoc.map.clean(apoc.map.submap(" \
                               "CASE WHEN `site`{.*} IS NULL THEN {} ELSE `site`{.*} END, " \
                               "['STUDYID', 'SITEID'], NULL, False),[],[NULL])])) as `Site`"

    generated_string, data_dict = qbr.generate_return(['Study', 'Site'],
                                           return_disjoint=False, return_properties=["STUDYID", "SITEID"])
    assert generated_string.replace("\n","") == "RETURN apoc.map.mergeList([" \
                               "apoc.map.clean(apoc.map.submap(CASE WHEN `study`{.*} IS NULL THEN {} ELSE `study`{.*} END, " \
                               "['STUDYID', 'SITEID'], NULL, False),[],[NULL]), " \
                               "apoc.map.clean(apoc.map.submap(CASE WHEN `site`{.*} IS NULL THEN {} ELSE `site`{.*} END, " \
                               "['STUDYID', 'SITEID'], NULL, False),[],[NULL])]) as all"