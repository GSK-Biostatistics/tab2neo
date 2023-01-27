import json
import os

filepath = os.path.dirname(__file__)
import pytest
from model_managers.model_manager import ModelManager
from utils.utils import compare_recordsets


# Provide a DataProvider object (which contains a database connection)
# that can be used by the various tests that need it
@pytest.fixture(scope="module")
def mm():
    mm = ModelManager(verbose=False, debug=True)
    yield mm


def test_create_class_list(mm):
    mm.clean_slate()

    mm.create_class("My First Class")
    result = mm.get_nodes()
    assert result == [{'label': 'My First Class'}]

    mm.create_class(["A", "B"])
    result = mm.get_nodes()
    assert compare_recordsets(result, [{'label': 'My First Class'}, {'label': 'A'}, {'label': 'B'}])

    mm.create_class("A")     # Merge is True by default; so, nothing gets created since class "A" already exists
    result = mm.get_nodes()
    assert compare_recordsets(result, [{'label': 'My First Class'}, {'label': 'A'}, {'label': 'B'}])

    mm.create_class("A", merge=False)     # A 2nd class named "A" will get created
    result = mm.get_nodes()
    assert compare_recordsets(result, [{'label': 'My First Class'}, {'label': 'A'}, {'label': 'A'}, {'label': 'B'}])

    mm.create_class(["B", "X"], merge=True)  # Only class "X" gets created, because "B" already exists
    result = mm.get_nodes()
    assert compare_recordsets(result, [{'label': 'My First Class'}, {'label': 'A'}, {'label': 'A'}, {'label': 'B'}, {'label': 'X'}])

    with pytest.raises(AssertionError):  # Invalid merge format
        mm.create_class(["B", "X"], merge='true')


def test_create_class_dict(mm):
    mm.clean_slate()

    # Dictionary format
    mm.create_class([{"label": "A"}])
    result = mm.get_nodes()
    assert result == [{'label': 'A'}]

    # Verifying create
    mm.create_class([{"label": "A"}], merge=False)
    result = mm.get_nodes()
    assert compare_recordsets(result, [{'label': 'A'}, {'label': 'A'}])

    # Merge multiple
    mm.create_class([{"label": "A"}, {"label": "B"}])
    result = mm.get_nodes()
    assert compare_recordsets(result, [{'label': 'A'}, {'label': 'A'}, {"label": "B"}])

    # Setting multiple properties
    mm.create_class([{"label": "Bravo", "short_label": "B"}])
    result = mm.get_nodes()
    assert compare_recordsets(result, [
        {'label': 'A'}, {'label': 'A'}, {"label": "B"},
        {"label": "Bravo", "short_label": "B"}
    ])

    # Using merge on
    mm.clean_slate()
    mm.create_class([{"label": "A", "type": "original_type"}])
    mm.create_class([{"label": "A", "type": "new_type"}], merge=True, merge_on=['label'])

    result = mm.get_nodes()
    assert compare_recordsets(result, [
        {"label": "A", "type": "new_type"}
    ])

    # Merge on non existing
    mm.create_class([{"label": "B", "type": "new_type"}], merge=True, merge_on=['label'])
    result = mm.get_nodes()
    assert compare_recordsets(result, [
        {"label": "A", "type": "new_type"},
        {"label": "B", "type": "new_type"}
    ])

    with pytest.raises(AssertionError):
        mm.create_class([{"label": "B", "type": "new_type"}], merge=False, merge_on=['label'])


def test_delete_class(mm):
    mm.clean_slate()

    with open(os.path.join(filepath, 'data', 'test_delete_classes.json')) as jsonfile:
        dct = json.load(jsonfile)
    mm.load_arrows_dict(dct)

    # Delete single
    mm.delete_class(['Delete 1'])
    result = mm.get_nodes()
    assert compare_recordsets(result, [
        {'short_label': 'RM1', 'label': 'Remain 1'},
        {'short_label': 'RM2', 'label': 'Remain 2'},
        {'relationship_type': 'Remain'},
        {'rdfs:label': 'Remaining term 1', 'Term Code': 'Term R1', 'Codelist Code': 'Codelist R1'},
        {'short_label': 'D3', 'label': 'Delete 3'},
        {'short_label': 'D2', 'label': 'Delete 2'},
        {'relationship_type': 'Delete 2'}
    ])

    # Delete multiple with identifier
    mm.delete_class(['D2', 'D3'], identifier='short_label')
    result = mm.get_nodes()
    assert compare_recordsets(result, [
        {'short_label': 'RM1', 'label': 'Remain 1'},
        {'short_label': 'RM2', 'label': 'Remain 2'},
        {'relationship_type': 'Remain'},
        {'rdfs:label': 'Remaining term 1', 'Term Code': 'Term R1', 'Codelist Code': 'Codelist R1'}
    ])


def test_get_missing_classes(mm):
    mm.clean_slate()

    class_list = ["A", "B", "C"]
    mm.create_class(class_list)

    res = mm.get_missing_classes(["A", "B", "C"])
    assert not res

    res = mm.get_missing_classes(["D"])
    assert res == set("D")

    class_list = [{"short_label": 'A'}, {"short_label": 'B'}]
    mm.create_class(class_list)

    res = mm.get_missing_classes(["A", "B"], 'short_label')
    assert not res

    res = mm.get_missing_classes(["D"], 'short_label')
    assert res == set("D")


def test_get_all_classes(mm):
    mm.clean_slate()

    class_list = ["G", "S", "K"]
    mm.create_class(class_list)

    sorted = mm.get_all_classes_with_nodeids()
    assert sorted == [{'Class': 'G', 'short_label': None}, {'Class': 'K', 'short_label': None}, {'Class': 'S', 'short_label': None}]

    long = mm.get_all_classes_with_nodeids(include_id=True)
    for entry in long:
        assert type(entry["_id_Class"]) == int
        assert entry["Class"] in class_list


def test_get_all_classes_props(mm):
    mm.clean_slate()

    class_list = ["A", "B", "C"]
    mm.create_class(class_list)

    for class_label in class_list:
        mm.set_short_label(class_label, class_label.lower())

    short_labels = mm.get_all_classes_props(['short_label'])
    expected_short_labels = [{"short_label": label.lower()} for label in class_list]

    assert sorted(short_labels, key=lambda d: d['short_label']) == expected_short_labels

    with pytest.raises(AssertionError):
        mm.get_all_classes_props([])

    with pytest.raises(AssertionError):
        mm.get_all_classes_props(['short_label', 'short_label'])


def test_get_rels_where(mm: ModelManager):
    mm.clean_slate()

    with open(os.path.join(filepath, 'data', 'test_infer_rels.json')) as jsonfile:
        dct = json.load(jsonfile)
    mm.load_arrows_dict(dct)

    # Test without where clause (all rels)
    res1 = mm.get_rels_where()
    assert res1 == [
        {'from': 'Person', 'to': 'Name of Treatment', 'type': 'HAS'},
        {'from': 'Subject', 'to': 'Exposure Name of Treatment', 'type': None}
    ]

    # Test with where clause
    res2 = mm.get_rels_where('WHERE from_class.label = "Person"')
    assert res2 == [{'from': 'Person', 'to': 'Name of Treatment', 'type': 'HAS'}]

    # Test with custom return prop
    res3 = mm.get_rels_where('WHERE from_class.short_label = "PERSON"', 'short_label')
    assert res3 == [{'from': 'PERSON', 'to': '--TRT', 'type': 'HAS'}]


def test_get_rels_btw2(mm:ModelManager):
    mm.clean_slate()
    # loading metadata (Class from json created in arrows.app)
    with open(os.path.join(filepath, 'data', 'test_infer_rels.json')) as jsonfile:
        dct = json.load(jsonfile)
    mm.load_arrows_dict(dct)
    res = mm.get_rels_btw2('Subject', 'Exposure Name of Treatment')
    expected_res = [
        {'from': 'Person', 'to': 'Name of Treatment', 'type': 'HAS'},
        {'from': 'Subject', 'to': 'Exposure Name of Treatment', 'type': None}
    ]
    assert res == expected_res

    res = mm.get_rels_btw2('Subject', 'Name of Treatment')
    expected_res = [
        {'from': 'Person', 'to': 'Name of Treatment', 'type': 'HAS'},
        {'from': 'Subject', 'to': 'Exposure Name of Treatment', 'type': None}
    ]
    assert res == expected_res

    res = mm.get_rels_btw2('Person', 'Exposure Name of Treatment')
    expected_res = [
        {'from': 'Person', 'to': 'Name of Treatment', 'type': 'HAS'},
        {'from': 'Subject', 'to': 'Exposure Name of Treatment', 'type': None}
    ]
    assert res == expected_res

    res = mm.get_rels_btw2('Person', 'Name of Treatment')
    expected_res = [
        {'from': 'Person', 'to': 'Name of Treatment', 'type': 'HAS'},
        {'from': 'Subject', 'to': 'Exposure Name of Treatment', 'type': None}
    ]
    assert res == expected_res

    res = mm.get_rels_btw2('USUBJID', 'EXTRT', identifier='short_label')
    expected_res = [
        {'from': 'PERSON', 'to': '--TRT', 'type': 'HAS'},
        {'from': 'USUBJID', 'to': 'EXTRT', 'type': None}
    ]
    assert res == expected_res


def test_delete_relationship(mm):
    mm.clean_slate()

    with open(os.path.join(filepath, 'data', 'test_infer_rels.json')) as jsonfile:
        dct = json.load(jsonfile)
    mm.load_arrows_dict(dct)

    mm.delete_relationship([['Person', 'Name of Treatment', 'HAS']])

    res = mm.get_rels_btw2('Person', 'Name of Treatment')
    expected_res = [
        {'from': 'Subject', 'to': 'Exposure Name of Treatment', 'type': None}
    ]
    assert res == expected_res


def test_create_ct(mm):
    # Prepare test
    mm.clean_slate()

    q = """
    MERGE (:Class {label: 'G'})
    MERGE (:Class {label: 'S'})
    MERGE (:Class {label: 'K'})
    MERGE (:Class {short_label: 'A', label: 'Class A'})
    """
    mm.query(q)

    # Create ct with existing terms
    mm.create_ct({
        'G': [{'Codelist Code': 'term1'}, {'Codelist Code': 'term2'}],
        'S': [{'Codelist Code': 'term3'}]
    })

    res = mm.get_class_ct_map(classes=['G', 'S', 'K'], ct_props=['Codelist Code', 'Order'])

    assert sorted(res.get('G'), key=lambda d: d['Codelist Code']) == [{'Order': 1, 'Codelist Code': 'term1'},
                                                              {'Order': 2, 'Codelist Code': 'term2'}]
    assert res.get('S') == [{'Order': 1, 'Codelist Code': 'term3'}]

    # Ensure class labels are inherited by ct inheritance
    q = """
    MATCH (c:Class)-[:HAS_CONTROLLED_TERM]-(t:Term)
    RETURN c.label as label, labels(t) as term_labels
    """
    res = mm.query(q)
    for res in res:
        assert res.get('label') in res.get('term_labels'), 'Class label not present on CT'

    # Test order increment
    mm.create_ct({
        'S': [{'Codelist Code': 'term4'}]
    })

    res = mm.get_class_ct_map(classes=['S'], ct_props=['Codelist Code', 'Order'])

    assert sorted(res.get('S'), key=lambda d: d['Codelist Code']) == [{'Order': 1, 'Codelist Code': 'term3'},
                                                              {'Order': 2, 'Codelist Code': 'term4'}]

    # Test NEXT relationship creation
    q = """
    MATCH (t1:Term)-[:NEXT]->(t2:Term)
    WHERE t1.`Codelist Code` = 'term3'
    RETURN t2.`Codelist Code` as `Codelist Code`
    """
    res = mm.query(q)[0]
    assert res == {'Codelist Code': 'term4'}

    # Test without order
    mm.create_ct({
        'K': [{'Codelist Code': 'term5'}, {'Codelist Code': 'term6'}]
    }, order_terms=False)

    res = mm.get_class_ct_map(classes=['K'], ct_props=['Codelist Code', 'Order'])
    assert sorted(res.get('K'), key=lambda d: d['Codelist Code']) == [{'Order': None, 'Codelist Code': 'term5'},
                                                              {'Order': None, 'Codelist Code': 'term6'}]

    q = """
    MATCH (t1:Term)-[:NEXT]->(t2:Term)
    WHERE t1.`Codelist Code` = 'term5'
    RETURN t2.`Codelist Code` as cc
    """
    res = mm.query(q)
    assert not res

    # Term for undefined class assertion error
    with pytest.raises(AssertionError):
        mm.create_ct({
            'X': [{'Codelist Code': 'term7'}]
        })

    # Short_label identifier
    mm.create_ct({
        'A': [{'Codelist Code': 'term7'}]
    }, 'short_label', merge_on=['Codelist Code'])

    res = mm.get_class_ct_map(classes=['A'], ct_props=['Codelist Code', 'Order'], identifier='short_label')
    assert res.get('A') == [{'Order': 1, 'Codelist Code': 'term7'}]

    # With on_merge
    mm.clean_slate()

    q = """
    MERGE (a:Class {label: 'Apple'})-[:HAS_CONTROLLED_TERM]->(t1:Term {`Codelist Code`: 'term1c', `Term Code`: 'term1t', `Order`:2})
    MERGE (a)-[:HAS_CONTROLLED_TERM]->(t3:Term {`Codelist Code`: 'term3c', `Term Code`: 'term3t', `Order`:1})
    MERGE (:Class {label: 'Banana'})-[:HAS_CONTROLLED_TERM]->(t2:Term {`Codelist Code`: 'term2c', `Term Code`: 'term2t', `Order`:1})
    SET t1.`rdfs:label` = 'original'
    SET t1 :Apple
    SET t2 :Banana
    SET t3 :Apple
    """
    mm.query(q)

    mm.create_ct({
        'Apple': [{'Codelist Code': 'term1c', 'Term Code': 'term1t', 'rdfs:label': 'updated'}]
    }, merge_on=['Codelist Code', 'Term Code'])

    res = mm.get_all_ct(term_props=['Codelist Code', 'Term Code', 'rdfs:label', 'Order'])

    assert sorted(res, key=lambda d: d['Codelist Code']) == [
        {'label': 'Apple', 'Codelist Code': 'term1c', 'Term Code': 'term1t', 'rdfs:label': 'updated', 'Order': 2},
        {'label': 'Banana', 'Codelist Code': 'term2c', 'Term Code': 'term2t', 'rdfs:label': None, 'Order': 1},
        {'label': 'Apple', 'Codelist Code': 'term3c', 'Term Code': 'term3t', 'rdfs:label': None, 'Order': 1},
    ]


def test_create_same_as_ct(mm):
    mm.clean_slate()

    q1 = """
    MERGE (a:Class {label: 'Avocado', short_label: 'A'})
    MERGE (a)-[:HAS_CONTROLLED_TERM]->(:Term {`Codelist Code`: 'term1c', `Term Code`: 'term1t'})
    MERGE (b:Class {label: 'Banana', short_label: 'B'})
    MERGE (b)-[:HAS_CONTROLLED_TERM]->(:Term {`Codelist Code`: 'term2c', `Term Code`: 'term2t'})
    """

    mm.query(q1)

    mm.create_same_as_ct([
        {'from_class': 'Avocado', 'to_class': 'Banana',
         'from_codelist_code': 'term1c', 'to_codelist_code': 'term2c'}
    ], ['Codelist Code'])

    q2 = """
    MATCH (t1:Term)-[:SAME_AS]->(t2:Term)
    RETURN t1.`Codelist Code` as t1, t2.`Codelist Code` as t2
    """

    res = mm.query(q2)
    assert res == [{'t1': 'term1c', 't2': 'term2c'}]

    mm.clean_slate()
    mm.query(q1)

    mm.create_same_as_ct([
        {'from_class': 'B', 'to_class': 'A',
         'from_codelist_code': 'term2c', 'to_codelist_code': 'term1c',
         'from_term_code': 'term2t', 'to_term_code': 'term1t'
         }
    ], ['Codelist Code', 'Term Code'], identifier='short_label')

    q3 = """
    MATCH (t1:Term)-[:SAME_AS]->(t2:Term)
    RETURN t1.`Codelist Code` as t1, t2.`Codelist Code` as t2
    """

    res = mm.query(q3)
    assert res == [{'t1': 'term2c', 't2': 'term1c'}]


def test_remove_same_as_ct(mm):
    mm.clean_slate()

    q1 = """
    MERGE (a:Class {label: 'Avocado', short_label: 'A'})
    MERGE (a)-[:HAS_CONTROLLED_TERM]->(t1:Term {`Codelist Code`: 'term1c', `Term Code`: 'term1t'})
    MERGE (b:Class {label: 'Banana', short_label: 'B'})
    MERGE (b)-[:HAS_CONTROLLED_TERM]->(t2:Term {`Codelist Code`: 'term2c', `Term Code`: 'term2t'})
    MERGE (t1)-[:SAME_AS]-(t2)
    """

    mm.query(q1)

    mm.remove_same_as_ct([
        {'from_class': 'Avocado', 'to_class': 'Banana',
         'from_codelist_code': 'term1c', 'to_codelist_code': 'term2c'}
    ], ['Codelist Code'])

    q2 = """
    MATCH (t1:Term)-[:SAME_AS]->(t2:Term)
    RETURN t1.`Codelist Code` as t1, t2.`Codelist Code` as t2
    """

    res = mm.query(q2)
    assert res == []

    mm.clean_slate()
    mm.query(q1)

    mm.remove_same_as_ct([
        {'from_class': 'A', 'to_class': 'B',
         'from_codelist_code': 'term1c', 'to_codelist_code': 'term2c',
         'from_term_code': 'term1t', 'to_term_code': 'term2t'
         }
    ], ['Codelist Code', 'Term Code'], identifier='short_label')

    q3 = """
    MATCH ()-[r:SAME_AS]->()
    RETURN r
    """

    res = mm.query(q3)
    assert res == []


def test_delete_ct(mm):
    mm.clean_slate()

    with open(os.path.join(filepath, 'data', 'test_delete_ct.json')) as jsonfile:
        dct = json.load(jsonfile)
    mm.load_arrows_dict(dct)

    mm.delete_ct({'Subject': [['Codelist2']]}, ['Codelist Code'])

    res = mm.get_class_ct_map(['Subject', 'Exposure Name of Treatment'], ct_props=['Codelist Code', 'Term Code', 'rdfs:label'])
    assert res == {
        'Subject': [{'Term Code': 'Termcode3', 'rdfs:label': 'Term3', 'Codelist Code': 'Codelist3'}],
        'Exposure Name of Treatment': [{'Term Code': 'Termcode1', 'rdfs:label': 'Term1', 'Codelist Code': 'Codelist1'}]
    }

    mm.delete_ct({'EXTRT': [['Codelist1']]}, ['Codelist Code'], identifier='short_label')

    res = mm.get_class_ct_map(['Subject', 'Exposure Name of Treatment'],
                              ct_props=['Codelist Code', 'Term Code', 'rdfs:label'])
    assert res == {
        'Subject': [{'Term Code': 'Termcode3', 'rdfs:label': 'Term3', 'Codelist Code': 'Codelist3'}]
    }


def test_get_class_ct_map(mm):
    mm.clean_slate()

    with open(os.path.join(filepath, 'data', 'test_delete_ct.json')) as jsonfile:
        dct = json.load(jsonfile)
    mm.load_arrows_dict(dct)

    res = mm.get_class_ct_map('Exposure Name of Treatment')
    print(res)
    assert res == {'Exposure Name of Treatment': [{'rdfs:label': 'Term1'}]}

    res = mm.get_class_ct_map('Exposure Name of Treatment', 'Codelist Code')
    print(res)
    assert res == {'Exposure Name of Treatment': [{'Codelist Code': 'Codelist1'}]}

    res = mm.get_class_ct_map(['USUBJID'], ct_props=['rdfs:label', 'Codelist Code'], identifier='short_label')
    print(res)
    assert sorted(res.get('USUBJID'), key=lambda d: d['rdfs:label']) == [
        {'rdfs:label': 'Term2', 'Codelist Code': 'Codelist2'}, {'rdfs:label': 'Term3', 'Codelist Code': 'Codelist3'}
    ]

    res = mm.get_class_ct_map(['Undefined Class'])
    print(res)
    assert res == {}


def test_get_all_ct(mm):
    mm.clean_slate()

    with open(os.path.join(filepath, 'data', 'test_delete_ct.json')) as jsonfile:
        dct = json.load(jsonfile)
    mm.load_arrows_dict(dct)

    res = mm.get_all_ct(['Codelist Code', 'Term Code', 'rdfs:label'], derived_only=True)
    assert res == [{'label': 'Exposure Name of Treatment',
                    'Codelist Code': 'Codelist1', 'Term Code': 'Termcode1',
                    'rdfs:label': 'Term1'}]

    res = mm.get_all_ct(['Codelist Code', 'Term Code', 'rdfs:label'], class_prop='short_label', derived_only=False)
    assert sorted(res, key=lambda d: d['Codelist Code']) == [
        {'short_label': 'EXTRT', 'Codelist Code': 'Codelist1',
         'Term Code': 'Termcode1', 'rdfs:label': 'Term1'},
        {'short_label': 'USUBJID', 'Codelist Code': 'Codelist2',
         'Term Code': 'Termcode2', 'rdfs:label': 'Term2'},
        {'short_label': 'USUBJID', 'Codelist Code': 'Codelist3',
         'Term Code': 'Termcode3', 'rdfs:label': 'Term3'},
    ]

    with pytest.raises(AssertionError):
        mm.get_all_ct([], class_prop='short_label', derived_only=False)

    with pytest.raises(AssertionError):
        mm.get_all_ct(['short_label'], class_prop='short_label')


def test_create_relationship(mm):
    mm.clean_slate()

    q = """
    MERGE (a:Class{label:"class1", short_label:"C1"})-[:SUBCLASS_OF]->(b:Class{label:"class2", short_label:"C2"})
    MERGE (c:Class{label:"class3", short_label:"C3"})
    MERGE (d:Class{label:"class4", short_label:"C4"})
    """
    mm.query(q)

    res1 = mm.create_relationship([['class1', 'class3', 'rel1']])
    assert res1 == [['class1', 'class3', 'rel1']]

    res2 = mm.get_rels_btw2("class1", "class3")
    assert res2 == [{'from': 'class1', 'to': 'class3', 'type': 'rel1'}]

    res1 = mm.create_relationship([['class1', 'MISSING CLASS', 'rel1']])
    assert res1 == []


def test_create_related_classes_from_list(mm):
    mm.clean_slate()

    rel_list = [["A", "B", "type1"], ["B", "C", "type2"], ["D", "E", "type3"]]
    res1 = mm.create_related_classes_from_list(rel_list=rel_list)
    assert res1 == ["A", "B", "C", "D", "E"]

    res2 = mm.get_rels_btw2("A", "B")
    assert res2 == [{'from': 'A', 'to': 'B', 'type': 'type1'}]
    res3 = mm.get_rels_btw2("B", "C")
    assert res3 == [{'from': 'B', 'to': 'C', 'type': 'type2'}]
    res4 = mm.get_rels_btw2("D", "E")
    assert res4 == [{'from': 'D', 'to': 'E', 'type': 'type3'}]

    res4 = mm.get_rels_btw2("A", "C")
    assert res4 == []
    res4 = mm.get_rels_btw2("C", "D")
    assert res4 == []

    res5 = mm.create_related_classes_from_list(rel_list=[["F", "G", "type4"], ["G", "H", "type5"]],
                                               identifier='short_label')
    assert res5 == ['F', 'G', 'H']

    res6 = mm.get_rels_btw2("F", "G", identifier='short_label')
    assert res6 == [{'from': 'F', 'to': 'G', 'type': 'type4'}]


def test_get_rels_from_labels(mm):
    mm.clean_slate()

    q = '''
    MERGE (a:Class{label:"A"})-[:SUBCLASS_OF]->(b:Class{label:"B"})
    MERGE (c:Class{label:"C"})
    MERGE (d:Class{label:"D"})
    MERGE (a)<-[:FROM]-(:Relationship{relationship_type:"type1"})-[:TO]->(c)
    MERGE (c)<-[:FROM]-(:Relationship{relationship_type:"type2"})-[:TO]->(d)
    '''

    mm.query(q)

    res = mm.get_rels_from_labels(labels=['A', 'D'])

    expected_res = [{'from': 'A', 'to': 'C', 'type': 'type1'}, {'from': 'C', 'to': 'D', 'type': 'type2'}]

    assert res == expected_res


def test_get_labels_from_rels_list(mm):
    res = mm.get_labels_from_rels_list(rels_list=[
        {'from': 'A', 'to': 'C', 'type': 'type1'},
        {'from': 'C', 'to': 'D', 'type': 'type2'}
    ])

    expected_res = ['A', 'C', 'D']
    assert expected_res == res


def test_infer_rels(mm:ModelManager):
    mm.clean_slate()
    # loading metadata (Class from json created in arrows.app)
    with open(os.path.join(filepath, 'data', 'test_infer_rels.json')) as jsonfile:
        dct = json.load(jsonfile)
    mm.load_arrows_dict(dct)

    g_list = mm.infer_rels(['Subject', 'Exposure Name of Treatment'])
    expected_list = [{'short_label': None, 'from': 'Subject', 'to': 'Exposure Name of Treatment', 'type': 'Exposure Name of Treatment'}]
    assert g_list == expected_list

    g_list = mm.infer_rels(['Exposure Name of Treatment', 'Subject'])
    expected_list = [{'short_label': None, 'from': 'Subject', 'to': 'Exposure Name of Treatment', 'type': 'Exposure Name of Treatment'}]
    assert g_list == expected_list

    g_list = mm.infer_rels(['Subject', 'Name of Treatment'])
    expected_list = [{'short_label': None, 'from': 'Subject', 'to': 'Name of Treatment', 'type': 'HAS'}]
    # print(g_list)
    # print(expected_list)
    assert g_list == expected_list

    g_list = mm.infer_rels(['Name of Treatment', 'Subject'])
    expected_list = [{'short_label': None, 'from': 'Subject', 'to': 'Name of Treatment', 'type': 'HAS'}]
    # print(g_list)
    # print(expected_list)
    assert g_list == expected_list

    g_list = mm.infer_rels(['Person', 'Exposure Name of Treatment'])
    expected_list = [{'short_label': None, 'from': 'Person', 'to': 'Exposure Name of Treatment', 'type': 'HAS'}]
    # print(g_list)
    # print(expected_list)
    assert g_list == expected_list

    g_list = mm.infer_rels(['Exposure Name of Treatment', 'Person'])
    expected_list = [{'short_label': None, 'from': 'Person', 'to': 'Exposure Name of Treatment', 'type': 'HAS'}]
    # print(g_list)
    # print(expected_list)
    assert g_list == expected_list

    g_list = mm.infer_rels(['Person', 'Name of Treatment'])
    expected_list = [{'short_label': None, 'from': 'Person', 'to': 'Name of Treatment', 'type': 'HAS'}]
    # print(g_list)
    # print(expected_list)
    assert g_list == expected_list

    g_list = mm.infer_rels(['Name of Treatment', 'Person'])
    expected_list = [{'short_label': None, 'from': 'Person', 'to': 'Name of Treatment', 'type': 'HAS'}]
    # print(g_list)
    # print(expected_list)
    assert g_list == expected_list


def test_translate_to_shortlabel(mm):
    mm.clean_slate()
    q = '''
    MERGE (:Class {label:'Subject', short_label:'USUBJID'})
    MERGE (:Class {label:'Population', short_label:'POP'})
    MERGE (:Class {label:'Analysis Age', short_label:'AAGE'})
    MERGE (:Class {label:'Age Group', short_label:'AGEGR'})
    MERGE (:Class {label:'Age Group Definition', short_label:'AGEGRDEF'})
    MERGE (:Class {label:'Safety Population', short_label:'SAFFL'})
    MERGE (:Class {label:'Completers Population', short_label:'COMPFL'})
    '''

    mm.query(q)

    labels = ['Subject', 'Population', 'Analysis Age', 'Age Group', 'Age Group Definition']
    rels = [
        {'from': 'Subject', 'to': 'Population', 'type': 'Population'},
        {'from': 'Subject', 'to': 'Analysis Age', 'type': 'Analysis Age'},
        {'from': 'Subject', 'to': 'Age Group', 'type': 'Age Group'},
        {'from': 'Age Group', 'to': 'Age Group Definition', 'type': 'Definition'},
    ]
    where_map = {
        'Population': {'rdfs:label': ['Safety Population']},
        'Age Group': {'rdfs:label': {'not_in': ['<30 Years']}},
        'Analysis Age': {
            'rdfs:label': {
                'how': 'range', 'min': 40, 'max': 60, 'max_include': False, 'min_include': True,
                'include_nulls': True}
        }
    }
    where_rel_map = {'Population': {'NOT EXISTS': {'exclude': ['Subject', 'Class']}}}
    labels_to_pack = {
        'Age Group': 'Subject',
        'Population': ['Safety Population', 'Completers Population'],
    }

    labels, rels, labels_to_pack, where_map, where_rel_map = mm.translate_to_shortlabel(labels=labels, rels=rels,
                                                                                        labels_to_pack=labels_to_pack,
                                                                                        where_map=where_map,
                                                                                        where_rel_map=where_rel_map)

    expected_labels = [
        {'label': 'Subject', 'short_label': 'USUBJID'},
        {'label': 'Population', 'short_label': 'POP'},
        {'label': 'Analysis Age', 'short_label': 'AAGE'},
        {'label': 'Age Group', 'short_label': 'AGEGR'},
        {'label': 'Age Group Definition', 'short_label': 'AGEGRDEF'}]
    assert labels == expected_labels

    expected_rels = [
        {'from': 'USUBJID', 'to': 'POP', 'type': 'Population'},
        {'from': 'USUBJID', 'to': 'AAGE', 'type': 'Analysis Age'},
        {'from': 'USUBJID', 'to': 'AGEGR', 'type': 'Age Group'},
        {'from': 'AGEGR', 'to': 'AGEGRDEF', 'type': 'Definition'}]
    assert rels == expected_rels

    expected_labels_to_pack = {'AGEGR': 'USUBJID', 'POP': ['SAFFL', 'COMPFL']}
    assert labels_to_pack == expected_labels_to_pack

    expected_where_map = {
        'POP': {'rdfs:label': ['Safety Population']},
        'AGEGR': {'rdfs:label': {'not_in': ['<30 Years']}},
        'AAGE': {'rdfs:label': {'how': 'range', 'min': 40, 'max': 60, 'max_include': False, 'min_include': True, 'include_nulls': True}}
    }
    assert where_map == expected_where_map

    expected_where_rel_map = {'POP': {'NOT EXISTS': {'exclude': ['Subject', 'Class']}}}
    assert where_rel_map == expected_where_rel_map


def test_arrows_dict_uri_dict_enrich(mm:ModelManager):
    with open(os.path.join(filepath, 'data', 'test_arrows_dict_uri_dict_enrich.json')) as jsonfile:
        dct = json.load(jsonfile)
    dct, merge_on = mm.arrows_dict_uri_dict_enrich(dct, mm.URI_MAP)
    for nd in dct['nodes']:
        if nd['labels'] == ['Relationship']:
            assert nd['properties'] == {
                'relationship_type': 'HAS_DATETIME OF FIRST EXPOSURE TO TREATMENT',
                'FROM.Class.label': 'Subject',
                'TO.Class.label': 'Datetime of First Exposure to Treatment'
            }
    for key in ['FROM.Class.label', 'TO.Class.label']:
        assert key in merge_on['Relationship']


def test_get_class_ct(mm):
    mm.clean_slate()

    q = '''
    MERGE (c1:Class {label: 'Test Class'})
    MERGE (c1)-[:HAS_CONTROLLED_TERM]->(:Term {`Codelist Code`: 'CODELISTCODE', `Term Code`: 'TERMCODE', `rdfs:label`: 'test term text 1'})
    MERGE (c1)-[:HAS_CONTROLLED_TERM]->(:Term {`Codelist Code`: 'CODELISTCODE', `Term Code`: 'TERMCODE2', `rdfs:label`: 'test term text 2'})
    
    // extra class
    MERGE (c2:Class {label: 'Test Class 2'})
    MERGE (c2)-[:HAS_CONTROLLED_TERM]->(:Term {`Codelist Code`: 'CODELISTCODE', `Term Code`: 'TERMCODE3', `rdfs:label`: 'test term text 3'})
    '''
    mm.query(q)
    res = mm.get_class_ct(class_='Test Class')
    expected = ['test term text 2', 'test term text 1']
    assert set(res) == set(expected)


def test_propagate_rels_to_parent_class(mm):
    mm.clean_slate()

    q1 = '''
    MERGE (a:Class{label:"A"})-[:SUBCLASS_OF]->(b:Class{label:"B"})-[:SUBCLASS_OF]->(c:Class{label:"C"})
    MERGE (d:Class{label:"D"})
    MERGE (a)<-[:FROM]-(:Relationship{relationship_type:"type1"})-[:TO]->(d)
    '''
    mm.query(q1)

    mm.propagate_rels_to_parent_class()

    q2 = '''
    MATCH path = (c)<-[:FROM]-(r:Relationship{relationship_type:"type1"})-[:TO]->(d)
    WHERE c.label = 'C' AND d.label = 'D'
    RETURN [c.label, d.label, r.relationship_type] as res
    '''
    res = mm.query(q2)[0]['res']
    assert res == ['C', 'D', 'type1']


def test_remove_unmapped_classes(mm):
    mm.clean_slate()

    q1 = '''
    MERGE (a:Class{label:"A"})-[:SUBCLASS_OF]->(b:Class{label:"B"})
    MERGE (c:Class{label:"C"})-[:SUBCLASS_OF]->(d:Class{label:"D"})
    MERGE (e:Class{label:"E"})-[:FROM]-(:Relationship)
    MERGE (f:Class{label:"F"})
    MERGE (g:Class{label:"G"})
    MERGE (a)<-[:MAPS_TO_CLASS]-(g)
    MERGE (f)<-[:MAPS_TO_CLASS]-(g)
    '''
    mm.query(q1)

    mm.remove_unmapped_classes()

    q2 = '''
    MATCH (c:Class)
    RETURN collect(c.label) as class_labels
    '''
    res = mm.query(q2)[0]['class_labels']

    assert res == ['A', 'B', 'F']


def test_remove_auxilary_term_labels(mm):
    mm.clean_slate()

    q1 = '''
    MERGE (t1:Term:ExtraLabel1:ExtraLabel{label:'Term 1'})
    MERGE (t2:Term:ExtraLabel1:ExtraLabel{label:'Term 2'})-[:FROM_DATA]->(d1:Data)
    MERGE (t3:Term:ExtraLabel1:Class{label:'Term 3'})
    MERGE (t4:Term:ExtraLabel1:Class{label:'Term 4'})-[:FROM_DATA]->(d2:Data)
    '''
    mm.query(q1)

    mm.remove_auxilary_term_labels()

    q2 = '''
    MATCH (t:Term)
    RETURN apoc.map.fromPairs(collect([t.label, labels(t)])) as map
    '''
    res = mm.query(q2)[0]
    assert 'map' in res
    assert all(i in res.get('map') for i in ['Term 1', 'Term 2', 'Term 3', 'Term 4'])
    assert not set(res.get('map').get('Term 1')) ^ {'Term'}
    assert not set(res.get('map').get('Term 2')) ^ {'Term', 'ExtraLabel1', 'ExtraLabel'}
    assert not set(res.get('map').get('Term 3')) ^ {'Class', 'Term'}
    assert not set(res.get('map').get('Term 4')) ^ {'Class', 'Term', 'ExtraLabel1'}
