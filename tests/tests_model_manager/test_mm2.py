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
    mm = ModelManager(verbose=False)
    yield mm


def test_create_class(mm):
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

    mm.create_class(["B", "X"], merge=True) # Only class "X" gets created, because "B" already exists
    result = mm.get_nodes()
    assert compare_recordsets(result, [{'label': 'My First Class'}, {'label': 'A'}, {'label': 'A'}, {'label': 'B'}, {'label': 'X'}])


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
    # print(res)
    # print(expected_res)
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
    assert res == ['test term text 2', 'test term text 1']


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


def test_set_sort_order(mm):
    mm.clean_slate()

    q1 = '''
    MERGE (sdf:`Data Extraction Standard`{_tag_:'standard1'})-[:HAS_TABLE]->(sdt:`Source Data Table`{_domain_:'Domain 1'})
    MERGE (sdt)-[:HAS_COLUMN]->(sdc1:`Source Data Column`{_columnname_: 'Col 1', Order: 2})
    MERGE (sdt)-[:HAS_COLUMN]->(sdc2:`Source Data Column`{_columnname_: 'Col 2', Order: 3})
    MERGE (sdt)-[:HAS_COLUMN]->(sdc3:`Source Data Column`{_columnname_: 'Col 3', Order: 1})
    '''
    mm.query(q1)

    mm.set_sort_order(domain=['Domain 1'], standard='standard1')

    q1 = '''
    MATCH (sdt:`Source Data Table`)
    WHERE sdt._domain_ = 'Domain 1'
    RETURN sdt.SortOrder as SortOrder
    '''
    res = mm.query(q1)[0]['SortOrder']
    assert res == ['Col 3', 'Col 1', 'Col 2']


def test_extend_extraction_metadata(mm):
    mm.clean_slate()

    q1 = '''
    MERGE (sdf:`Data Extraction Standard`{_tag_:'standard1'})-[:HAS_TABLE]->(sdt:`Source Data Table`{_domain_:'Domain 1'})
    MERGE (sdt)-[:HAS_COLUMN]->(sdc1:`Source Data Column`{_columnname_: 'Col 1', Order: 2})
    MERGE (sdt)-[:HAS_COLUMN]->(sdc2:`Source Data Column`{_columnname_: 'Col 2', Order: 3})
    MERGE (sdt)-[:HAS_COLUMN]->(sdc3:`Source Data Column`{_columnname_: 'Col 3', Order: 1})
    
    MERGE (sdc1)-[:MAPS_TO_CLASS]->(c1:Class{label: 'Class 1'})
    MERGE (sdc2)-[:MAPS_TO_CLASS]->(c2:Class{label: 'Class 2'})
    MERGE (c1)<-[:TO]-(r1:Relationship)-[:FROM]->(c3:Class{short_label:'Domain 1'})
    MERGE (c2)<-[:TO]-(r2:Relationship)-[:FROM]->(c4:Class{short_label:'Domain 1'})
    '''

    mm.query(q1)

    mm.extend_extraction_metadata(domain=['Domain 1'], standard='standard1')

    q2 = '''
    MATCH (x)-[:MAPS_TO_CLASS]->(y)
    RETURN x, y
    '''

    res = mm.query(q2)
    expected_res = [{'x': {'Order': 2, '_columnname_': 'Col 1'}, 'y': {'label': 'Class 1'}}, {'x': {'Order': 3, '_columnname_': 'Col 2'}, 'y': {'label': 'Class 2'}}]
    assert res == expected_res


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
    expected_res = {'map': {'Term 4': ['Class', 'Term', 'ExtraLabel1'],
                            'Term 3': ['Class', 'Term'],
                            'Term 2': ['Term', 'ExtraLabel1', 'ExtraLabel'],
                            'Term 1': ['Term']}
                    }
    assert res == expected_res
