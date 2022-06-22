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