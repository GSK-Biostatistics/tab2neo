import pytest
from model_managers.model_manager import ModelManager
import pandas as pd

@pytest.fixture(scope="module")
def mm():
    mm = ModelManager(verbose=False)
    yield mm
    
@pytest.fixture(scope="module")
def linkml_example1():
    #ordered by class, attr.range (rdfs:label at the end)
    linkml_example1 = {
        'classes': [
            {
                'label': 'Age', 
                'short_label': 'AGE',
                'data_type': 'integer',
                'attributes': [
                    {
                    "name": "Age rdfs:label",
                    "alias": "rdfs:label",
                    "range": "integer"
                    }
                ]
            },
            {
                'label': 'Sex', 
                'short_label': 'SEX',
                'data_type': 'string',
                'attributes': [
                    {
                    "name": "Sex rdfs:label",
                    "alias": "rdfs:label",
                    "range": "Sex CT"
                    }
                ]
            },            
            {
                'label': 'Subject', 
                'short_label': 'USUBJID',
                'attributes': [                    
                    {
                    "name": "Subject S Age",
                    "alias": "S Age",
                    "range": "Age"
                    },
                    {
                    "name": "Subject S Sex",
                    "alias": "S Sex",
                    "range": "Sex"
                    },     
                    {
                    "name": "Subject rdfs:label",
                    "alias": "rdfs:label",
                    "range": "string"
                    },                                                       
                ]
            },            
        ],
        "enums": {"Sex CT": {"permissible_values": {
            'M': {'description': 'Cxxxx1_Cyyyyy1'},
            'F': {'description': 'Cxxxx1_Cyyyyy2'},
        }}}
    }
    yield linkml_example1

def test_export_model_to_linkml(mm, linkml_example1):
    mm.clean_slate()
    mm.create_class([
        {'label': 'Subject', 'short_label': 'USUBJID'},
        {'label': 'Sex', 'short_label': 'SEX', 'data_type': 'string'},
        {'label': 'Age', 'short_label': 'AGE', 'data_type': 'integer'}
    ])
    mm.create_relationship(
        [
            ['Subject', 'Sex', 'S Sex'],
            ['Subject', 'Age', 'S Age']
        ]
    )
    mm.create_ct(
        {
            'Sex': [
                {'rdfs:label': 'M', 'Codelist Code': 'Cxxxx1', 'Term Code': 'Cyyyyy1'}, 
                {'rdfs:label': 'F', 'Codelist Code': 'Cxxxx1', 'Term Code': 'Cyyyyy2'}
            ]
        }
    )    
    res = mm.export_model_to_linkml()
    # import json
    # print (json.dumps(res,sort_keys=True) )
    # print (json.dumps(expected_result,sort_keys=True) )
    assert res == linkml_example1    
    
def test_create_model_from_linkml(mm, linkml_example1):
    mm.clean_slate()
    mm.create_model_from_linkml(linkml_example1)
    
    res = mm.query("""
    MATCH (x)
    OPTIONAL MATCH (x)-[r]->(y)
    //RETURN apoc.map.merge(x{.*}, {_type: labels(x)}) as subject, type(r) as predicate, CASE WHEN y IS NULL THEN NULL ELSE apoc.map.merge(y{.*}, {_type: labels(x)}) END as object
    RETURN 
        coalesce(x['label'], x['relationship_type'], x['rdfs:label']) as subject, 
        type(r) as predicate,
        coalesce(y['label'], y['relationship_type'], y['rdfs:label']) as object
    ORDER BY id(x), id(y), id(r)
    """) 
    expected_retult = [
        {'subject': 'Age', 'predicate': None, 'object': None}, 
        {'subject': 'Sex', 'predicate': 'HAS_CONTROLLED_TERM', 'object': 'M'}, 
        {'subject': 'Sex', 'predicate': 'HAS_CONTROLLED_TERM', 'object': 'F'}, 
        {'subject': 'Subject', 'predicate': None, 'object': None}, 
        {'subject': 'S Age', 'predicate': 'TO', 'object': 'Age'}, 
        {'subject': 'S Age', 'predicate': 'FROM', 'object': 'Subject'}, 
        {'subject': 'S Sex', 'predicate': 'TO', 'object': 'Sex'},
        {'subject': 'S Sex', 'predicate': 'FROM', 'object': 'Subject'},
        {'subject': 'M', 'predicate': 'NEXT', 'object': 'F'}, 
        {'subject': 'F', 'predicate': None, 'object': None}
    ]
    assert res == expected_retult
    