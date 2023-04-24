import pytest
from model_managers.model_manager import ModelManager
import pandas as pd

@pytest.fixture(scope="module")
def mm():
    mm = ModelManager(verbose=False)
    yield mm

def test_export_model_to_linkml(mm):
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
    #ordered by class, attr.range (rdfs:label at the end)
    expected_result = {
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
    res = mm.export_model_to_linkml()
    # import json
    # print (json.dumps(res,sort_keys=True) )
    # print (json.dumps(expected_result,sort_keys=True) )
    assert res == expected_result    