import json
import os
from pathlib import Path

import pytest

from data_providers import DataProvider
from model_managers import ModelManager
from derivation_method import derivation_method_factory

filepath = os.path.dirname(__file__)
test_file_directory = Path(os.path.join(filepath, 'data', 'raw'))
test_validate_file_directory = Path(os.path.join(filepath, 'data', 'validate_schema'))
study = 'test_study'


@pytest.fixture
def interface():
    return DataProvider(rdf=True)


def set_up_validation_schema(interface):
    interface.clean_slate()

    mm = ModelManager()

    mm.create_class(classes=[
        {'label': 'Test Name', 'short_label': 'TS'},
        {'label': 'Numeric Result', 'short_label': 'NR'},
        {'label': 'Vital Signs', 'short_label': 'VS'},
        {'label': 'Subject', 'short_label': 'USUBJID'},
        {'label': 'Merge Result', 'short_label': 'MR'},
    ])

    mm.create_relationship(rel_list=[
        ['Subject', 'Test Name', 'HAS TEST NAME'],
        ['Test Name', 'Numeric Result', 'HAS NUMERIC RESULT'],
        ['Vital Signs', 'Numeric Result', 'HAS NUMERIC RESULT'],
        ['Vital Signs', 'Test Name', 'HAS TEST NAME'],
        ['Vital Signs', 'Subject', 'SUBJECT'],
        ['Vital Signs', 'Merge Result', 'HAS MERGE RESULT'],
    ])

    mm.create_ct(
        controlled_terminology={
            'Test Name': [
                {'Codelist Code': 'TS', 'Term Code': 'Height', 'rdfs:label': 'Height'},
                {'Codelist Code': 'TS', 'Term Code': 'Weight', 'rdfs:label': 'Weight'},
                {'Codelist Code': 'TS', 'Term Code': 'BMI', 'rdfs:label': 'BMI'}
            ],
            'Subject': [
                {'Codelist Code': 'USUBJID', 'Term Code': '0001', 'rdfs:label': '0001'}
            ]
        }
    )


def test_derivation_method_factory(interface):
    interface.clean_slate()

    for file in os.listdir(test_file_directory):
        test_file_path = os.path.join(test_file_directory, file)
        with open(test_file_path) as jsonfile:
            inline_dct = json.load(jsonfile)
        method = derivation_method_factory(data=inline_dct, interface=interface, study=study)
        assert method.__class__.__name__ == "DictDerivationMethod"

    for file in os.listdir(test_file_directory):
        method = derivation_method_factory(data=test_file_directory / file, interface=interface, study=study,
                                           overwrite_db=True)
        assert method.__class__.__name__ == "DictDerivationMethod"

    # pass invalid string
    method = derivation_method_factory(data="i'm not a file string", interface=interface, study=study,
                                       overwrite_db=True)
    assert method.__class__.__name__ == "OnlineDerivationMethod"

    # pass nothing
    method = derivation_method_factory(interface=interface, study=study)
    assert method.__class__.__name__ == "OnlineDerivationMethod"


def test_validate_method_schema(interface):

    for file in os.listdir(os.path.join(test_validate_file_directory, 'valid_derivations')):
        test_file_path = os.path.join(test_validate_file_directory, 'valid_derivations', file)
        set_up_validation_schema(interface)
        try:
            derivation_method_factory(data=test_file_path, interface=interface, study=study, overwrite_db=True, check_schema=True)
        except KeyError as err:
            raise err

    for file in os.listdir(os.path.join(test_validate_file_directory, 'invalid_derivations')):
        test_file_path = os.path.join(test_validate_file_directory, 'invalid_derivations', file)
        set_up_validation_schema(interface)
        try:
            derivation_method_factory(data=test_file_path, interface=interface, study=study, overwrite_db=True, check_schema=True)
        except KeyError:
            pass
        except Exception as err:
            raise err
        else:
            raise Exception(f'Method {file} should have raised a KeyError as it has invalid schema!')



def test_validate_method_dict(interface):
    interface.clean_slate()

    for file in os.listdir(test_file_directory):
        test_file_path = os.path.join(test_file_directory, file)
        with open(test_file_path) as jsonfile:
            inline_dct = json.load(jsonfile)
        method = derivation_method_factory(data=inline_dct, interface=interface, study=study)
        assert method.__class__.__name__ == "DictDerivationMethod"

    for file in os.listdir(test_file_directory):
        method = derivation_method_factory(data=test_file_directory / file, interface=interface, study=study,
                                           overwrite_db=True)
        assert method.validate_method_dict(method.content['nodes'], method.content["relationships"])
