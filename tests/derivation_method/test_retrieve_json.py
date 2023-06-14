import json
import os
import pandas as pd
import pytest
from pathlib import Path
from data_providers import DataProvider
from tests.test_comparison_utilities import compare_recordsets, format_nodes, format_relationships, compare_method_json
from derivation_method import derivation_method_factory

filepath = os.path.dirname(__file__)
study = 'test_study'


@pytest.fixture
def interface():
    return DataProvider(rdf=True)


class TestRetrieveJson:

    def test_run_script_retrieve_json(self, interface):
        filename = 'test_run_script_retrieve_json'
        exp_filename = 'expected_run_script_json'
        interface.clean_slate()
        test_file_path = os.path.join(filepath, 'data', 'raw', f'{filename}.json')
        expected_file_path = os.path.join(filepath, 'data', 'expected_action_json', f'{exp_filename}.json')

        with open(test_file_path, 'r') as jsonfile:
            inline_dct = json.load(jsonfile)
            method = derivation_method_factory(data=inline_dct, interface=interface, study=study, overwrite_db=False)
            method_json = method.actions[1].retrieve_json()
        
        with open(expected_file_path, 'r') as exp_json_file:
            expected_json = json.load(exp_json_file)
            assert compare_recordsets([method_json], [expected_json])

    def test_get_data_retrieve_json_wo_filter(self, interface):
        interface.clean_slate()
        filename = 'test_get_data_retrieve_json_wo_filter'
        exp_filename = 'expected_getdata_json_wo_filter'
        test_file_path = os.path.join(filepath, 'data', 'raw', f'{filename}.json')
        expected_file_path = os.path.join(filepath, 'data', 'expected_action_json', f'{exp_filename}.json')
        
        with open(test_file_path, 'r') as jsonfile:
            inline_dct = json.load(jsonfile)
            method = derivation_method_factory(data=inline_dct, interface=interface, study=study, overwrite_db=False)
            method_json = method.actions[0].retrieve_json()

        with open(expected_file_path, 'r') as exp_json_file:
            expected_json = json.load(exp_json_file)

        assert compare_method_json(method_json, expected_json)

    def test_get_data_retrieve_json_with_filter_and_source_rels(self, interface):
        interface.clean_slate()
        filename = 'test_get_data_retrieve_json_with_filter'
        exp_filename = 'expected_getdata_json_with_filter'
        test_file_path = os.path.join(filepath, 'data', 'raw', f'{filename}.json')
        expected_file_path = os.path.join(filepath, 'data', 'expected_action_json', f'{exp_filename}.json')
        
        with open(test_file_path, 'r') as jsonfile:
            inline_dct = json.load(jsonfile)
            method = derivation_method_factory(data=inline_dct, interface=interface, study=study, overwrite_db=False)
            method_json = method.actions[0].retrieve_json()

        with open(expected_file_path, 'r') as exp_json_file:
            expected_json = json.load(exp_json_file)
        assert compare_method_json(method_json, expected_json)
        

    def test_get_data_retrieve_json_with_range_filter(self, interface):
        interface.clean_slate()
        filename = 'test_get_data_retrieve_json_with_range_filter'
        exp_filename = 'expected_getdata_json_with_range_filter'
        test_file_path = os.path.join(filepath, 'data', 'raw', f'{filename}.json')
        expected_file_path = os.path.join(filepath, 'data', 'expected_action_json', f'{exp_filename}.json')

        with open(test_file_path, 'r') as jsonfile:
            inline_dct = json.load(jsonfile)
            method = derivation_method_factory(data=inline_dct, interface=interface, study=study, overwrite_db=False)
            method_json = method.actions[0].retrieve_json()

        with open(expected_file_path, 'r') as exp_json_file:
            expected_json = json.load(exp_json_file)
        assert compare_method_json(method_json, expected_json)

    def test_get_data_retrieve_json_with_source_classes(self, interface):
        interface.clean_slate()
        filename = 'test_get_data_retrieve_json_with_source_class'
        exp_filename = 'expected_getdata_json_with_source_class'
        test_file_path = os.path.join(filepath, 'data', 'raw', f'{filename}.json')
        expected_file_path = os.path.join(filepath, 'data', 'expected_action_json', f'{exp_filename}.json')

        with open(test_file_path, 'r') as jsonfile:
            inline_dct = json.load(jsonfile)
            method = derivation_method_factory(data=inline_dct, interface=interface, study=study, overwrite_db=False)
            method_json = method.actions[0].retrieve_json()

        with open(expected_file_path, 'r') as exp_json_file:
            expected_json = json.load(exp_json_file)
        assert compare_method_json(method_json, expected_json)            

    def test_call_api_retrieve_json(self, interface):
        filename = 'test_call_api_retrieve_json'
        exp_filename = 'expected_call_api_json'
        interface.clean_slate()
        test_file_path = os.path.join(filepath, 'data', 'raw', f'{filename}.json')
        expected_file_path = os.path.join(filepath, 'data', 'expected_action_json', f'{exp_filename}.json')

        with open(test_file_path, 'r') as jsonfile:
            inline_dct = json.load(jsonfile)
            method = derivation_method_factory(data=inline_dct, interface=interface, study=study, overwrite_db=False)
            method_json = method.actions[1].retrieve_json()

        with open(expected_file_path, 'r') as exp_json_file:
            expected_json = json.load(exp_json_file)
            assert compare_recordsets([method_json], [expected_json])

    def test_normal_link_retrieve_json(self, interface):
        interface.clean_slate()
        filename = 'test_link_retrieve_json'
        exp_filename = 'expected_link_json'
        test_file_path = os.path.join(filepath, 'data', 'raw', f'{filename}.json')
        expected_file_path = os.path.join(filepath, 'data', 'expected_action_json', f'{exp_filename}.json')

        with open(test_file_path, 'r') as jsonfile:
            inline_dct = json.load(jsonfile)
            method = derivation_method_factory(data=inline_dct, interface=interface, study=study, overwrite_db=False)
            method_json = method.actions[1].retrieve_json()

        with open(expected_file_path, 'r') as exp_json_file:
            expected_json = json.load(exp_json_file)
        assert compare_method_json(method_json, expected_json)

    def test_link_retrieve_json_to_from_value(self, interface):
        interface.clean_slate()
        filename = 'test_link_retrieve_json_to_from_value'
        exp_filename = 'expected_link_json_to_from_value'
        test_file_path = os.path.join(filepath, 'data', 'raw', f'{filename}.json')
        expected_file_path = os.path.join(filepath, 'data', 'expected_action_json', f'{exp_filename}.json')

        with open(test_file_path, 'r') as jsonfile:
            inline_dct = json.load(jsonfile)
            method = derivation_method_factory(data=inline_dct, interface=interface, study=study, overwrite_db=False)
            method_json = method.actions[1].retrieve_json()
        
        with open(expected_file_path, 'r') as exp_json_file:
            expected_json = json.load(exp_json_file)
        assert compare_method_json(method_json, expected_json)    

    def test_link_retrieve_json_to_merge(self, interface):
        interface.clean_slate()
        filename = 'test_link_retrieve_json_to_merge'
        exp_filename = 'expected_link_json_to_merge'
        test_file_path = os.path.join(filepath, 'data', 'raw', f'{filename}.json')
        expected_file_path = os.path.join(filepath, 'data', 'expected_action_json', f'{exp_filename}.json')

        with open(test_file_path, 'r') as jsonfile:
            inline_dct = json.load(jsonfile)
            method = derivation_method_factory(data=inline_dct, interface=interface, study=study, overwrite_db=False)
            method_json = method.actions[2].retrieve_json()

        with open(expected_file_path, 'r') as exp_json_file:
            expected_json = json.load(exp_json_file)
        assert compare_method_json(method_json, expected_json)    

    def test_build_uri_retrieve_json(self, interface):

        interface.clean_slate()
        filename = 'test_build_uri_retrieve_json'
        exp_filename = 'expected_build_uri_json'
        test_file_path = os.path.join(filepath, 'data', 'raw', f'{filename}.json')
        expected_file_path = os.path.join(filepath, 'data', 'expected_action_json', f'{exp_filename}.json')

        with open(test_file_path, 'r') as jsonfile:
            inline_dct = json.load(jsonfile)
            method = derivation_method_factory(data=inline_dct, interface=interface, study=study, overwrite_db=False)
            method_json = method.actions[1].retrieve_json()

        with open(expected_file_path, 'r') as exp_json_file:
            expected_json = json.load(exp_json_file)
        assert compare_method_json(method_json, expected_json)

    def test_assign_retrieve_json(self, interface):

        interface.clean_slate()
        filename = 'derive_test_assign'
        exp_filename = 'expected_assign_json'
        test_file_path = os.path.join(filepath, 'data', 'raw', f'{filename}.json')
        expected_file_path = os.path.join(filepath, 'data', 'expected_action_json', f'{exp_filename}.json')

        with open(test_file_path, 'r') as jsonfile:
            inline_dct = json.load(jsonfile)
            method = derivation_method_factory(data=inline_dct, interface=interface, study=study, overwrite_db=False)
            method_json = method.actions[1].retrieve_json()

        with open(expected_file_path, 'r') as exp_json_file:
            expected_json = json.load(exp_json_file)
        assert compare_method_json(method_json, expected_json), f"\n{method_json=}\n{expected_json=}"

    def test_run_cypher_retrieve_json(self, interface):

        interface.clean_slate()
        filename = 'derive_run_cypher_with_data'
        exp_filename = 'expected_run_cypher_json'
        test_file_path = os.path.join(filepath, 'data', 'raw', f'{filename}.json')
        expected_file_path = os.path.join(filepath, 'data', 'expected_action_json', f'{exp_filename}.json')

        with open(test_file_path, 'r') as jsonfile:
            inline_dct = json.load(jsonfile)
            method = derivation_method_factory(data=inline_dct, interface=interface, study=study, overwrite_db=False)
            method_json = method.actions[1].retrieve_json()

        with open(expected_file_path, 'r') as exp_json_file:
            expected_json = json.load(exp_json_file)

        assert compare_method_json(method_json, expected_json), f"\n{method_json=}\n{expected_json=}"

