import json
import os
import pandas as pd
import pytest
from data_providers import DataProvider
from model_managers import ModelManager
from derivation_method import derivation_method_factory, OnlineDerivationMethod, DerivationMethod
from derivation_method.action import GetData, Link, CallAPI, BuildUri
from derivation_method.super_method import SubjectLevelLinkSuperMethod
from tests.test_comparison_utilities import compare_recordsets, format_json, compare_method_json
from derivation_method.utils import visualise_json, topological_sort

filepath = os.path.dirname(__file__)
study = 'test_study'


def read_json_file(path):
    with open(path, 'r') as jsonfile:
        json_ = json.load(jsonfile)
    return json_


@pytest.fixture
def interface():
    return DataProvider(rdf=True, verbose=False)


class TestDelete:

    def test_single_delete_method(self, interface):
        interface.clean_slate()

        filename = 'derive_simple_002'
        test_file_path = os.path.join(filepath, 'data', 'raw', f'{filename}.json')

        with open(test_file_path) as jsonfile:
            inline_dct = json.load(jsonfile)
        derivation_method_factory(data=inline_dct, interface=interface, study=study, overwrite_db=False)
        method = OnlineDerivationMethod(name=filename, interface=interface, study=study)
        method.delete()

        result = interface.query("""
        MATCH (m:Method)
        RETURN m
        """)
        assert not result

    def test_multiple_delete_method(self, interface):
        interface.clean_slate()

        test_file_directory = os.path.join(filepath, 'data', 'raw')

        for file in os.listdir(test_file_directory):
            test_file_path = os.path.join(test_file_directory, file)
            with open(test_file_path) as jsonfile:
                inline_dct = json.load(jsonfile)
                derivation_method_factory(data=inline_dct, interface=interface, study=study, overwrite_db=False)

        method_batch = derivation_method_factory(interface=interface, study=study)
        method_batch.delete()

        result = interface.query("""
        MATCH (m:Method)
        RETURN m
        """)
        assert not result


class TestRollback:

    def test_link_rollback(self, interface):
        interface.clean_slate()
        with open(os.path.join(filepath, 'data', 'test_data_single.json')) as jsonfile:
            dct = json.load(jsonfile)
        interface.load_arrows_dict(dct)

        # loading test Method metdata
        with open(os.path.join(filepath, 'data', 'raw', 'derive_test_link.json')) as jsonfile:
            inline = json.load(jsonfile)
            derivation_method_factory(data=inline, interface=interface, study=study)

        # load method and get a snapshot of nodes before we apply and then rollback
        method = OnlineDerivationMethod(name='derive_test_link', interface=interface, study=study)
        nodes_before_apply = interface.get_nodes()

        # apply actions
        df = pd.DataFrame
        for action in method.actions:
            df = action.apply(df)

        # rollback actions
        method.rollback()
        nodes_after_rollback = interface.get_nodes()
        # test that nodes after rollback are the same as the snapshot before apply & rollback
        assert nodes_before_apply == nodes_after_rollback

        # make sure the relationship created in the link method has been removed
        q = '''
        MATCH path = (c1:`Test Name`)-[r:`HAS NUMERICAL RESULT`]->(c2:`Numeric Result`)
        RETURN path
        '''
        res = interface.query(q)
        assert not res

    def test_assign_rollback(self, interface):
        interface.clean_slate()
        with open(os.path.join(filepath, 'data', 'test_data_single.json')) as jsonfile:
            dct = json.load(jsonfile)
        interface.load_arrows_dict(dct)

        # loading test Method metdata
        with open(os.path.join(filepath, 'data', 'raw', 'derive_test_assign.json')) as jsonfile:
            inline = json.load(jsonfile)
            derivation_method_factory(data=inline, interface=interface, study=study)

        # load method and get a snapshot of nodes before we apply and then rollback
        method = OnlineDerivationMethod(name='derive_test_assign', interface=interface, study=study)
        nodes_before_apply = interface.get_nodes()

        # apply actions
        df = pd.DataFrame
        for action in method.actions:
            df = action.apply(df)

        # rollback actions
        method.rollback()
        nodes_after_rollback = interface.get_nodes()
        # test that nodes after rollback are the same as the snapshot before apply & rollback
        assert nodes_before_apply == nodes_after_rollback

        # make sure the label created in the assign method has been removed
        q = '''
        MATCH (c1:`Numeric Result`)
        RETURN labels(c1) as labels
        '''
        res = interface.query(q)
        assert res[0]['labels'] == ['Numeric Result']


class TestExplicitPrerequisites:

    def test_explicit_prerequisites_of_method_all(self, interface):
        interface.clean_slate()
        for method in ['derive_rand_001', 'derive_asta_001', 'derive_trt01a_001']:
            with open(os.path.join(filepath, 'data', 'raw', f'{method}.json')) as jsonfile:
                inline = json.load(jsonfile)
            derivation_method_factory(data=inline, interface=interface, study=study)

        # get prerequisites of all methods
        method = OnlineDerivationMethod(name='online', interface=interface, study=study)
        method.explicit_prerequisites_of_method()

        result = interface.query(
            """
            MATCH (m:Method)-[:`METHOD_PREREQ`]->(prereq:Method)
            RETURN m.id, prereq.id
            """
        )

        print(result)

        expected = [
            {'m.id': 'derive_asta_001', 'prereq.id': 'derive_trt01a_001'},
            {'m.id': 'derive_trt01a_001', 'prereq.id': 'derive_rand_001'}
        ]

        assert len(result) == len(expected)
        for i in result:
            assert i in expected

    def test_explicit_prerequisites_of_method_single(self, interface):
        interface.clean_slate()
        for method in ['derive_rand_001', 'derive_asta_001', 'derive_trt01a_001']:
            with open(os.path.join(filepath, 'data', 'raw', f'{method}.json')) as jsonfile:
                inline = json.load(jsonfile)
            derivation_method_factory(data=inline, interface=interface, study=study)

        # get prerequisites of specific method
        method = OnlineDerivationMethod(name='derive_trt01a_001', interface=interface, study=study)
        method.explicit_prerequisites_of_method()

        result = interface.query(
            """
            MATCH (m:Method)-[:`METHOD_PREREQ`]->(prereq:Method)
            RETURN m.id, prereq.id
            """
        )

        expected = [{'m.id': 'derive_trt01a_001', 'prereq.id': 'derive_rand_001'}]

        assert len(result) == len(expected)
        for i in result:
            assert i in expected

    def test_resolve_methods_order(self, interface):
        interface.clean_slate()
        for method in ['derive_rand_001', 'derive_asta_001', 'derive_trt01a_001']:
            with open(os.path.join(filepath, 'data', 'raw', f'{method}.json')) as jsonfile:
                inline = json.load(jsonfile)
            derivation_method_factory(data=inline, interface=interface, study=study)

        # get prerequisites of all methods
        method = OnlineDerivationMethod(name='online', interface=interface, study=study)
        method.explicit_prerequisites_of_method()

        res = method.resolve_methods_order()
        assert res == ['derive_rand_001', 'derive_trt01a_001', 'derive_asta_001']


class TestDerivationJson:

    action_json_path = os.path.join(filepath, 'data', 'merge_method_test_derivations', 'action_json')
    method_json_path = os.path.join(filepath, 'data', 'merge_method_test_derivations', 'method_json')
    expected_json_path = os.path.join(filepath, 'data', 'merge_method_test_derivations', 'expected_method_json')

    def setup_merge_db_content(self, interface, uri=False):
        '''
        Set up schema and load method to db. Also create a dummy changes node to use for predicting.
        :param interface: DataProvider instance

        :return: DictDerivationMethod instance
        '''
        interface.clean_slate()
        mm = ModelManager()
        mm.create_class(classes=[
                {'label': 'Vital Signs', 'short_label': 'VS'},
                {'label': 'Numeric Result', 'short_label': 'NR'},
                {'label': 'Parameter', 'short_label': 'PARAM'},
                {'label': 'New Class', 'short_label': 'NEW', 'classes_for_uri': 'VS|NR', 'derived': 'true'} if uri else 
                {'label': 'New Class', 'short_label': 'NEW', 'aval_repr': 'true', 'derived': 'true'}
                ])
        mm.create_relationship(rel_list=[['NR', 'NEW', 'New Class']], identifier='short_label')
        mm.create_ct(controlled_terminology={'Parameter': {'rdfs:label': 'New Class'}})
        
        method = self.dict_derivation_method

        interface.query("""
        MATCH (m:Method) WHERE m.id = 'add_col'
        MERGE (m)-[:APPLIED]->(changes:Changes)
        SET changes.cols_before = ['VS', 'NR']
        SET changes.cols_after = ['VS', 'NR', 'NEW']
        """)

        return method

    @property
    def dict_derivation_method(self) -> DerivationMethod:
        return derivation_method_factory(interface=DataProvider(rdf=True), data=os.path.join(self.method_json_path, f'derive_partial_method.json'), study=study)

    def test_merge_one_action_json(self, interface):
        interface.clean_slate()
        
        # start with a base derivation method and add a link action to it.
        link_action_json = read_json_file(os.path.join(self.action_json_path, f'link_action.json'))
        
        action_json_list = [link_action_json]
        
        method_json = format_json(self.dict_derivation_method.merge_action_json(name="derive_partial_method", action_json_list=action_json_list, derivation_method_json=self.dict_derivation_method.content))
        expected_json = format_json(read_json_file(os.path.join(self.expected_json_path, f'expected_method_with_link.json')))
        
        assert method_json == expected_json, f'\n\n{method_json=}\n\n{expected_json=}'

    def test_merge_multiple_action_json(self, interface):
        interface.clean_slate()

        # This time start with an empty derivation method which we will fill up with actions.
        get_data_filter_action_json = read_json_file(os.path.join(self.action_json_path, f'get_data_filter_action.json'))
        call_api_action_json = read_json_file(os.path.join(self.action_json_path, f'call_api_action.json'))
        link_value_action_json = read_json_file(os.path.join(self.action_json_path, f'link_value_action.json'))
        build_uri_action_json = read_json_file(os.path.join(self.action_json_path, f'build_uri_action.json'))

        action_json_list = [get_data_filter_action_json, call_api_action_json, link_value_action_json, build_uri_action_json]
        
        method_json = format_json(self.dict_derivation_method.merge_action_json(name="derive_full_method", action_json_list=action_json_list, derivation_method_json=None))
        expected_json = format_json(read_json_file(os.path.join(self.expected_json_path, f'expected_method_full.json')))
        
        assert method_json == expected_json, f'\n\n{method_json=}\n\n{expected_json=}\n\n'
        
    def test_build_derivation_method_json(self, interface):

        mm = ModelManager()

        for file in os.listdir(self.method_json_path):
            interface.clean_slate()
            # load classes that require terms
            mm.create_class(classes=[
                {'label': 'Test Name', 'short_label': 'TS'},
                {'label': 'Subject', 'short_label': 'USUBJID'},
                {'label': 'Population', 'short_label': 'POP'},
                {'label': 'Analysis Age', 'short_label': 'AAGE'},
                {'label': 'Analysis Act Stratum and Act Treatment', 'short_label': 'ASTA'},
                {'label': 'Number of observations', 'short_label': 'n'},
                {'label': 'Mean Value of Analysis Parameter', 'short_label': 'MEAN'},
                ])
            # Load Controlled Terms
            mm.create_ct(controlled_terminology={
                'Test Name': [
                    {'Codelist Code': 'TS', 'Term Code': 'Weight', 'rdfs:label': 'Weight'},
                    {'Codelist Code': 'TS', 'Term Code': 'Height', 'rdfs:label': 'Height'},
                    {'Codelist Code': 'TS', 'Term Code': 'BMI', 'rdfs:label': 'BMI'}
                    ],
                'Subject': [
                    {'Codelist Code': 'USUBJID', 'Term Code': '0001', 'rdfs:label': '0001'}
                    ]
            })

            test_file_path = os.path.join(self.method_json_path, file)
            original_method_json = read_json_file(test_file_path)
            
            method = derivation_method_factory(data=original_method_json, interface=interface, study=study, overwrite_db=True)
            method_json = method.build_derivation_method_json(json_str=False)

            compare_method_json(method_json, original_method_json)

            assert compare_method_json(method_json, original_method_json), f'Failed method: {method.name}\n\ncomp_method_json=\n{json.dumps(format_json(method_json), indent=4, sort_keys=True)}\n\ncomp_original_method_json=\n{json.dumps(format_json(original_method_json), indent=4, sort_keys=True)}'


    def test_predict_output_classes(self, interface):
        method = self.setup_merge_db_content(interface)

        predicted_output_class_info = method.predict_output_classes
        expected_class_info = {'ASSIGN_CLASSES': [], 'CLASSES AFTER RUNSCRIPT': ['VS', 'NR'], 'NEW RUNSCRIPT CLASSES': ['NEW'], 'PREDICTED CLASSES': ['NEW']}

        assert predicted_output_class_info == expected_class_info, f'\n{predicted_output_class_info=}\n{expected_class_info=}'

    def test_predict_links(self, interface):
        method = self.setup_merge_db_content(interface)

        predicted_links, sl_classes = method._predict_links()
        expected_links = [{
                        "from_class": "Numeric Result",
                        "from_short_label": "NR",
                        "to_class": "New Class",
                        "to_short_label": "NEW",
                        "relationship_type": "New Class"
                    }]
        expected_sl = [{'short_label': 'NEW', 'label': 'New Class', 'aval_repr': 'true', 'derived': 'true'}]

        assert predicted_links == expected_links, f'\n{predicted_links=}\n{expected_links=}'
        assert sl_classes == expected_sl, f'\n{sl_classes=}\n{expected_sl=}'

    def test_generate_link_actions(self, interface):
        method = self.setup_merge_db_content(interface)

        predicted_link_jsons = method._generate_link_actions()
        expected_link_json = {
            'nodes': [
                {'id': 'n-34', 'position': {'x': 0, 'y': 0}, 'properties': {'type': 'link', 'id': 'link1'}, 'labels': ['Method']}, 
                {'id': 'n11', 'position': {'x': 0, 'y': 200}, 'properties': {'relationship_type': 'New Class'}, 'labels': ['Relationship']}, 
                {'id': 'n-36', 'position': {'x': 200, 'y': 0}, 'properties': {'label': 'New Class'}, 'labels': ['Class']}, 
                {'id': 'n-35', 'position': {'x': 200, 'y': 200}, 'properties': {'label': 'Numeric Result'}, 'labels': ['Class']}
                ], 
            'relationships': [
                {'toId': 'n11', 'id': 'r-32', 'type': 'LINK', 'fromId': 'n-34', 'properties': {'how': 'merge'}}, 
                {'toId': 'n-36', 'id': 'r-31', 'type': 'TO', 'fromId': 'n11', 'properties': {}}, 
                {'toId': 'n-35', 'id': 'r-30', 'type': 'FROM', 'fromId': 'n11', 'properties': {}}
                ], 
            'style': {}}
        expected_sl_link_json = {
            'nodes': [
                {'id': 'subject_level_link1', 'labels': ['Method'], 'properties': {'type': 'subject_level_link', 'id': 'subject_level_link1'}}, 
                {'id': 'New Class', 'labels': ['Class'], 'properties': {'label': 'New Class'}}
                ], 
            'relationships': [
                {'id': 'New Class_SUBJECT_LEVEL_rel', 'toId': 'New Class', 'fromId': 'subject_level_link1', 'type': 'SUBJECT_LEVEL'}
                ]
                }
        
        expected_json = [expected_link_json, expected_sl_link_json]

        assert len(predicted_link_jsons) == len(expected_json)

        for pred, comp in zip(predicted_link_jsons, expected_json):
            assert compare_method_json(pred, comp), f'\n{pred=}\n{comp=}'

    def test_merge_predicted_links(self, interface):
        method = self.setup_merge_db_content(interface)

        method.merge_predicted_links()

        assert len(method.actions) == 4
        assert isinstance(method.actions[2], Link)
        assert isinstance(method.actions[3], SubjectLevelLinkSuperMethod)

        assert method.actions[2].meta.get('from_class') == 'Numeric Result'
        assert method.actions[2].meta.get('to_class') == 'New Class'
        assert method.actions[2].meta.get('relationship_type') == 'New Class'

        assert method.actions[3].meta.get('class') == {'label': 'New Class', 'short_label': 'NEW', 'aval_repr': 'true', 'derived': 'true'}
        assert not method.actions[2].meta.get('term', False)

    def test_fetch_uri_meta_from_db(self, interface):
        method = self.setup_merge_db_content(interface, uri=True)
        uri_meta = method._fetch_uri_meta_from_db()
        expected_uri_meta = [{
            'class': {'short_label': 'NEW', 'label': 'New Class', 'derived': 'true', 'classes_for_uri': 'VS|NR'}, 
            'uri_labels': [
                {'short_label': 'VS', 'label': 'Vital Signs'}, 
                {'short_label': 'NR', 'label': 'Numeric Result'}
            ]
        }]
        assert uri_meta == expected_uri_meta, f'\n{uri_meta=}\n{expected_uri_meta=}'

    def test_create_build_uri_actions(self, interface):
        method = self.setup_merge_db_content(interface, uri=True)
        build_uri_actions = method._create_build_uri_actions()

        expected_build_uri_action = {
            'nodes': [
                {'id': 'n-55', 'position': {'x': 0, 'y': 0}, 'properties': {'type': 'build_uri', 'id': 'build_uri1'}, 'labels': ['Method']}, 
                {'id': 'n-56', 'position': {'x': 0, 'y': 200}, 'properties': {'label': 'New Class'}, 'labels': ['Class']}, 
                {'id': 'n-59', 'position': {'x': 0, 'y': 200}, 'properties': {'label': 'Numeric Result'}, 'labels': ['Class']}, 
                {'id': 'n-58', 'position': {'x': 200, 'y': 0}, 'properties': {'label': 'Vital Signs'}, 'labels': ['Class']}
            ], 
            'relationships': [
                {'toId': 'n-56', 'id': 'r-51', 'type': 'URI_FOR', 'fromId': 'n-55', 'properties': {}}, 
                {'toId': 'n-59', 'id': 'r-53', 'type': 'URI_BY', 'fromId': 'n-55', 'properties': {}}, 
                {'toId': 'n-58', 'id': 'r-52', 'type': 'URI_BY', 'fromId': 'n-55', 'properties': {}}
            ], 
            'style': {}
        }
        
        assert len(build_uri_actions) == 1
        assert compare_method_json(build_uri_actions[0], expected_build_uri_action), f'\n{build_uri_actions[0]=}\n{expected_build_uri_action=}'

    def test_merge_build_uri_from_schema(self, interface):
        method = self.setup_merge_db_content(interface, uri=True)
        method.merge_build_uri_from_schema()

        assert len(method.actions) == 3
        assert isinstance(method.actions[2], BuildUri)

        assert method.actions[2].meta.get('uri_fors') == ['NEW']
        assert method.actions[2].meta.get('uri_bys') == ['VS', 'NR']
        assert method.actions[2].meta.get('uri_labels') == []
        assert not method.actions[2].meta.get('prefix')

