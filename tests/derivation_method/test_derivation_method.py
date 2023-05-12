import json
import os
import pandas as pd
import pytest
from data_providers import DataProvider
from derivation_method import derivation_method_factory, OnlineDerivationMethod
from derivation_method.action import GetData, Link, CallAPI

filepath = os.path.dirname(__file__)
study = 'test_study'


@pytest.fixture
def interface():
    return DataProvider(rdf=True)


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

    def test_merge_action_json_from_empty(self, interface):
        pass

    def test_merge_action_json_from_partial(self, interface):
        pass

    def test_build_derivation_method_json_from_empty(self, interface):
        pass

    def test_build_derivation_method_json_from_partial(self, interface):
        pass


# class TestMergeLinks:

#     def test_
