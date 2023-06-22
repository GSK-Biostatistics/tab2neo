import json
import os
import pandas as pd
import pytest
from data_providers import DataProvider
from derivation_method import derivation_method_factory, OnlineDerivationMethod
from derivation_method import action as Action

filepath = os.path.dirname(__file__)
study = 'test_study'


@pytest.fixture
def interface():
    return DataProvider(rdf=True)


class TestApply:

    def test_load_get_method_metadata(self, interface):
        interface.clean_slate()

        test_file_directory = os.path.join(filepath, 'data', 'raw')

        for file in os.listdir(test_file_directory):
            test_file_path = os.path.join(test_file_directory, file)
            with open(test_file_path) as jsonfile:
                inline_dct = json.load(jsonfile)
                derivation_method_factory(data=inline_dct, interface=interface, study=study, overwrite_db=False)

        result_assign = [a.meta for a in OnlineDerivationMethod(name='derive_test_assign', interface=interface, study=study).actions]
        result_link = [a.meta for a in OnlineDerivationMethod(name='derive_test_link', interface=interface, study=study).actions]
        result_build_uri = [a.meta for a in OnlineDerivationMethod(name='derive_build_uri', interface=interface, study=study).actions]
        result_filter = [a.meta for a in OnlineDerivationMethod(name='derive_test_filter', interface=interface, study=study).actions]
        result_filter_multiple = [a.meta for a in OnlineDerivationMethod(name='derive_test_filter_multiple', interface=interface, study=study).actions]
        result_run_script = [a.meta for a in OnlineDerivationMethod(name='derive_test_run_script', interface=interface, study=study).actions]
        result_call_api = [a.meta for a in OnlineDerivationMethod(name='derive_test_call_api', interface=interface, study=study).actions]
        result_call_api_source_class = [a.meta for a in OnlineDerivationMethod(name='derive_test_call_api_source_class', interface=interface, study=study).actions]
        result_run_cypher_basic = [a.meta for a in OnlineDerivationMethod(name='derive_run_cypher_basic', interface=interface, study=study).actions]
        result_run_cypher_data = [a.meta for a in OnlineDerivationMethod(name='derive_run_cypher_with_data', interface=interface, study=study).actions]

        assert len(result_assign) == 2
        assert len(result_link) == 2
        assert len(result_filter) == 2
        assert len(result_build_uri) == 2
        assert len(result_filter_multiple) == 2
        assert len(result_run_script) == 3
        assert len(result_call_api) == 3
        assert len(result_call_api_source_class) == 3
        assert len(result_run_cypher_basic) == 2
        assert len(result_run_cypher_data) == 2
        assert all([isinstance(x, dict) for x in result_assign])
        assert all([isinstance(x, dict) for x in result_link])
        assert all([isinstance(x, dict) for x in result_filter])
        assert all([isinstance(x, dict) for x in result_build_uri])
        assert all([isinstance(x, dict) for x in result_filter_multiple])
        assert all([isinstance(x, dict) for x in result_run_script])
        assert all([isinstance(x, dict) for x in result_call_api])
        assert all([isinstance(x, dict) for x in result_call_api_source_class])
        assert all([isinstance(x, dict) for x in result_run_cypher_basic])
        assert all([isinstance(x, dict) for x in result_run_cypher_data])

        # TODO: review build uri action structured differently

        for result in [result_assign, result_link, result_filter, result_filter_multiple, result_run_script,
                       result_call_api, result_call_api_source_class, result_run_cypher_basic, result_run_cypher_data]:
            for method in result:

                if method['type'] == 'get_data':

                    for key in ['type', 'source_classes', 'source_rels', 'get_classes']:
                        assert key in method.keys(), f'Property: {key} not in method {method["type"]} properties'

                elif method['type'] == 'assign_class':

                    for key in ['type', 'assign_label', 'assign_short_label', 'on_class', 'on_class_short_label',
                                'from',
                                'relationship_type', 'to']:
                        assert key in method.keys(), f'Property: {key} not in method {method["type"]} properties'

                elif method['type'] == 'link':

                    for key in ['type', 'relationship_type', 'from_class', 'to_class', 'to_class_property', 'merge',
                                'to_value', 'to_class_property_ct', 'to_short_label', 'from_class_property_ct',
                                'from_short_label']:
                        assert key in method.keys(), f'Property: {key} not in method {method["type"]} properties'

                elif (method['type'] == 'run_script') or (method['type'] == 'call_api'):

                    for key in ['package', 'parent_id', 'id', 'type', 'lang', 'params', 'uri', 'script']:
                        assert key in method.keys(), f'Property: {key} not in method {method["type"]} properties'

                elif method['type'] == 'run_cypher':

                    for key in ['id', 'type', 'query']:
                        assert key in method.keys(), f'Property: {key} not in method {method["type"]} properties'

                elif method['type'] == 'build_uri':

                    for key in ['id', 'type']:
                        assert key in method.keys(), f'Property: {key} not in method {method["type"]} properties'

                if method == result_filter:
                    assert method['where_map'] == {'Test Name': {'rdfs:label': ['Height', 'BMI']}}
                elif method == result_filter_multiple:
                    assert method['where_map'] == {'Test Name': {'rdfs:label': 'Height'}}

    def test_apply_method_assign(self, interface):
        # Test a assign_class method. PNG of method can be found in data/method png

        interface.clean_slate()

        with open(os.path.join(filepath, 'data', 'test_data_single.json')) as jsonfile:
            dct = json.load(jsonfile)
        interface.load_arrows_dict(dct)

        with open(os.path.join(filepath, 'data', 'raw', 'derive_test_assign.json')) as jsonfile:
            inline = json.load(jsonfile)
        method = derivation_method_factory(data=inline, interface=interface, study=study)
        df = pd.DataFrame
        for action in method.actions:
            df = action.apply(df)
        # df = method.actions.apply()

        result = interface.query(
            """
            MATCH (aval:`Analysis Value`)
            RETURN aval
            """
        )

        expected = {'aval': {'rdfs:label': '100'}}

        print(result)
        for i in result[0]:
            print(i)
            print(expected)
            assert i in expected

        assert all(i in df.columns for i in ["NR", "_id_NR", "_id_AVAL"])
        assert df.at[0, "NR"] == '100'  # todo data loaded by load_arrows_dict makes this a string not int
        # assert df["_id_NR"].equals(df["_id_AVAL"])
        assert df.at[0, "_id_NR"] == df.at[0, "_id_AVAL"]

    def test_apply_method_link(self, interface):
        # Test a link method. PNG of method can be found in data/method png

        # -------------------- derive_test_link.json -----------------------
        # loading test data and Class-Relionship schema
        interface.clean_slate()
        with open(os.path.join(filepath, 'data', 'test_data_single.json')) as jsonfile:
            dct = json.load(jsonfile)
        interface.load_arrows_dict(dct)

        # loading test Method metdata
        with open(os.path.join(filepath, 'data', 'raw', 'derive_test_link.json')) as jsonfile:
            inline = json.load(jsonfile)
        method = derivation_method_factory(data=inline, interface=interface, study=study)
        df = pd.DataFrame
        for action in method.actions:
            df = action.apply(df)

        result = interface.query(
            """
            MATCH (ts:`Test Name`)-[:`HAS NUMERIC RESULT`]->(nr:`Numeric Result`)
            RETURN ts, nr
            """
        )

        expected = [{'ts': {'rdfs:label': 'Weight'}, 'nr': {'rdfs:label': '100'}}]

        for i in result:
            assert i in expected

        assert all(i in df.columns for i in ["NR", "_id_NR", "VS", "_id_VS", "TS", "_id_TS"])
        assert df.at[0, "NR"] == '100' and df.at[0, "TS"] == "Weight"

        # -------------------- derive_test_link_from_value -----------------------
        interface.clean_slate()
        # loading test data and Class-Relationship schema
        with open(os.path.join(filepath, 'data', 'test_data_multiple.json')) as jsonfile:
            dct = json.load(jsonfile)
        interface.load_arrows_dict(dct)

        # loading test Method metadata
        with open(os.path.join(filepath, 'data', 'raw', 'derive_test_link_from_value.json')) as jsonfile:
            inline = json.load(jsonfile)
        method = derivation_method_factory(data=inline, interface=interface, study=study)
        df = pd.DataFrame
        for action in method.actions:
            df = action.apply(df)

        result = interface.query(
            """
            MATCH (ts:`Test Name`)-[:`Subject`]->(s:`Subject`)
            RETURN ts, s
            """
        )

        expected = [{'ts': {'rdfs:label': 'Weight'}, 's': {'rdfs:label': '0001'}},
                    {'ts': {'rdfs:label': 'Weight'}, 's': {'rdfs:label': '0002'}}]
        for i in result:
            assert i in expected

        # -------------------- derive_test_link_to_value -----------------------
        interface.clean_slate()
        # loading test data and Class-Relationship schema
        with open(os.path.join(filepath, 'data', 'test_data_multiple.json')) as jsonfile:
            dct = json.load(jsonfile)
        interface.load_arrows_dict(dct)
        # loading test Method metadata
        with open(os.path.join(filepath, 'data', 'raw', 'derive_test_link_to_value.json')) as jsonfile:
            inline = json.load(jsonfile)
        method = derivation_method_factory(data=inline, interface=interface, study=study)
        df = pd.DataFrame
        for action in method.actions:
            df = action.apply(df)

        result = interface.query(
            """
            MATCH (ts:`Test Name`)<-[:`HAS TEST NAME`]-(s:`Subject`)
            RETURN ts, s
            """
        )

        expected = [{'ts': {'rdfs:label': 'Weight'}, 's': {'rdfs:label': '0001'}},
                    {'ts': {'rdfs:label': 'Weight'}, 's': {'rdfs:label': '0002'}}]
        for i in result:
            assert i in expected

        # -------------------- derive_test_link_to_from_value -----------------------
        interface.clean_slate()
        # loading test data and Class-Relationship schema
        with open(os.path.join(filepath, 'data', 'test_data_multiple.json')) as jsonfile:
            dct = json.load(jsonfile)
        interface.load_arrows_dict(dct)
        # loading test Method metadata
        with open(os.path.join(filepath, 'data', 'raw', 'derive_test_link_to_from_value.json')) as jsonfile:
            inline = json.load(jsonfile)
        method = derivation_method_factory(data=inline, interface=interface, study=study)
        df = pd.DataFrame
        for action in method.actions:
            df = action.apply(df)

        result = interface.query(
            """
            MATCH (ts:`Test Name`)-[:`Subject`]->(s:`Subject`)
            RETURN ts, s
            """
        )

        expected = [{'ts': {'rdfs:label': 'Weight'}, 's': {'rdfs:label': '0001'}}]
        assert result == expected

        # -------------------- derive_test_link_on_uri -----------------------
        interface.clean_slate()
        # loading test data and Class-Relationship schema
        with open(os.path.join(filepath, 'data', 'test_data_multiple_merge.json')) as jsonfile:
            dct = json.load(jsonfile)
        interface.load_arrows_dict(dct)

        # loading test Method metadata
        with open(os.path.join(filepath, 'data', 'raw', 'derive_test_link_merge_on_uri.json')) as jsonfile:
            inline = json.load(jsonfile)
        method = derivation_method_factory(data=inline, interface=interface, study=study)

        df = pd.DataFrame()

        for action in method.actions:
            df = action.apply(df)
            if type(action) == Action.GetData:
                df['MR'] = [100, 100, 100, 100, 50, 50]

        result = interface.query(
            """
            MATCH (vs:`Vital Signs`)-[r:`HAS MERGE RESULT`]->(mr:`Merge Result`)
            RETURN count(r) as links, count(distinct mr) as mr_nodes
            """
        )

        assert result[0].get('links') == 6
        assert result[0].get('mr_nodes') == 6

    def test_apply_method_filter(self, interface):
        # Test a filter method (followed by link method). PNG of method can be found in data/method png

        interface.clean_slate()
        with open(os.path.join(filepath, 'data', 'test_data_multiple.json')) as jsonfile:
            dct = json.load(jsonfile)
        interface.load_arrows_dict(dct)

        with open(os.path.join(filepath, 'data', 'raw', 'derive_test_filter.json')) as jsonfile:
            inline = json.load(jsonfile)
        method = derivation_method_factory(data=inline, interface=interface, study=study)
        df = pd.DataFrame
        for action in method.actions:
            df = action.apply(df)

        result = interface.query(
            """
            MATCH (ts:`Test Name`)-[:`HAS NUMERIC RESULT`]->(nr:`Numeric Result`)
            RETURN ts, nr
            """
        )

        expected = [
            {'ts': {'rdfs:label': 'Height'}, 'nr': {'rdfs:label': '130'}},
            {'ts': {'rdfs:label': 'Height'}, 'nr': {'rdfs:label': '110'}}
        ]

        for i in result:
            assert i in expected

        assert all(i in df.columns for i in ["NR", "_id_NR", "VS", "_id_VS", "TS", "_id_TS"])
        assert set(df["TS"].unique()) == {"Height"}

    def test_apply_method_filter_multiple(self, interface):
        # Test a filter method (followed by link method). PNG of method can be found in data/method png

        interface.clean_slate()
        with open(os.path.join(filepath, 'data', 'test_data_multiple.json')) as jsonfile:
            dct = json.load(jsonfile)
        interface.load_arrows_dict(dct)

        with open(os.path.join(filepath, 'data', 'raw', 'derive_test_filter_multiple.json')) as jsonfile:
            inline = json.load(jsonfile)
        method = derivation_method_factory(data=inline, interface=interface, study=study)
        df = pd.DataFrame
        for action in method.actions:
            df = action.apply(df)

        result = interface.query(
            """
            MATCH (ts:`Test Name`)-[:`HAS NUMERIC RESULT`]->(nr:`Numeric Result`)
            RETURN ts, nr
            """
        )

        expected = [
            {'ts': {'rdfs:label': 'Height'}, 'nr': {'rdfs:label': '130'}},
            {'ts': {'rdfs:label': 'Height'}, 'nr': {'rdfs:label': '110'}},
            {'ts': {'rdfs:label': 'BMI'}, 'nr': {'rdfs:label': '25'}},
            {'ts': {'rdfs:label': 'BMI'}, 'nr': {'rdfs:label': '26'}}
        ]

        for i in result:
            assert i in expected

        assert all(i in df.columns for i in ["NR", "_id_NR", "VS", "_id_VS", "TS", "_id_TS"])
        assert set(df["TS"].unique()) == {"Height", "BMI"}

    # Test a run_script method. PNG of method can be found in data/method png
    # def test_apply_method_run_script(self, interface):
    #
    #     interface.clean_slate()
    #     with open(os.path.join(filepath, 'data', 'test_data_single.json')) as jsonfile:
    #         inline = json.load(jsonfile)
    #     method = derivation_method_factory(data=inline, interface=interface, study=study)
    #     df = pd.DataFrame
    #     for action in method.actions:
    #         df = action.apply(df)
    #
    #     result = interface.query(
    #         """
    #         MATCH (nr:`Numeric Result`)-[:`HAS`]->(nrm:`Numeric Result Modified`)
    #         RETURN nr, nrm
    #         """
    #     )

    def test_apply_method_call_api(self, interface):
        # Test a call_api method. PNG of method can be found in data/method png

        interface.clean_slate()
        with open(os.path.join(filepath, 'data', 'test_data_multiple.json')) as jsonfile:
            dct = json.load(jsonfile)
        interface.load_arrows_dict(dct)

        with open(os.path.join(filepath, 'data', 'raw', 'derive_test_call_api.json')) as jsonfile:
            inline = json.load(jsonfile)
        method = derivation_method_factory(data=inline, interface=interface, study=study)
        df = pd.DataFrame
        for action in method.actions:
            df = action.apply(df)

        result = interface.query(
            """
            MATCH (nr:`Numeric Result`)-[:`HAS`]->(ts:`Test Name`)
            RETURN nr, ts
            """
        )

        assert len(result) == 1  # only one row of data

    def test_apply_method_call_api_source_class(self, interface):
        # Test a call_api method, with source_classes (not relationships). PNG of method can be found in data/method png

        interface.clean_slate()
        with open(os.path.join(filepath, 'data', 'test_data_multiple.json')) as jsonfile:
            dct = json.load(jsonfile)
        interface.load_arrows_dict(dct)

        with open(os.path.join(filepath, 'data', 'raw', 'derive_test_call_api_source_class.json')) as jsonfile:
            inline = json.load(jsonfile)
        method = derivation_method_factory(data=inline, interface=interface, study=study)
        df = pd.DataFrame
        for action in method.actions:
            df = action.apply(df)
        result = interface.query(
            """
            MATCH (vs:`Vital Signs`:`First Vital Signs`)
            RETURN vs
            """
        )

        assert len(result) == 1  # only one row of data

    def test_get_data_allow_unrelated_subgraphs(self, interface):
        # Test a call_api method, with source_classes (not relationships). PNG of method can be found in data/method png

        interface.clean_slate()
        with open(os.path.join(filepath, 'data', 'test_data_nan.json')) as jsonfile:
            dct = json.load(jsonfile)
        interface.load_arrows_dict(dct)

        with open(os.path.join(filepath, 'data', 'raw', 'derive_test_allow_unrelated_subgraphs.json')) as jsonfile:
            inline = json.load(jsonfile)
        method = derivation_method_factory(data=inline, interface=interface, study=study)
        df = pd.DataFrame
        action = method.actions[0] #get_data
        df = action.apply(df)

        assert len(df) == 12  # 2(Subject) x (6)(Numeric Result) - cartesian product dataframe expected

    def test_run_cypher_action_basic(self, interface):
        # Performs a simple cypher query which runs a match and returns data

        # --------------------- test_run_cypher action basic ------------------------------
        # loading test data and Class-Relationship schema
        interface.clean_slate()
        with open(os.path.join(filepath, 'data', 'test_data_multiple.json')) as jsonfile:
            dct = json.load(jsonfile)
        interface.load_arrows_dict(dct)

        with open(os.path.join(filepath, 'data', 'raw', 'derive_run_cypher_basic.json')) as jsonfile:
            inline = json.load(jsonfile)
        method = derivation_method_factory(data=inline, interface=interface, study=study)

        df = pd.DataFrame
        for action in method.actions:
            df = action.apply(df)

        assert not df.empty

        expected_df = pd.DataFrame({'vs.rdfs:label': ['"100"', '"110"', '"25"']})
        pd.testing.assert_frame_equal(df.sort_values(by='vs.rdfs:label'), expected_df, check_index_type=False)

    def test_run_cypher_action_with_data(self, interface):
        # Performs a GetData action followed by a RunCypher action that unwinds data to create new nodes

        # --------------------- test_run_cypher action with data ------------------------------
        # loading test data and Class-Relationship schema
        interface.clean_slate()
        with open(os.path.join(filepath, 'data', 'test_data_multiple.json')) as jsonfile:
            dct = json.load(jsonfile)
        interface.load_arrows_dict(dct)

        with open(os.path.join(filepath, 'data', 'raw', 'derive_run_cypher_with_data.json')) as jsonfile:
            inline = json.load(jsonfile)
        method = derivation_method_factory(data=inline, interface=interface, study=study)

        df = pd.DataFrame
        for action in method.actions:
            df = action.apply(df)

        assert not df.empty
        assert df['new_nodes'][0] == 6

        result = interface.query(
            """
            MATCH (q:`Query Column`)
            RETURN count(q) as count
            """
        )
        assert result[0].get('count') == 6

    def test_build_uri_action(self, interface):
        # --------------------- test_build_uri_with_label-action ------------------------------

        # loading test data and Class-Relationship schema
        interface.clean_slate()
        with open(os.path.join(filepath, 'data', 'test_data_multiple.json')) as jsonfile:
            dct = json.load(jsonfile)
        interface.load_arrows_dict(dct)

        # test build uri with additional label
        with open(os.path.join(filepath, 'data', 'raw', 'derive_build_uri.json')) as jsonfile:
            inline = json.load(jsonfile)
        method = derivation_method_factory(data=inline, interface=interface, study=study)

        df = pd.DataFrame
        for action in method.actions:
            df = action.apply(df)

        assert all(col in df.columns for col in ['NR', 'VS', 'TS', '_uri_NR'])

        for index, row in df.iterrows():
            assert row['_uri_NR'] == f'_NR_by_TS:{row["TS"]}/NR:{row["NR"]}_label_VS', \
                f"The following row contained an incomplete uri: {row}"

        # Re-test method without additional label by removing URI_LABEL method relationships
        keep_rels = []
        for rel in inline.get('relationships'):
            if rel.get('type') != 'URI_LABEL':
                keep_rels.append(rel)

        inline['relationships'] = keep_rels
        method = derivation_method_factory(data=inline, overwrite_db=True, interface=interface, study=study)

        df = pd.DataFrame
        for action in method.actions:
            df = action.apply(df)

        assert all(col in df.columns for col in ['NR', 'VS', 'TS', '_uri_NR'])

        for index, row in df.iterrows():
            assert row['_uri_NR'] == f'_NR_by_TS:{row["TS"]}/NR:{row["NR"]}', \
                f"The following row contained an incomplete uri: {row}"

    def test_run_cypher_action_with_remove_col_prefixes(self, interface):
        # Performs a GetData action followed by a RunCypher action 
        
        interface.clean_slate()
        with open(os.path.join(filepath, 'data', 'test_data_multiple.json')) as jsonfile:
            dct = json.load(jsonfile)
        interface.load_arrows_dict(dct)

        with open(os.path.join(filepath, 'data', 'raw', 'derive_run_cypher_with_remove_col_prefixes.json')) as jsonfile:
            inline = json.load(jsonfile)
        method = derivation_method_factory(data=inline, interface=interface, study=study)

        df = pd.DataFrame
        for action in method.actions:
            df = action.apply(df)
        
        assert list(df.columns)==['label', 'relationship_type', 'short_label']
