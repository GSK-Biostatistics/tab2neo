import pytest
import os
import json
from model_managers import ModelManager
from data_loaders import FileDataLoader
from model_appliers import ModelApplier
from data_providers import DataProvider
from derivation_method import derivation_method_factory
from derivation_method.super_method import SuperMethod

filepath = os.path.dirname(__file__)
study = 'test_study'


def load_schema_and_terms(interface):

    mm = ModelManager()

    # Load Schema Metadata
    with open(os.path.join(filepath, 'data', 'schemas', 'schema.json')) as jsonfile:
        dct = json.load(jsonfile)
    interface.load_arrows_dict(dct)

    # Load Controlled Terms
    mm.create_ct(controlled_terminology={
        'Age Group': [
            {'Codelist Code': 'AGEGR2C', 'Term Code': '65-84 Years', 'rdfs:label': '65-84 Years', 'data_type': 'str'}, 
            {'Codelist Code': 'AGEGR2C', 'Term Code': '>=85 Years', 'rdfs:label': '>=85 Years', 'data_type': 'str'}
            ],
        'Age Group A': [
            {'Codelist Code': 'AGEGRA', 'Term Code': 'age group 2', 'rdfs:label': 'age group 2', 'data_type': 'str'}
            ]
    })
    # Load Term relationship Schema
    with open(os.path.join(filepath, 'data', 'controlled_terms', 'term_schema.json')) as jsonfile:
        dct = json.load(jsonfile)
    interface.load_arrows_dict(dct, merge_on={'Term': ['Codelist Code', 'Term Code']})


def load_subject_level_data():

    fdl = FileDataLoader()
    fdl.clean_slate()
    fdl.load_file(
        folder=os.path.join(filepath, 'data'), 
        filename='test_data_subject_level.csv'
    )
    mm = ModelManager()
    mm.create_model_from_data()
    mm.query("MATCH (c:Class{label:'Subject'}) set c.short_label = 'USUBJID'")
    mm.query("MATCH (c:Class{label:'Age'}) set c.data_type = 'int', c.short_label = 'AGE'")

    ma = ModelApplier(mode="schema_CLASS")
    ma.refactor_all()

    mm.create_class([
        {'label': 'Parameter', 'short_label': 'PARAM'}, 
        {'label': 'Analysis Value (C)', 'short_label': 'AVALC'}, 
        {'label': 'Analysis Value', 'short_label': 'AVAL'}, 
        {'label': 'Record', 'short_label': 'RECORD'}
        ])
    mm.create_related_classes_from_list([
        ['USUBJID', 'Record', 'Record'],
        ['Record', 'Parameter', 'Parameter'],
        ['Record', 'Analysis Value', 'Analysis Value'],
        ['Record', 'Analysis Value (C)', 'Analysis Value (C)'],
    ]
    )
    mm.create_ct(
        {
        'Parameter': [{'rdfs:label': 'Age'}, {'rdfs:label': 'Sex'}],               
        }
    )


@pytest.fixture
def apply_stat_meta():
    interface = DataProvider(rdf=True)
    interface.clean_slate()
    with open(os.path.join(filepath, 'data', 'test_data_apply_stat.json')) as jsonfile:
        dct = json.load(jsonfile)
    interface.load_arrows_dict(dct)
    with open(os.path.join(filepath, 'data', 'raw', 'test_apply_stat.json')) as jsonfile:
        inline = json.load(jsonfile)
    method = derivation_method_factory(data=inline, interface=interface, study=study)

    action_metas = []
    for a in method.actions:
        if isinstance(a, SuperMethod):
            action_metas.extend([_a.meta for _a in a.actions])
        else:
            action_metas.append(a.meta)
    return action_metas


@pytest.fixture
def apply_stat_meta_non_distinct():
    interface = DataProvider(rdf=True)
    interface.clean_slate()
    with open(os.path.join(filepath, 'data', 'test_data_apply_stat.json')) as jsonfile:
        dct = json.load(jsonfile)
    interface.load_arrows_dict(dct, always_create=['Analysis Value'])
    with open(os.path.join(filepath, 'data', 'raw', 'test_apply_stat_non_distinct.json')) as jsonfile:
        inline = json.load(jsonfile)
    method = derivation_method_factory(data=inline, interface=interface, study=study)

    action_metas = []
    for a in method.actions:
        if isinstance(a, SuperMethod):
            action_metas.extend([_a.meta for _a in a.actions])
        else:
            action_metas.append(a.meta)
    return action_metas


class TestMethodApplierApplyStat:
    def test_get_apply_stat_meta_len(self, apply_stat_meta):
        assert len(apply_stat_meta) == 9

    def test_get_apply_stat_meta2(self, apply_stat_meta):
        assert json.loads(apply_stat_meta[2].get('params'))["value_cols"] == ["AAGE"]
        assert set(json.loads(apply_stat_meta[2].get('params'))["by"]) == {"_id_POP", "POP"}
        assert set(json.loads(apply_stat_meta[2].get('params'))["agg"]) == {"n", "MEAN"}

    def test_get_apply_stat_meta3(self, apply_stat_meta):
        assert set(apply_stat_meta[3].get('uri_fors')) == {'n', 'MEAN'}
        assert apply_stat_meta[3].get('prefix') == 'AAGE'
        assert set(apply_stat_meta[3].get('uri_fors_long')) == {
            'Number of observations',
            'Mean Value of Analysis Parameter'
        }
        assert set(apply_stat_meta[3].get('uri_bys')) == {'POP'}
        assert set(apply_stat_meta[3].get('uri_bys_long')) == {'Population'}

    def test_get_apply_stat_meta4(self, apply_stat_meta):
        assert apply_stat_meta[4]['type'] == 'link_stat'
        assert len(apply_stat_meta[4]['dimensions']) == 1
        assert len(apply_stat_meta[4]['dimensions_long']) == 1

    def test_get_apply_stat_meta6(self, apply_stat_meta):
        assert set(json.loads(apply_stat_meta[6].get('params'))["by"]) == {"_id_POP", "POP", "_id_ASTA", "ASTA"}

    def test_get_apply_stat_meta7(self, apply_stat_meta):
        assert set(apply_stat_meta[7].get('uri_bys')) == {'POP', 'ASTA'}
        assert set(apply_stat_meta[7].get('uri_bys_long')) == {'Population', 'Analysis Act Stratum and Act Treatment'}

    def test_get_apply_stat_meta8(self, apply_stat_meta):
        assert len(apply_stat_meta[8]['dimensions']) == 2
        assert len(apply_stat_meta[8]['dimensions_long']) == 2
        
    # ------------------------------------------------------------------------------------------
        
    def test_get_apply_stat_non_distinct_meta_len(self, apply_stat_meta_non_distinct):
        assert len(apply_stat_meta_non_distinct) == 10

    def test_get_apply_stat_non_distinct_meta1(self, apply_stat_meta_non_distinct):
        run_cypher_meta = apply_stat_meta_non_distinct[1]
        assert run_cypher_meta.get('type') == 'run_cypher'
        assert json.loads(run_cypher_meta.get('params')) == {"class_labels": [{"short_label": "AVAL", "long_label": "Analysis Value"}, {"short_label": "POP", "long_label": "Population"}]}
        assert run_cypher_meta.get('include_data') == 'true'
        assert run_cypher_meta.get('update_df') == 'true'

    def test_get_apply_stat_non_distinct_meta3(self, apply_stat_meta_non_distinct):
        assert json.loads(apply_stat_meta_non_distinct[3].get('params'))["value_cols"] == []
        assert set(json.loads(apply_stat_meta_non_distinct[3].get('params'))["by"]) == {"_id_POP", "POP"}
        assert set(json.loads(apply_stat_meta_non_distinct[3].get('params'))["agg"]) == {"n", "MEAN"}

    def test_get_apply_stat_non_distinct_meta4(self, apply_stat_meta_non_distinct):
        assert set(apply_stat_meta_non_distinct[4].get('uri_fors')) == {'n', 'MEAN'}
        assert apply_stat_meta_non_distinct[4].get('prefix') == ''
        assert set(apply_stat_meta_non_distinct[4].get('uri_fors_long')) == {
            'Number of observations',
            'Mean Value of Analysis Parameter'
        }
        assert set(apply_stat_meta_non_distinct[4].get('uri_bys')) == {'POP'}
        assert set(apply_stat_meta_non_distinct[4].get('uri_bys_long')) == {'Population'}

    def test_get_apply_stat_non_distinct_meta5(self, apply_stat_meta_non_distinct):
        assert apply_stat_meta_non_distinct[5]['type'] == 'link_stat'
        assert len(apply_stat_meta_non_distinct[5]['dimensions']) == 1
        assert len(apply_stat_meta_non_distinct[5]['dimensions_long']) == 1

    def test_get_apply_stat_non_distinct_meta7(self, apply_stat_meta_non_distinct):
        assert set(json.loads(apply_stat_meta_non_distinct[7].get('params'))["by"]) == {"_id_POP", "POP", "_id_AVAL", "AVAL"}

    def test_get_apply_stat_non_distinct_meta8(self, apply_stat_meta_non_distinct):
        assert set(apply_stat_meta_non_distinct[8].get('uri_bys')) == {'POP', 'AVAL'}
        assert set(apply_stat_meta_non_distinct[8].get('uri_bys_long')) == {'Population', 'Analysis Value'}

    def test_get_apply_stat_non_distinct_meta9(self, apply_stat_meta_non_distinct):
        assert len(apply_stat_meta_non_distinct[9]['dimensions']) == 2
        assert len(apply_stat_meta_non_distinct[9]['dimensions_long']) == 2


@pytest.fixture
def decode_meta():
    interface = DataProvider(rdf=True)
    interface.clean_slate()
    load_schema_and_terms(interface)
    with open(os.path.join(filepath, 'data', 'raw', 'derive_test_decode.json')) as jsonfile:
        inline = json.load(jsonfile)
    method = derivation_method_factory(data=inline, interface=interface, study=study)

    action_metas = []
    for a in method.actions:
        if isinstance(a, SuperMethod):
            action_metas.extend([_a.meta for _a in a.actions])
        else:
            action_metas.append(a.meta)
    return action_metas


class TestMethodDecode:
    def test_get_decode_meta_len(self, decode_meta):
        assert len(decode_meta) == 4

    # get_data action
    def test_get_decode_meta0(self, decode_meta):
        assert not decode_meta[0].get('source_classes')
        assert not decode_meta[0].get('get_classes')
        assert len(decode_meta[0].get('source_rels')) == 1
        assert decode_meta[0].get('source_rels')[0]["short_label"] is None
        assert decode_meta[0].get('source_rels')[0]["optional"] is None
        assert decode_meta[0].get('source_rels')[0]["from"] == 'Subject'
        assert decode_meta[0].get('source_rels')[0]["to"] == 'Age Group'
        assert decode_meta[0].get('source_rels')[0]["type"] == 'Age Group'

    # decode_run_script action
    def test_get_decode_meta1(self, decode_meta):
        assert decode_meta[1].get('id') == 'decode1_run_script'
        assert decode_meta[1].get('type') == 'call_api'
        assert decode_meta[1].get('script') == 'remap_term_values'
        assert decode_meta[1].get('lang') == 'python'
        assert decode_meta[1].get('package') == 'basic_df_ops'
        print(json.loads(decode_meta[1].get('params')))
        assert json.loads(decode_meta[1].get('params')) == {
            "original_col": "AGEGR",
            "new_col": "AGEGRA",
            "term_pairs": [[">=85 Years", "age group 2"], ["65-84 Years", "age group 2"]]
        }

    # decode_link action
    def test_get_decode_meta2(self, decode_meta):
        assert decode_meta[2].get('id') == 'decode1_link'
        assert decode_meta[2].get('relationship_type') == 'SAME_AS'
        assert decode_meta[2].get('from_class') == 'Age Group'
        assert decode_meta[2].get('to_class') == 'Age Group A'
        assert decode_meta[2].get('from_short_label') == 'AGEGR'
        assert decode_meta[2].get('to_short_label') == 'AGEGRA'
        assert decode_meta[2].get('to_class_property') == 'rdfs:label'
        assert decode_meta[2].get('merge')


@pytest.fixture
def subject_level_link_meta():
    interface = DataProvider(rdf=True)
    interface.clean_slate()
    load_subject_level_data()
    with open(os.path.join(filepath, 'data', 'raw', 'test_subject_level_link.json')) as jsonfile:
        inline = json.load(jsonfile)
    method = derivation_method_factory(data=inline, interface=interface, study=study)

    action_metas = []
    for a in method.actions:
        if isinstance(a, SuperMethod):
            action_metas.extend([_a.meta for _a in a.actions])
        else:
            action_metas.append(a.meta)
    return action_metas


@pytest.fixture
def subject_level_link_method():
    interface = DataProvider(rdf=True)
    interface.clean_slate()
    load_subject_level_data()
    with open(os.path.join(filepath, 'data', 'raw', 'test_subject_level_link.json')) as jsonfile:
        inline = json.load(jsonfile)
    method = derivation_method_factory(data=inline, interface=interface, study=study)
    return method


class TestMethodSubjectLevelLink:
    def test_get_subject_level_link_meta_len(self, subject_level_link_meta):
        assert len(subject_level_link_meta) == 6

    # AssignLabel action
    def test_get_subject_level_link_meta1(self, subject_level_link_meta):
        assert subject_level_link_meta[1].get('id') == 'subject_level_link1_assign_class'
        assert subject_level_link_meta[1].get('type') == 'assign_class'
        assert subject_level_link_meta[1].get('on_class_short_label') == 'AGE'
        assert subject_level_link_meta[1].get('on_class') == 'Age'
        assert subject_level_link_meta[1].get('assign_short_label') == 'AVAL'
        assert subject_level_link_meta[1].get('assign_label') == 'Analysis Value'

    # BuildURI action
    def test_get_subject_level_link_meta2(self, subject_level_link_meta):
        assert subject_level_link_meta[2].get('id') == 'subject_level_link1_build_uri'
        assert subject_level_link_meta[2].get('type') == 'build_uri'
        assert subject_level_link_meta[2].get('prefix') == 'Subject_level_Age_'
        assert subject_level_link_meta[2].get('uri_fors') == ['RECORD']
        assert subject_level_link_meta[2].get('uri_fors_long') == ['Record']
        assert subject_level_link_meta[2].get('uri_bys') == ['USUBJID']
        assert subject_level_link_meta[2].get('uri_bys_long') == ['Subject']
        assert subject_level_link_meta[2].get('uri_labels') == []
        assert subject_level_link_meta[2].get('uri_labels_long') == []

    # Link USUBJID -> RECORD action
    def test_get_subject_level_link_meta3(self, subject_level_link_meta):
        assert subject_level_link_meta[3].get('id') == 'subject_level_link1_record_link'
        assert subject_level_link_meta[3].get('type') == 'link'
        assert subject_level_link_meta[3].get('relationship_type') == 'Record'
        assert subject_level_link_meta[3].get('from_class') == 'Subject'
        assert subject_level_link_meta[3].get('from_short_label') == 'USUBJID'
        assert subject_level_link_meta[3].get('to_class') == 'Record'
        assert subject_level_link_meta[3].get('to_short_label') == 'RECORD'
        assert subject_level_link_meta[3].get('to_class_property') == 'rdfs:label'
        assert subject_level_link_meta[3].get('use_uri') == True

    # Link RECORD -> PARAM action
    def test_get_subject_level_link_meta4(self, subject_level_link_meta):
        assert subject_level_link_meta[4].get('id') == 'subject_level_link1_param_link'
        assert subject_level_link_meta[4].get('type') == 'link'
        assert subject_level_link_meta[4].get('relationship_type') == 'Parameter'
        assert subject_level_link_meta[4].get('from_class') == 'Record'
        assert subject_level_link_meta[4].get('from_short_label') == 'RECORD'
        assert subject_level_link_meta[4].get('to_class') == 'Parameter'
        assert subject_level_link_meta[4].get('to_short_label') == 'PARAM'
        assert subject_level_link_meta[4].get('to_class_property') == 'rdfs:label'
        assert subject_level_link_meta[4].get('to_value') == 'Age'
        assert subject_level_link_meta[4].get('merge') == True

    # Link RECORD -> AVAL action
    def test_get_subject_level_link_meta5(self, subject_level_link_meta):
        assert subject_level_link_meta[5].get('id') == 'subject_level_link1_record_aval_link'
        assert subject_level_link_meta[5].get('type') == 'link'
        assert subject_level_link_meta[5].get('relationship_type') == 'Analysis Value'
        assert subject_level_link_meta[5].get('from_class') == 'Record'
        assert subject_level_link_meta[5].get('from_short_label') == 'RECORD'
        assert subject_level_link_meta[5].get('to_class') == 'Analysis Value'
        assert subject_level_link_meta[5].get('to_short_label') == 'AVAL'

    def test_subject_level_link_apply(self, subject_level_link_method):

        subject_level_link_method.apply()
        
        interface = DataProvider(rdf=True)

        result = interface.query(
            """
            MATCH (analysis_value:`Analysis Value`)<-[:`Analysis Value`]-(record:Record)-[:Parameter]->(parameter:Parameter)
            WHERE analysis_value.`rdfs:label` = 40
            RETURN labels(analysis_value) as aval_labels, labels(parameter) as param_labels
            """
        )
        assert result
        print(result)
        assert 'Age' in result[0]['aval_labels']
        assert 'Term' in result[0]['param_labels']