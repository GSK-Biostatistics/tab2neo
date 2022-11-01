import os
filepath = os.path.dirname(__file__)
import pytest
from model_managers.model_manager import ModelManager
from data_loaders.file_data_loader import FileDataLoader
from model_appliers.model_applier import ModelApplier
from data_providers.data_provider import DataProvider
import pandas as pd

# Provide a DataProvider object (which contains a database connection)
# that can be used by the various tests that need it
@pytest.fixture(scope="module")
def mm():
    mm = ModelManager(verbose=False)
    yield mm

@pytest.fixture(scope="module")
def dl():
    dl = FileDataLoader(verbose=False)
    dl.clean_slate()
    yield dl

@pytest.fixture(scope="module")
def ma():
    ma = ModelApplier(verbose=False, mode="schema_CLASS")
    yield ma

@pytest.fixture(scope="module")
def dp():
    dp = DataProvider(verbose=False)
    yield dp

def test_create_model_from_data(dl, mm, ma, dp):
    df = dl.load_file(folder="tests/tests_model_manager/data", filename="create_model_from_data.csv")
    mm.create_model_from_data()
    ma.refactor_all()
    df_out, q, params= dp.get_data_cld(
        labels=[
                   col for col in df.columns
                   if col not in ['_domain_', '_filename_', '_folder_']
               ] + ['CREATE_MODEL_FROM_DATA'],
        #infer_rels=True,
        return_nodeid=False,
        #only_props=['rdfs:label'],

    )
    df_out.drop('CREATE_MODEL_FROM_DATA', axis=1, inplace=True)
    df_out.sort_values(by="A", inplace=True, ignore_index=True)
    df = df[list(df_out.columns)]
    pd.testing.assert_frame_equal(df_out, df)

