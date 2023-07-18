import pytest
import pandas as pd
from data_loaders import file_data_loader


@pytest.fixture(scope="module")
def fdl():
    fdl = file_data_loader.FileDataLoader(autoconnect=False)
    yield fdl


def test_load_of_parquet(fdl):
    df, meta = fdl.read_file('tests/test_file_data_loader/data', 'test.parquet')
    assert meta == {'column_names': ['test col1', 'test col in quotes']}
    expected_df = pd.DataFrame([["ABC D", "TRRRDFF"], ["11", "22"]], columns=["test col1", "test col in quotes"])
    assert df.equals(expected_df)