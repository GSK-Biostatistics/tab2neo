import pytest
import pandas as pd
from data_loaders import file_data_loader


@pytest.fixture(scope="module")
def fdl():
    fdl = file_data_loader.FileDataLoader(autoconnect=True)
    yield fdl


def test_load_of_xlsx(fdl):
    fdl.clean_slate()
    for tab in ["First", "Second"]:
        df = fdl.load_file("tests/test_file_data_loader/data", "test.xlsx", tab)    
        df_res = fdl.query(f"MATCH (f:`Source Data Row`) WHERE f._domain_ = 'TEST.{tab.upper()}' RETURN f", return_type="pd")
        df_res.columns = [col[2:] for col in df_res.columns]
        df_res = df_res[list(df.columns)]        
        assert df.equals(df_res)

