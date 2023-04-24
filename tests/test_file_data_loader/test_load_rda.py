import pytest
import os
import pandas as pd
from data_loaders import file_data_loader
filepath = os.path.dirname(__file__)
dummy_data_folder = os.path.join(filepath, '..', '..', 'dummy_data')

def test_debug_verbose_attr_inherited():
    dl = file_data_loader.FileDataLoader(verbose=False, debug=True)
    assert dl.verbose == False
    #assert dl.debug == True

def test_load_rda_data():
    dl = file_data_loader.FileDataLoader()       # Rely on default username/pass
    try:
        df = dl.load_file(filename="adsl.rda", folder=dummy_data_folder, load_to_neo=False)
    except:
        df = None

    assert not df.empty

def test_read_file():
    dl = file_data_loader.FileDataLoader(verbose=False, debug=True)
    df, meta = dl.read_file(filename="adsl.rda", folder=dummy_data_folder, query="SEX == 'F'")
    assert not df.empty
    assert set(df['SEX']) == {'F'}


test_convert_datetime_columns_data = [
    (  # test_input0
        {'df': pd.DataFrame(
            {
                'int_values': [2, 1, 3],
                'str_values': ['abc', 'def', 'ghi'],
                'col1DTM': [1700000000, 1800000000, 1900000000],
                "col2DTM": [1800000000.5, -10000, 0],
                'col1DT': [17000, 18000, 19000],
                "col2DT": [18000.5, -1000, 0]
            }
        ),
            'date_format': 'sas'},
        pd.DataFrame({  # expected0
            'int_values': [2, 1, 3],
            'str_values': ['abc', 'def', 'ghi'],
            'col1DTM': [pd.Timestamp(year=2013, month=11, day=13, hour=22, minute=13, second=20),
                        pd.Timestamp(year=2017, month=1, day=14, hour=8),
                        pd.Timestamp(year=2020, month=3, day=16, hour=17, minute=46, second=40)],
            "col2DTM": [pd.Timestamp(year=2017, month=1, day=14, hour=8, microsecond=500000),
                        pd.Timestamp(year=1959, month=12, day=31, hour=21, minute=13, second=20),
                        pd.Timestamp(year=1960, month=1, day=1)],
            "col1DT": [pd.Timestamp(year=2006, month=7, day=18),
                       pd.Timestamp(year=2009, month=4, day=13),
                       pd.Timestamp(year=2012, month=1, day=8)],
            "col2DT": [pd.Timestamp(year=2009, month=4, day=13, hour=12),
                       pd.Timestamp(year=1957, month=4, day=6),
                       pd.Timestamp(year=1960, month=1, day=1)]
        })
     ),
    (  # test_input1
        {'df': pd.DataFrame(
            {
                'int_values': [2, 1, 3],
                'str_values': ['abc', 'def', 'ghi'],
                'col1DTM': [1700000000, 1800000000, 1900000000],
                "col2DTM": [1800000000.5, -10000, 0],
                'col1DT': [17000, 18000, 19000],
                "col2DT": [18000.5, -1000, 0]
            }
        ),
            'date_format': 'UNIX'},
        pd.DataFrame({  # expected1
            'int_values': [2, 1, 3],
            'str_values': ['abc', 'def', 'ghi'],
            'col1DTM': [pd.Timestamp(year=2023, month=11, day=14, hour=22, minute=13, second=20),
                        pd.Timestamp(year=2027, month=1, day=15, hour=8),
                        pd.Timestamp(year=2030, month=3, day=17, hour=17, minute=46, second=40)],
            "col2DTM": [pd.Timestamp(year=2027, month=1, day=15, hour=8, microsecond=500000),
                        pd.Timestamp(year=1969, month=12, day=31, hour=21, minute=13, second=20),
                        pd.Timestamp(year=1970, month=1, day=1)],
            "col1DT": [pd.Timestamp(year=2016, month=7, day=18),
                       pd.Timestamp(year=2019, month=4, day=14),
                       pd.Timestamp(year=2022, month=1, day=8)],
            "col2DT": [pd.Timestamp(year=2019, month=4, day=14, hour=12),
                       pd.Timestamp(year=1967, month=4, day=7),
                       pd.Timestamp(year=1970, month=1, day=1)]
        })
    ),
    (  # test_input2
        {'df': pd.DataFrame(
            {
                'int_values': [2, 1, 3],
                'str_values': ['abc', 'def', 'ghi'],
                'col1DTM': [1700000000, 1800000000, 1900000000],
                "col2DTM": [1800000000.5, -10000, 0],
                'col1DT': [17000, 18000, 19000],
                "col2DT": [18000.5, -1000, 0]
            }
        ),
            'date_format': 'sas',
            'datetime_col_regex': '^.non matching regex',
            'date_col_regex': '^.non matching regex'},
        pd.DataFrame(  # expected2
            {
                'int_values': [2, 1, 3],
                'str_values': ['abc', 'def', 'ghi'],
                'col1DTM': [1700000000, 1800000000, 1900000000],
                "col2DTM": [1800000000.5, -10000, 0],
                'col1DT': [17000, 18000, 19000],
                "col2DT": [18000.5, -1000, 0]
            }
        )
     )
]


@pytest.mark.parametrize('test_input, expected', test_convert_datetime_columns_data)
def test_convert_datetime_columns(test_input, expected):
    dl = file_data_loader.FileDataLoader()
    pd.testing.assert_frame_equal(dl.convert_datetime_columns(**test_input), expected)
