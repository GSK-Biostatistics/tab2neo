import pytest
import pandas as pd
import numpy as np
from datetime import date, datetime
from derivation_method import action

# TODO: relocate these tests to other action unit tests

sample_date = date(2022, 1, 1)
converted_sample_date = datetime(2022, 1, 1, 0, 0, 0)
sample_datetime = datetime(2022, 1, 1, 1, 1, 1)

types_df_with_dates = pd.DataFrame(
        [[sample_date, sample_datetime, sample_date, 2, 2.3, 'string', '2022-08-08',
          '2022-08-08T08:45', '2022-08-08', '0001234', False, None, np.nan, 'string'],
         [sample_date, sample_datetime, sample_datetime, 2, 2.3, 'string', '2022-08-08',
          '2022-08-08T08:45', '2022-08-08T08:45', '1234567', True, None, np.nan, None]],
        columns=["date", "datetime", "date_datetime_mix", "int", "float", "str", "str_date",
                 "str_datetime", "str_date_datetime_mix", "str_int", "bool", "none_col", "nan", "str_and_none"]
    )

types_df_without_dates = pd.DataFrame(
        [[2, 2.3, 'string', '2022-08-08', '2022-08-08T08:45', '2022-08-08', '0001234', False, None, np.nan, 'string'],
         [2, 2.3, 'string', '2022-08-08', '2022-08-08T08:45', '2022-08-08T08:45', '1234567', True, None, np.nan, None]],
        columns=["int", "float", "str", "str_date", "str_datetime", "str_date_datetime_mix",
                 "str_int", "bool", "none_col", "nan", "str_and_none"]
    )

test_determine_data_types_data = [
    (types_df_with_dates,
     {"date": 'date', "datetime": "datetime64[ns]", "date_datetime_mix": "datetime64[ns]", "int": "int64",
      "float": "float64", "str": "string", "str_date": "string", "str_datetime": "string",
      "str_date_datetime_mix": "string", "str_int": "string", "bool": "bool", "none_col": str(None),
      "nan": "float64", "str_and_none": "string"}),
    (types_df_without_dates,
     {"int": "int64", "float": "float64", "str": "string", "str_date": "string",
      "str_datetime": "string", "str_date_datetime_mix": "string", "str_int": "string",
      "bool": "bool", "none_col": str(None), "nan": "float64", "str_and_none": "string"})
]

test_load_json_dataframe_data = [
    # Test 1 - load with dates
    (types_df_with_dates.to_json(orient="records"),
     {"date": 'date', "datetime": "datetime64[ns]", "date_datetime_mix": "datetime64[ns]", "int": "int64",
      "float": "float64", "str": "string", "str_date": "string", "str_datetime": "string",
      "str_date_datetime_mix": "string", "str_int": "string", "bool": "bool", "none_col": "float64",
      "nan": "float64", "str_and_none": "string"},
     # Nones converted to nans in numeric columns and sample_date in date_datetime_mix column converted to datetime
     pd.DataFrame(
         [[sample_date, sample_datetime, converted_sample_date, 2, 2.3, 'string', '2022-08-08',
           '2022-08-08T08:45', '2022-08-08', '0001234', False, np.nan, np.nan, 'string'],
          [sample_date, sample_datetime, sample_datetime, 2, 2.3, 'string', '2022-08-08',
           '2022-08-08T08:45', '2022-08-08T08:45', '1234567', True, np.nan, np.nan, None]],
         columns=["date", "datetime", "date_datetime_mix", "int", "float", "str", "str_date",
                  "str_datetime", "str_date_datetime_mix", "str_int", "bool", "none_col", "nan", "str_and_none"]
     )),
    # Test 2 - load without dates
    (types_df_without_dates.to_json(orient="records"),
     {"int": "int64", "float": "float64", "str": "string", "str_date": "string",
      "str_datetime": "string", "str_date_datetime_mix": "string", "str_int": "string",
      "bool": "bool", "none_col": "float64", "nan": "float64", "str_and_none": "string"},
     # Nones converted to nans in numeric columns
     pd.DataFrame(
         [[2, 2.3, 'string', '2022-08-08', '2022-08-08T08:45', '2022-08-08',
           '0001234', False, np.nan, np.nan, 'string'],
          [2, 2.3, 'string', '2022-08-08', '2022-08-08T08:45', '2022-08-08T08:45',
           '1234567', True, np.nan, np.nan, None]],
         columns=["int", "float", "str", "str_date", "str_datetime", "str_date_datetime_mix",
                  "str_int", "bool", "none_col", "nan", "str_and_none"]
     )),
    # Test 3 - load with conversion
    # int -> float, string_int -> int, float -> int
    (types_df_without_dates.to_json(orient="records"),
     {"int": "float64", "float": "int64", "str": "string", "str_date": "string",
      "str_datetime": "string", "str_date_datetime_mix": "string", "str_int": "int64",
      "bool": "bool", "none_col": "float64", "nan": "float64", "str_and_none": "string"},
     pd.DataFrame(
         [[2.0, 2, 'string', '2022-08-08', '2022-08-08T08:45', '2022-08-08', 1234, False, np.nan, np.nan, 'string'],
          [2.0, 2, 'string', '2022-08-08', '2022-08-08T08:45', '2022-08-08T08:45', 1234567, True, np.nan, np.nan, None]],
         columns=["int", "float", "str", "str_date", "str_datetime", "str_date_datetime_mix",
                  "str_int", "bool", "none_col", "nan", "str_and_none"]
     ))
]


@pytest.mark.parametrize('df, dtypes', test_determine_data_types_data)
def test_determine_data_types(df, dtypes):
    found_dtypes = action.CallAPI.determine_data_types(df)
    assert found_dtypes == dtypes


@pytest.mark.parametrize('json_df, dtypes, expected_df', test_load_json_dataframe_data)
def test_load_json_dataframe(json_df, dtypes, expected_df):
    loaded_df = action.CallAPI.load_json_dataframe(json_df, dtypes)
    pd.testing.assert_frame_equal(loaded_df, expected_df)
