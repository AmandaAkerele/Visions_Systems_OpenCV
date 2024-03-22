---------------------------------------------------------------------------
AssertionError                            Traceback (most recent call last)
~/.local/lib/python3.10/site-packages/pandas/core/internals/construction.py in _finalize_columns_and_data(content, columns, dtype)
    933     try:
--> 934         columns = _validate_or_indexify_columns(contents, columns)
    935     except AssertionError as err:

~/.local/lib/python3.10/site-packages/pandas/core/internals/construction.py in _validate_or_indexify_columns(content, columns)
    980             # caller's responsibility to check for this...
--> 981             raise AssertionError(
    982                 f"{len(columns)} columns passed, passed data had "

AssertionError: 5 columns passed, passed data had 4 columns

The above exception was the direct cause of the following exception:

ValueError                                Traceback (most recent call last)
/tmp/ipykernel_369/1790367679.py in <cell line: 83>()
     81     return stacked_df
     82 
---> 83 all_years_data = pd.concat([generate_data_for_year(year) for year in range(18, 23)])

/tmp/ipykernel_369/1790367679.py in <listcomp>(.0)
     81     return stacked_df
     82 
---> 83 all_years_data = pd.concat([generate_data_for_year(year) for year in range(18, 23)])

/tmp/ipykernel_369/1790367679.py in generate_data_for_year(year)
     62         stacked_data.append([row['reporting_entity_code'], row['metric_result'], 'PerformanceComparison', row['compare_descriptor_code']])
     63 
---> 64     stacked_df = pd.DataFrame(stacked_data, columns=['reporting_entity_code', 'metric_result', 'metric_descriptor_group_code', 'metric_descriptor_code', 'missing_reason_code'])
     65 
     66     stacked_df['reporting_period_code'] = 'FY20' + str(year)

~/.local/lib/python3.10/site-packages/pandas/core/frame.py in __init__(self, data, index, columns, dtype, copy)
    780                     if columns is not None:
    781                         columns = ensure_index(columns)
--> 782                     arrays, columns, index = nested_data_to_arrays(
    783                         # error: Argument 3 to "nested_data_to_arrays" has incompatible
    784                         # type "Optional[Collection[Any]]"; expected "Optional[Index]"

~/.local/lib/python3.10/site-packages/pandas/core/internals/construction.py in nested_data_to_arrays(data, columns, index, dtype)
    496         columns = ensure_index(data[0]._fields)
    497 
--> 498     arrays, columns = to_arrays(data, columns, dtype=dtype)
    499     columns = ensure_index(columns)
    500 

~/.local/lib/python3.10/site-packages/pandas/core/internals/construction.py in to_arrays(data, columns, dtype)
    838         arr = _list_to_arrays(data)
    839 
--> 840     content, columns = _finalize_columns_and_data(arr, columns, dtype)
    841     return content, columns
    842 

~/.local/lib/python3.10/site-packages/pandas/core/internals/construction.py in _finalize_columns_and_data(content, columns, dtype)
    935     except AssertionError as err:
    936         # GH#26429 do not raise user-facing AssertionError
--> 937         raise ValueError(err) from err
    938 
    939     if len(contents) and contents[0].dtype == np.object_:

ValueError: 5 columns passed, passed data had 4 columns
