--------------------------------------------------------------------------
KeyError                                  Traceback (most recent call last)
~/.local/lib/python3.10/site-packages/pandas/core/indexes/base.py in get_loc(self, key)
   3651         try:
-> 3652             return self._engine.get_loc(casted_key)
   3653         except KeyError as err:

~/.local/lib/python3.10/site-packages/pandas/_libs/index.pyx in pandas._libs.index.IndexEngine.get_loc()

~/.local/lib/python3.10/site-packages/pandas/_libs/index.pyx in pandas._libs.index.IndexEngine.get_loc()

pandas/_libs/hashtable_class_helper.pxi in pandas._libs.hashtable.PyObjectHashTable.get_item()

pandas/_libs/hashtable_class_helper.pxi in pandas._libs.hashtable.PyObjectHashTable.get_item()

KeyError: 'missing_reason_code'

The above exception was the direct cause of the following exception:

KeyError                                  Traceback (most recent call last)
/tmp/ipykernel_296/3733936803.py in <cell line: 85>()
     83 
     84 # Generate data for each year from FY2018 to FY2022
---> 85 all_years_data = pd.concat([generate_data_for_year(year) for year in range(18, 23)])
     86 
     87 # Write to CSV

/tmp/ipykernel_296/3733936803.py in <listcomp>(.0)
     83 
     84 # Generate data for each year from FY2018 to FY2022
---> 85 all_years_data = pd.concat([generate_data_for_year(year) for year in range(18, 23)])
     86 
     87 # Write to CSV

/tmp/ipykernel_296/3733936803.py in generate_data_for_year(year)
     53     for index, row in EDWT_Indicator_File.iterrows():
     54         # For Row 1
---> 55         if row['missing_reason_code'] != 999:
     56             stacked_data.append([row['reporting_entity_code'], row['metric_result'], '', '', row['missing_reason_code'], ''])
     57 

~/.local/lib/python3.10/site-packages/pandas/core/series.py in __getitem__(self, key)
   1005 
   1006         elif key_is_scalar:
-> 1007             return self._get_value(key)
   1008 
   1009         if is_hashable(key):

~/.local/lib/python3.10/site-packages/pandas/core/series.py in _get_value(self, label, takeable)
   1114 
   1115         # Similar to Index.get_value, but we do not fall back to positional
-> 1116         loc = self.index.get_loc(label)
   1117 
   1118         if is_integer(loc):

~/.local/lib/python3.10/site-packages/pandas/core/indexes/base.py in get_loc(self, key)
   3652             return self._engine.get_loc(casted_key)
   3653         except KeyError as err:
-> 3654             raise KeyError(key) from err
   3655         except TypeError:
   3656             # If we have a listlike key, _check_indexing_error will raise

KeyError: 'missing_reason_code'
