---------------------------------------------------------------------------
KeyError                                  Traceback (most recent call last)
~/.local/lib/python3.10/site-packages/pandas/core/indexes/base.py in get_loc(self, key)
   3651         try:
-> 3652             return self._engine.get_loc(casted_key)
   3653         except KeyError as err:

~/.local/lib/python3.10/site-packages/pandas/_libs/index.pyx in pandas._libs.index.IndexEngine.get_loc()

~/.local/lib/python3.10/site-packages/pandas/_libs/index.pyx in pandas._libs.index.IndexEngine.get_loc()

pandas/_libs/hashtable_class_helper.pxi in pandas._libs.hashtable.PyObjectHashTable.get_item()

pandas/_libs/hashtable_class_helper.pxi in pandas._libs.hashtable.PyObjectHashTable.get_item()

KeyError: 'reporting_period_code'

The above exception was the direct cause of the following exception:

KeyError                                  Traceback (most recent call last)
/tmp/ipykernel_492/2300969136.py in <cell line: 96>()
     94 
     95 # Generate data for each year from FY2018 to FY2022
---> 96 all_years_data = pd.concat([generate_data_for_year(year) for year in range(18, 23)])
     97 
     98 # Write to CSV

/tmp/ipykernel_492/2300969136.py in <listcomp>(.0)
     94 
     95 # Generate data for each year from FY2018 to FY2022
---> 96 all_years_data = pd.concat([generate_data_for_year(year) for year in range(18, 23)])
     97 
     98 # Write to CSV

/tmp/ipykernel_492/2300969136.py in generate_data_for_year(year)
     76 
     77     # Map reporting_period_code
---> 78     stacked_df['reporting_period_code'] = 'FY20' + stacked_df['reporting_period_code'].astype(str)
     79 
     80     stacked_df['reporting_entity_type_code'] = 'ORG'

~/.local/lib/python3.10/site-packages/pandas/core/frame.py in __getitem__(self, key)
   3759             if self.columns.nlevels > 1:
   3760                 return self._getitem_multilevel(key)
-> 3761             indexer = self.columns.get_loc(key)
   3762             if is_integer(indexer):
   3763                 indexer = [indexer]

~/.local/lib/python3.10/site-packages/pandas/core/indexes/base.py in get_loc(self, key)
   3652             return self._engine.get_loc(casted_key)
   3653         except KeyError as err:
-> 3654             raise KeyError(key) from err
   3655         except TypeError:
   3656             # If we have a listlike key, _check_indexing_error will raise

KeyError: 'reporting_period_code'
