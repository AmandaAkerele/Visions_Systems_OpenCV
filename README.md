KeyError: "['public_metric_result'] not in index"

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

KeyError: 'public_metric_result'

The above exception was the direct cause of the following exception:

KeyError                                  Traceback (most recent call last)
/tmp/ipykernel_280/1692326656.py in <cell line: 24>()
     22 IP_admit_ED_CORP_All_File['missing_reason_code'] = ''
     23 
---> 24 nan_rows = IP_admit_ED_CORP_All_File[IP_admit_ED_CORP_All_File['metric_result'].isna() | IP_admit_ED_CORP_All_File['public_metric_result'].isna()]
     25 nan_indices = nan_rows.index
     26 IP_admit_ED_CORP_All_File.loc[nan_indices, 'missing_reason_code'] = 'M02'

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

KeyError: 'public_metric_result'
