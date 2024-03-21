KeyError                                  Traceback (most recent call last)
/tmp/ipykernel_1073/2947733991.py in <cell line: 19>()
     17 # Create file for shallow slice pilot
     18 # Indicator: Emergency Department Wait Time for Physician Initial Assessment (90% Spent Less, in Hours)
---> 19 EDWT_Indicator_File = EDWT_Indicators[["ORGANIZATION_ID",  "INDICATOR_VALUE", "IMPROVEMENT_IND_CODE", "COMPARE_IND_CODE", "PerformanceTrend", "PerformanceComparison"]]
     20 EDWT_Indicator_File.rename(columns={"ORGANIZATION_ID": "reporting_entity_code", "INDICATOR_VALUE": "metric_result", "IMPROVEMENT_IND_CODE": "improvement_code", "COMPARE_IND_CODE": "compare_code"}, inplace=True)
     21 

~/.local/lib/python3.10/site-packages/pandas/core/frame.py in __getitem__(self, key)
   3765             if is_iterator(key):
   3766                 key = list(key)
-> 3767             indexer = self.columns._get_indexer_strict(key, "columns")[1]
   3768 
   3769         # take() does not accept boolean indexers

~/.local/lib/python3.10/site-packages/pandas/core/indexes/base.py in _get_indexer_strict(self, key, axis_name)
   5874             keyarr, indexer, new_indexer = self._reindex_non_unique(keyarr)
   5875 
-> 5876         self._raise_if_missing(keyarr, indexer, axis_name)
   5877 
   5878         keyarr = self.take(indexer)

~/.local/lib/python3.10/site-packages/pandas/core/indexes/base.py in _raise_if_missing(self, key, indexer, axis_name)
   5936 
   5937             not_found = list(ensure_index(key)[missing_mask.nonzero()[0]].unique())
-> 5938             raise KeyError(f"{not_found} not in index")
   5939 
   5940     @overload

KeyError: "['PerformanceTrend', 'PerformanceComparison'] not in index"
