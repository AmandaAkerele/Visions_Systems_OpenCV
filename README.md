---------------------------------------------------------------------------
KeyError                                  Traceback (most recent call last)
/tmp/ipykernel_1073/3384990878.py in <cell line: 106>()
    104 
    105 # Map metric_descriptor_group_code to metric_descriptor_code
--> 106 EDWT_Indicator_File['metric_descriptor_code'] = EDWT_Indicator_File['metric_descriptor_group_code'].map(lambda x: performance_trend_mapping[x] if x in performance_trend_mapping else performance_comparison_mapping[x])
    107 
    108 # Drop rows where metric_descriptor_code is 999 for both PerformanceTrend and PerformanceComparison

~/.local/lib/python3.10/site-packages/pandas/core/series.py in map(self, arg, na_action)
   4395         dtype: object
   4396         """
-> 4397         new_values = self._map_values(arg, na_action=na_action)
   4398         return self._constructor(new_values, index=self.index, copy=False).__finalize__(
   4399             self, method="map"

~/.local/lib/python3.10/site-packages/pandas/core/base.py in _map_values(self, mapper, na_action)
    922 
    923         # mapper is a function
--> 924         new_values = map_f(values, mapper)
    925 
    926         return new_values

~/.local/lib/python3.10/site-packages/pandas/_libs/lib.pyx in pandas._libs.lib.map_infer()

/tmp/ipykernel_1073/3384990878.py in <lambda>(x)
    104 
    105 # Map metric_descriptor_group_code to metric_descriptor_code
--> 106 EDWT_Indicator_File['metric_descriptor_code'] = EDWT_Indicator_File['metric_descriptor_group_code'].map(lambda x: performance_trend_mapping[x] if x in performance_trend_mapping else performance_comparison_mapping[x])
    107 
    108 # Drop rows where metric_descriptor_code is 999 for both PerformanceTrend and PerformanceComparison

KeyError: 999
