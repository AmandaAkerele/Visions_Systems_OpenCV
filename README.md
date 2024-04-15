---------------------------------------------------------------------------
TypeError                                 Traceback (most recent call last)
/tmp/ipykernel_3755/1526969618.py in <cell line: 19>()
     17 all_results = []
     18 
---> 19 for group_name, group_data in grouped:
     20     X = sm.add_constant(group_data.select('TIME').rdd.flatMap(lambda x: x).collect())
     21     y = group_data.select('PERCENTILE_90').rdd.flatMap(lambda x: x).collect()

TypeError: 'GroupedData' object is not iterable

SLOVE ERROR 
