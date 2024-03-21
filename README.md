---------------------------------------------------------------------------
ValueError                                Traceback (most recent call last)
/tmp/ipykernel_1073/2253745321.py in <cell line: 29>()
     27 
     28 # Map IMPROVEMENT_IND_CODE to improvement_mapping
---> 29 EDWT_Indicator_File['metric_descriptor_code'] = EDWT_Indicator_File['metric_descriptor_group_code'].replace(improvement_mapping)
     30 
     31 # Map COMPARE_IND_CODE to compare_mapping

~/.local/lib/python3.10/site-packages/pandas/core/frame.py in __setitem__(self, key, value)
   3938             self._setitem_array(key, value)
   3939         elif isinstance(value, DataFrame):
-> 3940             self._set_item_frame_value(key, value)
   3941         elif (
   3942             is_list_like(value)

~/.local/lib/python3.10/site-packages/pandas/core/frame.py in _set_item_frame_value(self, key, value)
   4092 
   4093         if len(value.columns) != 1:
-> 4094             raise ValueError(
   4095                 "Cannot set a DataFrame with multiple columns to the single "
   4096                 f"column {key}"

ValueError: Cannot set a DataFrame with multiple columns to the single column metric_descriptor_code
