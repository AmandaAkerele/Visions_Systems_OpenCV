ValueError                                Traceback (most recent call last)
/tmp/ipykernel_1073/350955085.py in <cell line: 6>()
      5 
      6 for ind in EDWT_Indicator_File.index:
----> 7     EDWT_Indicator_File["metric_result"][ind] = round_half_up((EDWT_Indicator_File["metric_result"][ind]), 1)
      8 
      9 EDWT_Indicator_File['reporting_period_code'] = 'FY20' + yr

/tmp/ipykernel_1073/2840964500.py in round_half_up(n, decimals)
      5         value_2 = math.ceil(value_1)
      6     else:
----> 7         value_2 = math.floor(value_1)
      8     return (value_2 / multiplier)

ValueError: cannot convert float NaN to integer
