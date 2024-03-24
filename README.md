--------------------------------------------------------------------------
TypeError                                 Traceback (most recent call last)
/tmp/ipykernel_369/1347954805.py in <cell line: 41>()
     39 
     40 # Remove rows with value 999 in any column
---> 41 EDWT_Indicators = EDWT_Indicators[~EDWT_Indicators.eq(999).any(1)]
     42 
     43 # Define a function to generate data for a specific year

TypeError: NDFrame._add_numeric_operations.<locals>.any() takes 1 positional argument but 2 were given
