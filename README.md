correct this section of the code only

---------------------------------------------------------------------------
TypeError                                 Traceback (most recent call last)
/tmp/ipykernel_6045/2160161953.py in <cell line: 39>()
    146         return still_birth_flag
    147 
--> 148     stillbirth_flag = find_flag(stillbirth_code)
    149     nacrs_ed_1 = nacrs_ed.withColumn('stillbirth_flags', when(stillbirth_flag, 1).otherwise(0))
    150 

/tmp/ipykernel_6045/2160161953.py in find_flag(codes)
    143         if not problem_col:
    144             return lit(False)  # No problem columns, return a false literal
--> 145         still_birth_flag = functools.reduce(lambda a, b: a | b, [col(column_name).rtrim() == codes for column_name in problem_col])
    146         return still_birth_flag
    147 

/tmp/ipykernel_6045/2160161953.py in <listcomp>(.0)
    143         if not problem_col:
    144             return lit(False)  # No problem columns, return a false literal
--> 145         still_birth_flag = functools.reduce(lambda a, b: a | b, [col(column_name).rtrim() == codes for column_name in problem_col])
    146         return still_birth_flag
    147 

TypeError: 'Column' object is not callable

