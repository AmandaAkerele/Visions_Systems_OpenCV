from pyspark.sql.functions import col, rtrim, when 
import functools

# define filter for still birth
stillbirth_code = ('P95')
problem_col = [col for col in df_nacrs.columns if col.startswith('problem_')]

def find_flag(codes):
    still_birth_flag = functools.reduce(lambda a, b: a | b, [rtrim(col(column_name)) == codes for column_name in problem_col])
    return still_birth_flag

stillbirth_flag = find_flag(stillbirth_code)
nacrs_ed_1 = nacrs_ed.withColumn('stillbirth_flags', when(stillbirth_flag, 1).otherwise(0))

stillbirth = nacrs_ed_1.filter('stillbirth_flags == 1').select("AM_CARE_KEY")

solve the error below:
---------------------------------------------------------------------------
TypeError                                 Traceback (most recent call last)
/tmp/ipykernel_7034/3428343671.py in <cell line: 12>()
     10     return still_birth_flag
     11 
---> 12 stillbirth_flag = find_flag(stillbirth_code)
     13 nacrs_ed_1 = nacrs_ed.withColumn('stillbirth_flags', when(stillbirth_flag, 1).otherwise(0))
     14 

/tmp/ipykernel_7034/3428343671.py in find_flag(codes)
      7 
      8 def find_flag(codes):
----> 9     still_birth_flag = functools.reduce(lambda a, b: a | b, [rtrim(col(column_name)) == codes for column_name in problem_col])
     10     return still_birth_flag
     11 

TypeError: reduce() of empty iterable with no initial value
