from pyspark.sql import functions as F
import functools

# Define filter for still birth
stillbirth_code = 'P95'
problem_col = [col for col in df_nacrs.columns if col.startswith('problem_')]

def find_flag(codes):
    if not problem_col:
        return F.lit(False)  # No problem columns, return a false literal
    still_birth_flag = functools.reduce(lambda a, b: a | b, [F.rtrim(F.col(column_name)) == codes for column_name in problem_col])
    return still_birth_flag

stillbirth_flag = find_flag(stillbirth_code)
nacrs_ed_1 = nacrs_ed.withColumn('stillbirth_flags', F.when(stillbirth_flag, 1).otherwise(0))

stillbirth = nacrs_ed_1.filter(col('stillbirth_flags') == 1).select("AM_CARE_KEY")

# Show the result
stillbirth.show()
