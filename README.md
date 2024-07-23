from pyspark.sql.functions import col, when, lit, rtrim

# Define filter for still birth
stillbirth_code = 'P95'
problem_col = [column_name for column_name in df_nacrs[index].columns if column_name.startswith('problem_')]

def find_flag(codes):
    if not problem_col:
        return lit(False)  # No problem columns, return a false literal
    still_birth_flag = functools.reduce(lambda a, b: a | b, [(rtrim(col(column_name)) == codes) for column_name in problem_col])
    return still_birth_flag

stillbirth_flag = find_flag(stillbirth_code)
nacrs_ed_1 = nacrs_ed.withColumn('stillbirth_flags', when(stillbirth_flag, 1).otherwise(0))
