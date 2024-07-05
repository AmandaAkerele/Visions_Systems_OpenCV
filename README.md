from pyspark.sql import functions as F
from functools import reduce

# Combine all DataFrames in df_list into a single DataFrame
combined_df = reduce(lambda x, y: x.unionByName(y), df_list)

# Define the conditions for numerator and denominator
numerator_condition = (F.col('AMCARE_GROUP_CODE') == 'ED') & (F.col('ED_VISIT_IND_CODE') == '1') & (F.col('amcare_type_code') == '11')
denominator_condition = (F.col('AMCARE_GROUP_CODE') == 'ED') & (F.col('ED_VISIT_IND_CODE') == '1')

# Apply the conditions and create temporary dataframes
numerator_temp = combined_df.filter(numerator_condition)
denominator_temp = combined_df.filter(denominator_condition)

# Group by necessary columns and aggregate
numerator = numerator_temp.groupBy('SUBMISSION_FISCAL_YEAR', 'FACILITY_AM_CARE_NUM').count().withColumnRenamed('count', 'numerator')
denominator = denominator_temp.groupBy('SUBMISSION_FISCAL_YEAR', 'FACILITY_AM_CARE_NUM').count().withColumnRenamed('count', 'denominator')

# Join numerator and denominator dataframes
ucc = numerator.join(denominator, on=['SUBMISSION_FISCAL_YEAR', 'FACILITY_AM_CARE_NUM'])

# Calculate the UCC percentage and filter
ucc = ucc.withColumn('ucc_pct', F.col('numerator') / F.col('denominator')).filter(F.col('ucc_pct') >= 0.98)

# Append the ucc df to make a list
ucc_all.append(ucc)

# Combine all dataframes in the list
ucc_all_corp = reduce(lambda x, y: x.unionByName(y), ucc_all)

# Show the final result
ucc_all_corp.show()
