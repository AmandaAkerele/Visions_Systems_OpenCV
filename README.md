---------------------------------------------------------------------------
AnalysisException                         Traceback (most recent call last)
/tmp/ipykernel_299/1304976524.py in <cell line: 5>()
      3 # Concatenate, merge, and rename for Los Org
      4 los_org_all_yr_b = (
----> 5     los_org_20.union(los_org_21).union(los_org_22_a)
      6     .join(los_org_3x3.select('CORP_ID'), on='CORP_ID', how='inner')
      7     .withColumnRenamed('FISCAL_YEAR', 'TIME')

/usr/local/lib/python3.10/dist-packages/pyspark/sql/dataframe.py in union(self, other)
   3921         +---+-----+
   3922         """
-> 3923         return DataFrame(self._jdf.union(other._jdf), self.sparkSession)
   3924 
   3925     def unionAll(self, other: "DataFrame") -> "DataFrame":

/usr/local/lib/python3.10/dist-packages/py4j/java_gateway.py in __call__(self, *args)
   1320 
   1321         answer = self.gateway_client.send_command(command)
-> 1322         return_value = get_return_value(
   1323             answer, self.gateway_client, self.target_id, self.name)
   1324 

/usr/local/lib/python3.10/dist-packages/pyspark/errors/exceptions/captured.py in deco(*a, **kw)
    183                 # Hide where the exception came from that shows a non-Pythonic
    184                 # JVM exception message.
--> 185                 raise converted from None
    186             else:
    187                 raise

AnalysisException: [NUM_COLUMNS_MISMATCH] UNION can only be performed on inputs with the same number of columns, but the first input has 4 columns and the third input has 5 columns.;
'Union false, false

use the method below to address this error message above 

# Add columns from df_dq, renaming those that conflict
conflicting_columns = ['SITE_ID', 'FACILITY_AM_CARE_NUM', 'CORP_ID', 'REGION_ID', 'PROVINCE_ID', 'REGION_NAME']  # Adjust based on your data
for column_name in df_dq.columns:
    if column_name not in conflicting_columns:
        selected_columns.append(col(f"dq.{column_name}"))

# Construct the final DataFrame with selected columns
merged_df = merged_df.select(selected_columns)

# Define a list of columns to keep
columns_to_keep = [
    'FACILITY_AM_CARE_NUM', 'SUBMISSION_FISCAL_YEAR', 'SITE_ID', 'CORP_ID',
    'REGION_ID', 'PROVINCE_ID', 'NACRS_ED_FLG'
]

# Add 'TYPE' column with values 'DQ' for merged_df
merged_df = merged_df.withColumn('TYPE', lit('DQ'))




# Create DataFrames t3 and t4 based on conditions
t3 = df_fac.filter(df_fac['NACRS_ED_FLG'] == 1) \
          .select(*columns_to_keep) \
          .withColumn('TYPE', lit('SL')) \
          .withColumn('IND', lit(''))

# Handle conflicting column names for t4
selected_columns_t4 = [col(f"fac.{column_name}") for column_name in columns_to_keep]

# Add columns from df_ps, renaming those that conflict
conflicting_columns_t4 = ['FACILITY_AM_CARE_NUM']  # Adjust based on your data
for column_name in df_ps.columns:
    if column_name not in conflicting_columns_t4:
        selected_columns_t4.append(col(f"ps.{column_name}"))
