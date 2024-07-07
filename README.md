from pyspark.sql.functions import year

# Assuming there is a date column, filter for the year 2022
df_2022 = tpia_supp_org_ucc_22.filter(year(tpia_supp_org_ucc_22['date_column']) == 2022)

# Show the filtered DataFrame
df_2022.show()


AnalysisException                         Traceback (most recent call last)
/tmp/ipykernel_12720/2940029620.py in <cell line: 4>()
      2 
      3 # Assuming there is a date column, filter for the year 2022
----> 4 df_2022 = tpia_supp_org_ucc_22.filter(year(tpia_supp_org_ucc_22['date_column']) == 2022)
      5 
      6 # Show the filtered DataFrame

/usr/local/lib/python3.10/dist-packages/pyspark/sql/dataframe.py in __getitem__(self, item)
   3072         """
   3073         if isinstance(item, str):
-> 3074             jc = self._jdf.apply(item)
   3075             return Column(jc)
   3076         elif isinstance(item, Column):

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

AnalysisException: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter with name `date_column` cannot be resolved. Did you mean one of the following? [`SUBMISSION_FISCAL_YEAR`, `CORP_ID`, `Total_CASE`, `tpia_calc_cnt`, `tpia_elig_cnt`, `tpia_rec_pct`].
