df_nacrs = df_nacrs.select(columns)

# Apply filter conditions
nacrs_ed = df_nacrs.filter(
    (col("ED_VISIT_IND_CODE") == "1") & (col("AMCARE_GROUP_CODE") == "ED")
)

# Get the row count
row_count = nacrs_ed.count()
print(f"Row count: {row_count}")

# Get the column count
column_count = len(nacrs_ed.columns)
print(f"Column count: {column_count}")


solve the error please 
---------------------------------------------------------------------------
AnalysisException                         Traceback (most recent call last)
/tmp/ipykernel_7034/3056992023.py in <cell line: 1>()
----> 1 df_nacrs = df_nacrs.select(columns)
      2 
      3 # Apply filter conditions
      4 nacrs_ed = df_nacrs.filter(
      5     (col("ED_VISIT_IND_CODE") == "1") & (col("AMCARE_GROUP_CODE") == "ED")

/usr/local/lib/python3.10/dist-packages/pyspark/sql/dataframe.py in select(self, *cols)
   3221         +-----+---+
   3222         """
-> 3223         jdf = self._jdf.select(self._jcols(*cols))
   3224         return DataFrame(jdf, self.sparkSession)
   3225 

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

AnalysisException: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter with name `type` cannot be resolved. Did you mean one of the following? [`city`, `fax`, `street`, `gender`, `age_num`].;
