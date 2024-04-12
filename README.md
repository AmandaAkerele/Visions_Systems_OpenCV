---------------------------------------------------------------------------
AnalysisException                         Traceback (most recent call last)
/tmp/ipykernel_286/1683732808.py in <cell line: 2>()
      1 # Concatenate tpia_org DataFrames
----> 2 tpia_org_all_yr = tpia_org_20.union(tpia_org_21).union(tpia_org_22).dropDuplicates()
      3 
      4 # Concatenate tpia_reg DataFrames
      5 tpia_reg_all_yr = tpia_reg_20.union(tpia_reg_21).union(tpia_reg_22).dropDuplicates()

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
