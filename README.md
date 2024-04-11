---------------------------------------------------------------------------
AnalysisException                         Traceback (most recent call last)
/tmp/ipykernel_763/41871877.py in <cell line: 48>()
     46 
     47 # Concatenate, merge, and rename for Los Org and Tpia Org
---> 48 los_org_all_yr_b = los_org_20.union(los_org_21).union(los_org_22_a).join(los_org_3x3, on='CORP_ID', how='inner').withColumnRenamed('FISCAL_YEAR', 'TIME')
     49 tpia_org_all_yr_b = tpia_org_20.union(tpia_org_21).union(tpia_org_22_a).join(tpia_org_3x3, on='CORP_ID', how='inner').withColumnRenamed('FISCAL_YEAR', 'TIME')
     50 

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
