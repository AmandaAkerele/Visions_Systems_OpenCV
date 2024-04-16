---------------------------------------------------------------------------
AnalysisException                         Traceback (most recent call last)
/tmp/ipykernel_324/1711155419.py in <cell line: 21>()
     19 
     20 # Rename one and drop one 'CORP_PEER' column
---> 21 los_org_all_yr_b = rename_and_drop_duplicate_column(los_org_all_yr_b, "CORP_PEER", "CORP_PEER_NEW")
     22 
     23 # Show the result DataFrame structure

/tmp/ipykernel_324/1711155419.py in rename_and_drop_duplicate_column(df, column_name, new_name)
      5         # Rename the first occurrence of the column
      6         columns = [col(c).alias(new_name) if c == column_name else c for c in df.columns]
----> 7         df = df.select(*columns)
      8     # Drop the duplicate column
      9     df = df.drop(column_name)

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

AnalysisException: [AMBIGUOUS_REFERENCE] Reference `CORP_PEER` is ambiguous, could be: [`CORP_PEER`, `CORP_PEER`].
