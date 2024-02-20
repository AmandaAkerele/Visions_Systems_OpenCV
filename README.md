--------------------------------------------------------------------------
AnalysisException                         Traceback (most recent call last)
/tmp/ipykernel_6145/4235826331.py in <cell line: 13>()
     11                      'NACRS_ED_FLG']
     12 
---> 13 ed_records_22_aa_df = ed_records_22_aa_df.select(columns_to_select)

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

AnalysisException: [AMBIGUOUS_REFERENCE] Reference `SUBMISSION_FISCAL_YEAR` is ambiguous, could be: [`SUBMISSION_FISCAL_YEAR`, `SUBMISSION_FISCAL_YEAR`].
