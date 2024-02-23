---------------------------------------------------------------------------
AnalysisException                         Traceback (most recent call last)
/tmp/ipykernel_2055/2375582027.py in <cell line: 5>()
      3 # Calculation for los_nt_22
      4 los_nt_22 = calculate_percentile_spark(los_nt_record_ucc_22, 'LOS_HOURS', [0, 0.5, 0.9, 0.999, 1])
----> 5 los_nt_22 = los_nt_22.select([upper(col(c)).alias(c) for c in los_nt_22.columns])
      6 
      7 # Calculation for los_reg_22

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

AnalysisException: [INVALID_EXTRACT_BASE_FIELD_TYPE] Can't extract a value from "percentile_0". Need a complex type [STRUCT, ARRAY, MAP] but got "DOUBLE".
