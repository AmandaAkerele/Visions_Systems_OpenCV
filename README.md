---------------------------------------------------------------------------
AnalysisException                         Traceback (most recent call last)
/tmp/ipykernel_763/3366526521.py in <cell line: 54>()
     52 
     53 # Apply conditions for tpia_org_cmp and tpia_reg_cmp
---> 54 tpia_org_cmp_a = tpia_org_cmp.withColumn('COMPARE_IND_CODE', apply_conditions(F.col('PERCENTILE_90'), F.col('20th_Percentile'), F.col('80th_Percentile')).getItem(0)) \
     55                             .withColumn('COMPARE_IND_E_DESC', apply_conditions(F.col('PERCENTILE_90'), F.col('20th_Percentile'), F.col('80th_Percentile')).getItem(1)) \
     56                             .orderBy('CORP_ID')

/usr/local/lib/python3.10/dist-packages/pyspark/sql/dataframe.py in withColumn(self, colName, col)
   5168                 message_parameters={"arg_name": "col", "arg_type": type(col).__name__},
   5169             )
-> 5170         return DataFrame(self._jdf.withColumn(colName, col._jc), self.sparkSession)
   5171 
   5172     def withColumnRenamed(self, existing: str, new: str) -> "DataFrame":

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

AnalysisException: [INVALID_EXTRACT_BASE_FIELD_TYPE] Can't extract a value from "apply_conditions(PERCENTILE_90, 20th_Percentile, 80th_Percentile)". Need a complex type [STRUCT, ARRAY, MAP] but got "STRING".
