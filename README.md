---------------------------------------------------------------------------
AnalysisException                         Traceback (most recent call last)
/tmp/ipykernel_317/3951404005.py in <cell line: 7>()
      5 
      6 # Update tpia_rec based on conditions in TIME_PHYSICAN_INIT_ASSESSMENT column
----> 7 tpia_peer_cnt_a_df = tpia_peer_cnt_a_df.withColumn(
      8     'tpia_rec',
      9     F.when(F.col('TIME_PHYSICAN_INIT_ASSESSMENT').isNull() | F.trim(F.col('TIME_PHYSICAN_INIT_ASSESSMENT')) == '', 'B') \

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

AnalysisException: [DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES] Cannot resolve "((TIME_PHYSICAN_INIT_ASSESSMENT IS NULL) OR trim(TIME_PHYSICAN_INIT_ASSESSMENT))" due to data type mismatch: the left and right operands of the binary operator have incompatible types ("BOOLEAN" and "STRING").;
