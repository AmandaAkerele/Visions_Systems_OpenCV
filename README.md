---------------------------------------------------------------------------
IllegalArgumentException                  Traceback (most recent call last)
/tmp/ipykernel_324/4198431249.py in <cell line: 52>()
     52 for corp_id_row in los_org_all_yr_b.select('CORP_ID').distinct().rdd.flatMap(lambda x: x).collect():
     53     group_data = los_org_all_yr_b.filter(F.col('CORP_ID') == corp_id_row)
---> 54     all_results.append(perform_logistic(group_data))
     55 
     56 # Convert results to DataFrame

/tmp/ipykernel_324/4198431249.py in perform_logistic(group_data)
     20     # Create a vector assembler
     21     assembler = VectorAssembler(inputCols=["TIME"], outputCol="features")
---> 22     data = assembler.transform(group_data)
     23 
     24     # Create and fit the model

/usr/local/lib/python3.10/dist-packages/pyspark/ml/base.py in transform(self, dataset, params)
    260                 return self.copy(params)._transform(dataset)
    261             else:
--> 262                 return self._transform(dataset)
    263         else:
    264             raise TypeError("Params must be a param map but got %s." % type(params))

/usr/local/lib/python3.10/dist-packages/pyspark/ml/wrapper.py in _transform(self, dataset)
    396 
    397         self._transfer_params_to_java()
--> 398         return DataFrame(self._java_obj.transform(dataset._jdf), dataset.sparkSession)
    399 
    400 

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

IllegalArgumentException: Data type string of column TIME is not supported.


