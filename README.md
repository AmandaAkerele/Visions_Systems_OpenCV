---------------------------------------------------------------------------
PySparkTypeError                          Traceback (most recent call last)
/tmp/ipykernel_324/607714401.py in <cell line: 39>()
     37 
     38 # Applying the logistic regression function to each group and collecting the results
---> 39 all_results = [perform_logistic(group) for group in los_org_all_yr_b.groupby('CORP_ID').agg(F.collect_list('TIME').alias('TIME'))]
     40 
     41 # Convert results into a DataFrame

/tmp/ipykernel_324/607714401.py in <listcomp>(.0)
     37 
     38 # Applying the logistic regression function to each group and collecting the results
---> 39 all_results = [perform_logistic(group) for group in los_org_all_yr_b.groupby('CORP_ID').agg(F.collect_list('TIME').alias('TIME'))]
     40 
     41 # Convert results into a DataFrame

/tmp/ipykernel_324/607714401.py in perform_logistic(group_data)
      9 def perform_logistic(group_data):
     10     assembler = VectorAssembler(inputCols=["TIME"], outputCol="features")
---> 11     data = assembler.transform(group_data)
     12 
     13     lr = LogisticRegression(featuresCol="features", labelCol="PERCENTILE_90", maxIter=10)

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
   1312 
   1313     def __call__(self, *args):
-> 1314         args_command, temp_args = self._build_args(*args)
   1315 
   1316         command = proto.CALL_COMMAND_NAME +\

/usr/local/lib/python3.10/dist-packages/py4j/java_gateway.py in _build_args(self, *args)
   1275     def _build_args(self, *args):
   1276         if self.converters is not None and len(self.converters) > 0:
-> 1277             (new_args, temp_args) = self._get_args(args)
   1278         else:
   1279             new_args = args

/usr/local/lib/python3.10/dist-packages/py4j/java_gateway.py in _get_args(self, args)
   1262                 for converter in self.gateway_client.converters:
   1263                     if converter.can_convert(arg):
-> 1264                         temp_arg = converter.convert(arg, self.gateway_client)
   1265                         temp_args.append(temp_arg)
   1266                         new_args.append(temp_arg)

/usr/local/lib/python3.10/dist-packages/py4j/java_collections.py in convert(self, object, gateway_client)
    508         ArrayList = JavaClass("java.util.ArrayList", gateway_client)
    509         java_list = ArrayList()
--> 510         for element in object:
    511             java_list.add(element)
    512         return java_list

/usr/local/lib/python3.10/dist-packages/pyspark/sql/column.py in __iter__(self)
    716 
    717     def __iter__(self) -> None:
--> 718         raise PySparkTypeError(
    719             error_class="NOT_ITERABLE", message_parameters={"objectName": "Column"}
    720         )

PySparkTypeError: [NOT_ITERABLE] Column is not iterable.
Selection deleted
