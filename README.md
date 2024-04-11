---------------------------------------------------------------------------
SparkRuntimeException                     Traceback (most recent call last)
/tmp/ipykernel_763/3218910333.py in <cell line: 20>()
     18 
     19 # Apply CORP_ID mapping for tpia_org_21
---> 20 mapping_expr = F.create_map([F.lit(x) for x in 
     21                              [(1019, 81170), (10038, 81124), (7077, 80960), 
     22                               (5045, 81131), (5085, 81180), (5049, 81263), (5160, None)]])

/tmp/ipykernel_763/3218910333.py in <listcomp>(.0)
     18 
     19 # Apply CORP_ID mapping for tpia_org_21
---> 20 mapping_expr = F.create_map([F.lit(x) for x in 
     21                              [(1019, 81170), (10038, 81124), (7077, 80960), 
     22                               (5045, 81131), (5085, 81180), (5049, 81263), (5160, None)]])

/usr/local/lib/python3.10/dist-packages/pyspark/sql/utils.py in wrapped(*args, **kwargs)
    172             return getattr(functions, f.__name__)(*args, **kwargs)
    173         else:
--> 174             return f(*args, **kwargs)
    175 
    176     return cast(FuncT, wrapped)

/usr/local/lib/python3.10/dist-packages/pyspark/sql/functions.py in lit(col)
    191             if dt is not None:
    192                 return _invoke_function("lit", col).astype(dt).alias(str(col))
--> 193         return _invoke_function("lit", col)
    194 
    195 

/usr/local/lib/python3.10/dist-packages/pyspark/sql/functions.py in _invoke_function(name, *args)
     95     assert SparkContext._active_spark_context is not None
     96     jf = _get_jvm_function(name, SparkContext._active_spark_context)
---> 97     return Column(jf(*args))
     98 
     99 

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

SparkRuntimeException: [UNSUPPORTED_FEATURE.LITERAL_TYPE] The feature is not supported: Literal for '[1019, 81170]' of class java.util.ArrayList.

