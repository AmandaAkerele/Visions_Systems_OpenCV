--------------------------------------------------------------------------
Py4JJavaError                             Traceback (most recent call last)
/tmp/ipykernel_324/293192236.py in <cell line: 53>()
     51 perform_logistic_sql = udf(perform_logistic_udf, returnType=FloatType())
     52 
---> 53 results = grouped.agg(perform_logistic_sql(F.collect_list(F.struct('TIME', 'PERCENTILE_90')).alias("data"))).select(
     54     "CORP_ID",
     55     "data.coef",

/usr/local/lib/python3.10/dist-packages/pyspark/sql/group.py in agg(self, *exprs)
    184             assert all(isinstance(c, Column) for c in exprs), "all exprs should be Column"
    185             exprs = cast(Tuple[Column, ...], exprs)
--> 186             jdf = self._jgd.agg(exprs[0]._jc, _to_seq(self.session._sc, [c._jc for c in exprs[1:]]))
    187         return DataFrame(jdf, self.session)
    188 

/usr/local/lib/python3.10/dist-packages/py4j/java_gateway.py in __call__(self, *args)
   1320 
   1321         answer = self.gateway_client.send_command(command)
-> 1322         return_value = get_return_value(
   1323             answer, self.gateway_client, self.target_id, self.name)
   1324 

/usr/local/lib/python3.10/dist-packages/pyspark/errors/exceptions/captured.py in deco(*a, **kw)
    177     def deco(*a: Any, **kw: Any) -> Any:
    178         try:
--> 179             return f(*a, **kw)
    180         except Py4JJavaError as e:
    181             converted = convert_exception(e.java_exception)

/usr/local/lib/python3.10/dist-packages/py4j/protocol.py in get_return_value(answer, gateway_client, target_id, name)
    324             value = OUTPUT_CONVERTER[type](answer[2:], gateway_client)
    325             if answer[1] == REFERENCE_TYPE:
--> 326                 raise Py4JJavaError(
    327                     "An error occurred while calling {0}{1}{2}.\n".
    328                     format(target_id, ".", name), value)

Py4JJavaError: An error occurred while calling o3463.agg.
: org.apache.spark.SparkException: [INTERNAL_ERROR] Cannot evaluate expression: TIME
