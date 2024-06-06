---------------------------------------------------------------------------
Py4JJavaError                             Traceback (most recent call last)
/tmp/ipykernel_13391/3877138878.py in <cell line: 27>()
     25 
     26 # Calculating percentiles for different categorizations
---> 27 tpia_nt_22 = calculate_percentile(TPIA_nt_record_ucc_22, 'WAIT_TIME_TO_PIA_HOURS', [0, 0.5, 0.9, 0.999, 1], confidence_interval=True)
     28 tpia_reg_22 = calculate_percentile(TPIA_nt_record_ucc_22, 'WAIT_TIME_TO_PIA_HOURS', [0.9], bycols=['SUBMISSION_FISCAL_YEAR', 'adm_prov_id', 'adm_region_id', 'adm_prov_plldj_name_e_desc'])
     29 tpia_prov_22 = calculate_percentile(TPIA_nt_record_ucc_22, 'WAIT_TIME_TO_PIA_HOURS', [0.9], bycols=['SUBMISSION_FISCAL_YEAR', 'adm_prov_plldj_name_e_desc', 'adm_prov_id',])

/tmp/ipykernel_13391/3877138878.py in calculate_percentile(df, column, percentiles, bycols, confidence_interval)
     14     else:
     15         # Calculate global percentiles without grouping
---> 16         global_percentiles = {f'PERCENTILE_{int(p * 100)}': df.approxQuantile(column, [p], 0.05)[0] for p in percentiles}
     17         results = spark.createDataFrame([global_percentiles])
     18 

/tmp/ipykernel_13391/3877138878.py in <dictcomp>(.0)
     14     else:
     15         # Calculate global percentiles without grouping
---> 16         global_percentiles = {f'PERCENTILE_{int(p * 100)}': df.approxQuantile(column, [p], 0.05)[0] for p in percentiles}
     17         results = spark.createDataFrame([global_percentiles])
     18 

/usr/local/lib/python3.10/dist-packages/pyspark/sql/dataframe.py in approxQuantile(self, col, probabilities, relativeError)
   4839         relativeError = float(relativeError)
   4840 
-> 4841         jaq = self._jdf.stat().approxQuantile(col, probabilities, relativeError)
   4842         jaq_list = [list(j) for j in jaq]
   4843         return jaq_list[0] if isStr else jaq_list

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

Py4JJavaError: An error occurred while calling o1191.approxQuantile.
: java.lang.ClassCastException: class java.lang.Integer cannot be cast to class java.lang.Double (java.lang.Integer and java.lang.Double are in module java.base of loader 'bootstrap')
	at scala.runtime.BoxesRunTime.unboxToDouble(BoxesRunTime.java:116)
	at scala.runtime.ScalaRunTime$.array_update(ScalaRunTime.scala:76)
	at scala.collection.IterableLike.copyToArray(IterableLike.scala:256)
	at scala.collection.IterableLike.copyToArray$(IterableLike.scala:251)
	at scala.collection.AbstractIterable.copyToArray(Iterable.scala:56)
	at scala.collection.TraversableOnce.copyToArray(TraversableOnce.scala:334)
	at scala.collection.TraversableOnce.copyToArray$(TraversableOnce.scala:333)
	at scala.collection.AbstractTraversable.copyToArray(Traversable.scala:108)
	at scala.collection.TraversableOnce.toArray(TraversableOnce.scala:342)
	at scala.collection.TraversableOnce.toArray$(TraversableOnce.scala:339)
	at scala.collection.AbstractTraversable.toArray(Traversable.scala:108)
	at org.apache.spark.sql.DataFrameStatFunctions.approxQuantile(DataFrameStatFunctions.scala:115)
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:77)
	at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.base/java.lang.reflect.Method.invoke(Method.java:568)
	at py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:244)
	at py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:374)
	at py4j.Gateway.invoke(Gateway.java:282)
	at py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)
	at py4j.commands.CallCommand.execute(CallCommand.java:79)
	at py4j.ClientServerConnection.waitForCommands(ClientServerConnection.java:182)
	at py4j.ClientServerConnection.run(ClientServerConnection.java:106)
	at java.base/java.lang.Thread.run(Thread.java:840)
