help solve this error 

24/04/19 12:33:17 WARN TaskSetManager: Lost task 0.0 in stage 13022.0 (TID 13779) (10.4.7.191 executor 2): org.apache.spark.api.python.PythonException: Traceback (most recent call last):
  File "/tmp/ipykernel_324/2085256895.py", line 26, in percentile_ci_udf
  File "/opt/spark/python/lib/pyspark.zip/pyspark/sql/utils.py", line 174, in wrapped
    return f(*args, **kwargs)
  File "/opt/spark/python/lib/pyspark.zip/pyspark/sql/functions.py", line 4827, in round
    return _invoke_function("round", _to_java_column(col), scale)
  File "/opt/spark/python/lib/pyspark.zip/pyspark/sql/column.py", line 65, in _to_java_column
    raise PySparkTypeError(
pyspark.errors.exceptions.base.PySparkTypeError: [NOT_COLUMN_OR_STR] Argument `col` should be a Column or str, got float.

	at org.apache.spark.api.python.BasePythonRunner$ReaderIterator.handlePythonException(PythonRunner.scala:572)
	at org.apache.spark.sql.execution.python.BasePythonUDFRunner$$anon$1.read(PythonUDFRunner.scala:94)
	at org.apache.spark.sql.execution.python.BasePythonUDFRunner$$anon$1.read(PythonUDFRunner.scala:75)
	at org.apache.spark.api.python.BasePythonRunner$ReaderIterator.hasNext(PythonRunner.scala:525)
	at org.apache.spark.InterruptibleIterator.hasNext(InterruptibleIterator.scala:37)
	at scala.collection.Iterator$$anon$11.hasNext(Iterator.scala:491)
	at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:460)
	at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:460)
	at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIteratorForCodegenStage16.processNext(Unknown Source)
	at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)
	at org.apache.spark.sql.execution.WholeStageCodegenEvaluatorFactory$WholeStageCodegenPartitionEvaluator$$anon$1.hasNext(WholeStageCodegenEvaluatorFactory.scala:43)
	at org.apache.spark.sql.execution.SparkPlan.$anonfun$getByteArrayRdd$1(SparkPlan.scala:388)
	at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2(RDD.scala:890)
	at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2$adapted(RDD.scala:890)
	at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:364)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:328)
	at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:93)
	at org.apache.spark.TaskContext.runTaskWithListeners(TaskContext.scala:161)
	at org.apache.spark.scheduler.Task.run(Task.scala:141)
	at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$4(Executor.scala:620)
	at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally(SparkErrorUtils.scala:64)
	at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally$(SparkErrorUtils.scala:61)
	at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:94)
	at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:623)
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136)
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635)
	at java.base/java.lang.Thread.run(Thread.java:840)

24/04/19 12:33:18 ERROR TaskSetManager: Task 0 in stage 13022.0 failed 4 times; aborting job
---------------------------------------------------------------------------
PythonException                           Traceback (most recent call last)
/tmp/ipykernel_324/2790008844.py in <cell line: 1>()
----> 1 los_reg_22.show()

/usr/local/lib/python3.10/dist-packages/pyspark/sql/dataframe.py in show(self, n, truncate, vertical)
    957 
    958         if isinstance(truncate, bool) and truncate:
--> 959             print(self._jdf.showString(n, 20, vertical))
    960         else:
    961             try:

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

PythonException: 
  An exception was thrown from the Python worker. Please see the stack trace below.
Traceback (most recent call last):
  File "/tmp/ipykernel_324/2085256895.py", line 26, in percentile_ci_udf
  File "/opt/spark/python/lib/pyspark.zip/pyspark/sql/utils.py", line 174, in wrapped
    return f(*args, **kwargs)
  File "/opt/spark/python/lib/pyspark.zip/pyspark/sql/functions.py", line 4827, in round
    return _invoke_function("round", _to_java_column(col), scale)
  File "/opt/spark/python/lib/pyspark.zip/pyspark/sql/column.py", line 65, in _to_java_column
    raise PySparkTypeError(
pyspark.errors.exceptions.base.PySparkTypeError: [NOT_COLUMN_OR_STR] Argument `col` should be a Column or str, got float.
