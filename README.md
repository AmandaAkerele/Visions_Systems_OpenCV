24/04/16 10:32:40 WARN Instrumentation: [c5197b4e] regParam is zero, which might cause numerical instability and overfitting.
24/04/16 10:32:41 WARN InstanceBuilder: Failed to load implementation from:dev.ludovic.netlib.blas.VectorBLAS
24/04/16 10:32:41 WARN Instrumentation: [c5197b4e] Cholesky solver failed due to singular covariance matrix. Retrying with Quasi-Newton solver.
---------------------------------------------------------------------------
UnsupportedOperationException             Traceback (most recent call last)
/tmp/ipykernel_324/3354341125.py in <cell line: 20>()
     40     # Extract coefficients and confidence intervals
     41     params = model.coefficients.toArray()
---> 42     std_err = model.summary.coefficientStandardErrors
     43     t_values = model.summary.tValues
     44     p_values = model.summary.pValues

/usr/local/lib/python3.10/dist-packages/pyspark/ml/regression.py in coefficientStandardErrors(self)
    700         LinearRegression.solver
    701         """
--> 702         return self._call_java("coefficientStandardErrors")
    703 
    704     @property

/usr/local/lib/python3.10/dist-packages/pyspark/ml/wrapper.py in _call_java(self, name, *args)
     70 
     71         java_args = [_py2java(sc, arg) for arg in args]
---> 72         return _java2py(sc, m(*java_args))
     73 
     74     @staticmethod

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

UnsupportedOperationException: No Std. Error of coefficients available for this LinearRegressionModel
