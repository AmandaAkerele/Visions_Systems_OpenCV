---------------------------------------------------------------------------
IllegalArgumentException                  Traceback (most recent call last)
/tmp/ipykernel_324/3418406917.py in <cell line: 19>()
     41     std_err = model.summary.coefficientStandardErrors
     42     t_values = model.summary.tValues
---> 43     p_values = model.summary.pValues
     44     conf_int = model.summary.confidenceIntervals()
     45 

/usr/local/lib/python3.10/dist-packages/pyspark/ml/regression.py in pValues(self)
    734         LinearRegression.solver
    735         """
--> 736         return self._call_java("pValues")
    737 
    738 

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

IllegalArgumentException: requirement failed: degreesOfFreedom must be positive, but got 0.0
