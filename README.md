24/04/15 11:24:35 WARN Instrumentation: [b558bf3e] regParam is zero, which might cause numerical instability and overfitting.
24/04/15 11:24:35 WARN Instrumentation: [b558bf3e] Cholesky solver failed due to singular covariance matrix. Retrying with Quasi-Newton solver.
                                                                                
---------------------------------------------------------------------------
UnsupportedOperationException             Traceback (most recent call last)
/tmp/ipykernel_3755/3708711566.py in <cell line: 50>()
     50 for group_name in los_org_all_yr_b.select('TIME').distinct().rdd.flatMap(lambda x: x).collect():
     51     group_data = los_org_all_yr_b.filter(F.col('TIME') == group_name)
---> 52     all_results.append(perform_ols(group_data))
     53 
     54 # Convert results to DataFrame

/tmp/ipykernel_3755/3708711566.py in perform_ols(group_data)
     28     # Extract coefficients and confidence intervals
     29     coef = model.coefficients[0]
---> 30     std_err = model.summary.coefficientStandardErrors[0]
     31     t_val = coef / std_err
     32     p_val = model.summary.pValues[0]

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
