---------------------------------------------------------------------------
AttributeError                            Traceback (most recent call last)
/tmp/ipykernel_324/984858162.py in <cell line: 20>()
     47     p_values = [2 * (1 - stats.t.cdf(abs(t), degrees_of_freedom)) for t in t_values]
     48 
---> 49     conf_int = model.summary.confidenceIntervals()
     50 
     51     for param_type, values in zip(['PARAMS', 'STDERR', 'T', 'PVALUE', 'L95B', 'U95B'],

AttributeError: 'LinearRegressionTrainingSummary' object has no attribute 'confidenceIntervals'
