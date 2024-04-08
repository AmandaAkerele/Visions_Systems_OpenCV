---------------------------------------------------------------------------
AttributeError                            Traceback (most recent call last)
/tmp/ipykernel_1015/2706484269.py in <cell line: 64>()
     62 
     63 # Call the calculate_percentile function
---> 64 los_regg = calculate_percentile(ed_record_admit_with_ucc_22_spark, 'LOS_HOURS', [0.9], confidence_interval=True)
     65 los_regg.show()

/tmp/ipykernel_1015/2706484269.py in calculate_percentile(df, metric, ppt, confidence_interval, bycols)
     41     for i in range(len(ppt)):
     42         if bycols == '':
---> 43             df = df.withColumn('__', F.lit(1))  # Add a constant column '__' with value 1
     44             bycols = '__'
     45         y = df.groupby(bycols).apply(lambda x: percentile_ci(x[metric], ppt[i], confidence_interval))

~/.local/lib/python3.10/site-packages/pandas/core/generic.py in __getattr__(self, name)
   5987         ):
   5988             return self[name]
-> 5989         return object.__getattribute__(self, name)
   5990 
   5991     def __setattr__(self, name: str, value) -> None:

AttributeError: 'DataFrame' object has no attribute 'withColumn'
