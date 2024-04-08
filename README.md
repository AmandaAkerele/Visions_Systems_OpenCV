---------------------------------------------------------------------------
ValueError                                Traceback (most recent call last)
/tmp/ipykernel_1015/2973903392.py in <cell line: 64>()
     62 
     63 # Call the calculate_percentile function
---> 64 los_regg = calculate_percentile(ed_record_admit_with_ucc_22, 'LOS_HOURS', [0.9], confidence_interval=True)
     65 los_regg.show()

/tmp/ipykernel_1015/2973903392.py in calculate_percentile(df, metric, ppt, confidence_interval, bycols)
     43             df = df.withColumn('__', F.lit(1))  # Add a constant column '__' with value 1
     44             bycols = '__'
---> 45         y = df.groupby(bycols).apply(lambda x: percentile_ci(x[metric], ppt[i], confidence_interval))
     46         out = y.tolist()
     47         index = y.index.tolist()

/usr/local/lib/python3.10/dist-packages/pyspark/sql/pandas/group_ops.py in apply(self, udf)
     98             )
     99         ):
--> 100             raise ValueError(
    101                 "Invalid udf: the udf argument must be a pandas_udf of type " "GROUPED_MAP."
    102             )

ValueError: Invalid udf: the udf argument must be a pandas_udf of type GROUPED_MAP.
