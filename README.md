                                                                               
---------------------------------------------------------------------------
PySparkTypeError                          Traceback (most recent call last)
/tmp/ipykernel_1015/1610451872.py in <cell line: 62>()
     60 # ed_record_admit_with_ucc_22_spark = spark.createDataFrame(ed_record_admit_with_ucc_22)
     61 
---> 62 los_regg = calculate_percentile(ed_record_admit_with_ucc_22, 'LOS_HOURS', [0.9], confidence_interval=True)
     63 los_regg.show()

/tmp/ipykernel_1015/1610451872.py in calculate_percentile(df, metric, ppt, confidence_interval, bycols)
     53             bycols = ['__']
     54 
---> 55         df = percentile_ci(df, metric, ppt[i], confidence_interval)
     56 
     57     return df

/tmp/ipykernel_1015/1610451872.py in percentile_ci(df, metric, percentile, confidence_interval)
     13     if count > 0:
     14         kf = (count - 1) * percentile
---> 15         pt_low_n = F.floor(kf) + 1 - 1
     16         pt_low_n = F.when(pt_low_n < 0, 0).otherwise(pt_low_n)
     17         pt_upp_n = F.floor(kf) + 2 - 1

/usr/local/lib/python3.10/dist-packages/pyspark/sql/utils.py in wrapped(*args, **kwargs)
    172             return getattr(functions, f.__name__)(*args, **kwargs)
    173         else:
--> 174             return f(*args, **kwargs)
    175 
    176     return cast(FuncT, wrapped)

/usr/local/lib/python3.10/dist-packages/pyspark/sql/functions.py in floor(col)
   1589     +----------+
   1590     """
-> 1591     return _invoke_function_over_columns("floor", col)
   1592 
   1593 

/usr/local/lib/python3.10/dist-packages/pyspark/sql/functions.py in _invoke_function_over_columns(name, *cols)
    103     and wraps the result with :class:`~pyspark.sql.Column`.
    104     """
--> 105     return _invoke_function(name, *(_to_java_column(col) for col in cols))
    106 
    107 

/usr/local/lib/python3.10/dist-packages/pyspark/sql/functions.py in <genexpr>(.0)
    103     and wraps the result with :class:`~pyspark.sql.Column`.
    104     """
--> 105     return _invoke_function(name, *(_to_java_column(col) for col in cols))
    106 
    107 

/usr/local/lib/python3.10/dist-packages/pyspark/sql/column.py in _to_java_column(col)
     63         jcol = _create_column_from_name(col)
     64     else:
---> 65         raise PySparkTypeError(
     66             error_class="NOT_COLUMN_OR_STR",
     67             message_parameters={"arg_name": "col", "arg_type": type(col).__name__},

PySparkTypeError: [NOT_COLUMN_OR_STR] Argument `col` should be a Column or str, got float.
