---------------------------------------------------------------------------
PySparkTypeError                          Traceback (most recent call last)
/tmp/ipykernel_1015/1958876069.py in <cell line: 63>()
     61     return df
     62 
---> 63 los_regg = calculate_percentile(ed_record_admit_with_ucc_22, 'LOS_HOURS', [0.9], confidence_interval=True)
     64 los_regg.show()

/tmp/ipykernel_1015/1958876069.py in calculate_percentile(df, metric, ppt, confidence_interval, bycols)
     57             bycols = ['__']
     58 
---> 59         df = percentile_ci(df, metric, ppt[i], confidence_interval)
     60 
     61     return df

/tmp/ipykernel_1015/1958876069.py in percentile_ci(df, metric, percentile, confidence_interval)
     22         pt_upp_value = df.select(metric).orderBy(metric).limit(1).collect()[0][0]
     23 
---> 24         point_est = F.when(F.isnull(pt_upp_value), 
     25                            pt_low_value - pt_low_value * d)\
     26                       .otherwise(pt_low_value + d * (pt_upp_value - pt_low_value))

/usr/local/lib/python3.10/dist-packages/pyspark/sql/utils.py in wrapped(*args, **kwargs)
    172             return getattr(functions, f.__name__)(*args, **kwargs)
    173         else:
--> 174             return f(*args, **kwargs)
    175 
    176     return cast(FuncT, wrapped)

/usr/local/lib/python3.10/dist-packages/pyspark/sql/functions.py in isnull(col)
   4354     +----+----+-----+-----+
   4355     """
-> 4356     return _invoke_function_over_columns("isnull", col)
   4357 
   4358 

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
