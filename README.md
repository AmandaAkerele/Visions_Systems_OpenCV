from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import FloatType, IntegerType

# Convert columns to numeric and drop NaNs
los_org_all_yr_b = los_org_all_yr_b.select(
    *[F.col(col).cast("float") for col in ['PERCENTILE_90', 'TIME']]
).na.drop(subset=['PERCENTILE_90', 'TIME'])

# Define a window specification
windowSpec = Window.partitionBy('CORP_ID')

# Create a GroupBy object and prepare DataFrame for results
grouped = los_org_all_yr_b.groupBy('CORP_ID').agg(
    F.collect_list('TIME').alias('TIME_LIST'),
    F.collect_list('PERCENTILE_90').alias('PERCENTILE_90_LIST')
)

all_results = []

# Iterate, perform regression, and store results
for row in grouped.collect():
    corp_id = row['CORP_ID']
    X = list(zip(row['TIME_LIST'], [1]*len(row['TIME_LIST'])))
    y = row['PERCENTILE_90_LIST']

    # Create a DataFrame from the data
    df = spark.createDataFrame(X, ["TIME", "const"]).withColumn("PERCENTILE_90", F.lit(None))
    
    # VectorAssembler
    assembler = VectorAssembler(inputCols=["TIME", "const"], outputCol="features")
    df = assembler.transform(df)
    
    # Linear Regression
    lr = LinearRegression(featuresCol='features', labelCol='PERCENTILE_90', regParam=0.0)
    model = lr.fit(df)
    
    # Extract coefficients and confidence intervals
    params = model.coefficients.toArray()
    std_err = model.summary.coefficientStandardErrors
    t_values = model.summary.tValues
    p_values = model.summary.pValues
    conf_int = model.summary.confidenceIntervals()

    for param_type, values in zip(['PARAMS', 'STDERR', 'T', 'PVALUE', 'L95B', 'U95B'],
                                  [params, std_err, t_values, p_values,
                                   [conf_int[0][0]], [conf_int[0][1]]]):
        all_results.append({'CORP_ID': corp_id, '_TYPE_': param_type, 'TIME': float(values[0])})

# Convert to DataFrame and analyze results
los_org_trend_a = spark.createDataFrame(all_results)

# Add linr column
los_org_trend_a = los_org_trend_a.withColumn(
    'linr', F.when((F.col('_TYPE_') == 'PVALUE') & (F.col('TIME') < 0.05), 1).otherwise(0)
)

# Create subsets and merge
p_val = los_org_trend_a.filter(los_org_trend_a['_TYPE_'] == 'PVALUE').select('CORP_ID', 'linr')
parms = los_org_trend_a.filter(los_org_trend_a['_TYPE_'] == 'PARAMS').select('CORP_ID', 'TIME')

merged = p_val.join(parms, on='CORP_ID', how='inner')

# Define improvement indicators
merged = merged.withColumn('IMPROVEMENT_IND_CODE', F.lit('002')) \
               .withColumn('IMPROVEMENT_IND_E_DESC', F.lit('No Change'))

merged = merged.withColumn(
    'IMPROVEMENT_IND_CODE', 
    F.when((F.col('linr') == 1) & (F.col('TIME') > 0), '003')
    .when((F.col('linr') == 1) & (F.col('TIME') < 0), '001')
    .otherwise(F.col('IMPROVEMENT_IND_CODE'))
)

merged = merged.withColumn(
    'IMPROVEMENT_IND_E_DESC', 
    F.when((F.col('linr') == 1) & (F.col('TIME') > 0), 'Weakening')
    .when((F.col('linr') == 1) & (F.col('TIME') < 0), 'Improving')
    .otherwise(F.col('IMPROVEMENT_IND_E_DESC'))
)

# Final filtering
los_org_trend_b = merged.join(ed_nacrs_flg_1_22, on='CORP_ID', how='left_anti')



solve the error in the code above. 

Error is below:
---------------------------------------------------------------------------
AnalysisException                         Traceback (most recent call last)
/tmp/ipykernel_3755/1566202595.py in <cell line: 16>()
     14 
     15 # Create a GroupBy object and prepare DataFrame for results
---> 16 grouped = los_org_all_yr_b.groupBy('CORP_ID').agg(
     17     F.collect_list('TIME').alias('TIME_LIST'),
     18     F.collect_list('PERCENTILE_90').alias('PERCENTILE_90_LIST')

/usr/local/lib/python3.10/dist-packages/pyspark/sql/group.py in agg(self, *exprs)
    184             assert all(isinstance(c, Column) for c in exprs), "all exprs should be Column"
    185             exprs = cast(Tuple[Column, ...], exprs)
--> 186             jdf = self._jgd.agg(exprs[0]._jc, _to_seq(self.session._sc, [c._jc for c in exprs[1:]]))
    187         return DataFrame(jdf, self.session)
    188 

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

AnalysisException: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter with name `CORP_ID` cannot be resolved. Did you mean one of the following? [`TIME`, `PERCENTILE_90`].;
'Aggregate ['CORP_ID], ['CORP_ID, collect_list(TIME#40594, 0, 0) AS TIME_LIST#40602, collect_list(PERCENTILE_90#40593, 0, 0) AS PERCENTILE_90_LIST#40604]
