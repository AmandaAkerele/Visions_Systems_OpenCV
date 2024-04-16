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
    F.collect_list('TIME').alias('TIME'),
    F.collect_list('PERCENTILE_90').alias('PERCENTILE_90')
)

all_results = []

# Iterate, perform regression, and store results
for row in grouped.collect():
    corp_id = row['CORP_ID']
    X = list(zip(row['TIME'], [1]*len(row['TIME'])))
    y = row['PERCENTILE_90']

    # Create a DataFrame from the data
    df = spark.createDataFrame(X, ["TIME", "const"]).withColumn("PERCENTILE_90", F.lit(None))
    
    # Cast 'TIME' column to float
    df = df.withColumn("TIME", F.col("TIME").cast(FloatType()))
    
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
