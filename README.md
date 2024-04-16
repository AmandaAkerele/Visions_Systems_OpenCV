from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import FloatType, IntegerType
import scipy.stats as stats

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
    df = spark.createDataFrame(zip(row['TIME'], row['PERCENTILE_90']), ["TIME", "PERCENTILE_90"]).withColumn("const", F.lit(1))
    
    # Cast 'TIME' and 'PERCENTILE_90' columns to float
    df = df.withColumn("TIME", F.col("TIME").cast(FloatType()))
    df = df.withColumn("PERCENTILE_90", F.col("PERCENTILE_90").cast(FloatType()))
    
    # VectorAssembler
    assembler = VectorAssembler(inputCols=["TIME", "const"], outputCol="features")
    df = assembler.transform(df)
    
    # Linear Regression
    lr = LinearRegression(featuresCol='features', labelCol='PERCENTILE_90', regParam=0.01)
    model = lr.fit(df)
    
    # Extract coefficients and confidence intervals
    params = model.coefficients.toArray()
    std_err = model.summary.coefficientStandardErrors
    t_values = model.summary.tValues

    # Compute p-values manually
    degrees_of_freedom = model.summary.degreesOfFreedom
    p_values = [2 * (1 - stats.t.cdf(abs(t), degrees_of_freedom)) for t in t_values]

    conf_int = model.summary.confidenceIntervalTable()

    for param_type, values in zip(['PARAMS', 'STDERR', 'T', 'PVALUE', 'L95B', 'U95B'],
                                  [params, std_err, t_values, p_values,
                                   [conf_int[0][1]], [conf_int[1][1]]]):
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

