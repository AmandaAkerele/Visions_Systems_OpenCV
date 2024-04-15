from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import statsmodels.api as sm

# Initialize Spark session
spark = SparkSession.builder.appName("Convert to PySpark").getOrCreate()

# Assuming 'los_org_all_yr_b' is a PySpark DataFrame
# Convert columns to numeric and drop NaNs
columns_to_convert = ['PERCENTILE_90', 'TIME']
for col in columns_to_convert:
    los_org_all_yr_b = los_org_all_yr_b.withColumn(col, F.col(col).cast('float'))
los_org_all_yr_b = los_org_all_yr_b.dropna(subset=columns_to_convert)

# Define a function to perform linear regression and return results
def perform_ols_sql(group_data):
    X = sm.add_constant(group_data.select('TIME').collect())
    y = group_data.select('PERCENTILE_90').collect()
    model = sm.OLS(y, X).fit()
    conf_int = model.conf_int(alpha=0.05)
    
    results = []
    for param_type, value in zip(['PARAMS', 'STDERR', 'T', 'PVALUE', 'L95B', 'U95B'],
                                 [model.params[1], model.bse[1], model.tvalues[1], model.pvalues[1],
                                  conf_int[1][0], conf_int[1][1]]):
        results.append({'CORP_ID': group_data.select('CORP_ID').first()[0], '_TYPE_': param_type, 'VALUE': value})
    
    return results

# Register UDF to perform OLS
spark.udf.register("perform_ols_sql", perform_ols_sql)

# Group by 'CORP_ID', perform linear regression, and aggregate results
los_org_trend_a = los_org_all_yr_b.groupBy('CORP_ID').agg(F.expr("perform_ols_sql(collect_list(TIME), collect_list(PERCENTILE_90)) AS ols_result"))

# Explode the aggregated results
los_org_trend_a = los_org_trend_a.select('CORP_ID', F.explode('ols_result').alias('result')) \
                                 .select('CORP_ID', 'result._TYPE_', 'result.VALUE')

# Create 'linr' column
los_org_trend_a = los_org_trend_a.withColumn('linr', (F.col('_TYPE_') == 'PVALUE') & (F.col('VALUE') < 0.05).cast('int'))

# Create subsets and merge
p_val = los_org_trend_a.filter(F.col('_TYPE_') == 'PVALUE').select('CORP_ID', 'linr')
parms = los_org_trend_a.filter(F.col('_TYPE_') == 'PARAMS').select('CORP_ID', 'VALUE')
merged = p_val.join(parms, 'CORP_ID', 'inner')

# Define improvement indicators
merged = merged.withColumn('IMPROVEMENT_IND_CODE', F.lit('002')) \
               .withColumn('IMPROVEMENT_IND_E_DESC', F.lit('No Change'))

mask = (F.col('linr') == 1)
merged = merged.withColumn('IMPROVEMENT_IND_CODE', F.when(mask & (F.col('VALUE') > 0), '003').otherwise(F.col('IMPROVEMENT_IND_CODE'))) \
               .withColumn('IMPROVEMENT_IND_E_DESC', F.when(mask & (F.col('VALUE') > 0), 'Weakening') \
                                                          .when(mask & (F.col('VALUE') < 0), 'Improving') \
                                                          .otherwise(F.col('IMPROVEMENT_IND_E_DESC')))

# Final filtering 
ed_nacrs_flg_1_22_corp_ids = ed_nacrs_flg_1_22.select('CORP_ID')
los_org_trend_b = merged.join(ed_nacrs_flg_1_22_corp_ids, 'CORP_ID', 'left_anti')
