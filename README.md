from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
import statsmodels.api as sm

# Assuming 'los_org_all_yr_b' is a PySpark DataFrame
# Convert columns to numeric and drop NaNs
columns_to_convert = ['PERCENTILE_90', 'TIME']
for col in columns_to_convert:
    los_org_all_yr_b = los_org_all_yr_b.withColumn(col, F.col(col).cast('float'))
los_org_all_yr_b = los_org_all_yr_b.dropna(subset=columns_to_convert)

# Create a GroupBy object and prepare DataFrame for results
grouped = los_org_all_yr_b.groupBy('CORP_ID')

# Iterate, perform regression, and store results
all_results = []

for group_name, group_data in grouped:
    X = sm.add_constant(group_data.select('TIME').rdd.flatMap(lambda x: x).collect())
    y = group_data.select('PERCENTILE_90').rdd.flatMap(lambda x: x).collect()
    model = sm.OLS(y, X).fit()
    conf_int = model.conf_int(alpha=0.05)

    for param_type, values in zip(['PARAMS', 'STDERR', 'T', 'PVALUE', 'L95B', 'U95B'],
                                  [model.params, model.bse, model.tvalues, model.pvalues,
                                   [conf_int['TIME'][0]], [conf_int['TIME'][1]]]):
        all_results.append({'CORP_ID': group_name, '_TYPE_': param_type, 'VALUE': values[0]})

# Convert to DataFrame and analyze results
los_org_trend_a = spark.createDataFrame(all_results)
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
