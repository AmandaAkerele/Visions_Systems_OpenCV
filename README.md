from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import statsmodels.api as sm
import pandas as pd



# Function to perform linear regression and return results
def perform_ols(data):
    X = sm.add_constant(data.select('TIME').rdd.flatMap(lambda x: [float(i) for i in x]).collect())
    y = data.select('PERCENTILE_90').rdd.flatMap(lambda x: [float(i) for i in x]).collect()
    model = sm.OLS(y, X).fit()
    conf_int = model.conf_int(alpha=0.05)
    
    results = []
    for param_type, value in zip(['PARAMS', 'STDERR', 'T', 'PVALUE', 'L95B', 'U95B'],
                                 [model.params, model.bse, model.tvalues, model.pvalues,
                                  [conf_int['TIME'][0]], [conf_int['TIME'][1]]]):
        results.append({'CORP_ID': data.select('CORP_ID').first()[0], '_TYPE_': param_type, 'VALUE': value[0]})
    
    return results

# Iterate over groups, perform regression, and store results
all_results = []
for group_name in los_org_all_yr_b.select('CORP_ID').distinct().rdd.flatMap(lambda x: x).collect():
    group_data = los_org_all_yr_b.filter(F.col('CORP_ID') == group_name)
    all_results.extend(perform_ols(group_data))

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


solve the error below 

---------------------------------------------------------------------------
IndexError                                Traceback (most recent call last)
/tmp/ipykernel_324/1957905950.py in <cell line: 25>()
     25 for group_name in los_org_all_yr_b.select('CORP_ID').distinct().rdd.flatMap(lambda x: x).collect():
     26     group_data = los_org_all_yr_b.filter(F.col('CORP_ID') == group_name)
---> 27     all_results.extend(perform_ols(group_data))
     28 
     29 # Convert to DataFrame and analyze results

/tmp/ipykernel_324/1957905950.py in perform_ols(data)
     16     for param_type, value in zip(['PARAMS', 'STDERR', 'T', 'PVALUE', 'L95B', 'U95B'],
     17                                  [model.params, model.bse, model.tvalues, model.pvalues,
---> 18                                   [conf_int['TIME'][0]], [conf_int['TIME'][1]]]):
     19         results.append({'CORP_ID': data.select('CORP_ID').first()[0], '_TYPE_': param_type, 'VALUE': value[0]})
     20 

IndexError: only integers, slices (`:`), ellipsis (`...`), numpy.newaxis (`None`) and integer or boolean arrays are valid indices


