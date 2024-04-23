using this code below 

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, collect_list, when, lit
from pyspark.sql.types import FloatType, ArrayType
from scipy.stats import linregress

# # Assume los_org_all_yr_b is already loaded as a DataFrame
# # Convert columns to numeric and drop NaNs
los_org_all_yr_b = los_org_all_yr_b.withColumn("PERCENTILE_90", col("PERCENTILE_90").cast("float"))
los_org_all_yr_b = los_org_all_yr_b.withColumn("TIME", col("TIME").cast("float"))
los_org_all_yr_b = los_org_all_yr_b.na.drop(subset=["PERCENTILE_90", "TIME"])

# Group by 'CORP_ID' and collect necessary columns for regression
grouped_data = los_org_all_yr_b.groupBy("CORP_ID").agg(
    collect_list("TIME").alias("times"),
    collect_list("PERCENTILE_90").alias("percentiles")
)

# Define a UDF for performing linear regression using scipy
def perform_regression(times, percentiles):
    slope, intercept, r_value, p_value, std_err = linregress(times, percentiles)
    return [float(slope), float(intercept), float(p_value), float(std_err)]

regression_udf = udf(perform_regression, ArrayType(FloatType()))

# Apply the UDF to compute regression parameters
results = grouped_data.withColumn("regression_results", regression_udf(col("times"), col("percentiles")))

# Expand the results into separate columns
results = results.select(
    "CORP_ID",
    results.regression_results[0].alias("slope"),
    results.regression_results[1].alias("intercept"),
    results.regression_results[2].alias("p_value"),
    results.regression_results[3].alias("std_err")
)

# Define improvement indicators based on p-value and slope
results = results.withColumn(
    "IMPROVEMENT_IND_CODE",
    when((col("p_value") < 0.05) & (col("slope") > 0), lit("001"))
    .when((col("p_value") < 0.05) & (col("slope") < 0), lit("003"))
    .otherwise(lit("002"))
)

# Define descriptions
results = results.withColumn(
    "IMPROVEMENT_IND_E_DESC",
    when(col("IMPROVEMENT_IND_CODE") == "001", lit("Improving"))
    .when(col("IMPROVEMENT_IND_CODE") == "003", lit("Weakening"))
    .otherwise(lit("No Change"))
)

# Show final results
results.show()

# Assuming you might also want to save or further process the results
# results.write.format("parquet").save("/path/to/output")

ensure that the dataframe and functionality in the code below is incorporated in the code above. 

Note the code above is in pyspark and the code below is using the pandas library so ensure that the code below is converted to the code above maintaining functionality and dataframe names 


# Convert columns to numeric, just in case they are not
tpia_org_all_yr_b['PERCENTILE_90'] = pd.to_numeric(tpia_org_all_yr_b['PERCENTILE_90'], errors='coerce')
tpia_org_all_yr_b['TIME'] = pd.to_numeric(tpia_org_all_yr_b['TIME'], errors='coerce')

# Drop rows where either 'percentile_90' or 'time' is NaN after the conversion
tpia_org_all_yr_b.dropna(subset=['PERCENTILE_90', 'TIME'], inplace=True)

# Create a GroupBy object for the 'CORP_ID' variable
grouped = tpia_org_all_yr_b.groupby('CORP_ID')

# Create empty DataFrame to store results
tpia_org_trend = pd.DataFrame()

# Iterate over groups and perform regression
for group_name, group_data in grouped:
    X = sm.add_constant(group_data['TIME'])
    y = group_data['PERCENTILE_90']
    model = sm.OLS(y, X)
    results = model.fit()


   # Prepare list to store results
all_results = []

# Iterate over each group and perform regression
for group_name, group_data in grouped:
    X = sm.add_constant(group_data['TIME'])
    y = group_data['PERCENTILE_90']
    model = sm.OLS(y, X)
    results = model.fit()

    # Extract confidence intervals
    conf_int = results.conf_int(alpha=0.05)
    l95b = conf_int.loc['TIME', 0]  # Lower bound for 'time'
    u95b = conf_int.loc['TIME', 1]  # Upper bound for 'time'

    # Extracting results and appending to the list
    for param_type, values in zip(['PARAMS', 'STDERR', 'T', 'PVALUE', 'L95B', 'U95B'],
                                  [results.params, results.bse, results.tvalues, results.pvalues,
                                   pd.Series([l95b], index=['TIME']), pd.Series([u95b], index=['TIME'])]):
        row = {'CORP_ID': group_name, '_TYPE_': param_type}
        row.update({name: values[name] for name in values.index if name in values})
        all_results.append(row)

# Convert results to DataFrame
tpia_org_trend = pd.DataFrame(all_results)

# Step 1: Creating tpia_org_trend_a with a new variable 'linr'
tpia_org_trend_a = tpia_org_trend.copy()
tpia_org_trend_a['linr'] = 0
tpia_org_trend_a.loc[(tpia_org_trend_a['_TYPE_'] == 'PVALUE') & (tpia_org_trend_a['TIME'].notna()) & (tpia_org_trend_a['TIME'] < 0.05), 'linr'] = 1


# Step 2: Creating subsets based on _TYPE_
tpia_org_p_val = tpia_org_trend_a[tpia_org_trend_a['_TYPE_'] == 'PVALUE'][['CORP_ID', 'linr']]
tpia_org_parms = tpia_org_trend_a[tpia_org_trend_a['_TYPE_'] == 'PARAMS'][['CORP_ID', 'TIME']]



# Steps 3 and 4: Sorting and merging the datasets
tpia_org_p_val_parms = pd.merge(tpia_org_p_val.sort_values('CORP_ID'),
                     tpia_org_parms.sort_values('CORP_ID'),
                     on='CORP_ID')

# Step 5: Assigning new variables based on conditions

def apply_trend(row):
    if row['linr'] == 0:
        row['IMPROVEMENT_IND_CODE'] = '002'
        row['IMPROVEMENT_IND_E_DESC'] = 'No Change'
    elif row['linr'] ==1 and row['TIME'] > 0:
        row['IMPROVEMENT_IND_CODE'] = '003'
        row['IMPROVEMENT_IND_E_DESC'] = 'Weakening'
    elif row['linr'] ==1 and row['TIME'] < 0:
        row['IMPROVEMENT_IND_CODE'] = '001'
        row['IMPROVEMENT_IND_E_DESC'] = 'Improving'
    return row


#tpia_org_p_val_parms_aa=tpia_org_p_val_parms.apply(apply_trend, axis=1)
mask = tpia_org_p_val_parms['linr'] == 1
tpia_org_p_val_parms.loc[mask & (tpia_org_p_val_parms['TIME'] > 0), ['IMPROVEMENT_IND_CODE', 'IMPROVEMENT_IND_E_DESC']] = ['003', 'Weakening']
tpia_org_p_val_parms.loc[mask & (tpia_org_p_val_parms['TIME'] < 0), ['IMPROVEMENT_IND_CODE', 'IMPROVEMENT_IND_E_DESC']] = ['001', 'Improving']

# Step 6: Assuming ed_nacrs_flg_1 is another DataFrame you have
# Replace 'ed_nacrs_flg_1' with the actual DataFrame
tpia_org_trend_b = tpia_org_p_val_parms[~tpia_org_p_val_parms['CORP_ID'].isin(ed_nacrs_flg_1_SL['CORP_ID'])]
