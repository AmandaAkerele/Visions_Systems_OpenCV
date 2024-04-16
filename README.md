from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.classification of LogisticRegression
from pyspark.ml.feature import VectorAssembler
import numpy as np

# Initialize Spark session
spark = SparkSession.builder.appName("Logistic Regression Analysis").getOrCreate()

# Assuming 'los_org_all_yr_b' is a DataFrame and already loaded
# Make sure the 'PERCENTILE_90' and 'TIME' columns are of numeric type
los_org_all_yr_b = los_org_all_yr_b.withColumn('PERCENTILE_90', F.col('PERCENTILE_90').cast('float')) \
                                   .withColumn('TIME', F.col('TIME').cast('float'))

# Drop any rows with NaN values in these columns
los_org_all_yr_b = los_org_all_yr_b.dropna(subset=['PERCENTILE_90', 'TIME'])

# Collecting distinct CORP_IDs to handle groups individually
corp_ids = los_org_all_yr_b.select("CORP_ID").distinct().collect()

all_results = []

# Looping over each CORP_ID to apply logistic regression
for corp_id_row in corp_ids:
    corp_id = corp_id_row["CORP_ID"]
    group_data = los_org_all_yr_b.filter(F.col("CORP_ID") == corp_id)
    result = perform_logistic(group_data)
    all_results.append(result)

# Convert results into a DataFrame
los_org_trend = spark.createDataFrame(all_results)

# Further processing for creating variables and subsetting
los_org_trend = los_org_trend.withColumn('linr', (F.col('PVALUE') < 0.05).cast('int'))

# Filtering and sorting
los_org_p_val = los_org_trend.filter(F.col('_TYPE_') == 'PVALUE').select('CORP_ID', 'linr')
los_org_parms = los_org_trend.filter(F.col('_TYPE_') == 'PARAMS').select('CORP_ID', 'VALUE')
los_org_p_val_parms = los_org_p_val.join(los_org_parms, 'CORP_ID', 'inner').orderBy('CORP_ID')

# Variable assignment based on conditions
mask = F.col('linr') == 1
los_org_p_val_parms = los_org_p_val_parms.withColumn('IMPROVEMENT_IND_CODE', F.when(mask & (F.col('VALUE') > 0), '003').otherwise('002'))
los_org_p_val_parms = los_org_p_val_parms.withColumn('IMPROVEMENT_IND_E_DESC', F.when(mask & (F.col('VALUE') > 0), 'Weakening').when(mask & (F.col('VALUE') < 0), 'Improving').otherwise('No Change'))

# Assuming another DataFrame for final filtering
ed_nacrs_flg_1_SL_corp_ids = ed_nacrs_flg_1_SL.select('CORP_ID')
los_org_trend_b = los_org_p_val_parms.join(ed_nacrs_flg_1_SL_corp_ids, 'CORP_ID', 'left_anti')
