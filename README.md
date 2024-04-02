from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
import numpy as np
from scipy.stats import expon
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Generate Data for Shallow Slice Pilot") \
    .getOrCreate()

# Define mappings
improvement_mapping = {
    '1': 'Improving',
    '2': 'No Change',
    '3': 'Weakening'
}

compare_mapping = {
    '1': 'Above',
    '2': 'Same',
    '3': 'Below'
}

suppression_mapping = {
    '7': '',
    '2': 'S03',
    '3': 'S10',
    '6': 'S10',
    '901': 'S08'
}

# Assume EDWT_Indicators is a PySpark DataFrame
# Convert columns to appropriate data types
EDWT_Indicators = EDWT_Indicators.withColumn("COMPARE_IND_CODE", col("COMPARE_IND_CODE").cast("int"))
EDWT_Indicators = EDWT_Indicators.withColumn("IMPROVEMENT_IND_CODE", col("IMPROVEMENT_IND_CODE").cast("int"))
EDWT_Indicators = EDWT_Indicators.withColumn("INDICATOR_SUPPRESSION_CODE", col("INDICATOR_SUPPRESSION_CODE").cast("int"))

# Apply mappings to relevant columns
EDWT_Indicators = EDWT_Indicators.withColumn("compare_descriptor_code", col("COMPARE_IND_CODE").cast("string")).replace(compare_mapping, subset="compare_descriptor_code")
EDWT_Indicators = EDWT_Indicators.withColumn("improvement_descriptor_code", col("IMPROVEMENT_IND_CODE").cast("string")).replace(improvement_mapping, subset="improvement_descriptor_code")
EDWT_Indicators = EDWT_Indicators.withColumn("missing_reason_code", col("INDICATOR_SUPPRESSION_CODE").cast("string")).replace(suppression_mapping, subset="missing_reason_code")

# Define a UDF to generate data for a specific year
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType, StructType, StructField, FloatType

def generate_data_for_year(year):
    @udf(returnType=ArrayType(StructType([
        StructField("reporting_entity_code", StringType()),
        StructField("metric_result", FloatType()),
        StructField("metric_descriptor_group_code", StringType()),
        StructField("metric_descriptor_code", StringType()),
        StructField("missing_reason_code", StringType()),
        StructField("public_metric_result", StringType())
    ])))
    def generate_data(reporting_entity_code, improvement_descriptor_code, compare_descriptor_code, missing_reason_code):
        stacked_data = []
        
        # For Row 1
        if missing_reason_code != '999':
            stacked_data.append((reporting_entity_code, np.random.exponential(scale=5) + 1, '', '', missing_reason_code, ''))
        
        # For Row 2
        if improvement_descriptor_code != '999':
            stacked_data.append((reporting_entity_code, None, 'PerformanceTrend', improvement_descriptor_code, '', None))
        
        # For Row 3
        if compare_descriptor_code != '999':
            stacked_data.append((reporting_entity_code, None, 'PerformanceComparison', compare_descriptor_code, '', None))
        
        return stacked_data

    # Apply UDF to generate data
    result = EDWT_Indicators.select(
        "reporting_entity_code",
        "improvement_descriptor_code",
        "compare_descriptor_code",
        "missing_reason_code"
    ).rdd.flatMap(lambda x: generate_data(x[0], x[1], x[2], x[3])).toDF()

    result = result.withColumn("reporting_period_code", f"FY20{year}") \
                   .withColumn("reporting_entity_type_code", "ORG") \
                   .withColumn("indicator_code", "811") \
                   .withColumn("metric_code", "PCTL_90") \
                   .withColumn("breakdown_type_code_l1", "N/A") \
                   .withColumn("breakdown_value_code_l1", "N/A") \
                   .withColumn("breakdown_type_code_l2", "N/A") \
                   .withColumn("breakdown_value_code_l2", "N/A") \
                   .select(
                       "reporting_period_code",
                       "reporting_entity_code",
                       "reporting_entity_type_code",
                       "indicator_code",
                       "metric_code",
                       "breakdown_type_code_l1",
                       "breakdown_value_code_l1",
                       "breakdown_type_code_l2",
                       "breakdown_value_code_l2",
                       "metric_result",
                       "metric_descriptor_group_code",
                       "metric_descriptor_code",
                       "missing_reason_code",
                       "public_metric_result"
                   )

    return result

# Generate data for each year from FY2018 to FY2022
all_years_data = spark.createDataFrame(pd.concat([generate_data_for_year(year).toPandas() for year in range(18, 23)]))

# Write to CSV
all_years_data.coalesce(1).write.csv('811_agg.csv', header=True, mode='overwrite')

# Stop the Spark session
spark.stop()


or

import pandas as pd
import numpy as np
from scipy.stats import expon

# Define mapping for IMPROVEMENT_IND_CODE values
improvement_mapping = {
    '1': 'Improving',
    '2': 'No Change',
    '3': 'Weakening'
}

# Define mapping for COMPARE_IND_CODE values
compare_mapping = {
    '1': 'Above',
    '2': 'Same',
    '3': 'Below'
}

# Define mapping for INDICATOR_SUPPRESSION_CODE values 
suppression_mapping = {
    '7': '',
    '2': 'S03',
    '3': 'S10',
    '6': 'S10',
    '901': 'S08'
}

# Convert COMPARE_IND_CODE column to numeric type
EDWT_Indicators["COMPARE_IND_CODE"] = pd.to_numeric(EDWT_Indicators["COMPARE_IND_CODE"], errors='coerce')
EDWT_Indicators['compare_descriptor_code'] = EDWT_Indicators['COMPARE_IND_CODE'].astype(str).replace(compare_mapping)

# Convert IMPROVEMENT_IND_CODE column to numeric type
EDWT_Indicators["IMPROVEMENT_IND_CODE"] = pd.to_numeric(EDWT_Indicators["IMPROVEMENT_IND_CODE"], errors='coerce')
EDWT_Indicators['improvement_descriptor_code'] = EDWT_Indicators['IMPROVEMENT_IND_CODE'].astype(str).replace(improvement_mapping)

# Convert INDICATOR_SUPPRESSION_CODE column to numeric type
EDWT_Indicators["INDICATOR_SUPPRESSION_CODE"] = pd.to_numeric(EDWT_Indicators["INDICATOR_SUPPRESSION_CODE"], errors='coerce')
EDWT_Indicators['missing_reason_code'] = EDWT_Indicators['INDICATOR_SUPPRESSION_CODE'].astype(str).replace(suppression_mapping)

# Define a function to generate data for a specific year
def generate_data_for_year(year):
    EDWT_Indicator_File = EDWT_Indicators[["ORGANIZATION_ID", "improvement_descriptor_code", "compare_descriptor_code", "missing_reason_code"]]
    EDWT_Indicator_File.rename(columns={"ORGANIZATION_ID": "reporting_entity_code"}, inplace=True)

    np.random.seed(0)
    scale_param = 5
    size = len(EDWT_Indicator_File)

    random_data = expon.ppf(np.random.rand(size), scale=scale_param)
    random_data_shifted = random_data + 1

    stacked_data = []
    for index, row in EDWT_Indicator_File.iterrows():
        # For Row 1
        if row['missing_reason_code'] == '7':
            stacked_data.append([row['reporting_entity_code'], random_data_shifted[index].round(1), '', '', row['missing_reason_code'], random_data_shifted[index].round(1)])
        else:
            stacked_data.append([row['reporting_entity_code'], '', '', '', row['missing_reason_code'], ''])
        
        # For Row 2
        if row['improvement_descriptor_code'] != '999':
            stacked_data.append([row['reporting_entity_code'], '', 'PerformanceTrend', row['improvement_descriptor_code'], '', ''])
        
        # For Row 3
        if row['compare_descriptor_code'] != '999':
            stacked_data.append([row['reporting_entity_code'], '', 'PerformanceComparison', row['compare_descriptor_code'], '', ''])

    stacked_df = pd.DataFrame(stacked_data, columns=['reporting_entity_code', 'metric_result', 'metric_descriptor_group_code', 'metric_descriptor_code', 'missing_reason_code', 'public_metric_result'])

    stacked_df['reporting_period_code'] = 'FY20' + str(year)
    stacked_df['reporting_entity_type_code'] = 'ORG'
    stacked_df['indicator_code'] = '811'
    stacked_df['metric_code'] = 'PCTL_90'
    stacked_df['breakdown_type_code_l1'] = 'N/A'
    stacked_df['breakdown_value_code_l1'] = 'N/A'
    stacked_df['breakdown_type_code_l2'] = 'N/A'
    stacked_df['breakdown_value_code_l2'] = 'N/A'

    stacked_df = stacked_df[['reporting_period_code', 'reporting_entity_code', 'reporting_entity_type_code', \
                        'indicator_code', 'metric_code', 'breakdown_type_code_l1', 'breakdown_value_code_l1', 'breakdown_type_code_l2', \
                       'breakdown_value_code_l2', 'metric_result', 'metric_descriptor_group_code', \
                       'metric_descriptor_code', 'missing_reason_code', 'public_metric_result']]

    return stacked_df

# Generate data for each year from FY2018 to FY2022
all_years_data = pd.concat([generate_data_for_year(year) for year in range(18, 23)])

# Write to CSV
all_years_data.to_csv('811_agg.csv', index=False)
