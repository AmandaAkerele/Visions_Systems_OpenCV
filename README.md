]from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.linalg import Vectors

spark = SparkSession.builder.appName("Logistic Regression Analysis").getOrCreate()

# Assume los_org_all_yr_b and ed_nacrs_flg_1 are already loaded as DataFrames
# Conversion of column types and dropping NaNs are handled as per the provided code
los_org_all_yr_b = los_org_all_yr_b.withColumn("PERCENTILE_90", col("PERCENTILE_90").cast("float"))
los_org_all_yr_b = los_org_all_yr_b.withColumn("TIME", col("TIME").cast("float"))
los_org_all_yr_b = los_org_all_yr_b.na.drop(subset=["PERCENTILE_90", "TIME"])

# Assume you've already transformed "PERCENTILE_90" to a binary variable for logistic regression
# For example, we might decide that percentile values above 80 are 1, others are 0
los_org_all_yr_b = los_org_all_yr_b.withColumn("label", when(col("PERCENTILE_90") >= 80, 1).otherwise(0))

# Prepare data for logistic regression
assembler = VectorAssembler(inputCols=["TIME"], outputCol="features")
training_data = assembler.transform(los_org_all_yr_b)

# Fit the logistic regression model
lr = LogisticRegression(featuresCol="features", labelCol="label")
model = lr.fit(training_data)

# Make predictions
predictions = model.transform(training_data)

# Select example rows to display.
results = predictions.select("CORP_ID", "features", "label", "probability", "prediction")

# Define improvement indicators based on prediction results
results = results.withColumn(
    "IMPROVEMENT_IND_CODE",
    when(col("prediction") == 1, lit("001")).otherwise(lit("002")),
)
results = results.withColumn(
    "IMPROVEMENT_IND_E_DESC",
    when(col("IMPROVEMENT_IND_CODE") == "001", lit("Improving")).otherwise(lit("No Change"))
)

# Join with another DataFrame to exclude certain IDs (using left anti-join)
results = results.join(
    ed_nacrs_flg_1_SL,
    results['CORP_ID'] == ed_nacrs_flg_1_SL['CORP_ID'],
    'left_anti'
)

# Show results
results.show()

# Optional: Save results
# results.write.format("parquet").save("/path/to/output")
