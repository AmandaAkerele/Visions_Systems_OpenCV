from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline

spark = SparkSession.builder.appName("Logistic Regression Analysis for TPia Org").getOrCreate()

# Assume tpia_org_all_yr_b is already loaded as a DataFrame
# Convert columns to numeric and ensure there are no NaNs
tpia_org_all_yr_b = spark.table("tpia_org_all_yr_b")
tpia_org_all_yr_b = tpia_org_all_yr_b.withColumn("PERCENTILE_90", col("PERCENTILE_90").cast("float"))
tpia_org_all_yr_b = tpia_org_all_yr_b.withColumn("TIME", col("TIME").cast("float"))
tpia_org_all_yr_b = tpia_org_all_yr_b.na.drop(subset=["PERCENTILE_90", "TIME"])

# Convert PERCENTILE_90 into a binary label for logistic regression
tpia_org_all_yr_b = tpia_org_all_yr_b.withColumn("label", when(col("PERCENTILE_90") >= 0.9, 1).otherwise(0))

# Assemble features
assembler = VectorAssembler(inputCols=["TIME"], outputCol="features")

# Standardize features
scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")

# Logistic Regression
lr = LogisticRegression(featuresCol="scaledFeatures", labelCol="label")

# Create a Pipeline
pipeline = Pipeline(stages=[assembler, scaler, lr])

# Fit the model
model = pipeline.fit(tpia_org_all_yr_b)

# Make predictions
predictions = model.transform(tpia_org_all_yr_b)

# Select example rows to display.
results = predictions.select("CORP_ID", "label", "prediction", "probability")
results = results.withColumn("prob", col("probability").getItem(1))

# Define improvement indicators based on prediction probability
results = results.withColumn(
    "IMPROVEMENT_IND_CODE",
    when(col("prob") > 0.5, lit("Improving")).otherwise(lit("No Change"))
)

# Assuming ed_nacrs_flg_1_SL is another DataFrame
ed_nacrs_flg_1_SL = spark.table("ed_nacrs_flg_1_SL")
tpia_org_trend_b = results.join(
    ed_nacrs_flg_1_SL,
    results["CORP_ID"] == ed_nacrs_flg_1_SL["CORP_ID"],
    "left_anti"
)

# Show and possibly save the results
tpia_org_trend_b.show()
# tpia_org_trend_b.write.format("parquet").save("/path/to/output")

# Stop the session
spark.stop()


orrr


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, collect_list
from pyspark.sql.types import FloatType, ArrayType, DoubleType
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors
from pyspark.ml import Pipeline

spark = SparkSession.builder.appName("Linear Regression Analysis").getOrCreate()

# Assume tpia_org_all_yr_b is already loaded as a DataFrame
tpia_org_all_yr_b = spark.table("tpia_org_all_yr_b")

# Convert columns to numeric and drop NaNs
tpia_org_all_yr_b = tpia_org_all_yr_b.withColumn("PERCENTILE_90", col("PERCENTILE_90").cast("float"))
tpia_org_all_yr_b = tpia_org_all_yr_b.withColumn("TIME", col("TIME").cast("float"))
tpia_org_all_yr_b = tpia_org_all_yr_b.na.drop(subset=["PERCENTILE_90", "TIME"])

# Prepare data for linear regression
assembler = VectorAssembler(inputCols=["TIME"], outputCol="features")
tpia_org_all_yr_b = assembler.transform(tpia_org_all_yr_b)
tpia_org_all_yr_b = tpia_org_all_yr_b.withColumn("label", col("PERCENTILE_90"))

# Group data by 'CORP_ID' and apply regression within each group
grouped_data = tpia_org_all_yr_b.groupBy("CORP_ID").agg(
    collect_list("features").alias("features"),
    collect_list("label").alias("labels")
)

# Define a UDF to perform linear regression per group
def linear_regression(features, labels):
    from pyspark.ml.linalg import Vectors
    from pyspark.ml.regression import LinearRegression
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml import Pipeline
    from pyspark.sql import Row
    import pandas as pd

    if len(features) < 2:  # Not enough data to perform regression
        return [None, None, None, None]
    df = pd.DataFrame({"features": features, "label": labels})
    local_df = spark.createDataFrame(df)
    lr = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)
    model = lr.fit(local_df)
    return [float(model.coefficients[0]), float(model.intercept), model.summary.pValues[0], model.summary.tValues[0]]

linear_regression_udf = udf(linear_regression, ArrayType(DoubleType()))

# Apply the UDF
results = grouped_data.withColumn("regression_results", linear_regression_udf(col("features"), col("labels")))

# Expand the results into separate columns and define improvement indicators
results = results.select(
    "CORP_ID",
    col("regression_results").getItem(0).alias("slope"),
    col("regression_results").getItem(1).alias("intercept"),
    col("regression_results").getItem(2).alias("p_value"),
    col("regression_results").getItem(3).alias("t_value")
)

# Assuming ed_nacrs_flg_1_SL is loaded as another DataFrame
ed_nacrs_flg_1_SL = spark.table("ed_nacrs_flg_1_SL")
tpia_org_trend_b = results.join(
    ed_nacrs_flg_1_SL,
    results["CORP_ID"] == ed_nacrs_flg_1_SL["CORP_ID"],
    "left_anti"
)

# Show the results
tpia_org_trend_b.show()

# Optionally save the results
# tpia_org_trend_b.write.format("parquet").save("/path/to/output")

# Stop the session
spark.stop()


or 

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, collect_list, udf
from pyspark.sql.types import DoubleType, ArrayType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.linalg import Vectors
from pyspark.ml import Pipeline

spark = SparkSession.builder.appName("Logistic Regression Analysis").getOrCreate()

# Load the DataFrame
tpia_org_all_yr_b = spark.table("tpia_org_all_yr_b")

# Convert columns to numeric and drop NaNs
tpia_org_all_yr_b = tpia_org_all_yr_b.withColumn("PERCENTILE_90", col("PERCENTILE_90").cast("float"))
tpia_org_all_yr_b = tpia_org_all_yr_b.withColumn("TIME", col("TIME").cast("float"))
tpia_org_all_yr_b = tpia_org_all_yr_b.na.drop(subset=["PERCENTILE_90", "TIME"])

# Assume that we convert PERCENTILE_90 into a binary label for logistic regression
# For example, setting a threshold at 80% percentile as a cut-off for classification
tpia_org_all_yr_b = tpia_org_all_yr_b.withColumn("label", when(col("PERCENTILE_90") >= 80, 1).otherwise(0))

# Assemble features into a vector
assembler = VectorAssembler(inputCols=["TIME"], outputCol="features")

# Apply the assembler
featured_data = assembler.transform(tpia_org_all_yr_b)

# Group data by 'CORP_ID' and collect features for logistic regression
grouped_data = featured_data.groupBy("CORP_ID").agg(
    collect_list("features").alias("features"),
    collect_list("label").alias("labels")
)

# Define a UDF to perform logistic regression per group
def logistic_regression(features, labels):
    if not features or not labels:
        return [None, None, None]
    local_df = spark.createDataFrame(list(zip(features, labels)), ["features", "label"])
    lr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)
    model = lr.fit(local_df)
    return [float(model.coefficients[0]), float(model.intercept), model.summary.areaUnderROC]

logistic_regression_udf = udf(logistic_regression, ArrayType(DoubleType()))

# Apply the UDF to compute logistic regression parameters
results = grouped_data.withColumn("regression_results", logistic_regression_udf(col("features"), col("labels")))

# Expand the results into separate columns
results = results.select(
    "CORP_ID",
    col("regression_results").getItem(0).alias("coefficient"),
    col("regression_results").getItem(1).alias("intercept"),
    col("regression_results").getItem(2).alias("areaUnderROC")
)

# Assuming ed_nacrs_flg_1_SL is loaded as another DataFrame for exclusion filtering
ed_nacrs_flg_1_SL = spark.table("ed_nacrs_flg_1_SL")
tpia_org_trend_b = results.join(
    ed_nacrs_flg_1_SL,
    results["CORP_ID"] == ed_nacrs_flg_1_SL["CORP_ID"],
    "left_anti"
)

# Show the results
tpia_org_trend_b.show()

# Stop the session
spark.stop()
