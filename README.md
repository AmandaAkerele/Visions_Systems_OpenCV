from pyspark.sql.functions import col

# Assuming SUBMISSION_FISCAL_YEAR represents the year
df_2022 = tpia_supp_org_ucc_22.filter(col('SUBMISSION_FISCAL_YEAR') == 2022)

# Show the filtered DataFrame
df_2022.show()
