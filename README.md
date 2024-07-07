from pyspark.sql.functions import year

# Assuming there is a date column, filter for the year 2022
df_2022 = tpia_supp_org_ucc_22.filter(year(tpia_supp_org_ucc_22['date_column']) == 2022)

# Show the filtered DataFrame
df_2022.show()
