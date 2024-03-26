# Rename columns for los_org_cmp
los_org_cmp_a = los_org_cmp.withColumnRenamed("0.2", "20th_Percentile").withColumnRenamed("0.8", "80th_Percentile")

# Rename columns for tpia_org_cmp
tpia_org_cmp_a = tpia_org_cmp.withColumnRenamed("0.2", "20th_Percentile").withColumnRenamed("0.8", "80th_Percentile")

# Rename columns for los_reg_cmp
los_reg_cmp_a = los_reg_cmp.withColumnRenamed("0.2", "20th_Percentile").withColumnRenamed("0.8", "80th_Percentile")

# Rename columns for tpia_reg_cmp
tpia_reg_cmp_a = tpia_reg_cmp.withColumnRenamed("0.2", "20th_Percentile").withColumnRenamed("0.8", "80th_Percentile")


from pyspark.sql import functions as F

def apply_conditions(df):
    conditions = [
        (F.col('PERCENTILE_90') < F.col('20th_Percentile')),
        (F.col('PERCENTILE_90') >= F.col('20th_Percentile')) & (F.col('PERCENTILE_90') <= F.col('80th_Percentile')),
        (F.col('PERCENTILE_90') > F.col('80th_Percentile'))
    ]

    values = ['001', '002', '003']
    descriptions = ['Above average performance', 'Same as average', 'Below average performance']

    # Apply conditions
    df = df.withColumn('COMPARE_IND_CODE', F.when(conditions[0], values[0])
                                           .when(conditions[1], values[1])
                                           .when(conditions[2], values[2])
                                           .otherwise(''))

    df = df.withColumn('COMPARE_IND_E_DESC', F.when(conditions[0], descriptions[0])
                                               .when(conditions[1], descriptions[1])
                                               .when(conditions[2], descriptions[2])
                                               .otherwise(''))

    # Sort the DataFrame by CORP_ID or REGION_ID as needed 
    if 'CORP_ID' in df.columns:
        df = df.orderBy('CORP_ID')
    elif 'REGION_ID' in df.columns:
        df = df.orderBy('REGION_ID')

    return df

# Apply conditions for each DataFrame
los_org_cmp_a = apply_conditions(los_org_cmp_a)
tpia_org_cmp_a = apply_conditions(tpia_org_cmp_a)
los_reg_cmp_a = apply_conditions(los_reg_cmp_a)
tpia_reg_cmp_a = apply_conditions(tpia_reg_cmp_a)
