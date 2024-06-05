from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

# Initialize Spark session
spark = SparkSession.builder.appName("ResolveAmbiguity").getOrCreate()

# Sample data for df_org_dim
data_df_org_dim = [
    (1, 'A', 29061), (2, 'B', 48006), (3, 'C', 48008), (4, 'D', 48015)
]
columns_df_org_dim = ['org_id', 'corp_id', 'FACILITY_AM_CARE_NUM']
df_org_dim = spark.createDataFrame(data_df_org_dim, columns_df_org_dim)

# Sample data for ed_nodup_nosb_22
data_ed_nodup_nosb_22 = [
    (1,), (2,), (3,), (4,)
]
columns_ed_nodup_nosb_22 = ['org_id']
ed_nodup_nosb_22 = spark.createDataFrame(data_ed_nodup_nosb_22, columns_ed_nodup_nosb_22)

# Prepare data for stand alones
alone = spark.createDataFrame(
    [
        (99012, 29061, 1), (80335, 48006, 1), (80335, 48008, 1), (80337, 48015, 1), 
        (80337, 48022, 1), (80337, 48023, 1), (80337, 48024, 1), (80345, 48029, 1), 
        (80338, 48032, 1), (80339, 48037, 1), (80340, 48039, 1), (80344, 48044, 1), 
        (80341, 48053, 1), (80345, 48063, 1), (80347, 48076, 1), (80348, 48083, 1), 
        (80348, 48085, 1), (80348, 48086, 1), (80338, 48116, 1), (80347, 48117, 1), 
        (80337, 48120, 1), (80338, 48121, 1), (5160, 54242, 1), (7043, 71117, 1), 
        (7070, 71163, 1), (973, 88050, 1), (1006, 88080, 1), (986, 88132, 1), 
        (20390, 88142, 1), (99718, 88149, 1), (99724, 88155, 1), (20282, 88349, 1), 
        (20400, 88350, 1), (99725, 88391, 1), (99726, 88394, 1), (80226, 88578, 1), 
        (80517, 88595, 1), (99768, 88922, 1)
    ],
    ['CORP_ID', 'FACILITY_AM_CARE_NUM', 'NACRS_ED_FLG']
)

# Join df_org_dim with ed_nodup_nosb_22 based on org_id
df_fac = df_org_dim.join(ed_nodup_nosb_22, 'org_id')

# Join df_fac with alone to add NACRS_ED_FLG column, resolving ambiguity by aliasing
df_fac = df_fac.alias('df_fac').join(
    alone.alias('alone'), 
    (col('df_fac.corp_id') == col('alone.CORP_ID')) & (col('df_fac.FACILITY_AM_CARE_NUM') == col('alone.FACILITY_AM_CARE_NUM')), 
    how='left'
).select(
    col('df_fac.*'), 
    col('alone.NACRS_ED_FLG')
)

# Fill null NACRS_ED_FLG values with 0
df_fac = df_fac.fillna({'NACRS_ED_FLG': 0})

# Show df_fac with NACRS_ED_FLG
df_fac.show()

# Columns to keep (assuming you want to keep all columns from df_fac)
columns_to_keep = df_fac.columns

# Create DataFrame t3 based on condition
t3 = df_fac.filter(df_fac['NACRS_ED_FLG'] == 1).select(columns_to_keep)
t3 = t3.withColumn('TYPE', lit('SL')).withColumn('IND', lit(''))

# Show t3 DataFrame
t3.show()
