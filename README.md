# now lets make it work 

def update_hsp_ind_organization_fact34(df):
    conditions = [
        ((df['FISCAL_YEAR_WH_ID'] == 22) &
         (df['ORGANIZATION_ID'].isin([600, 2000, 9054, 9058, 80286, 80287, 81404]))),
        ((df['FISCAL_YEAR_WH_ID'] < 22) & (df['IMPROVEMENT_IND_CODE'].isin(['001', '002', '003']))),
        ((df['FISCAL_YEAR_WH_ID'] < 22) & (df['COMPARE_IND_CODE'].isin(['001', '002', '003']))),
        ((df['FISCAL_YEAR_WH_ID'] == 22) & (df['ORGANIZATION_ID'].isin(organization_ids_to_updateTPIA))),
        ((df['FISCAL_YEAR_WH_ID'] == 22) & (df['ORGANIZATION_ID'].isin(DQ_TPIA))),
        ((df['FISCAL_YEAR_WH_ID'] == 22) & (df['ORGANIZATION_ID'].isin(PS)))
    ]

    suppression_codes = ['901', '002', '002', '002', '006']
    indicator_values = [None, None, None, None, None]

    for condition, suppression_code, indicator_value in zip(conditions, suppression_codes, indicator_values):
        df.loc[condition, ['INDICATOR_SUPPRESSION_CODE', 'TOP_PERFORMER_IND_CODE', 'IMPROVEMENT_IND_CODE', 'COMPARE_IND_CODE']] = [suppression_code, '999', '999', '999']
        df.loc[condition, 'INDICATOR_VALUE'] = indicator_value

    return df

hsp_ind_organization_fact34 = update_hsp_ind_organization_fact34(hsp_ind_organization_fact_tpia_34_a)

# Specify the columns in the desired order
columns_to_keep = [
    "ORGANIZATION_ID",	"INDICATOR_CODE",	"FISCAL_YEAR_WH_ID",	"SEX_WH_ID",	"INDICATOR_SUPPRESSION_CODE",	"TOP_PERFORMER_IND_CODE",	"IMPROVEMENT_IND_CODE",	"COMPARE_IND_CODE",	"INDICATOR_VALUE",	"UPPER_CONFIDENCE_INTERVAL",	"LOWER_CONFIDENCE_INTERVAL",	"NUMERATOR_VALUE",	"DENOMINATOR_VALUE",	"FUNNEL_PLOT_X_VALUE",	"DATA_PERIOD_CODE",	"DATA_PERIOD_TYPE_CODE"]

# Select the desired columns from the DataFrame
hsp_ind_organization_fact34 = hsp_ind_organization_fact34[columns_to_keep]

# Display the resulting DataFrame
display(hsp_ind_organization_fact34)

