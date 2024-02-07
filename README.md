

Hi Team

Trust you are doing great.
I would like to make enquiries about the stage and application status for a referral I put in place for the position below: 
J0124-0054- Consultant, Procurement

Looking forward to hearing from you on if there is any update. 

Thank You
Amanda

























or 
def update_hsp_ind_organization_fact34(df):
    # Define the conditions for each update
    conditions = [
        # For fiscal year 22 and specific organization IDs, excluding 80286
        ((df['FISCAL_YEAR_WH_ID'] == 22) & 
         (df['ORGANIZATION_ID'].isin([600, 2000, 9054, 9058, 80287, 81404]))),

        # For fiscal years before 22 and specific improvement and compare codes
        ((df['FISCAL_YEAR_WH_ID'] < 22) & (df['IMPROVEMENT_IND_CODE'].isin(['001', '002', '003']))),
        ((df['FISCAL_YEAR_WH_ID'] < 22) & (df['COMPARE_IND_CODE'].isin(['001', '002', '003']))),

        # For fiscal year 22 and other organization IDs
        ((df['FISCAL_YEAR_WH_ID'] == 22) & (df['ORGANIZATION_ID'].isin(organization_ids_to_updateTPIA))),
        ((df['FISCAL_YEAR_WH_ID'] == 22) & (df['ORGANIZATION_ID'].isin(DQ_TPIA))),
        ((df['FISCAL_YEAR_WH_ID'] == 22) & (df['ORGANIZATION_ID'].isin(PS)))
    ]

    # Define corresponding suppression codes and indicator values
    suppression_codes = ['901', '002', '002', '002', '006']
    indicator_values = [None, None, None, None, None]

    # Apply updates based on conditions
    for condition, suppression_code, indicator_value in zip(conditions, suppression_codes, indicator_values):
        df.loc[condition, ['INDICATOR_SUPPRESSION_CODE', 'TOP_PERFORMER_IND_CODE', 'IMPROVEMENT_IND_CODE', 'COMPARE_IND_CODE']] = [suppression_code, '999', '999', '999']
        df.loc[condition, 'INDICATOR_VALUE'] = indicator_value

    # Separate condition specifically for ORGANIZATION_ID 80286
    condition_80286 = ((df['FISCAL_YEAR_WH_ID'] == 22) & (df['ORGANIZATION_ID'] == 80286))
    df.loc[condition_80286, ['INDICATOR_SUPPRESSION_CODE', 'TOP_PERFORMER_IND_CODE', 'IMPROVEMENT_IND_CODE', 'COMPARE_IND_CODE']] = ['901', '999', '999', '999']
    df.loc[condition_80286, 'INDICATOR_VALUE'] = None

    return df

# Assuming hsp_ind_organization_fact_tpia_34_a is your DataFrame
hsp_ind_organization_fact34 = update_hsp_ind_organization_fact34(hsp_ind_organization_fact_tpia_34_a)

# Specify the columns in the desired order
columns_to_keep = [
    "ORGANIZATION_ID", "INDICATOR_CODE", "FISCAL_YEAR_WH_ID", "SEX_WH_ID", "INDICATOR_SUPPRESSION_CODE", "TOP_PERFORMER_IND_CODE", "IMPROVEMENT_IND_CODE", "COMPARE_IND_CODE", "INDICATOR_VALUE", "UPPER_CONFIDENCE_INTERVAL", "LOWER_CONFIDENCE_INTERVAL", "NUMERATOR_VALUE", "DENOMINATOR_VALUE", "FUNNEL_PLOT_X_VALUE", "DATA_PERIOD_CODE", "DATA_PERIOD_TYPE_CODE"]

# Select the desired columns from the DataFrame
hsp_ind_organization_fact34 = hsp_ind_organization_fact34[columns_to_keep]

# Display the DataFrame
display(hsp_ind_organization_fact34)



ir 

def update_hsp_ind_organization_fact34(df):
    # Define the conditions for each update
    conditions = [
        ((df['FISCAL_YEAR_WH_ID'] == 22) & (df['ORGANIZATION_ID'].isin([600, 2000, 9054, 9058, 80287, 81404]))),
        ((df['FISCAL_YEAR_WH_ID'] < 22) & (df['IMPROVEMENT_IND_CODE'].isin(['001', '002', '003']))),
        ((df['FISCAL_YEAR_WH_ID'] < 22) & (df['COMPARE_IND_CODE'].isin(['001', '002', '003']))),
        ((df['FISCAL_YEAR_WH_ID'] == 22) & (df['ORGANIZATION_ID'].isin(organization_ids_to_updateTPIA))),
        ((df['FISCAL_YEAR_WH_ID'] == 22) & (df['ORGANIZATION_ID'].isin(DQ_TPIA))),
        ((df['FISCAL_YEAR_WH_ID'] == 22) & (df['ORGANIZATION_ID'].isin(PS)))
    ]

    suppression_codes = ['901', '002', '002', '002', '006']
    indicator_values = [None, None, None, None, None]

    # Apply updates based on conditions
    for condition, suppression_code, indicator_value in zip(conditions, suppression_codes, indicator_values):
        df.loc[condition, ['INDICATOR_SUPPRESSION_CODE', 'TOP_PERFORMER_IND_CODE', 'IMPROVEMENT_IND_CODE', 'COMPARE_IND_CODE']] = [suppression_code, '999', '999', '999']
        df.loc[condition, 'INDICATOR_VALUE'] = indicator_value

    # Additional condition for ORGANIZATION_ID 80286
    condition_80286 = ((df['FISCAL_YEAR_WH_ID'] == 22) & (df['ORGANIZATION_ID'] == 80286))
    df.loc[condition_80286, 'INDICATOR_SUPPRESSION_CODE'] = '901'

    return df

# Assuming hsp_ind_organization_fact_tpia_34_a is your DataFrame
hsp_ind_organization_fact34 = update_hsp_ind_organization_fact34(hsp_ind_organization_fact_tpia_34_a)

# Specify the columns in the desired order
columns_to_keep = [
    "ORGANIZATION_ID", "INDICATOR_CODE", "FISCAL_YEAR_WH_ID", "SEX_WH_ID", "INDICATOR_SUPPRESSION_CODE", "TOP_PERFORMER_IND_CODE", "IMPROVEMENT_IND_CODE", "COMPARE_IND_CODE", "INDICATOR_VALUE", "UPPER_CONFIDENCE_INTERVAL", "LOWER_CONFIDENCE_INTERVAL", "NUMERATOR_VALUE", "DENOMINATOR_VALUE", "FUNNEL_PLOT_X_VALUE", "DATA_PERIOD_CODE", "DATA_PERIOD_TYPE_CODE"]

# Select the desired columns from the DataFrame
hsp_ind_organization_fact34 = hsp_ind_organization_fact34[columns_to_keep]

# Display the DataFrame
display(hsp_ind_organization_fact34)





