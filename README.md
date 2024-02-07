def update_hsp_ind_organization_fact34(df):
    # Define the conditions for each update
    conditions = [
        ((df['FISCAL_YEAR_WH_ID'] == 22) & (df['ORGANIZATION_ID'].isin([600, 2000]))),
        ((df['FISCAL_YEAR_WH_ID'] == 22) & (df['ORGANIZATION_ID'].isin([9054, 9058, 80286, 80287, 81404]))),
        ((df['FISCAL_YEAR_WH_ID'] < 22) & (df['IMPROVEMENT_IND_CODE'].isin(['001', '002', '003']))),
        ((df['FISCAL_YEAR_WH_ID'] < 22) & (df['COMPARE_IND_CODE'].isin(['001', '002', '003']))),
        ((df['FISCAL_YEAR_WH_ID'] < 22) & (df['ORGANIZATION_ID'].isin(organization_ids_to_updateTPIA))),
        ((df['FISCAL_YEAR_WH_ID'] < 22) & (df['ORGANIZATION_ID'].isin(filtered_ID_DQ_TPIA))),
        ((df['FISCAL_YEAR_WH_ID'] < 22) & (df['ORGANIZATION_ID'].isin(filtered_ID_PS)))
    ]

    # Define the suppression codes and indicator values for each condition
    suppression_codes = ['901', '901', '999', '999', '002', '002', '006']
    indicator_values = [None, None, None, None, None, None, None]

    # Apply the conditions and update the DataFrame
    for condition, suppression_code, indicator_value in zip(conditions, suppression_codes, indicator_values):
        df.loc[condition, ['INDICATOR_SUPPRESSION_CODE', 'TOP_PERFORMER_IND_CODE', 'IMPROVEMENT_IND_CODE', 'COMPARE_IND_CODE']] = [suppression_code, '999', '999', '999']
        df.loc[condition, 'INDICATOR_VALUE'] = indicator_value

    return df

# Assuming hsp_ind_organization_fact_tpia_34_c is your DataFrame
hsp_ind_organization_fact34 = update_hsp_ind_organization_fact34(hsp_ind_organization_fact_tpia_34_c)

# Display the updated DataFrame
display(hsp_ind_organization_fact34)


def update1(df):
    condition = (
        (df['FISCAL_YEAR_WH_ID'] == 22) &
        (df['ORGANIZATION_ID'].isin([600, 2000]))
    )
    df.loc[condition, ['INDICATOR_SUPPRESSION_CODE', 'TOP_PERFORMER_IND_CODE', 'IMPROVEMENT_IND_CODE', 'COMPARE_IND_CODE']] = ['901', '999', '999', '999']
    df.loc[condition, 'INDICATOR_VALUE'] = None

def update2(df):
    condition = (
        (df['FISCAL_YEAR_WH_ID'] == 22) &
        (df['ORGANIZATION_ID'].isin([9054, 9058, 80286, 80287, 81404]))
    )
    df.loc[condition, ['INDICATOR_SUPPRESSION_CODE', 'TOP_PERFORMER_IND_CODE', 'IMPROVEMENT_IND_CODE', 'COMPARE_IND_CODE']] = ['901', '999', '999', '999']
    df.loc[condition, 'INDICATOR_VALUE'] = None

# Similarly define update3, update4, update5, and update6

def update_hsp_ind_organization_fact34(df):
    update1(df)
    update2(df)
    # Call update3, update4, update5, and update6 in the same manner
    return df

# Assuming hsp_ind_organization_fact33 is your DataFrame
hsp_ind_organization_fact34 = update_hsp_ind_organization_fact34(hsp_ind_organization_fact_tpia_34_c)

display(hsp_ind_organization_fact34)

