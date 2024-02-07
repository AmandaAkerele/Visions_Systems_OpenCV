def update_hsp_ind_organization_fact34(df):
    conditions = [
        ((df['FISCAL_YEAR_WH_ID'] == 22) &
         (df['ORGANIZATION_ID'].isin([600, 2000, 9054, 9058, 80286, 80287, 81404]))),
        ((df['FISCAL_YEAR_WH_ID'] < 22) & (df['IMPROVEMENT_IND_CODE'].isin(['001', '002', '003']))),
        ((df['FISCAL_YEAR_WH_ID'] < 22) & (df['COMPARE_IND_CODE'].isin(['001', '002', '003']))),
        ((df['FISCAL_YEAR_WH_ID'] == 22) & (df['ORGANIZATION_ID'].isin(organization_ids_to_updateLOS))),
        ((df['FISCAL_YEAR_WH_ID'] == 22) & (df['ORGANIZATION_ID'].isin(filtered_ID_DQ_LOS))),
        ((df['FISCAL_YEAR_WH_ID'] == 22) & (df['ORGANIZATION_ID'].isin(filtered_ID_PS)))
    ]

    suppression_codes = ['901', '901', '901', '901', '006']  # Corrected suppression code for 80286
    indicator_values = [None, None, None, None, None]

    for condition, suppression_code, indicator_value in zip(conditions, suppression_codes, indicator_values):
        df.loc[condition, ['INDICATOR_SUPPRESSION_CODE', 'TOP_PERFORMER_IND_CODE', 'IMPROVEMENT_IND_CODE', 'COMPARE_IND_CODE']] = [suppression_code, '999', '999', '999']
        df.loc[condition, 'INDICATOR_VALUE'] = indicator_value

    return df

hsp_ind_organization_fact34 = update_hsp_ind_organization_fact34(hsp_ind_organization_fact_los_34_d)




lets see

def update_hsp_ind_organization_fact34(df):
    # First, handle the specific case for 80286
    specific_condition = ((df['FISCAL_YEAR_WH_ID'] == 22) & (df['ORGANIZATION_ID'] == 80286))
    df.loc[specific_condition, ['INDICATOR_SUPPRESSION_CODE', 'TOP_PERFORMER_IND_CODE', 'IMPROVEMENT_IND_CODE', 'COMPARE_IND_CODE']] = ['901', '999', '999', '999']
    df.loc[specific_condition, 'INDICATOR_VALUE'] = None

    # Then, handle the other conditions
    conditions = [
        ((df['FISCAL_YEAR_WH_ID'] == 22) &
         (df['ORGANIZATION_ID'].isin([600, 2000, 9054, 9058, 80287, 81404]))),
        ((df['FISCAL_YEAR_WH_ID'] < 22) & (df['IMPROVEMENT_IND_CODE'].isin(['001', '002', '003']))),
        ((df['FISCAL_YEAR_WH_ID'] < 22) & (df['COMPARE_IND_CODE'].isin(['001', '002', '003']))),
        ((df['FISCAL_YEAR_WH_ID'] == 22) & (df['ORGANIZATION_ID'].isin(organization_ids_to_updateLOS))),
        ((df['FISCAL_YEAR_WH_ID'] == 22) & (df['ORGANIZATION_ID'].isin(filtered_ID_DQ_LOS))),
        ((df['FISCAL_YEAR_WH_ID'] == 22) & (df['ORGANIZATION_ID'].isin(filtered_ID_PS)))
    ]

    suppression_codes = ['901', '901', '901', '901', '006']
    indicator_values = [None, None, None, None, None]

    for condition, suppression_code, indicator_value in zip(conditions, suppression_codes, indicator_values):
        df.loc[condition, ['INDICATOR_SUPPRESSION_CODE', 'TOP_PERFORMER_IND_CODE', 'IMPROVEMENT_IND_CODE', 'COMPARE_IND_CODE']] = [suppression_code, '999', '999', '999']
        df.loc[condition, 'INDICATOR_VALUE'] = indicator_value

    return df

hsp_ind_organization_fact34 = update_hsp_ind_organization_fact34(hsp_ind_organization_fact_los_34_d)
