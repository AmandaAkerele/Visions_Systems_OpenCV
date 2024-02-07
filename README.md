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

    # Define a lambda function to set suppression code based on organization ID
    suppression_code_lambda = lambda org_id: '901' if org_id == 80286 else '002'

    # Apply the lambda function to each organization ID to get suppression codes
    suppression_codes = df['ORGANIZATION_ID'].apply(suppression_code_lambda)

    # Keep previous suppression codes for other organizations
    suppression_codes = np.where(suppression_codes == '002', '002', '901')
    
    indicator_values = [None, None, None, None, None]

    for condition, suppression_code, indicator_value in zip(conditions, suppression_codes, indicator_values):
        df.loc[condition, ['INDICATOR_SUPPRESSION_CODE', 'TOP_PERFORMER_IND_CODE', 'IMPROVEMENT_IND_CODE', 'COMPARE_IND_CODE']] = [suppression_code, '999', '999', '999']
        df.loc[condition, 'INDICATOR_VALUE'] = indicator_value

    return df

hsp_ind_organization_fact34 = update_hsp_ind_organization_fact34(hsp_ind_organization_fact_los_34_d)
