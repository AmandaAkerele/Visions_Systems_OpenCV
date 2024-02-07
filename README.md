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

    # Set suppression codes
    suppression_codes = ['901' if org_id == 80286 else '002' for org_id in df['ORGANIZATION_ID']]
    # Keep previous suppression codes for other organizations
    suppression_codes = ['901' if org_id == 80286 else '002' if code == '002' else '901' for org_id, code in zip(df['ORGANIZATION_ID'], suppression_codes)]
    
    indicator_values = [None, None, None, None, None]

    for condition, suppression_code, indicator_value in zip(conditions, suppression_codes, indicator_values):
        df.loc[condition, ['INDICATOR_SUPPRESSION_CODE', 'TOP_PERFORMER_IND_CODE', 'IMPROVEMENT_IND_CODE', 'COMPARE_IND_CODE']] = [suppression_code, '999', '999', '999']
        df.loc[condition, 'INDICATOR_VALUE'] = indicator_value

    return df

hsp_ind_organization_fact34 = update_hsp_ind_organization_fact34(hsp_ind_organization_fact_los_34_d)
