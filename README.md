

def update_hsp_ind_organization_fact34(df):
    # Update 1
    #TPIA/ELOS Suppress Region and Province due to ED coverage (source from CAD OPS - DQ team);
    #Province  NS(2000), MB(600)
    #Region BC:Interior Health(9054) and Northern Health(9058)NS:Nova Scotia Health Authority Zone 1: Western(80286) and Nova Scotia Health Authority Zone 2: Northern(80287)SK: Far north zone (81404)			 
     #indicator_suppression_code = '901';
    condition = (
        (df['FISCAL_YEAR_WH_ID'] == 22) &
        (df['ORGANIZATION_ID'].isin([600, 2000]))
    )
    df.loc[condition, ['INDICATOR_SUPPRESSION_CODE', 'TOP_PERFORMER_IND_CODE', 'IMPROVEMENT_IND_CODE', 'COMPARE_IND_CODE']] = ['901', '999', '999', '999']
    df.loc[condition, 'INDICATOR_VALUE'] = None

    # Update 2
    condition = (
        (df['FISCAL_YEAR_WH_ID'] == 22) &
        (df['ORGANIZATION_ID'].isin([9054, 9058, 80286, 80287, 81404]))
    )
    df.loc[condition, ['INDICATOR_SUPPRESSION_CODE', 'TOP_PERFORMER_IND_CODE', 'IMPROVEMENT_IND_CODE', 'COMPARE_IND_CODE']] = ['901', '999', '999', '999']
    df.loc[condition, 'INDICATOR_VALUE'] = None

    # Update 3
    condition = (
        (df['FISCAL_YEAR_WH_ID'] < 22) &
        (df['IMPROVEMENT_IND_CODE'].isin(['001', '002', '003']))
    )
    df.loc[condition, 'IMPROVEMENT_IND_CODE'] = '999'

    # Update 4
    condition = (
        (df['FISCAL_YEAR_WH_ID'] < 22) &
        (df['COMPARE_IND_CODE'].isin(['001', '002', '003']))
    )
    df.loc[condition, 'COMPARE_IND_CODE'] = '999'

    #Update 5
    condition = (
       (df['FISCAL_YEAR_WH_ID'] < 22) &
       (df['ORGANIZATION_ID'].isin(organization_ids_to_updateTPIA))
    )
    df.loc[condition, ['INDICATOR_SUPPRESSION_CODE', 'TOP_PERFORMER_IND_CODE', 'IMPROVEMENT_IND_CODE', 'COMPARE_IND_CODE']] = ['002', '999', '999', '999']
    df.loc[condition, 'INDICATOR_VALUE'] = None

    #Update 6 -if any DQ exists
    condition = (
       (df['FISCAL_YEAR_WH_ID'] < 22) &
       (df['ORGANIZATION_ID'].isin(filtered_ID_DQ_TPIA))
    )
    df.loc[condition, ['INDICATOR_SUPPRESSION_CODE', 'TOP_PERFORMER_IND_CODE', 'IMPROVEMENT_IND_CODE', 'COMPARE_IND_CODE']] = ['002', '999', '999', '999']
    df.loc[condition, 'INDICATOR_VALUE'] = None

    #Update 6 -if any partial submission apply code 006
    condition = (
       (df['FISCAL_YEAR_WH_ID'] < 22) &
       (df['ORGANIZATION_ID'].isin(filtered_ID_PS))
    )
    df.loc[condition, ['INDICATOR_SUPPRESSION_CODE', 'TOP_PERFORMER_IND_CODE', 'IMPROVEMENT_IND_CODE', 'COMPARE_IND_CODE']] = ['006', '999', '999', '999']
    df.loc[condition, 'INDICATOR_VALUE'] = None

    return df

# Assuming hsp_ind_organization_fact33 is your DataFrame
hsp_ind_organization_fact34 = update_hsp_ind_organization_fact34(hsp_ind_organization_fact_tpia_34_c)

display(hsp_ind_organization_fact34)
