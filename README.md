if row['INDICATOR_SUPPRESSION_CODE'] != '999':
                
    # For Row 1
    if row['INDICATOR_SUPPRESSION_CODE'] not in ['S03', 'S10', 'M02', 'S08']:
        stacked_data.append([row['reporting_entity_code'], row['reporting_period_code'], row['metric_result'], '', '', row['INDICATOR_SUPPRESSION_CODE'], row['metric_result']])
    else:
        stacked_data.append([row['reporting_entity_code'], row['reporting_period_code'], row['metric_result'], '', '', row['INDICATOR_SUPPRESSION_CODE'], ''])
