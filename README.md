correctly convert the code below to pyspark 

# For corp level without UCC
# Vectorized approach for 'tpia_rec' calculation
ed_record['tpia_rec'] = 'Y'
ed_record.loc[ed_record['TIME_PHYSICAN_INIT_ASSESSMENT'].isna() | (ed_record['TIME_PHYSICAN_INIT_ASSESSMENT'].str.strip() == ''), 'tpia_rec'] = 'B'
ed_record.loc[ed_record['TIME_PHYSICAN_INIT_ASSESSMENT'] == '9999', 'tpia_rec'] = 'N'

# Filter records not equal to 'B'
tpia_org_cnt = ed_record[ed_record['tpia_rec'] != 'B']
