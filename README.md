organization_data = sas.sasdata(table='hsp_organization_ext', libref='fac')
organization_data  = organization_data.to_df()

# Create a dictionary to map ORGANIZATION_ID values to peer codes

id_to_peer_code = {
    2: 'T',
    3: 'H1',
    4: 'H2',
    5: 'H3'
}

# Filter the organization_data DataFrame for Organization_ID values in (2,3,4,5)
filtered_data = organization_data[organization_data['ORGANIZATION_ID'].isin([2,3,4,5])]

# Apply the mapping to create the 'peer_code' column
filtered_data['peer_code'] = filtered_data['ORGANIZATION_ID'].map(id_to_peer_code)

# Select the desired columns and rename them

peer_desc_df = filtered_data[['ORGANIZATION_ID', 'peer_code', 'ORGANIZATION_NAME_E_DESC']].rename(
    columns={'organization_id': 'peer_id', 'ORGANIZATION_NAME_E_DESC': 'peer_desc'}
)

# Print the resulting DataFrame 
display(peer_desc_df)


