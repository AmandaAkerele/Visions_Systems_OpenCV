# Replace '1' with '80235' in the ORGANIZATION_ID column
TT_Spent_ED['ORGANIZATION_ID'].replace('1', '80235', inplace=True)

# Display the first 5 columns of the updated DataFrame
print(TT_Spent_ED.iloc[:, :5])
