# Alias the DataFrames for clarity
los_reg_22_alias = los_reg_22.alias("a")
los_supp_reg_22_alias = los_supp_reg_22.alias("b")

# Perform the join using DataFrame aliases
los_reg_22_ta = los_reg_22_alias.join(
    los_supp_reg_22_alias,
    los_reg_22_alias['NEW_REGION_ID'] == los_supp_reg_22_alias['NEW_REGION_ID'],
    'left_anti'
)
