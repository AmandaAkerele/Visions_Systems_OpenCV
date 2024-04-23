los_reg_22_ta = los_reg_22.join(
    los_supp_reg_22, 
    los_reg_22['NEW_REGION_ID'] == los_reg_22['NEW_REGION_ID'],
    'left_anti'
)

 WARN Column: Constructing trivially true equals predicate, 'NEW_REGION_ID#5323 = NEW_REGION_ID#5323'. Perhaps you need to use aliases.
