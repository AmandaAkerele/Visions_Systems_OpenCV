# Dynamically rename 'SUBMISSION_FISCAL' to 'FISCAL_YEAR' and keep all other columns unchanged
all_columns = [col(c).alias("FISCAL_YEAR") if c == "SUBMISSION_FISCAL" else col(c) for c in tpia_org_22.columns]

tpia_org_22_a = tpia_org_22.select(*all_columns)

# Show the updated DataFrame
tpia_org_22_a.printSchema()
tpia_org_22_a.show()

