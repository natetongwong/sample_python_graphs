## Pipelines

1. **scd2_merge**
Type 2 SCD Merge using CSV and Delta

2. **scd1_merge**
These series of gems perform various operations to demonstrate a SCD type 1 change from a source CSV file to a target Delta table. 

This pipeline uses the sample Databricks retail-org dataset and splits the data into three dataframes simulating unchanged rows, updated rows, and new rows.




## Datasets

1. **customers_scd1**
Customer data including their tax information, name, and state.

2. **customers_scd2**
Tracks customer information including tax details, customer name, state, and effective time periods.

3. **customers_raw**
Raw customer data containing information such as customer details, addresses, loyalty segment, and purchase history.