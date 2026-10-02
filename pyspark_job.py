from pyspark.sql.functions import col

def clean_data(df):
    """
    Pure Transformation Function:
    1. Removes rows where amount <= 0
    2. Removes rows where name is NULL
    3. Adds amount_with_tax column (amount * 1.20)
    """
    # 1. Filtering Data (Removing rows where amount <= 0 and name is NULL)
    filtered_df = df.filter(
        (col("amount") > 0) & 
        (col("name").isNotNull())
    )
    
    # 2. (amount * 1.20)
    transformed_df = filtered_df.withColumn(
        "amount_with_tax", 
        col("amount") * 1.20
    )
    
    return transformed_df