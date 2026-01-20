# _____________________________________________
# ## *Author*: AAVA
# ## *Created on*:   
# ## *Description*: Enhanced PySpark ETL pipeline for regulatory reporting with BRANCH_OPERATIONAL_DETAILS integration
# ## *Version*: 1 
# ## *Updated on*: 
# _____________________________________________

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum as spark_sum, when, lit, current_date
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, DateType, LongType, DoubleType
from delta.tables import DeltaTable
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_spark_session(app_name: str = "RegulatoryReportingETL_Enhanced") -> SparkSession:
    """
    Initializes and returns a Spark session compatible with Spark Connect.
    """
    try:
        # Use getActiveSession() for Spark Connect compatibility
        spark = SparkSession.getActiveSession()
        if spark is None:
            spark = SparkSession.builder \
                .appName(app_name) \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                .getOrCreate()
        
        # Set log level for Spark logs (avoiding sparkContext for Spark Connect compatibility)
        spark.conf.set("spark.sql.adaptive.enabled", "true")
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        logger.info("Spark session created successfully.")
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark session: {e}")
        raise

def create_sample_data(spark: SparkSession) -> tuple:
    """
    Creates sample DataFrames for testing purposes.
    """
    logger.info("Creating sample data for testing")
    
    # Customer sample data
    customer_schema = StructType([
        StructField("CUSTOMER_ID", IntegerType(), True),
        StructField("NAME", StringType(), True),
        StructField("EMAIL", StringType(), True),
        StructField("PHONE", StringType(), True),
        StructField("ADDRESS", StringType(), True),
        StructField("CREATED_DATE", DateType(), True)
    ])
    
    customer_data = [
        (1, "John Doe", "john.doe@email.com", "123-456-7890", "123 Main St", "2023-01-15"),
        (2, "Jane Smith", "jane.smith@email.com", "098-765-4321", "456 Oak Ave", "2023-02-20"),
        (3, "Bob Johnson", "bob.johnson@email.com", "555-123-4567", "789 Pine Rd", "2023-03-10")
    ]
    
    customer_df = spark.createDataFrame(customer_data, customer_schema)
    
    # Branch sample data
    branch_schema = StructType([
        StructField("BRANCH_ID", IntegerType(), True),
        StructField("BRANCH_NAME", StringType(), True),
        StructField("BRANCH_CODE", StringType(), True),
        StructField("CITY", StringType(), True),
        StructField("STATE", StringType(), True),
        StructField("COUNTRY", StringType(), True)
    ])
    
    branch_data = [
        (101, "Downtown Branch", "DT001", "New York", "NY", "USA"),
        (102, "Uptown Branch", "UT002", "Los Angeles", "CA", "USA"),
        (103, "Central Branch", "CT003", "Chicago", "IL", "USA")
    ]
    
    branch_df = spark.createDataFrame(branch_data, branch_schema)
    
    # Account sample data
    account_schema = StructType([
        StructField("ACCOUNT_ID", IntegerType(), True),
        StructField("CUSTOMER_ID", IntegerType(), True),
        StructField("BRANCH_ID", IntegerType(), True),
        StructField("ACCOUNT_NUMBER", StringType(), True),
        StructField("ACCOUNT_TYPE", StringType(), True),
        StructField("BALANCE", DecimalType(15,2), True),
        StructField("OPENED_DATE", DateType(), True)
    ])
    
    account_data = [
        (1001, 1, 101, "ACC001001", "SAVINGS", 15000.50, "2023-01-20"),
        (1002, 2, 102, "ACC002002", "CHECKING", 8500.75, "2023-02-25"),
        (1003, 3, 103, "ACC003003", "SAVINGS", 22000.00, "2023-03-15"),
        (1004, 1, 101, "ACC001004", "CHECKING", 5500.25, "2023-04-10")
    ]
    
    account_df = spark.createDataFrame(account_data, account_schema)
    
    # Transaction sample data
    transaction_schema = StructType([
        StructField("TRANSACTION_ID", IntegerType(), True),
        StructField("ACCOUNT_ID", IntegerType(), True),
        StructField("TRANSACTION_TYPE", StringType(), True),
        StructField("AMOUNT", DecimalType(15,2), True),
        StructField("TRANSACTION_DATE", DateType(), True),
        StructField("DESCRIPTION", StringType(), True)
    ])
    
    transaction_data = [
        (10001, 1001, "DEPOSIT", 1000.00, "2023-05-01", "Salary deposit"),
        (10002, 1001, "WITHDRAWAL", 200.00, "2023-05-02", "ATM withdrawal"),
        (10003, 1002, "DEPOSIT", 1500.00, "2023-05-03", "Check deposit"),
        (10004, 1003, "TRANSFER", 500.00, "2023-05-04", "Online transfer"),
        (10005, 1004, "DEPOSIT", 750.00, "2023-05-05", "Cash deposit"),
        (10006, 1002, "WITHDRAWAL", 100.00, "2023-05-06", "Debit card purchase")
    ]
    
    transaction_df = spark.createDataFrame(transaction_data, transaction_schema)
    
    # Branch Operational Details sample data (NEW)
    branch_operational_schema = StructType([
        StructField("BRANCH_ID", IntegerType(), True),
        StructField("REGION", StringType(), True),
        StructField("MANAGER_NAME", StringType(), True),
        StructField("LAST_AUDIT_DATE", DateType(), True),
        StructField("IS_ACTIVE", StringType(), True)
    ])
    
    branch_operational_data = [
        (101, "Northeast", "Alice Johnson", "2023-04-15", "Y"),
        (102, "West Coast", "Bob Wilson", "2023-03-20", "Y"),
        (103, "Midwest", "Carol Davis", "2023-02-10", "N")  # Inactive branch for testing
    ]
    
    branch_operational_df = spark.createDataFrame(branch_operational_data, branch_operational_schema)
    
    return customer_df, account_df, transaction_df, branch_df, branch_operational_df

def read_delta_table(spark: SparkSession, table_path: str) -> DataFrame:
    """
    Reads a Delta table from the specified path.
    """
    logger.info(f"Reading Delta table from: {table_path}")
    try:
        df = spark.read.format("delta").load(table_path)
        return df
    except Exception as e:
        logger.error(f"Failed to read Delta table from {table_path}: {e}")
        raise

def create_aml_customer_transactions(customer_df: DataFrame, account_df: DataFrame, transaction_df: DataFrame) -> DataFrame:
    """
    Creates the AML_CUSTOMER_TRANSACTIONS DataFrame by joining customer, account, and transaction data.
    """
    logger.info("Creating AML Customer Transactions DataFrame.")
    return customer_df.join(account_df, "CUSTOMER_ID") \
                      .join(transaction_df, "ACCOUNT_ID") \
                      .select(
                          col("CUSTOMER_ID"),
                          col("NAME"),
                          col("ACCOUNT_ID"),
                          col("TRANSACTION_ID"),
                          col("AMOUNT"),
                          col("TRANSACTION_TYPE"),
                          col("TRANSACTION_DATE")
                      )

def create_enhanced_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, 
                                        branch_df: DataFrame, branch_operational_df: DataFrame) -> DataFrame:
    """
    Creates the enhanced BRANCH_SUMMARY_REPORT DataFrame by aggregating transaction data at the branch level
    and integrating BRANCH_OPERATIONAL_DETAILS for compliance and audit readiness.
    
    Enhancement: Integrates REGION and LAST_AUDIT_DATE from BRANCH_OPERATIONAL_DETAILS
    based on IS_ACTIVE = 'Y' condition as per technical specifications.
    """
    logger.info("Creating Enhanced Branch Summary Report DataFrame with BRANCH_OPERATIONAL_DETAILS integration.")
    
    # Step 1: Create base branch summary with transaction aggregations
    base_summary = transaction_df.join(account_df, "ACCOUNT_ID") \
                                 .join(branch_df, "BRANCH_ID") \
                                 .groupBy("BRANCH_ID", "BRANCH_NAME") \
                                 .agg(
                                     count("*").alias("TOTAL_TRANSACTIONS"),
                                     spark_sum("AMOUNT").alias("TOTAL_AMOUNT")
                                 )
    
    # Step 2: Join with BRANCH_OPERATIONAL_DETAILS and apply conditional logic
    # As per technical specifications: populate REGION and LAST_AUDIT_DATE only when IS_ACTIVE = 'Y'
    enhanced_summary = base_summary.join(
        branch_operational_df, 
        base_summary["BRANCH_ID"] == branch_operational_df["BRANCH_ID"], 
        "left"  # Left join to maintain all branches even if operational details are missing
    ).select(
        base_summary["BRANCH_ID"].cast(LongType()).alias("BRANCH_ID"),
        base_summary["BRANCH_NAME"],
        base_summary["TOTAL_TRANSACTIONS"],
        base_summary["TOTAL_AMOUNT"].cast(DoubleType()).alias("TOTAL_AMOUNT"),
        # Conditional population based on IS_ACTIVE = 'Y' as per technical specifications
        when(branch_operational_df["IS_ACTIVE"] == "Y", branch_operational_df["REGION"]).alias("REGION"),
        when(branch_operational_df["IS_ACTIVE"] == "Y", branch_operational_df["LAST_AUDIT_DATE"].cast(StringType())).alias("LAST_AUDIT_DATE")
    )
    
    logger.info("Enhanced Branch Summary Report created with conditional REGION and LAST_AUDIT_DATE population.")
    return enhanced_summary

def write_to_delta_table(df: DataFrame, table_path: str, mode: str = "overwrite"):
    """
    Writes a DataFrame to a Delta table with proper error handling and logging.
    """
    logger.info(f"Writing DataFrame to Delta table: {table_path} with mode: {mode}")
    try:
        df.write.format("delta") \
          .mode(mode) \
          .option("mergeSchema", "true") \
          .save(table_path)
        logger.info(f"Successfully written data to {table_path}")
    except Exception as e:
        logger.error(f"Failed to write to Delta table {table_path}: {e}")
        raise

def validate_data_quality(df: DataFrame, table_name: str) -> bool:
    """
    Performs basic data quality validations on the DataFrame.
    """
    logger.info(f"Performing data quality validation for {table_name}")
    
    try:
        # Check for empty DataFrame
        row_count = df.count()
        if row_count == 0:
            logger.warning(f"{table_name} is empty")
            return False
        
        # Check for null values in key columns
        if table_name == "BRANCH_SUMMARY_REPORT":
            null_branch_ids = df.filter(col("BRANCH_ID").isNull()).count()
            if null_branch_ids > 0:
                logger.error(f"{table_name} has {null_branch_ids} null BRANCH_ID values")
                return False
        
        logger.info(f"Data quality validation passed for {table_name}. Row count: {row_count}")
        return True
        
    except Exception as e:
        logger.error(f"Data quality validation failed for {table_name}: {e}")
        return False

def main():
    """
    Main ETL execution function with enhanced BRANCH_OPERATIONAL_DETAILS integration.
    """
    spark = None
    try:
        spark = get_spark_session()
        
        # Create sample data for self-contained execution
        customer_df, account_df, transaction_df, branch_df, branch_operational_df = create_sample_data(spark)
        
        logger.info("Sample data created successfully")
        
        # Display sample data for verification
        logger.info("Sample BRANCH_OPERATIONAL_DETAILS data:")
        branch_operational_df.show()
        
        # Create and write AML_CUSTOMER_TRANSACTIONS (unchanged logic)
        aml_transactions_df = create_aml_customer_transactions(customer_df, account_df, transaction_df)
        
        # Validate AML transactions data
        if validate_data_quality(aml_transactions_df, "AML_CUSTOMER_TRANSACTIONS"):
            # In a real scenario, this would write to actual Delta table path
            # write_to_delta_table(aml_transactions_df, "/tmp/delta/aml_customer_transactions")
            logger.info("AML_CUSTOMER_TRANSACTIONS processing completed successfully")
        
        # Create and write enhanced BRANCH_SUMMARY_REPORT with BRANCH_OPERATIONAL_DETAILS integration
        enhanced_branch_summary_df = create_enhanced_branch_summary_report(
            transaction_df, account_df, branch_df, branch_operational_df
        )
        
        # Display enhanced branch summary for verification
        logger.info("Enhanced BRANCH_SUMMARY_REPORT with REGION and LAST_AUDIT_DATE:")
        enhanced_branch_summary_df.show(truncate=False)
        
        # Validate enhanced branch summary data
        if validate_data_quality(enhanced_branch_summary_df, "BRANCH_SUMMARY_REPORT"):
            # In a real scenario, this would write to actual Delta table path
            # write_to_delta_table(enhanced_branch_summary_df, "/tmp/delta/branch_summary_report")
            logger.info("Enhanced BRANCH_SUMMARY_REPORT processing completed successfully")
        
        logger.info("ETL job completed successfully with BRANCH_OPERATIONAL_DETAILS integration.")
        
        return enhanced_branch_summary_df  # Return for testing purposes
        
    except Exception as e:
        logger.error(f"ETL job failed with exception: {e}")
        raise
    finally:
        if spark:
            # Don't stop the session in Databricks environment
            logger.info("ETL execution completed.")

if __name__ == "__main__":
    main()