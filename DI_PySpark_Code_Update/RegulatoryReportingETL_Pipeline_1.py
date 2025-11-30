_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Enhanced PySpark ETL pipeline for regulatory reporting with BRANCH_OPERATIONAL_DETAILS integration
## *Version*: 1 
## *Updated on*: 
_____________________________________________

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum as spark_sum, when, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, DateType, LongType, DoubleType
from delta.tables import DeltaTable
from datetime import date

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_spark_session(app_name: str = "RegulatoryReportingETL") -> SparkSession:
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
        
        # Set log level for Spark logs (avoiding sparkContext calls)
        spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session created successfully.")
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark session: {e}")
        raise

def create_sample_data(spark: SparkSession) -> tuple:
    """
    Creates sample data for testing the ETL pipeline.
    """
    logger.info("Creating sample data for testing")
    
    # Customer data
    customer_schema = StructType([
        StructField("CUSTOMER_ID", IntegerType(), True),
        StructField("NAME", StringType(), True),
        StructField("EMAIL", StringType(), True),
        StructField("PHONE", StringType(), True),
        StructField("ADDRESS", StringType(), True),
        StructField("CREATED_DATE", DateType(), True)
    ])
    
    customer_data = [
        (1, "John Doe", "john@email.com", "123-456-7890", "123 Main St", date(2023, 1, 15)),
        (2, "Jane Smith", "jane@email.com", "098-765-4321", "456 Oak Ave", date(2023, 2, 20)),
        (3, "Bob Johnson", "bob@email.com", "555-123-4567", "789 Pine Rd", date(2023, 3, 10))
    ]
    
    customer_df = spark.createDataFrame(customer_data, customer_schema)
    
    # Branch data
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
    
    # Account data
    account_schema = StructType([
        StructField("ACCOUNT_ID", IntegerType(), True),
        StructField("CUSTOMER_ID", IntegerType(), True),
        StructField("BRANCH_ID", IntegerType(), True),
        StructField("ACCOUNT_NUMBER", StringType(), True),
        StructField("ACCOUNT_TYPE", StringType(), True),
        StructField("BALANCE", DecimalType(15, 2), True),
        StructField("OPENED_DATE", DateType(), True)
    ]
    
    account_data = [
        (1001, 1, 101, "ACC001", "SAVINGS", 5000.00, date(2023, 1, 16)),
        (1002, 2, 102, "ACC002", "CHECKING", 2500.00, date(2023, 2, 21)),
        (1003, 3, 103, "ACC003", "SAVINGS", 7500.00, date(2023, 3, 11)),
        (1004, 1, 101, "ACC004", "CHECKING", 1200.00, date(2023, 4, 5))
    ]
    
    account_df = spark.createDataFrame(account_data, account_schema)
    
    # Transaction data
    transaction_schema = StructType([
        StructField("TRANSACTION_ID", IntegerType(), True),
        StructField("ACCOUNT_ID", IntegerType(), True),
        StructField("TRANSACTION_TYPE", StringType(), True),
        StructField("AMOUNT", DecimalType(15, 2), True),
        StructField("TRANSACTION_DATE", DateType(), True),
        StructField("DESCRIPTION", StringType(), True)
    ])
    
    transaction_data = [
        (10001, 1001, "DEPOSIT", 1000.00, date(2023, 5, 1), "Salary deposit"),
        (10002, 1001, "WITHDRAWAL", 200.00, date(2023, 5, 2), "ATM withdrawal"),
        (10003, 1002, "DEPOSIT", 500.00, date(2023, 5, 3), "Check deposit"),
        (10004, 1003, "TRANSFER", 300.00, date(2023, 5, 4), "Online transfer"),
        (10005, 1004, "DEPOSIT", 800.00, date(2023, 5, 5), "Cash deposit")
    ]
    
    transaction_df = spark.createDataFrame(transaction_data, transaction_schema)
    
    # Branch Operational Details data (NEW)
    branch_operational_schema = StructType([
        StructField("BRANCH_ID", IntegerType(), True),
        StructField("REGION", StringType(), True),
        StructField("MANAGER_NAME", StringType(), True),
        StructField("LAST_AUDIT_DATE", DateType(), True),
        StructField("IS_ACTIVE", StringType(), True)
    ])
    
    branch_operational_data = [
        (101, "East Region", "Alice Manager", date(2023, 4, 15), "Y"),
        (102, "West Region", "Bob Manager", date(2023, 3, 20), "Y"),
        (103, "Central Region", "Charlie Manager", date(2023, 2, 10), "N")  # Inactive branch
    ]
    
    branch_operational_df = spark.createDataFrame(branch_operational_data, branch_operational_schema)
    
    return customer_df, account_df, transaction_df, branch_df, branch_operational_df

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
    Creates the enhanced BRANCH_SUMMARY_REPORT DataFrame with BRANCH_OPERATIONAL_DETAILS integration.
    This function implements the new business requirements from the technical specifications.
    """
    logger.info("Creating Enhanced Branch Summary Report DataFrame with operational details.")
    
    # Step 1: Create base branch summary with transaction aggregations
    base_summary = transaction_df.join(account_df, "ACCOUNT_ID") \
                                 .join(branch_df, "BRANCH_ID") \
                                 .groupBy("BRANCH_ID", "BRANCH_NAME") \
                                 .agg(
                                     count("*").alias("TOTAL_TRANSACTIONS"),
                                     spark_sum("AMOUNT").alias("TOTAL_AMOUNT")
                                 )
    
    # Step 2: Join with BRANCH_OPERATIONAL_DETAILS and apply conditional logic
    # Only populate REGION and LAST_AUDIT_DATE when IS_ACTIVE = 'Y'
    enhanced_summary = base_summary.join(branch_operational_df, "BRANCH_ID", "left") \
                                   .select(
                                       col("BRANCH_ID").cast(LongType()),
                                       col("BRANCH_NAME"),
                                       col("TOTAL_TRANSACTIONS"),
                                       col("TOTAL_AMOUNT").cast(DoubleType()),
                                       # Conditional population based on IS_ACTIVE = 'Y'
                                       when(col("IS_ACTIVE") == "Y", col("REGION")).otherwise(lit(None)).alias("REGION"),
                                       when(col("IS_ACTIVE") == "Y", col("LAST_AUDIT_DATE").cast(StringType())).otherwise(lit(None)).alias("LAST_AUDIT_DATE")
                                   )
    
    logger.info("Enhanced Branch Summary Report created with conditional REGION and LAST_AUDIT_DATE population.")
    return enhanced_summary

def write_to_delta_table(df: DataFrame, table_name: str, mode: str = "overwrite"):
    """
    Writes a DataFrame to a Delta table with proper error handling.
    """
    logger.info(f"Writing DataFrame to Delta table: {table_name} in {mode} mode")
    try:
        df.write.format("delta") \
          .mode(mode) \
          .option("mergeSchema", "true") \
          .saveAsTable(table_name)
        logger.info(f"Successfully written data to {table_name}")
    except Exception as e:
        logger.error(f"Failed to write to Delta table {table_name}: {e}")
        raise

def validate_data_quality(df: DataFrame, table_name: str) -> bool:
    """
    Performs basic data quality validations on the DataFrame.
    """
    logger.info(f"Validating data quality for {table_name}")
    
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
        
        logger.info(f"Data quality validation passed for {table_name} with {row_count} rows")
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
        
        # Create sample data for testing
        customer_df, account_df, transaction_df, branch_df, branch_operational_df = create_sample_data(spark)
        
        logger.info("Sample data created successfully")
        
        # Create and validate AML_CUSTOMER_TRANSACTIONS
        aml_transactions_df = create_aml_customer_transactions(customer_df, account_df, transaction_df)
        
        if validate_data_quality(aml_transactions_df, "AML_CUSTOMER_TRANSACTIONS"):
            write_to_delta_table(aml_transactions_df, "AML_CUSTOMER_TRANSACTIONS")
        else:
            logger.error("Data quality validation failed for AML_CUSTOMER_TRANSACTIONS")
            return
        
        # Create and validate enhanced BRANCH_SUMMARY_REPORT with operational details
        enhanced_branch_summary_df = create_enhanced_branch_summary_report(
            transaction_df, account_df, branch_df, branch_operational_df
        )
        
        if validate_data_quality(enhanced_branch_summary_df, "BRANCH_SUMMARY_REPORT"):
            write_to_delta_table(enhanced_branch_summary_df, "workspace.default.branch_summary_report")
        else:
            logger.error("Data quality validation failed for BRANCH_SUMMARY_REPORT")
            return
        
        # Display sample results for verification
        logger.info("Displaying sample results:")
        print("\n=== Enhanced Branch Summary Report Sample ===")
        enhanced_branch_summary_df.show(truncate=False)
        
        logger.info("Enhanced ETL job completed successfully with BRANCH_OPERATIONAL_DETAILS integration.")
        
    except Exception as e:
        logger.error(f"ETL job failed with exception: {e}")
        raise
    finally:
        if spark:
            # Don't stop the session in Databricks environment
            logger.info("ETL job execution completed.")

if __name__ == "__main__":
    main()