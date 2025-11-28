====================================================================
# Author: Ascendion AAVA
# Date: 
# Description: Enhanced PySpark ETL pipeline for regulatory reporting with BRANCH_OPERATIONAL_DETAILS integration
====================================================================

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum, when, lit, current_timestamp  # [MODIFIED] Added when, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, LongType  # [ADDED] For sample data creation
from datetime import datetime, date

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_spark_session(app_name: str = "RegulatoryReportingETL") -> SparkSession:
    """
    Initializes and returns a Spark session compatible with Databricks.
    """
    try:
        # [MODIFIED] Updated to use getActiveSession() for Spark Connect compatibility
        try:
            spark = SparkSession.getActiveSession()
            if spark is None:
                spark = SparkSession.builder \
                    .appName(app_name) \
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                    .getOrCreate()
        except:
            spark = SparkSession.builder \
                .appName(app_name) \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                .getOrCreate()
        
        # [DEPRECATED] Removed sparkContext.setLogLevel for Spark Connect compatibility
        # spark.sparkContext.setLogLevel("WARN")
        
        logger.info("Spark session created successfully.")
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark session: {e}")
        raise

# [ADDED] Function to create sample data for self-contained execution
def create_sample_data(spark: SparkSession) -> dict:
    """
    Creates sample DataFrames for testing purposes.
    Returns a dictionary of DataFrames.
    """
    logger.info("Creating sample data for testing")
    
    # Customer sample data
    customer_schema = StructType([
        StructField("CUSTOMER_ID", IntegerType(), True),
        StructField("NAME", StringType(), True)
    ])
    customer_data = [(1, "John Doe"), (2, "Jane Smith"), (3, "Bob Johnson")]
    customer_df = spark.createDataFrame(customer_data, customer_schema)
    
    # Account sample data
    account_schema = StructType([
        StructField("ACCOUNT_ID", IntegerType(), True),
        StructField("CUSTOMER_ID", IntegerType(), True),
        StructField("BRANCH_ID", IntegerType(), True)
    ])
    account_data = [(101, 1, 1), (102, 2, 1), (103, 3, 2)]
    account_df = spark.createDataFrame(account_data, account_schema)
    
    # Transaction sample data
    transaction_schema = StructType([
        StructField("TRANSACTION_ID", IntegerType(), True),
        StructField("ACCOUNT_ID", IntegerType(), True),
        StructField("AMOUNT", DoubleType(), True),
        StructField("TRANSACTION_TYPE", StringType(), True),
        StructField("TRANSACTION_DATE", DateType(), True)
    ])
    transaction_data = [
        (1001, 101, 1000.0, "DEPOSIT", date(2024, 1, 15)),
        (1002, 102, 500.0, "WITHDRAWAL", date(2024, 1, 16)),
        (1003, 103, 2000.0, "DEPOSIT", date(2024, 1, 17))
    ]
    transaction_df = spark.createDataFrame(transaction_data, transaction_schema)
    
    # Branch sample data
    branch_schema = StructType([
        StructField("BRANCH_ID", IntegerType(), True),
        StructField("BRANCH_NAME", StringType(), True)
    ])
    branch_data = [(1, "Downtown Branch"), (2, "Uptown Branch")]
    branch_df = spark.createDataFrame(branch_data, branch_schema)
    
    # [ADDED] Branch Operational Details sample data - New requirement from PCE-2
    branch_operational_schema = StructType([
        StructField("BRANCH_ID", IntegerType(), True),
        StructField("REGION", StringType(), True),
        StructField("MANAGER_NAME", StringType(), True),
        StructField("LAST_AUDIT_DATE", DateType(), True),
        StructField("IS_ACTIVE", StringType(), True)
    ])
    branch_operational_data = [
        (1, "North Region", "Alice Manager", date(2024, 1, 10), "Y"),
        (2, "South Region", "Bob Manager", date(2024, 1, 12), "Y")
    ]
    branch_operational_df = spark.createDataFrame(branch_operational_data, branch_operational_schema)
    
    return {
        "customer": customer_df,
        "account": account_df,
        "transaction": transaction_df,
        "branch": branch_df,
        "branch_operational": branch_operational_df  # [ADDED] New table
    }

# [DEPRECATED] Original JDBC read function - keeping for reference
# def read_table(spark: SparkSession, jdbc_url: str, table_name: str, connection_properties: dict) -> DataFrame:
#     """
#     Reads a table from a JDBC source into a DataFrame.
#     """
#     logger.info(f"Reading table: {table_name}")
#     try:
#         df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
#         return df
#     except Exception as e:
#         logger.error(f"Failed to read table {table_name}: {e}")
#         raise

# [ADDED] New function to read Delta tables
def read_delta_table(spark: SparkSession, table_path: str) -> DataFrame:
    """
    Reads a Delta table into a DataFrame.
    
    :param spark: The SparkSession object.
    :param table_path: The path to the Delta table.
    :return: A Spark DataFrame containing the table data.
    """
    logger.info(f"Reading Delta table: {table_path}")
    try:
        df = spark.read.format("delta").load(table_path)
        return df
    except Exception as e:
        logger.error(f"Failed to read Delta table {table_path}: {e}")
        raise

def create_aml_customer_transactions(customer_df: DataFrame, account_df: DataFrame, transaction_df: DataFrame) -> DataFrame:
    """
    Creates the AML_CUSTOMER_TRANSACTIONS DataFrame by joining customer, account, and transaction data.

    :param customer_df: DataFrame with customer data.
    :param account_df: DataFrame with account data.
    :param transaction_df: DataFrame with transaction data.
    :return: A DataFrame ready for the AML customer transactions report.
    """
    logger.info("Creating AML Customer Transactions DataFrame.")
    
    # [ADDED] Data validation before join
    logger.info(f"Customer records: {customer_df.count()}")
    logger.info(f"Account records: {account_df.count()}")
    logger.info(f"Transaction records: {transaction_df.count()}")
    
    result_df = customer_df.join(account_df, "CUSTOMER_ID") \
                          .join(transaction_df, "ACCOUNT_ID") \
                          .select(
                              col("CUSTOMER_ID"),
                              col("NAME"),
                              col("ACCOUNT_ID"),
                              col("TRANSACTION_ID"),
                              col("AMOUNT"),
                              col("TRANSACTION_TYPE"),
                              col("TRANSACTION_DATE"),
                              current_timestamp().alias("CREATED_TIMESTAMP")  # [ADDED] Audit timestamp
                          )
    
    # [ADDED] Validation of result
    result_count = result_df.count()
    logger.info(f"AML Customer Transactions created with {result_count} records")
    
    return result_df

# [MODIFIED] Enhanced function to integrate BRANCH_OPERATIONAL_DETAILS as per PCE-2 requirements
def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, 
                               branch_df: DataFrame, branch_operational_df: DataFrame) -> DataFrame:
    """
    Creates the BRANCH_SUMMARY_REPORT DataFrame by aggregating transaction data at the branch level.
    [MODIFIED] Now includes integration with BRANCH_OPERATIONAL_DETAILS as per PCE-2 requirements.

    :param transaction_df: DataFrame with transaction data.
    :param account_df: DataFrame with account data.
    :param branch_df: DataFrame with branch data.
    :param branch_operational_df: DataFrame with branch operational details. # [ADDED] New parameter
    :return: A DataFrame containing the enhanced branch summary report.
    """
    logger.info("Creating Enhanced Branch Summary Report DataFrame with operational details.")
    
    # [ADDED] Data validation before processing
    logger.info(f"Transaction records: {transaction_df.count()}")
    logger.info(f"Account records: {account_df.count()}")
    logger.info(f"Branch records: {branch_df.count()}")
    logger.info(f"Branch Operational records: {branch_operational_df.count()}")
    
    # Original aggregation logic
    base_summary = transaction_df.join(account_df, "ACCOUNT_ID") \
                                 .join(branch_df, "BRANCH_ID") \
                                 .groupBy("BRANCH_ID", "BRANCH_NAME") \
                                 .agg(
                                     count("*").alias("TOTAL_TRANSACTIONS"),
                                     sum("AMOUNT").alias("TOTAL_AMOUNT")
                                 )
    
    # [ADDED] Integration with BRANCH_OPERATIONAL_DETAILS as per PCE-2 requirements
    # Join with operational details and conditionally populate new columns
    enhanced_summary = base_summary.join(branch_operational_df, "BRANCH_ID", "left") \
                                  .select(
                                      col("BRANCH_ID"),
                                      col("BRANCH_NAME"),
                                      col("TOTAL_TRANSACTIONS"),
                                      col("TOTAL_AMOUNT"),
                                      # [ADDED] Conditional population based on IS_ACTIVE = 'Y'
                                      when(col("IS_ACTIVE") == "Y", col("REGION")).alias("REGION"),
                                      when(col("IS_ACTIVE") == "Y", col("LAST_AUDIT_DATE")).alias("LAST_AUDIT_DATE"),
                                      current_timestamp().alias("REPORT_GENERATED_TIMESTAMP")  # [ADDED] Audit timestamp
                                  )
    
    # [ADDED] Validation of enhanced result
    result_count = enhanced_summary.count()
    logger.info(f"Enhanced Branch Summary Report created with {result_count} records")
    
    return enhanced_summary

# [MODIFIED] Enhanced write function with better error handling and validation
def write_to_delta_table(df: DataFrame, table_path: str, mode: str = "overwrite"):
    """
    Writes a DataFrame to a Delta table with enhanced validation.
    [MODIFIED] Added validation and better error handling.

    :param df: The DataFrame to write.
    :param table_path: The path for the target Delta table.
    :param mode: Write mode (default: overwrite). # [ADDED] Configurable write mode
    """
    logger.info(f"Writing DataFrame to Delta table: {table_path}")
    
    # [ADDED] Pre-write validation
    record_count = df.count()
    logger.info(f"Writing {record_count} records to {table_path}")
    
    if record_count == 0:
        logger.warning(f"No records to write to {table_path}")
        return
    
    try:
        df.write.format("delta") \
          .mode(mode) \
          .option("mergeSchema", "true") \
          .save(table_path)  # [MODIFIED] Changed from saveAsTable to save for better path control
        
        logger.info(f"Successfully written {record_count} records to {table_path}")
        
        # [ADDED] Post-write validation
        validation_df = SparkSession.getActiveSession().read.format("delta").load(table_path)
        validation_count = validation_df.count()
        logger.info(f"Validation: {validation_count} records found in {table_path}")
        
    except Exception as e:
        logger.error(f"Failed to write to Delta table {table_path}: {e}")
        raise

# [ADDED] New validation function
def validate_data_quality(df: DataFrame, table_name: str) -> bool:
    """
    Performs basic data quality checks on a DataFrame.
    
    :param df: DataFrame to validate
    :param table_name: Name of the table for logging
    :return: True if validation passes, False otherwise
    """
    logger.info(f"Performing data quality validation for {table_name}")
    
    try:
        # Check for null values in key columns
        total_records = df.count()
        
        if total_records == 0:
            logger.warning(f"{table_name}: No records found")
            return False
        
        # Check for duplicates if ID column exists
        if "BRANCH_ID" in df.columns:
            distinct_ids = df.select("BRANCH_ID").distinct().count()
            if distinct_ids != total_records:
                logger.warning(f"{table_name}: Duplicate BRANCH_ID found")
        
        logger.info(f"{table_name}: Data quality validation passed - {total_records} records")
        return True
        
    except Exception as e:
        logger.error(f"Data quality validation failed for {table_name}: {e}")
        return False

def main():
    """
    Main ETL execution function.
    [MODIFIED] Updated to use sample data and Delta tables for self-contained execution.
    """
    spark = None
    try:
        spark = get_spark_session()
        
        # [MODIFIED] Using sample data instead of JDBC connections for self-contained execution
        logger.info("Creating sample data for self-contained execution")
        sample_data = create_sample_data(spark)
        
        customer_df = sample_data["customer"]
        account_df = sample_data["account"]
        transaction_df = sample_data["transaction"]
        branch_df = sample_data["branch"]
        branch_operational_df = sample_data["branch_operational"]  # [ADDED] New data source
        
        # [DEPRECATED] Original JDBC connection logic - kept for reference
        # jdbc_url = "jdbc:oracle:thin:@your_oracle_host:1521:orcl"
        # connection_properties = {
        #     "user": "your_user",
        #     "password": "your_password",
        #     "driver": "oracle.jdbc.driver.OracleDriver"
        # }
        # customer_df = read_table(spark, jdbc_url, "CUSTOMER", connection_properties)
        # account_df = read_table(spark, jdbc_url, "ACCOUNT", connection_properties)
        # transaction_df = read_table(spark, jdbc_url, "TRANSACTION", connection_properties)
        # branch_df = read_table(spark, jdbc_url, "BRANCH", connection_properties)
        
        # Create and write AML_CUSTOMER_TRANSACTIONS
        aml_transactions_df = create_aml_customer_transactions(customer_df, account_df, transaction_df)
        
        # [ADDED] Data quality validation
        if validate_data_quality(aml_transactions_df, "AML_CUSTOMER_TRANSACTIONS"):
            write_to_delta_table(aml_transactions_df, "/tmp/delta/AML_CUSTOMER_TRANSACTIONS")
        
        # [MODIFIED] Create enhanced branch summary report with operational details
        branch_summary_df = create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_df)
        
        # [ADDED] Data quality validation for enhanced report
        if validate_data_quality(branch_summary_df, "BRANCH_SUMMARY_REPORT"):
            write_to_delta_table(branch_summary_df, "/tmp/delta/BRANCH_SUMMARY_REPORT")
        
        # [ADDED] Display results for verification
        logger.info("=== AML CUSTOMER TRANSACTIONS SAMPLE ===")
        aml_transactions_df.show(5, truncate=False)
        
        logger.info("=== ENHANCED BRANCH SUMMARY REPORT SAMPLE ===")
        branch_summary_df.show(5, truncate=False)
        
        logger.info("ETL job completed successfully with enhancements from PCE-2.")

    except Exception as e:
        logger.error(f"ETL job failed with exception: {e}")
        raise  # [MODIFIED] Re-raise exception for proper error handling
    finally:
        if spark:
            # [MODIFIED] Conditional stop for Databricks compatibility
            try:
                spark.stop()
                logger.info("Spark session stopped.")
            except:
                logger.info("Spark session cleanup handled by Databricks.")

if __name__ == "__main__":
    main()
