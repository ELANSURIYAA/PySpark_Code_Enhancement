====================================================================
# Author: Ascendion AAVA
# Date: 
# Description: Enhanced Regulatory Reporting ETL with Branch Operational Details integration
====================================================================

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum, when, lit, coalesce
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, DateType, LongType, DoubleType
from datetime import datetime, date
from delta.tables import DeltaTable

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_spark_session(app_name: str = "RegulatoryReportingETL") -> SparkSession:
    """
    Initializes and returns a Spark session compatible with Spark Connect.
    # [MODIFIED] - Updated to use getActiveSession() for Spark Connect compatibility
    """
    try:
        # [MODIFIED] - Use getActiveSession() instead of creating new session for Spark Connect compatibility
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
        
        # [MODIFIED] - Removed sparkContext.setLogLevel call for Spark Connect compatibility
        logger.info("Spark session created successfully.")
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark session: {e}")
        raise

# [DEPRECATED] - Original JDBC-based read_table function commented out for reference
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

# [ADDED] - New function to create sample data for self-contained execution
def create_sample_data(spark: SparkSession) -> dict:
    """
    Creates sample DataFrames for testing purposes.
    # [ADDED] - Self-contained data creation for testing without external dependencies
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
        (1, "John Doe", "john.doe@email.com", "123-456-7890", "123 Main St", date(2023, 1, 15)),
        (2, "Jane Smith", "jane.smith@email.com", "234-567-8901", "456 Oak Ave", date(2023, 2, 20)),
        (3, "Bob Johnson", "bob.johnson@email.com", "345-678-9012", "789 Pine Rd", date(2023, 3, 10))
    ]
    
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
    
    # [ADDED] - New Branch Operational Details sample data
    branch_operational_schema = StructType([
        StructField("BRANCH_ID", IntegerType(), True),
        StructField("REGION", StringType(), True),
        StructField("MANAGER_NAME", StringType(), True),
        StructField("LAST_AUDIT_DATE", DateType(), True),
        StructField("IS_ACTIVE", StringType(), True)
    ])
    
    branch_operational_data = [
        (101, "East Coast", "Alice Manager", date(2023, 12, 1), "Y"),
        (102, "West Coast", "Bob Manager", date(2023, 11, 15), "Y"),
        (103, "Central Region", "Carol Manager", date(2023, 10, 20), "Y")
    ]
    
    # Account sample data
    account_schema = StructType([
        StructField("ACCOUNT_ID", IntegerType(), True),
        StructField("CUSTOMER_ID", IntegerType(), True),
        StructField("BRANCH_ID", IntegerType(), True),
        StructField("ACCOUNT_NUMBER", StringType(), True),
        StructField("ACCOUNT_TYPE", StringType(), True),
        StructField("BALANCE", DecimalType(15, 2), True),
        StructField("OPENED_DATE", DateType(), True)
    ])
    
    account_data = [
        (1001, 1, 101, "ACC001", "CHECKING", 5000.00, date(2023, 1, 20)),
        (1002, 2, 102, "ACC002", "SAVINGS", 15000.00, date(2023, 2, 25)),
        (1003, 3, 103, "ACC003", "CHECKING", 7500.00, date(2023, 3, 15)),
        (1004, 1, 101, "ACC004", "SAVINGS", 25000.00, date(2023, 1, 25))
    ]
    
    # Transaction sample data
    transaction_schema = StructType([
        StructField("TRANSACTION_ID", IntegerType(), True),
        StructField("ACCOUNT_ID", IntegerType(), True),
        StructField("TRANSACTION_TYPE", StringType(), True),
        StructField("AMOUNT", DecimalType(15, 2), True),
        StructField("TRANSACTION_DATE", DateType(), True),
        StructField("DESCRIPTION", StringType(), True)
    ])
    
    transaction_data = [
        (10001, 1001, "DEPOSIT", 1000.00, date(2023, 4, 1), "Salary deposit"),
        (10002, 1001, "WITHDRAWAL", 200.00, date(2023, 4, 2), "ATM withdrawal"),
        (10003, 1002, "DEPOSIT", 2000.00, date(2023, 4, 1), "Transfer in"),
        (10004, 1003, "WITHDRAWAL", 500.00, date(2023, 4, 3), "Check payment"),
        (10005, 1004, "DEPOSIT", 5000.00, date(2023, 4, 1), "Investment return")
    ]
    
    # Create DataFrames
    customer_df = spark.createDataFrame(customer_data, customer_schema)
    branch_df = spark.createDataFrame(branch_data, branch_schema)
    branch_operational_df = spark.createDataFrame(branch_operational_data, branch_operational_schema)  # [ADDED]
    account_df = spark.createDataFrame(account_data, account_schema)
    transaction_df = spark.createDataFrame(transaction_data, transaction_schema)
    
    return {
        "customer": customer_df,
        "branch": branch_df,
        "branch_operational": branch_operational_df,  # [ADDED]
        "account": account_df,
        "transaction": transaction_df
    }

def create_aml_customer_transactions(customer_df: DataFrame, account_df: DataFrame, transaction_df: DataFrame) -> DataFrame:
    """
    Creates the AML_CUSTOMER_TRANSACTIONS DataFrame by joining customer, account, and transaction data.
    # [UNCHANGED] - Original logic preserved
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

# [MODIFIED] - Enhanced function to include branch operational details
def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, branch_df: DataFrame, branch_operational_df: DataFrame) -> DataFrame:
    """
    Creates the BRANCH_SUMMARY_REPORT DataFrame by aggregating transaction data at the branch level.
    # [MODIFIED] - Added branch_operational_df parameter and enhanced logic to include REGION and LAST_AUDIT_DATE
    """
    logger.info("Creating Enhanced Branch Summary Report DataFrame with operational details.")
    
    # [MODIFIED] - Enhanced aggregation with additional joins and fields
    base_summary = transaction_df.join(account_df, "ACCOUNT_ID") \
                                 .join(branch_df, "BRANCH_ID") \
                                 .groupBy("BRANCH_ID", "BRANCH_NAME") \
                                 .agg(
                                     count("*").alias("TOTAL_TRANSACTIONS"),
                                     sum("AMOUNT").alias("TOTAL_AMOUNT")
                                 )
    
    # [ADDED] - Join with branch operational details to include REGION and LAST_AUDIT_DATE
    enhanced_summary = base_summary.join(branch_operational_df, "BRANCH_ID", "left") \
                                  .select(
                                      col("BRANCH_ID").cast(LongType()),  # [MODIFIED] - Cast to match target schema
                                      col("BRANCH_NAME"),
                                      col("TOTAL_TRANSACTIONS"),
                                      col("TOTAL_AMOUNT").cast(DoubleType()),  # [MODIFIED] - Cast to match target schema
                                      coalesce(col("REGION"), lit("Unknown")).alias("REGION"),  # [ADDED] - Handle null regions
                                      col("LAST_AUDIT_DATE").cast(StringType()).alias("LAST_AUDIT_DATE")  # [ADDED] - Cast date to string as per target schema
                                  )
    
    return enhanced_summary

# [MODIFIED] - Enhanced write function with merge capability
def write_to_delta_table(df: DataFrame, table_name: str, mode: str = "overwrite"):
    """
    Writes a DataFrame to a Delta table with enhanced error handling.
    # [MODIFIED] - Added mode parameter and enhanced error handling
    """
    logger.info(f"Writing DataFrame to Delta table: {table_name} with mode: {mode}")
    try:
        df.write.format("delta") \
          .mode(mode) \
          .option("mergeSchema", "true") \
          .saveAsTable(table_name)  # [MODIFIED] - Added mergeSchema option for schema evolution
        logger.info(f"Successfully written data to {table_name}")
    except Exception as e:
        logger.error(f"Failed to write to Delta table {table_name}: {e}")
        raise

# [ADDED] - New function for merge operations
def merge_to_delta_table(source_df: DataFrame, target_table: str, merge_keys: list):
    """
    Performs merge operation to Delta table for upsert functionality.
    # [ADDED] - New merge functionality for incremental updates
    """
    logger.info(f"Performing merge operation on Delta table: {target_table}")
    try:
        # Check if table exists
        spark = SparkSession.getActiveSession()
        if spark.catalog.tableExists(target_table):
            delta_table = DeltaTable.forName(spark, target_table)
            
            # Build merge condition
            merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in merge_keys])
            
            delta_table.alias("target") \
                .merge(source_df.alias("source"), merge_condition) \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
            
            logger.info(f"Successfully merged data to {target_table}")
        else:
            # If table doesn't exist, create it
            write_to_delta_table(source_df, target_table, "overwrite")
            
    except Exception as e:
        logger.error(f"Failed to merge to Delta table {target_table}: {e}")
        raise

# [ADDED] - Data validation function
def validate_data_quality(df: DataFrame, table_name: str) -> bool:
    """
    Validates data quality for the given DataFrame.
    # [ADDED] - New data quality validation functionality
    """
    logger.info(f"Validating data quality for {table_name}")
    
    try:
        # Check for null values in critical columns
        if table_name == "BRANCH_SUMMARY_REPORT":
            null_checks = df.filter(
                col("BRANCH_ID").isNull() | 
                col("BRANCH_NAME").isNull() |
                col("TOTAL_TRANSACTIONS").isNull() |
                col("TOTAL_AMOUNT").isNull()
            ).count()
            
            if null_checks > 0:
                logger.warning(f"Found {null_checks} rows with null values in critical columns")
                return False
        
        # Check for negative amounts
        if "TOTAL_AMOUNT" in df.columns:
            negative_amounts = df.filter(col("TOTAL_AMOUNT") < 0).count()
            if negative_amounts > 0:
                logger.warning(f"Found {negative_amounts} rows with negative amounts")
                return False
        
        logger.info(f"Data quality validation passed for {table_name}")
        return True
        
    except Exception as e:
        logger.error(f"Data quality validation failed for {table_name}: {e}")
        return False

def main():
    """
    Main ETL execution function.
    # [MODIFIED] - Enhanced with new data sources and validation
    """
    spark = None
    try:
        spark = get_spark_session()
        
        # [MODIFIED] - Use sample data instead of JDBC connections for self-contained execution
        logger.info("Creating sample data for self-contained execution")
        sample_data = create_sample_data(spark)
        
        customer_df = sample_data["customer"]
        account_df = sample_data["account"]
        transaction_df = sample_data["transaction"]
        branch_df = sample_data["branch"]
        branch_operational_df = sample_data["branch_operational"]  # [ADDED]
        
        # [UNCHANGED] - Create and write AML_CUSTOMER_TRANSACTIONS
        aml_transactions_df = create_aml_customer_transactions(customer_df, account_df, transaction_df)
        
        # [ADDED] - Data quality validation
        if validate_data_quality(aml_transactions_df, "AML_CUSTOMER_TRANSACTIONS"):
            write_to_delta_table(aml_transactions_df, "AML_CUSTOMER_TRANSACTIONS")
        else:
            logger.error("Data quality validation failed for AML_CUSTOMER_TRANSACTIONS")
            raise Exception("Data quality validation failed")
        
        # [MODIFIED] - Create and write enhanced BRANCH_SUMMARY_REPORT with operational details
        branch_summary_df = create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_df)
        
        # [ADDED] - Data quality validation for branch summary
        if validate_data_quality(branch_summary_df, "BRANCH_SUMMARY_REPORT"):
            write_to_delta_table(branch_summary_df, "workspace.default.branch_summary_report")
        else:
            logger.error("Data quality validation failed for BRANCH_SUMMARY_REPORT")
            raise Exception("Data quality validation failed")
        
        # [ADDED] - Display sample results for verification
        logger.info("Displaying sample results:")
        print("\n=== AML Customer Transactions Sample ===")
        aml_transactions_df.show(5, truncate=False)
        
        print("\n=== Enhanced Branch Summary Report Sample ===")
        branch_summary_df.show(truncate=False)
        
        logger.info("Enhanced ETL job completed successfully with operational details integration.")
        
    except Exception as e:
        logger.error(f"ETL job failed with exception: {e}")
        raise
    finally:
        if spark:
            # [MODIFIED] - Removed spark.stop() for Spark Connect compatibility
            logger.info("ETL execution completed.")

if __name__ == "__main__":
    main()
