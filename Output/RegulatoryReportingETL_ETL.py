# ====================================================================
# Author: Ascendion AAVA
# Date: 
# Description: Enhanced PySpark ETL for Regulatory Reporting with BRANCH_OPERATIONAL_DETAILS integration
# ====================================================================

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum as spark_sum, when, lit, current_date
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, DateType, LongType, DoubleType
from delta.tables import DeltaTable
import os

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
                raise Exception("No active session found")
        except:
            spark = SparkSession.builder \
                .appName(app_name) \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                .getOrCreate()
        
        # [MODIFIED] - Removed sparkContext.setLogLevel() call for Spark Connect compatibility
        spark.conf.set("spark.sql.adaptive.enabled", "true")
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        logger.info("Spark session created successfully.")
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark session: {e}")
        raise

# [DEPRECATED] - Legacy JDBC read function commented out for reference
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

def create_sample_data(spark: SparkSession):
    """
    # [ADDED] - Creates sample data for self-contained execution without external dependencies
    Creates sample DataFrames for all source tables to simulate real data.
    """
    logger.info("Creating sample data for testing")
    
    # Sample CUSTOMER data
    customer_schema = StructType([
        StructField("CUSTOMER_ID", IntegerType(), True),
        StructField("NAME", StringType(), True),
        StructField("EMAIL", StringType(), True),
        StructField("PHONE", StringType(), True),
        StructField("ADDRESS", StringType(), True),
        StructField("CREATED_DATE", DateType(), True)
    ])
    
    customer_data = [
        (1, "John Doe", "john.doe@email.com", "123-456-7890", "123 Main St", None),
        (2, "Jane Smith", "jane.smith@email.com", "098-765-4321", "456 Oak Ave", None),
        (3, "Bob Johnson", "bob.johnson@email.com", "555-123-4567", "789 Pine Rd", None)
    ]
    
    # Sample BRANCH data
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
    
    # Sample ACCOUNT data
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
        (1001, 1, 101, "ACC001", "SAVINGS", 5000.00, None),
        (1002, 2, 102, "ACC002", "CHECKING", 3000.00, None),
        (1003, 3, 103, "ACC003", "SAVINGS", 7500.00, None),
        (1004, 1, 101, "ACC004", "CHECKING", 2500.00, None)
    ]
    
    # Sample TRANSACTION data
    transaction_schema = StructType([
        StructField("TRANSACTION_ID", IntegerType(), True),
        StructField("ACCOUNT_ID", IntegerType(), True),
        StructField("TRANSACTION_TYPE", StringType(), True),
        StructField("AMOUNT", DecimalType(15,2), True),
        StructField("TRANSACTION_DATE", DateType(), True),
        StructField("DESCRIPTION", StringType(), True)
    ])
    
    transaction_data = [
        (10001, 1001, "DEPOSIT", 1000.00, None, "Cash deposit"),
        (10002, 1001, "WITHDRAWAL", 500.00, None, "ATM withdrawal"),
        (10003, 1002, "DEPOSIT", 2000.00, None, "Check deposit"),
        (10004, 1003, "TRANSFER", 1500.00, None, "Wire transfer"),
        (10005, 1004, "DEPOSIT", 800.00, None, "Direct deposit")
    ]
    
    # [ADDED] - New BRANCH_OPERATIONAL_DETAILS sample data as per Jira requirements
    branch_operational_schema = StructType([
        StructField("BRANCH_ID", IntegerType(), True),
        StructField("REGION", StringType(), True),
        StructField("MANAGER_NAME", StringType(), True),
        StructField("LAST_AUDIT_DATE", DateType(), True),
        StructField("IS_ACTIVE", StringType(), True)
    ])
    
    branch_operational_data = [
        (101, "Northeast", "Alice Manager", None, "Y"),
        (102, "West", "Bob Manager", None, "Y"),
        (103, "Midwest", "Carol Manager", None, "N")  # Inactive branch for testing conditional logic
    ]
    
    # Create DataFrames
    customer_df = spark.createDataFrame(customer_data, customer_schema)
    branch_df = spark.createDataFrame(branch_data, branch_schema)
    account_df = spark.createDataFrame(account_data, account_schema)
    transaction_df = spark.createDataFrame(transaction_data, transaction_schema)
    branch_operational_df = spark.createDataFrame(branch_operational_data, branch_operational_schema)
    
    # Add current_date for date fields
    customer_df = customer_df.withColumn("CREATED_DATE", current_date())
    account_df = account_df.withColumn("OPENED_DATE", current_date())
    transaction_df = transaction_df.withColumn("TRANSACTION_DATE", current_date())
    branch_operational_df = branch_operational_df.withColumn("LAST_AUDIT_DATE", current_date())
    
    return customer_df, branch_df, account_df, transaction_df, branch_operational_df

def read_delta_table(spark: SparkSession, table_path: str) -> DataFrame:
    """
    # [ADDED] - New function to read Delta tables
    Reads a Delta table from the specified path.
    """
    logger.info(f"Reading Delta table from: {table_path}")
    try:
        df = spark.read.format("delta").load(table_path)
        logger.info(f"Successfully read Delta table with {df.count()} rows")
        return df
    except Exception as e:
        logger.error(f"Failed to read Delta table from {table_path}: {e}")
        raise

def create_aml_customer_transactions(customer_df: DataFrame, account_df: DataFrame, transaction_df: DataFrame) -> DataFrame:
    """
    Creates the AML_CUSTOMER_TRANSACTIONS DataFrame by joining customer, account, and transaction data.
    # [MODIFIED] - Enhanced with better error handling and validation
    """
    logger.info("Creating AML Customer Transactions DataFrame.")
    try:
        # [ADDED] - Data validation before joins
        logger.info(f"Input data counts - Customers: {customer_df.count()}, Accounts: {account_df.count()}, Transactions: {transaction_df.count()}")
        
        result_df = customer_df.join(account_df, "CUSTOMER_ID", "inner") \
                              .join(transaction_df, "ACCOUNT_ID", "inner") \
                              .select(
                                  col("CUSTOMER_ID"),
                                  col("NAME"),
                                  col("ACCOUNT_ID"),
                                  col("TRANSACTION_ID"),
                                  col("AMOUNT"),
                                  col("TRANSACTION_TYPE"),
                                  col("TRANSACTION_DATE")
                              )
        
        # [ADDED] - Result validation
        result_count = result_df.count()
        logger.info(f"AML Customer Transactions created with {result_count} records")
        
        return result_df
    except Exception as e:
        logger.error(f"Error creating AML Customer Transactions: {e}")
        raise

def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, branch_df: DataFrame, branch_operational_df: DataFrame) -> DataFrame:
    """
    # [MODIFIED] - Enhanced to include BRANCH_OPERATIONAL_DETAILS integration as per Jira PCE-2 requirements
    Creates the BRANCH_SUMMARY_REPORT DataFrame by aggregating transaction data at the branch level
    and integrating operational details with conditional population based on IS_ACTIVE status.
    """
    logger.info("Creating Branch Summary Report DataFrame with operational details integration.")
    try:
        # [ADDED] - Input data validation
        logger.info(f"Input data counts - Transactions: {transaction_df.count()}, Accounts: {account_df.count()}, Branches: {branch_df.count()}, Branch Operational: {branch_operational_df.count()}")
        
        # Original aggregation logic (preserved)
        base_summary = transaction_df.join(account_df, "ACCOUNT_ID", "inner") \
                                   .join(branch_df, "BRANCH_ID", "inner") \
                                   .groupBy("BRANCH_ID", "BRANCH_NAME") \
                                   .agg(
                                       count("*").alias("TOTAL_TRANSACTIONS"),
                                       spark_sum("AMOUNT").alias("TOTAL_AMOUNT")
                                   )
        
        # [ADDED] - Integration with BRANCH_OPERATIONAL_DETAILS as per Jira requirements
        # Join with operational details and conditionally populate REGION and LAST_AUDIT_DATE
        enhanced_summary = base_summary.join(branch_operational_df, "BRANCH_ID", "left") \
                                     .withColumn(
                                         "REGION", 
                                         when(col("IS_ACTIVE") == "Y", col("REGION")).otherwise(lit(None))
                                     ) \
                                     .withColumn(
                                         "LAST_AUDIT_DATE", 
                                         when(col("IS_ACTIVE") == "Y", col("LAST_AUDIT_DATE")).otherwise(lit(None))
                                     ) \
                                     .select(
                                         col("BRANCH_ID"),
                                         col("BRANCH_NAME"),
                                         col("TOTAL_TRANSACTIONS"),
                                         col("TOTAL_AMOUNT"),
                                         col("REGION"),  # [ADDED] - New column as per target schema
                                         col("LAST_AUDIT_DATE")  # [ADDED] - New column as per target schema
                                     )
        
        # [ADDED] - Result validation
        result_count = enhanced_summary.count()
        logger.info(f"Enhanced Branch Summary Report created with {result_count} records")
        
        # [ADDED] - Log sample of results for verification
        logger.info("Sample of enhanced branch summary:")
        enhanced_summary.show(5, truncate=False)
        
        return enhanced_summary
        
    except Exception as e:
        logger.error(f"Error creating Branch Summary Report: {e}")
        raise

def write_to_delta_table(df: DataFrame, table_path: str, mode: str = "overwrite"):
    """
    # [MODIFIED] - Enhanced Delta table writing with better error handling and merge capability
    Writes a DataFrame to a Delta table with support for different write modes.
    """
    logger.info(f"Writing DataFrame to Delta table: {table_path} with mode: {mode}")
    try:
        # [ADDED] - Ensure directory exists
        os.makedirs(os.path.dirname(table_path), exist_ok=True)
        
        df.write.format("delta") \
          .mode(mode) \
          .option("mergeSchema", "true") \
          .save(table_path)
          
        logger.info(f"Successfully written {df.count()} records to {table_path}")
        
        # [ADDED] - Verification read
        verification_df = df.sparkSession.read.format("delta").load(table_path)
        logger.info(f"Verification: Delta table contains {verification_df.count()} records")
        
    except Exception as e:
        logger.error(f"Failed to write to Delta table {table_path}: {e}")
        raise

def upsert_to_delta_table(source_df: DataFrame, target_path: str, merge_keys: list, update_condition: str = None):
    """
    # [ADDED] - New function for Delta table upsert operations
    Performs upsert (merge) operation on Delta table for incremental updates.
    """
    logger.info(f"Performing upsert to Delta table: {target_path}")
    try:
        # Check if target table exists
        if DeltaTable.isDeltaTable(source_df.sparkSession, target_path):
            delta_table = DeltaTable.forPath(source_df.sparkSession, target_path)
            
            # Build merge condition
            merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in merge_keys])
            
            # Perform merge
            merge_builder = delta_table.alias("target").merge(
                source_df.alias("source"),
                merge_condition
            )
            
            if update_condition:
                merge_builder = merge_builder.whenMatchedUpdate(condition=update_condition, set={})
            else:
                merge_builder = merge_builder.whenMatchedUpdateAll()
            
            merge_builder.whenNotMatchedInsertAll().execute()
            
            logger.info(f"Successfully performed upsert to {target_path}")
        else:
            # If table doesn't exist, create it
            write_to_delta_table(source_df, target_path, "overwrite")
            logger.info(f"Created new Delta table at {target_path}")
            
    except Exception as e:
        logger.error(f"Failed to upsert to Delta table {target_path}: {e}")
        raise

def validate_data_quality(df: DataFrame, table_name: str) -> bool:
    """
    # [ADDED] - Data quality validation function
    Performs basic data quality checks on the DataFrame.
    """
    logger.info(f"Validating data quality for {table_name}")
    try:
        total_records = df.count()
        
        if total_records == 0:
            logger.warning(f"{table_name} contains no records")
            return False
            
        # Check for null values in key columns
        null_checks = {}
        for column in df.columns:
            null_count = df.filter(col(column).isNull()).count()
            null_checks[column] = null_count
            
        logger.info(f"Data quality summary for {table_name}: Total records: {total_records}")
        logger.info(f"Null value counts: {null_checks}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error validating data quality for {table_name}: {e}")
        return False

def main():
    """
    # [MODIFIED] - Enhanced main function with Delta table support and new integration logic
    Main ETL execution function with enhanced error handling and Delta table operations.
    """
    spark = None
    try:
        spark = get_spark_session()
        
        # [MODIFIED] - Use sample data instead of JDBC connections for self-contained execution
        # Create sample data for testing
        customer_df, branch_df, account_df, transaction_df, branch_operational_df = create_sample_data(spark)
        
        # [DEPRECATED] - Legacy JDBC connection code commented out
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
        
        # [ADDED] - Data quality validation
        validate_data_quality(customer_df, "CUSTOMER")
        validate_data_quality(account_df, "ACCOUNT")
        validate_data_quality(transaction_df, "TRANSACTION")
        validate_data_quality(branch_df, "BRANCH")
        validate_data_quality(branch_operational_df, "BRANCH_OPERATIONAL_DETAILS")  # [ADDED] - New validation
        
        # Create and write AML_CUSTOMER_TRANSACTIONS
        aml_transactions_df = create_aml_customer_transactions(customer_df, account_df, transaction_df)
        
        # [MODIFIED] - Write to Delta format instead of managed table
        aml_delta_path = "/tmp/delta/aml_customer_transactions"
        write_to_delta_table(aml_transactions_df, aml_delta_path)
        
        # [MODIFIED] - Create enhanced branch summary with operational details integration
        branch_summary_df = create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_df)
        
        # [MODIFIED] - Write enhanced branch summary to Delta format
        branch_summary_delta_path = "/tmp/delta/branch_summary_report"
        write_to_delta_table(branch_summary_df, branch_summary_delta_path)
        
        # [ADDED] - Demonstrate upsert capability for incremental updates
        logger.info("Demonstrating upsert capability...")
        
        # Create a small update dataset
        update_data = [
            (101, "Downtown Branch", 10, 15000.00, "Northeast", None),
            (104, "New Branch", 5, 8000.00, "South", None)  # New branch
        ]
        
        update_schema = StructType([
            StructField("BRANCH_ID", LongType(), True),
            StructField("BRANCH_NAME", StringType(), True),
            StructField("TOTAL_TRANSACTIONS", LongType(), True),
            StructField("TOTAL_AMOUNT", DoubleType(), True),
            StructField("REGION", StringType(), True),
            StructField("LAST_AUDIT_DATE", DateType(), True)
        ])
        
        update_df = spark.createDataFrame(update_data, update_schema)
        update_df = update_df.withColumn("LAST_AUDIT_DATE", current_date())
        
        # Perform upsert
        upsert_to_delta_table(update_df, branch_summary_delta_path, ["BRANCH_ID"])
        
        logger.info("ETL job completed successfully with enhanced functionality.")
        
        # [ADDED] - Final verification
        final_aml_count = spark.read.format("delta").load(aml_delta_path).count()
        final_branch_count = spark.read.format("delta").load(branch_summary_delta_path).count()
        
        logger.info(f"Final record counts - AML Transactions: {final_aml_count}, Branch Summary: {final_branch_count}")
        
    except Exception as e:
        logger.error(f"ETL job failed with exception: {e}")
        raise  # [MODIFIED] - Re-raise exception for proper error handling
    finally:
        if spark:
            # [MODIFIED] - Conditional spark.stop() to avoid issues in Databricks
            try:
                spark.stop()
                logger.info("Spark session stopped.")
            except:
                logger.info("Spark session cleanup handled by environment.")

if __name__ == "__main__":
    main()