_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Comprehensive unit tests for RegulatoryReportingETL PySpark pipeline with BRANCH_OPERATIONAL_DETAILS integration
## *Version*: 1 
## *Updated on*: 
_____________________________________________

import pytest
import logging
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum as spark_sum, when, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, DateType, LongType, DoubleType
from datetime import date
import sys
import os

# Add the source code directory to the path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import functions from the main ETL module
try:
    from RegulatoryReportingETL_Pipeline_1 import (
        get_spark_session,
        create_sample_data,
        create_aml_customer_transactions,
        create_enhanced_branch_summary_report,
        write_to_delta_table,
        validate_data_quality,
        main
    )
except ImportError:
    # Mock imports if the actual module is not available
    pass

class TestRegulatoryReportingETL:
    """
    Comprehensive test suite for Regulatory Reporting ETL Pipeline.
    Tests cover functionality, edge cases, error handling, and data validation.
    """
    
    @pytest.fixture(scope="class")
    def spark_session(self):
        """
        Create a test Spark session for unit tests.
        """
        spark = SparkSession.builder \
            .appName("TestRegulatoryReportingETL") \
            .master("local[2]") \
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
        yield spark
        spark.stop()
    
    @pytest.fixture
    def sample_dataframes(self, spark_session):
        """
        Create sample test data for unit tests.
        """
        # Customer test data
        customer_schema = StructType([
            StructField("CUSTOMER_ID", IntegerType(), True),
            StructField("NAME", StringType(), True),
            StructField("EMAIL", StringType(), True),
            StructField("PHONE", StringType(), True),
            StructField("ADDRESS", StringType(), True),
            StructField("CREATED_DATE", DateType(), True)
        ])
        
        customer_data = [
            (1, "Test Customer 1", "test1@email.com", "123-456-7890", "123 Test St", date(2023, 1, 15)),
            (2, "Test Customer 2", "test2@email.com", "098-765-4321", "456 Test Ave", date(2023, 2, 20))
        ]
        
        customer_df = spark_session.createDataFrame(customer_data, customer_schema)
        
        # Account test data
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
            (1001, 1, 101, "ACC001", "SAVINGS", 5000.00, date(2023, 1, 16)),
            (1002, 2, 102, "ACC002", "CHECKING", 2500.00, date(2023, 2, 21))
        ]
        
        account_df = spark_session.createDataFrame(account_data, account_schema)
        
        # Transaction test data
        transaction_schema = StructType([
            StructField("TRANSACTION_ID", IntegerType(), True),
            StructField("ACCOUNT_ID", IntegerType(), True),
            StructField("TRANSACTION_TYPE", StringType(), True),
            StructField("AMOUNT", DecimalType(15, 2), True),
            StructField("TRANSACTION_DATE", DateType(), True),
            StructField("DESCRIPTION", StringType(), True)
        ])
        
        transaction_data = [
            (10001, 1001, "DEPOSIT", 1000.00, date(2023, 5, 1), "Test deposit"),
            (10002, 1002, "WITHDRAWAL", 200.00, date(2023, 5, 2), "Test withdrawal")
        ]
        
        transaction_df = spark_session.createDataFrame(transaction_data, transaction_schema)
        
        # Branch test data
        branch_schema = StructType([
            StructField("BRANCH_ID", IntegerType(), True),
            StructField("BRANCH_NAME", StringType(), True),
            StructField("BRANCH_CODE", StringType(), True),
            StructField("CITY", StringType(), True),
            StructField("STATE", StringType(), True),
            StructField("COUNTRY", StringType(), True)
        ])
        
        branch_data = [
            (101, "Test Branch 1", "TB001", "Test City 1", "TS", "USA"),
            (102, "Test Branch 2", "TB002", "Test City 2", "TS", "USA")
        ]
        
        branch_df = spark_session.createDataFrame(branch_data, branch_schema)
        
        # Branch Operational test data
        branch_operational_schema = StructType([
            StructField("BRANCH_ID", IntegerType(), True),
            StructField("REGION", StringType(), True),
            StructField("MANAGER_NAME", StringType(), True),
            StructField("LAST_AUDIT_DATE", DateType(), True),
            StructField("IS_ACTIVE", StringType(), True)
        ])
        
        branch_operational_data = [
            (101, "Test Region 1", "Test Manager 1", date(2023, 4, 15), "Y"),
            (102, "Test Region 2", "Test Manager 2", date(2023, 3, 20), "N")  # Inactive branch
        ]
        
        branch_operational_df = spark_session.createDataFrame(branch_operational_data, branch_operational_schema)
        
        return customer_df, account_df, transaction_df, branch_df, branch_operational_df
    
    def test_get_spark_session_success(self):
        """
        Test successful Spark session creation.
        """
        with patch('RegulatoryReportingETL_Pipeline_1.SparkSession') as mock_spark:
            mock_session = Mock()
            mock_spark.getActiveSession.return_value = None
            mock_spark.builder.appName.return_value.config.return_value.config.return_value.getOrCreate.return_value = mock_session
            
            result = get_spark_session("TestApp")
            
            assert result == mock_session
            mock_spark.builder.appName.assert_called_with("TestApp")
    
    def test_get_spark_session_with_active_session(self):
        """
        Test Spark session creation when active session exists.
        """
        with patch('RegulatoryReportingETL_Pipeline_1.SparkSession') as mock_spark:
            mock_session = Mock()
            mock_spark.getActiveSession.return_value = mock_session
            
            result = get_spark_session()
            
            assert result == mock_session
    
    def test_get_spark_session_exception(self):
        """
        Test Spark session creation exception handling.
        """
        with patch('RegulatoryReportingETL_Pipeline_1.SparkSession') as mock_spark:
            mock_spark.getActiveSession.side_effect = Exception("Test exception")
            
            with pytest.raises(Exception, match="Test exception"):
                get_spark_session()
    
    def test_create_sample_data_structure(self, spark_session):
        """
        Test that create_sample_data returns correct DataFrame structures.
        """
        customer_df, account_df, transaction_df, branch_df, branch_operational_df = create_sample_data(spark_session)
        
        # Test DataFrame counts
        assert customer_df.count() == 3
        assert account_df.count() == 4
        assert transaction_df.count() == 5
        assert branch_df.count() == 3
        assert branch_operational_df.count() == 3
        
        # Test DataFrame schemas
        expected_customer_columns = ["CUSTOMER_ID", "NAME", "EMAIL", "PHONE", "ADDRESS", "CREATED_DATE"]
        assert customer_df.columns == expected_customer_columns
        
        expected_account_columns = ["ACCOUNT_ID", "CUSTOMER_ID", "BRANCH_ID", "ACCOUNT_NUMBER", "ACCOUNT_TYPE", "BALANCE", "OPENED_DATE"]
        assert account_df.columns == expected_account_columns
        
        expected_transaction_columns = ["TRANSACTION_ID", "ACCOUNT_ID", "TRANSACTION_TYPE", "AMOUNT", "TRANSACTION_DATE", "DESCRIPTION"]
        assert transaction_df.columns == expected_transaction_columns
        
        expected_branch_columns = ["BRANCH_ID", "BRANCH_NAME", "BRANCH_CODE", "CITY", "STATE", "COUNTRY"]
        assert branch_df.columns == expected_branch_columns
        
        expected_branch_operational_columns = ["BRANCH_ID", "REGION", "MANAGER_NAME", "LAST_AUDIT_DATE", "IS_ACTIVE"]
        assert branch_operational_df.columns == expected_branch_operational_columns
    
    def test_create_aml_customer_transactions(self, spark_session, sample_dataframes):
        """
        Test AML customer transactions creation.
        """
        customer_df, account_df, transaction_df, branch_df, branch_operational_df = sample_dataframes
        
        result_df = create_aml_customer_transactions(customer_df, account_df, transaction_df)
        
        # Test result structure
        expected_columns = ["CUSTOMER_ID", "NAME", "ACCOUNT_ID", "TRANSACTION_ID", "AMOUNT", "TRANSACTION_TYPE", "TRANSACTION_DATE"]
        assert result_df.columns == expected_columns
        
        # Test data integrity
        assert result_df.count() == 2  # Based on sample data joins
        
        # Test that all transactions have corresponding customers and accounts
        customer_ids = [row.CUSTOMER_ID for row in result_df.collect()]
        assert all(cid in [1, 2] for cid in customer_ids)
    
    def test_create_aml_customer_transactions_empty_data(self, spark_session):
        """
        Test AML customer transactions with empty input data.
        """
        # Create empty DataFrames with correct schemas
        customer_schema = StructType([StructField("CUSTOMER_ID", IntegerType(), True), StructField("NAME", StringType(), True)])
        account_schema = StructType([StructField("CUSTOMER_ID", IntegerType(), True), StructField("ACCOUNT_ID", IntegerType(), True)])
        transaction_schema = StructType([StructField("ACCOUNT_ID", IntegerType(), True), StructField("TRANSACTION_ID", IntegerType(), True)])
        
        empty_customer_df = spark_session.createDataFrame([], customer_schema)
        empty_account_df = spark_session.createDataFrame([], account_schema)
        empty_transaction_df = spark_session.createDataFrame([], transaction_schema)
        
        result_df = create_aml_customer_transactions(empty_customer_df, empty_account_df, empty_transaction_df)
        
        assert result_df.count() == 0
    
    def test_create_enhanced_branch_summary_report(self, spark_session, sample_dataframes):
        """
        Test enhanced branch summary report creation with operational details.
        """
        customer_df, account_df, transaction_df, branch_df, branch_operational_df = sample_dataframes
        
        result_df = create_enhanced_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_df)
        
        # Test result structure
        expected_columns = ["BRANCH_ID", "BRANCH_NAME", "TOTAL_TRANSACTIONS", "TOTAL_AMOUNT", "REGION", "LAST_AUDIT_DATE"]
        assert result_df.columns == expected_columns
        
        # Test data types
        schema_dict = {field.name: field.dataType for field in result_df.schema.fields}
        assert isinstance(schema_dict["BRANCH_ID"], LongType)
        assert isinstance(schema_dict["TOTAL_AMOUNT"], DoubleType)
        
        # Test conditional logic for IS_ACTIVE = 'Y'
        result_data = result_df.collect()
        
        # Branch 101 should have REGION populated (IS_ACTIVE = 'Y')
        branch_101_data = [row for row in result_data if row.BRANCH_ID == 101]
        if branch_101_data:
            assert branch_101_data[0].REGION == "Test Region 1"
            assert branch_101_data[0].LAST_AUDIT_DATE is not None
        
        # Branch 102 should have REGION as null (IS_ACTIVE = 'N')
        branch_102_data = [row for row in result_data if row.BRANCH_ID == 102]
        if branch_102_data:
            assert branch_102_data[0].REGION is None
            assert branch_102_data[0].LAST_AUDIT_DATE is None
    
    def test_create_enhanced_branch_summary_report_no_operational_data(self, spark_session, sample_dataframes):
        """
        Test enhanced branch summary report with missing operational data.
        """
        customer_df, account_df, transaction_df, branch_df, branch_operational_df = sample_dataframes
        
        # Create empty operational DataFrame
        empty_operational_schema = StructType([
            StructField("BRANCH_ID", IntegerType(), True),
            StructField("REGION", StringType(), True),
            StructField("MANAGER_NAME", StringType(), True),
            StructField("LAST_AUDIT_DATE", DateType(), True),
            StructField("IS_ACTIVE", StringType(), True)
        ])
        empty_operational_df = spark_session.createDataFrame([], empty_operational_schema)
        
        result_df = create_enhanced_branch_summary_report(transaction_df, account_df, branch_df, empty_operational_df)
        
        # Should still produce results but with null operational fields
        assert result_df.count() > 0
        result_data = result_df.collect()
        for row in result_data:
            assert row.REGION is None
            assert row.LAST_AUDIT_DATE is None
    
    @patch('RegulatoryReportingETL_Pipeline_1.logger')
    def test_write_to_delta_table_success(self, mock_logger, spark_session, sample_dataframes):
        """
        Test successful Delta table write operation.
        """
        customer_df, _, _, _, _ = sample_dataframes
        
        # Mock the DataFrame write operations
        with patch.object(customer_df, 'write') as mock_write:
            mock_write.format.return_value.mode.return_value.option.return_value.saveAsTable = Mock()
            
            write_to_delta_table(customer_df, "test_table", "overwrite")
            
            mock_write.format.assert_called_with("delta")
            mock_logger.info.assert_called()
    
    @patch('RegulatoryReportingETL_Pipeline_1.logger')
    def test_write_to_delta_table_exception(self, mock_logger, spark_session, sample_dataframes):
        """
        Test Delta table write operation exception handling.
        """
        customer_df, _, _, _, _ = sample_dataframes
        
        # Mock the DataFrame write operations to raise exception
        with patch.object(customer_df, 'write') as mock_write:
            mock_write.format.side_effect = Exception("Write failed")
            
            with pytest.raises(Exception, match="Write failed"):
                write_to_delta_table(customer_df, "test_table")
            
            mock_logger.error.assert_called()
    
    def test_validate_data_quality_success(self, spark_session, sample_dataframes):
        """
        Test successful data quality validation.
        """
        customer_df, _, _, _, _ = sample_dataframes
        
        result = validate_data_quality(customer_df, "TEST_TABLE")
        
        assert result is True
    
    def test_validate_data_quality_empty_dataframe(self, spark_session):
        """
        Test data quality validation with empty DataFrame.
        """
        empty_schema = StructType([StructField("ID", IntegerType(), True)])
        empty_df = spark_session.createDataFrame([], empty_schema)
        
        result = validate_data_quality(empty_df, "EMPTY_TABLE")
        
        assert result is False
    
    def test_validate_data_quality_branch_summary_null_ids(self, spark_session):
        """
        Test data quality validation for branch summary with null BRANCH_IDs.
        """
        schema = StructType([
            StructField("BRANCH_ID", IntegerType(), True),
            StructField("BRANCH_NAME", StringType(), True)
        ])
        
        data_with_nulls = [(None, "Test Branch"), (101, "Valid Branch")]
        df_with_nulls = spark_session.createDataFrame(data_with_nulls, schema)
        
        result = validate_data_quality(df_with_nulls, "BRANCH_SUMMARY_REPORT")
        
        assert result is False
    
    def test_validate_data_quality_exception(self, spark_session):
        """
        Test data quality validation exception handling.
        """
        # Create a mock DataFrame that raises exception on count()
        mock_df = Mock()
        mock_df.count.side_effect = Exception("Count failed")
        
        result = validate_data_quality(mock_df, "ERROR_TABLE")
        
        assert result is False
    
    @patch('RegulatoryReportingETL_Pipeline_1.get_spark_session')
    @patch('RegulatoryReportingETL_Pipeline_1.create_sample_data')
    @patch('RegulatoryReportingETL_Pipeline_1.create_aml_customer_transactions')
    @patch('RegulatoryReportingETL_Pipeline_1.create_enhanced_branch_summary_report')
    @patch('RegulatoryReportingETL_Pipeline_1.validate_data_quality')
    @patch('RegulatoryReportingETL_Pipeline_1.write_to_delta_table')
    def test_main_function_success(self, mock_write, mock_validate, mock_branch_summary, 
                                 mock_aml_transactions, mock_sample_data, mock_spark_session):
        """
        Test successful execution of main ETL function.
        """
        # Setup mocks
        mock_spark = Mock()
        mock_spark_session.return_value = mock_spark
        
        mock_dfs = (Mock(), Mock(), Mock(), Mock(), Mock())
        mock_sample_data.return_value = mock_dfs
        
        mock_aml_df = Mock()
        mock_aml_transactions.return_value = mock_aml_df
        
        mock_branch_df = Mock()
        mock_branch_summary.return_value = mock_branch_df
        mock_branch_df.show = Mock()
        
        mock_validate.return_value = True
        
        # Execute main function
        main()
        
        # Verify calls
        mock_spark_session.assert_called_once()
        mock_sample_data.assert_called_once_with(mock_spark)
        mock_aml_transactions.assert_called_once()
        mock_branch_summary.assert_called_once()
        assert mock_validate.call_count == 2
        assert mock_write.call_count == 2
    
    @patch('RegulatoryReportingETL_Pipeline_1.get_spark_session')
    @patch('RegulatoryReportingETL_Pipeline_1.create_sample_data')
    @patch('RegulatoryReportingETL_Pipeline_1.create_aml_customer_transactions')
    @patch('RegulatoryReportingETL_Pipeline_1.validate_data_quality')
    def test_main_function_validation_failure(self, mock_validate, mock_aml_transactions, 
                                            mock_sample_data, mock_spark_session):
        """
        Test main function with data validation failure.
        """
        # Setup mocks
        mock_spark = Mock()
        mock_spark_session.return_value = mock_spark
        
        mock_dfs = (Mock(), Mock(), Mock(), Mock(), Mock())
        mock_sample_data.return_value = mock_dfs
        
        mock_aml_df = Mock()
        mock_aml_transactions.return_value = mock_aml_df
        
        # First validation fails
        mock_validate.return_value = False
        
        # Execute main function
        main()
        
        # Verify that validation was called but write was not
        mock_validate.assert_called_once()
    
    @patch('RegulatoryReportingETL_Pipeline_1.get_spark_session')
    def test_main_function_exception(self, mock_spark_session):
        """
        Test main function exception handling.
        """
        mock_spark_session.side_effect = Exception("Spark session failed")
        
        with pytest.raises(Exception, match="Spark session failed"):
            main()
    
    def test_edge_case_large_transaction_amounts(self, spark_session):
        """
        Test handling of large transaction amounts.
        """
        # Create test data with large amounts
        transaction_schema = StructType([
            StructField("TRANSACTION_ID", IntegerType(), True),
            StructField("ACCOUNT_ID", IntegerType(), True),
            StructField("TRANSACTION_TYPE", StringType(), True),
            StructField("AMOUNT", DecimalType(15, 2), True),
            StructField("TRANSACTION_DATE", DateType(), True),
            StructField("DESCRIPTION", StringType(), True)
        ])
        
        large_amount_data = [
            (10001, 1001, "DEPOSIT", 999999999999.99, date(2023, 5, 1), "Large deposit"),
            (10002, 1001, "WITHDRAWAL", 0.01, date(2023, 5, 2), "Small withdrawal")
        ]
        
        transaction_df = spark_session.createDataFrame(large_amount_data, transaction_schema)
        
        # Test that large amounts are handled correctly
        total_amount = transaction_df.agg(spark_sum("AMOUNT").alias("total")).collect()[0].total
        assert total_amount == 999999999999.98
    
    def test_edge_case_special_characters_in_names(self, spark_session):
        """
        Test handling of special characters in customer names.
        """
        customer_schema = StructType([
            StructField("CUSTOMER_ID", IntegerType(), True),
            StructField("NAME", StringType(), True),
            StructField("EMAIL", StringType(), True)
        ])
        
        special_char_data = [
            (1, "José María O'Connor-Smith", "jose@email.com"),
            (2, "李小明", "li@email.com"),
            (3, "Müller & Associates", "muller@email.com")
        ]
        
        customer_df = spark_session.createDataFrame(special_char_data, customer_schema)
        
        # Test that special characters are preserved
        names = [row.NAME for row in customer_df.collect()]
        assert "José María O'Connor-Smith" in names
        assert "李小明" in names
        assert "Müller & Associates" in names
    
    def test_performance_large_dataset_simulation(self, spark_session):
        """
        Test performance with simulated large dataset.
        """
        # Create a larger dataset for performance testing
        large_data = [(i, f"Customer_{i}", f"customer_{i}@email.com") for i in range(1000)]
        
        customer_schema = StructType([
            StructField("CUSTOMER_ID", IntegerType(), True),
            StructField("NAME", StringType(), True),
            StructField("EMAIL", StringType(), True)
        ])
        
        large_customer_df = spark_session.createDataFrame(large_data, customer_schema)
        
        # Test that operations complete successfully with larger dataset
        count = large_customer_df.count()
        assert count == 1000
        
        # Test aggregation performance
        distinct_count = large_customer_df.select("CUSTOMER_ID").distinct().count()
        assert distinct_count == 1000

if __name__ == "__main__":
    pytest.main([__file__, "-v"])