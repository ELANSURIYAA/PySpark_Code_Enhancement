_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Comprehensive unit tests for RegulatoryReportingETL PySpark pipeline
## *Version*: 1 
## *Updated on*: 
_____________________________________________

import pytest
import logging
from datetime import date
from decimal import Decimal
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum as spark_sum, when, lit
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DecimalType, 
    DateType, LongType, DoubleType
)
from pyspark.testing import assertDataFrameEqual
from chispa.dataframe_comparer import assert_df_equality

# Import the functions to test
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
    # Mock imports for testing environment
    pass

class TestRegulatoryReportingETL:
    """
    Comprehensive test suite for RegulatoryReportingETL pipeline.
    """
    
    @pytest.fixture(scope="class")
    def spark_session(self):
        """
        Create a Spark session for testing.
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
    def sample_customer_data(self, spark_session):
        """
        Create sample customer data for testing.
        """
        schema = StructType([
            StructField("CUSTOMER_ID", IntegerType(), True),
            StructField("NAME", StringType(), True),
            StructField("EMAIL", StringType(), True),
            StructField("PHONE", StringType(), True),
            StructField("ADDRESS", StringType(), True),
            StructField("CREATED_DATE", DateType(), True)
        ])
        
        data = [
            (1, "John Doe", "john@email.com", "123-456-7890", "123 Main St", date(2023, 1, 15)),
            (2, "Jane Smith", "jane@email.com", "098-765-4321", "456 Oak Ave", date(2023, 2, 20))
        ]
        
        return spark_session.createDataFrame(data, schema)
    
    @pytest.fixture
    def sample_account_data(self, spark_session):
        """
        Create sample account data for testing.
        """
        schema = StructType([
            StructField("ACCOUNT_ID", IntegerType(), True),
            StructField("CUSTOMER_ID", IntegerType(), True),
            StructField("BRANCH_ID", IntegerType(), True),
            StructField("ACCOUNT_NUMBER", StringType(), True),
            StructField("ACCOUNT_TYPE", StringType(), True),
            StructField("BALANCE", DecimalType(15, 2), True),
            StructField("OPENED_DATE", DateType(), True)
        ])
        
        data = [
            (1001, 1, 101, "ACC001", "SAVINGS", Decimal("5000.00"), date(2023, 1, 16)),
            (1002, 2, 102, "ACC002", "CHECKING", Decimal("2500.00"), date(2023, 2, 21))
        ]
        
        return spark_session.createDataFrame(data, schema)
    
    @pytest.fixture
    def sample_transaction_data(self, spark_session):
        """
        Create sample transaction data for testing.
        """
        schema = StructType([
            StructField("TRANSACTION_ID", IntegerType(), True),
            StructField("ACCOUNT_ID", IntegerType(), True),
            StructField("TRANSACTION_TYPE", StringType(), True),
            StructField("AMOUNT", DecimalType(15, 2), True),
            StructField("TRANSACTION_DATE", DateType(), True),
            StructField("DESCRIPTION", StringType(), True)
        ])
        
        data = [
            (10001, 1001, "DEPOSIT", Decimal("1000.00"), date(2023, 5, 1), "Salary deposit"),
            (10002, 1002, "WITHDRAWAL", Decimal("200.00"), date(2023, 5, 2), "ATM withdrawal")
        ]
        
        return spark_session.createDataFrame(data, schema)
    
    @pytest.fixture
    def sample_branch_data(self, spark_session):
        """
        Create sample branch data for testing.
        """
        schema = StructType([
            StructField("BRANCH_ID", IntegerType(), True),
            StructField("BRANCH_NAME", StringType(), True),
            StructField("BRANCH_CODE", StringType(), True),
            StructField("CITY", StringType(), True),
            StructField("STATE", StringType(), True),
            StructField("COUNTRY", StringType(), True)
        ])
        
        data = [
            (101, "Downtown Branch", "DT001", "New York", "NY", "USA"),
            (102, "Uptown Branch", "UT002", "Los Angeles", "CA", "USA")
        ]
        
        return spark_session.createDataFrame(data, schema)
    
    @pytest.fixture
    def sample_branch_operational_data(self, spark_session):
        """
        Create sample branch operational data for testing.
        """
        schema = StructType([
            StructField("BRANCH_ID", IntegerType(), True),
            StructField("REGION", StringType(), True),
            StructField("MANAGER_NAME", StringType(), True),
            StructField("LAST_AUDIT_DATE", DateType(), True),
            StructField("IS_ACTIVE", StringType(), True)
        ])
        
        data = [
            (101, "East Region", "Alice Manager", date(2023, 4, 15), "Y"),
            (102, "West Region", "Bob Manager", date(2023, 3, 20), "N")  # Inactive branch
        ]
        
        return spark_session.createDataFrame(data, schema)

    # Test Spark Session Creation
    def test_get_spark_session_success(self):
        """
        Test successful Spark session creation.
        """
        with patch('RegulatoryReportingETL_Pipeline_1.SparkSession') as mock_spark:
            mock_session = Mock()
            mock_spark.getActiveSession.return_value = mock_session
            mock_session.sparkContext.setLogLevel = Mock()
            
            result = get_spark_session("TestApp")
            
            assert result == mock_session
            mock_spark.getActiveSession.assert_called_once()
    
    def test_get_spark_session_create_new(self):
        """
        Test Spark session creation when no active session exists.
        """
        with patch('RegulatoryReportingETL_Pipeline_1.SparkSession') as mock_spark:
            mock_builder = Mock()
            mock_session = Mock()
            
            mock_spark.getActiveSession.return_value = None
            mock_spark.builder = mock_builder
            mock_builder.appName.return_value = mock_builder
            mock_builder.config.return_value = mock_builder
            mock_builder.getOrCreate.return_value = mock_session
            mock_session.sparkContext.setLogLevel = Mock()
            
            result = get_spark_session("TestApp")
            
            assert result == mock_session
            mock_builder.appName.assert_called_with("TestApp")
    
    def test_get_spark_session_exception(self):
        """
        Test Spark session creation exception handling.
        """
        with patch('RegulatoryReportingETL_Pipeline_1.SparkSession') as mock_spark:
            mock_spark.getActiveSession.side_effect = Exception("Connection failed")
            
            with pytest.raises(Exception, match="Connection failed"):
                get_spark_session()

    # Test Sample Data Creation
    def test_create_sample_data_structure(self, spark_session):
        """
        Test that create_sample_data returns correct number of DataFrames with expected schemas.
        """
        customer_df, account_df, transaction_df, branch_df, branch_operational_df = create_sample_data(spark_session)
        
        # Test return tuple length
        assert len([customer_df, account_df, transaction_df, branch_df, branch_operational_df]) == 5
        
        # Test DataFrame types
        assert isinstance(customer_df, DataFrame)
        assert isinstance(account_df, DataFrame)
        assert isinstance(transaction_df, DataFrame)
        assert isinstance(branch_df, DataFrame)
        assert isinstance(branch_operational_df, DataFrame)
        
        # Test schema structures
        assert "CUSTOMER_ID" in customer_df.columns
        assert "ACCOUNT_ID" in account_df.columns
        assert "TRANSACTION_ID" in transaction_df.columns
        assert "BRANCH_ID" in branch_df.columns
        assert "BRANCH_ID" in branch_operational_df.columns
        assert "IS_ACTIVE" in branch_operational_df.columns
    
    def test_create_sample_data_content(self, spark_session):
        """
        Test that create_sample_data returns DataFrames with expected content.
        """
        customer_df, account_df, transaction_df, branch_df, branch_operational_df = create_sample_data(spark_session)
        
        # Test row counts
        assert customer_df.count() == 3
        assert account_df.count() == 4
        assert transaction_df.count() == 5
        assert branch_df.count() == 3
        assert branch_operational_df.count() == 3
        
        # Test specific data values
        customer_names = [row.NAME for row in customer_df.collect()]
        assert "John Doe" in customer_names
        assert "Jane Smith" in customer_names

    # Test AML Customer Transactions Creation
    def test_create_aml_customer_transactions_success(self, spark_session, sample_customer_data, 
                                                     sample_account_data, sample_transaction_data):
        """
        Test successful creation of AML customer transactions.
        """
        result_df = create_aml_customer_transactions(sample_customer_data, sample_account_data, sample_transaction_data)
        
        # Test schema
        expected_columns = ["CUSTOMER_ID", "NAME", "ACCOUNT_ID", "TRANSACTION_ID", "AMOUNT", "TRANSACTION_TYPE", "TRANSACTION_DATE"]
        assert all(col in result_df.columns for col in expected_columns)
        
        # Test row count (should match number of transactions)
        assert result_df.count() == 2
        
        # Test data integrity
        collected_data = result_df.collect()
        assert collected_data[0].NAME in ["John Doe", "Jane Smith"]
    
    def test_create_aml_customer_transactions_empty_input(self, spark_session):
        """
        Test AML customer transactions creation with empty input DataFrames.
        """
        empty_customer_df = spark_session.createDataFrame([], StructType([
            StructField("CUSTOMER_ID", IntegerType(), True),
            StructField("NAME", StringType(), True)
        ]))
        
        empty_account_df = spark_session.createDataFrame([], StructType([
            StructField("CUSTOMER_ID", IntegerType(), True),
            StructField("ACCOUNT_ID", IntegerType(), True)
        ]))
        
        empty_transaction_df = spark_session.createDataFrame([], StructType([
            StructField("ACCOUNT_ID", IntegerType(), True),
            StructField("TRANSACTION_ID", IntegerType(), True)
        ]))
        
        result_df = create_aml_customer_transactions(empty_customer_df, empty_account_df, empty_transaction_df)
        assert result_df.count() == 0

    # Test Enhanced Branch Summary Report Creation
    def test_create_enhanced_branch_summary_report_success(self, spark_session, sample_transaction_data, 
                                                          sample_account_data, sample_branch_data, 
                                                          sample_branch_operational_data):
        """
        Test successful creation of enhanced branch summary report.
        """
        result_df = create_enhanced_branch_summary_report(
            sample_transaction_data, sample_account_data, sample_branch_data, sample_branch_operational_data
        )
        
        # Test schema
        expected_columns = ["BRANCH_ID", "BRANCH_NAME", "TOTAL_TRANSACTIONS", "TOTAL_AMOUNT", "REGION", "LAST_AUDIT_DATE"]
        assert all(col in result_df.columns for col in expected_columns)
        
        # Test data types
        schema_dict = {field.name: field.dataType for field in result_df.schema.fields}
        assert isinstance(schema_dict["BRANCH_ID"], LongType)
        assert isinstance(schema_dict["TOTAL_AMOUNT"], DoubleType)
        
        # Test row count
        assert result_df.count() == 2
    
    def test_create_enhanced_branch_summary_report_conditional_logic(self, spark_session):
        """
        Test conditional logic for REGION and LAST_AUDIT_DATE based on IS_ACTIVE status.
        """
        # Create test data with mixed IS_ACTIVE values
        transaction_schema = StructType([
            StructField("TRANSACTION_ID", IntegerType(), True),
            StructField("ACCOUNT_ID", IntegerType(), True),
            StructField("AMOUNT", DecimalType(15, 2), True)
        ])
        
        account_schema = StructType([
            StructField("ACCOUNT_ID", IntegerType(), True),
            StructField("BRANCH_ID", IntegerType(), True)
        ])
        
        branch_schema = StructType([
            StructField("BRANCH_ID", IntegerType(), True),
            StructField("BRANCH_NAME", StringType(), True)
        ])
        
        branch_operational_schema = StructType([
            StructField("BRANCH_ID", IntegerType(), True),
            StructField("REGION", StringType(), True),
            StructField("LAST_AUDIT_DATE", DateType(), True),
            StructField("IS_ACTIVE", StringType(), True)
        ])
        
        transaction_data = [(1, 1001, Decimal("100.00")), (2, 1002, Decimal("200.00"))]
        account_data = [(1001, 101), (1002, 102)]
        branch_data = [(101, "Active Branch"), (102, "Inactive Branch")]
        branch_operational_data = [
            (101, "East Region", date(2023, 4, 15), "Y"),  # Active
            (102, "West Region", date(2023, 3, 20), "N")   # Inactive
        ]
        
        transaction_df = spark_session.createDataFrame(transaction_data, transaction_schema)
        account_df = spark_session.createDataFrame(account_data, account_schema)
        branch_df = spark_session.createDataFrame(branch_data, branch_schema)
        branch_operational_df = spark_session.createDataFrame(branch_operational_data, branch_operational_schema)
        
        result_df = create_enhanced_branch_summary_report(
            transaction_df, account_df, branch_df, branch_operational_df
        )
        
        collected_data = result_df.collect()
        
        # Find active and inactive branch results
        active_branch = next((row for row in collected_data if row.BRANCH_NAME == "Active Branch"), None)
        inactive_branch = next((row for row in collected_data if row.BRANCH_NAME == "Inactive Branch"), None)
        
        # Test conditional logic
        assert active_branch is not None
        assert active_branch.REGION == "East Region"
        assert active_branch.LAST_AUDIT_DATE is not None
        
        assert inactive_branch is not None
        assert inactive_branch.REGION is None
        assert inactive_branch.LAST_AUDIT_DATE is None

    # Test Data Quality Validation
    def test_validate_data_quality_success(self, spark_session, sample_customer_data):
        """
        Test successful data quality validation.
        """
        result = validate_data_quality(sample_customer_data, "TEST_TABLE")
        assert result is True
    
    def test_validate_data_quality_empty_dataframe(self, spark_session):
        """
        Test data quality validation with empty DataFrame.
        """
        empty_df = spark_session.createDataFrame([], StructType([
            StructField("CUSTOMER_ID", IntegerType(), True)
        ]))
        
        result = validate_data_quality(empty_df, "EMPTY_TABLE")
        assert result is False
    
    def test_validate_data_quality_branch_summary_null_branch_id(self, spark_session):
        """
        Test data quality validation for branch summary with null BRANCH_ID.
        """
        schema = StructType([
            StructField("BRANCH_ID", IntegerType(), True),
            StructField("BRANCH_NAME", StringType(), True)
        ])
        
        data = [(None, "Test Branch"), (101, "Valid Branch")]
        df_with_nulls = spark_session.createDataFrame(data, schema)
        
        result = validate_data_quality(df_with_nulls, "BRANCH_SUMMARY_REPORT")
        assert result is False
    
    def test_validate_data_quality_exception_handling(self, spark_session):
        """
        Test data quality validation exception handling.
        """
        # Create a mock DataFrame that will raise an exception on count()
        mock_df = Mock()
        mock_df.count.side_effect = Exception("Count operation failed")
        
        result = validate_data_quality(mock_df, "ERROR_TABLE")
        assert result is False

    # Test Delta Table Writing
    @patch('RegulatoryReportingETL_Pipeline_1.logger')
    def test_write_to_delta_table_success(self, mock_logger, spark_session, sample_customer_data):
        """
        Test successful Delta table writing.
        """
        with patch.object(sample_customer_data.write, 'format') as mock_format:
            mock_format.return_value.mode.return_value.option.return_value.saveAsTable = Mock()
            
            write_to_delta_table(sample_customer_data, "test_table", "overwrite")
            
            mock_format.assert_called_once_with("delta")
            mock_logger.info.assert_called()
    
    @patch('RegulatoryReportingETL_Pipeline_1.logger')
    def test_write_to_delta_table_exception(self, mock_logger, spark_session, sample_customer_data):
        """
        Test Delta table writing exception handling.
        """
        with patch.object(sample_customer_data.write, 'format') as mock_format:
            mock_format.side_effect = Exception("Write operation failed")
            
            with pytest.raises(Exception, match="Write operation failed"):
                write_to_delta_table(sample_customer_data, "test_table")
            
            mock_logger.error.assert_called()

    # Test Main Function
    @patch('RegulatoryReportingETL_Pipeline_1.get_spark_session')
    @patch('RegulatoryReportingETL_Pipeline_1.create_sample_data')
    @patch('RegulatoryReportingETL_Pipeline_1.validate_data_quality')
    @patch('RegulatoryReportingETL_Pipeline_1.write_to_delta_table')
    def test_main_function_success(self, mock_write, mock_validate, mock_create_data, mock_get_spark):
        """
        Test successful execution of main function.
        """
        # Setup mocks
        mock_spark = Mock()
        mock_get_spark.return_value = mock_spark
        
        mock_dfs = [Mock() for _ in range(5)]  # 5 DataFrames returned by create_sample_data
        mock_create_data.return_value = tuple(mock_dfs)
        
        mock_validate.return_value = True
        
        # Mock the create functions
        with patch('RegulatoryReportingETL_Pipeline_1.create_aml_customer_transactions') as mock_aml, \
             patch('RegulatoryReportingETL_Pipeline_1.create_enhanced_branch_summary_report') as mock_branch:
            
            mock_aml_df = Mock()
            mock_branch_df = Mock()
            mock_aml.return_value = mock_aml_df
            mock_branch.return_value = mock_branch_df
            mock_branch_df.show = Mock()
            
            # Execute main function
            main()
            
            # Verify calls
            mock_get_spark.assert_called_once()
            mock_create_data.assert_called_once_with(mock_spark)
            assert mock_validate.call_count == 2
            assert mock_write.call_count == 2
    
    @patch('RegulatoryReportingETL_Pipeline_1.get_spark_session')
    @patch('RegulatoryReportingETL_Pipeline_1.create_sample_data')
    @patch('RegulatoryReportingETL_Pipeline_1.validate_data_quality')
    def test_main_function_validation_failure(self, mock_validate, mock_create_data, mock_get_spark):
        """
        Test main function with data quality validation failure.
        """
        # Setup mocks
        mock_spark = Mock()
        mock_get_spark.return_value = mock_spark
        
        mock_dfs = [Mock() for _ in range(5)]
        mock_create_data.return_value = tuple(mock_dfs)
        
        mock_validate.return_value = False  # Validation fails
        
        with patch('RegulatoryReportingETL_Pipeline_1.create_aml_customer_transactions') as mock_aml:
            mock_aml.return_value = Mock()
            
            # Execute main function - should return early due to validation failure
            main()
            
            # Verify validation was called but write was not
            mock_validate.assert_called_once()
    
    @patch('RegulatoryReportingETL_Pipeline_1.get_spark_session')
    def test_main_function_exception_handling(self, mock_get_spark):
        """
        Test main function exception handling.
        """
        mock_get_spark.side_effect = Exception("Spark initialization failed")
        
        with pytest.raises(Exception, match="Spark initialization failed"):
            main()

    # Edge Cases and Error Scenarios
    def test_create_aml_transactions_mismatched_schemas(self, spark_session):
        """
        Test AML transactions creation with mismatched schemas.
        """
        # Create DataFrames with incompatible schemas
        customer_df = spark_session.createDataFrame([(1, "John")], ["WRONG_ID", "NAME"])
        account_df = spark_session.createDataFrame([(1001, 1)], ["ACCOUNT_ID", "CUSTOMER_ID"])
        transaction_df = spark_session.createDataFrame([(10001, 1001)], ["TRANSACTION_ID", "ACCOUNT_ID"])
        
        with pytest.raises(Exception):
            create_aml_customer_transactions(customer_df, account_df, transaction_df)
    
    def test_branch_summary_with_missing_operational_data(self, spark_session, sample_transaction_data, 
                                                         sample_account_data, sample_branch_data):
        """
        Test branch summary creation when operational data is missing for some branches.
        """
        # Create operational data for only one branch
        partial_operational_schema = StructType([
            StructField("BRANCH_ID", IntegerType(), True),
            StructField("REGION", StringType(), True),
            StructField("MANAGER_NAME", StringType(), True),
            StructField("LAST_AUDIT_DATE", DateType(), True),
            StructField("IS_ACTIVE", StringType(), True)
        ])
        
        partial_operational_data = [(101, "East Region", "Alice Manager", date(2023, 4, 15), "Y")]
        partial_operational_df = spark_session.createDataFrame(partial_operational_data, partial_operational_schema)
        
        result_df = create_enhanced_branch_summary_report(
            sample_transaction_data, sample_account_data, sample_branch_data, partial_operational_df
        )
        
        # Should still work with left join
        assert result_df.count() >= 1
        
        collected_data = result_df.collect()
        # Branch without operational data should have null REGION and LAST_AUDIT_DATE
        branch_without_ops = next((row for row in collected_data if row.BRANCH_ID == 102), None)
        if branch_without_ops:
            assert branch_without_ops.REGION is None
            assert branch_without_ops.LAST_AUDIT_DATE is None

    # Performance and Load Testing
    def test_large_dataset_performance(self, spark_session):
        """
        Test performance with larger datasets (basic load test).
        """
        # Create larger test datasets
        large_customer_data = [(i, f"Customer_{i}", f"email_{i}@test.com", f"phone_{i}", f"address_{i}", date(2023, 1, 1)) 
                              for i in range(1, 1001)]
        
        customer_schema = StructType([
            StructField("CUSTOMER_ID", IntegerType(), True),
            StructField("NAME", StringType(), True),
            StructField("EMAIL", StringType(), True),
            StructField("PHONE", StringType(), True),
            StructField("ADDRESS", StringType(), True),
            StructField("CREATED_DATE", DateType(), True)
        ])
        
        large_customer_df = spark_session.createDataFrame(large_customer_data, customer_schema)
        
        # Test that validation works with larger datasets
        result = validate_data_quality(large_customer_df, "LARGE_CUSTOMER_TABLE")
        assert result is True
        assert large_customer_df.count() == 1000

if __name__ == "__main__":
    pytest.main(["-v", __file__])