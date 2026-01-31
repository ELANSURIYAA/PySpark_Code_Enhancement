'''
_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Comprehensive unit tests for G_MtgAmendment_To_MISMO_XML_LLD_Pipeline_1.py - Testing data transformations, edge cases, and error handling scenarios
## *Version*: 1 
## *Updated on*: 
_____________________________________________
'''

import pytest
import sys
import os
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pyspark.sql.functions import col, lit
from pyspark.sql import Row
import logging

# Import the classes and functions to be tested
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from G_MtgAmendment_To_MISMO_XML_LLD_Pipeline_1 import (
    Config,
    SchemaDefinitions,
    DataTransformationUtils,
    MortgageAmendmentProcessor
)

class TestConfig:
    """Test cases for Config class"""
    
    def test_config_initialization_default_values(self):
        """Test Config initialization with default values"""
        window_ts = "20231201_120000"
        config = Config(window_ts)
        
        assert config.window_ts == window_ts
        assert config.project_dir == "/apps/mortgage-amendment-mismo"
        assert config.aws_bucket_url == "/data/landing/mortgage_amendment"
        assert config.error_log_path == f"/data/out/rejects/rejects_{window_ts}.dat"
        assert config.product_miss_path == f"/data/out/raw_events/raw_events_{window_ts}.dat"
        assert config.mismo_out_path == f"/data/out/mismo_xml/mismo_amendment_{window_ts}.xml"
        assert config.template_record_file == f"/apps/mortgage-amendment-mismo/lookups/mismo_template_record.dat"
        assert config.landing_file == f"/data/landing/mortgage_amendment/amendment_events_{window_ts}.dat"
    
    def test_config_initialization_custom_values(self):
        """Test Config initialization with custom values"""
        window_ts = "20231201_120000"
        custom_project_dir = "/custom/project"
        custom_bucket_url = "/custom/bucket"
        
        config = Config(window_ts, custom_project_dir, custom_bucket_url)
        
        assert config.window_ts == window_ts
        assert config.project_dir == custom_project_dir
        assert config.aws_bucket_url == custom_bucket_url
        assert config.template_record_file == f"{custom_project_dir}/lookups/mismo_template_record.dat"
        assert config.landing_file == f"{custom_bucket_url}/amendment_events_{window_ts}.dat"
    
    def test_config_with_empty_window_ts(self):
        """Test Config with empty window timestamp"""
        window_ts = ""
        config = Config(window_ts)
        
        assert config.window_ts == ""
        assert config.error_log_path == "/data/out/rejects/rejects_.dat"
        assert config.product_miss_path == "/data/out/raw_events/raw_events_.dat"

class TestSchemaDefinitions:
    """Test cases for SchemaDefinitions class"""
    
    def test_kafka_event_schema(self):
        """Test Kafka event schema structure"""
        schema = SchemaDefinitions.get_kafka_event_schema()
        
        assert isinstance(schema, StructType)
        field_names = [field.name for field in schema.fields]
        expected_fields = ["kafka_key", "kafka_partition", "kafka_offset", "ingest_ts", "payload_json"]
        
        assert field_names == expected_fields
        assert schema.fields[1].dataType == IntegerType()
        assert schema.fields[2].dataType == LongType()
    
    def test_event_meta_schema(self):
        """Test event metadata schema structure"""
        schema = SchemaDefinitions.get_event_meta_schema()
        
        assert isinstance(schema, StructType)
        field_names = [field.name for field in schema.fields]
        expected_fields = [
            "event_id", "event_type", "event_ts", "loan_id", "source_system",
            "kafka_key", "kafka_partition", "kafka_offset", "ingest_ts", "payload_json"
        ]
        
        assert field_names == expected_fields
        assert len(schema.fields) == 10
    
    def test_template_record_schema(self):
        """Test template record schema structure"""
        schema = SchemaDefinitions.get_template_record_schema()
        
        assert isinstance(schema, StructType)
        field_names = [field.name for field in schema.fields]
        expected_fields = ["template_key", "template_text"]
        
        assert field_names == expected_fields
        assert len(schema.fields) == 2
    
    def test_reject_record_schema(self):
        """Test reject record schema structure"""
        schema = SchemaDefinitions.get_reject_record_schema()
        
        assert isinstance(schema, StructType)
        field_names = [field.name for field in schema.fields]
        expected_fields = [
            "reject_type", "reason", "event_id", "loan_id", "event_ts",
            "source_system", "kafka_partition", "kafka_offset", "ingest_ts", "payload_json"
        ]
        
        assert field_names == expected_fields
        assert len(schema.fields) == 10
    
    def test_xml_out_record_schema(self):
        """Test XML output record schema structure"""
        schema = SchemaDefinitions.get_xml_out_record_schema()
        
        assert isinstance(schema, StructType)
        field_names = [field.name for field in schema.fields]
        expected_fields = ["event_id", "loan_id", "xml_text"]
        
        assert field_names == expected_fields
        assert len(schema.fields) == 3

class TestDataTransformationUtils:
    """Test cases for DataTransformationUtils class"""
    
    def test_setup_logging(self):
        """Test logging setup"""
        logger = DataTransformationUtils.setup_logging()
        
        assert isinstance(logger, logging.Logger)
        assert logger.level == logging.INFO
    
    @patch('builtins.print')
    def test_log_dataframe_count(self, mock_print):
        """Test dataframe count logging"""
        # Mock dataframe
        mock_df = Mock()
        mock_df.count.return_value = 100
        
        # Mock logger
        mock_logger = Mock()
        
        count = DataTransformationUtils.log_dataframe_count(mock_df, "test_step", mock_logger)
        
        assert count == 100
        mock_df.count.assert_called_once()
        mock_logger.info.assert_called_once_with("Row count after test_step: 100")
    
    def test_json_find_string_value_valid_json(self):
        """Test JSON string value extraction with valid JSON"""
        json_str = '{"event_id": "12345", "loan_id": "LOAN_67890"}'
        
        result = DataTransformationUtils.json_find_string_value(json_str, "event_id")
        assert result == "12345"
        
        result = DataTransformationUtils.json_find_string_value(json_str, "loan_id")
        assert result == "LOAN_67890"
    
    def test_json_find_string_value_missing_key(self):
        """Test JSON string value extraction with missing key"""
        json_str = '{"event_id": "12345"}'
        
        result = DataTransformationUtils.json_find_string_value(json_str, "missing_key")
        assert result == ""
    
    def test_json_find_string_value_null_json(self):
        """Test JSON string value extraction with null JSON"""
        result = DataTransformationUtils.json_find_string_value(None, "event_id")
        assert result == ""
        
        result = DataTransformationUtils.json_find_string_value("", "event_id")
        assert result == ""
    
    def test_json_find_scalar_value_valid_json(self):
        """Test JSON scalar value extraction with valid JSON"""
        json_str = '{"new_rate": 3.5, "prior_rate": 4.0, "term_months": 360}'
        
        result = DataTransformationUtils.json_find_scalar_value(json_str, "new_rate")
        assert result == "3.5"
        
        result = DataTransformationUtils.json_find_scalar_value(json_str, "term_months")
        assert result == "360"
    
    def test_json_find_scalar_value_missing_key(self):
        """Test JSON scalar value extraction with missing key"""
        json_str = '{"new_rate": 3.5}'
        
        result = DataTransformationUtils.json_find_scalar_value(json_str, "missing_key")
        assert result == ""
    
    def test_json_find_scalar_value_null_json(self):
        """Test JSON scalar value extraction with null JSON"""
        result = DataTransformationUtils.json_find_scalar_value(None, "new_rate")
        assert result == ""
    
    def test_create_json_udfs(self):
        """Test UDF creation"""
        string_udf, scalar_udf = DataTransformationUtils.create_json_udfs()
        
        assert string_udf is not None
        assert scalar_udf is not None
    
    def test_business_reject_reason_missing_effective_date(self):
        """Test business validation with missing effective date"""
        result = DataTransformationUtils.business_reject_reason("", "RATE_CHANGE", "3.5")
        assert result == "Missing effective_date"
        
        result = DataTransformationUtils.business_reject_reason(None, "RATE_CHANGE", "3.5")
        assert result == "Missing effective_date"
    
    def test_business_reject_reason_missing_amendment_type(self):
        """Test business validation with missing amendment type"""
        result = DataTransformationUtils.business_reject_reason("2023-12-01", "", "3.5")
        assert result == "Missing amendment_type"
        
        result = DataTransformationUtils.business_reject_reason("2023-12-01", None, "3.5")
        assert result == "Missing amendment_type"
    
    def test_business_reject_reason_missing_new_rate_for_rate_change(self):
        """Test business validation with missing new rate for rate change"""
        result = DataTransformationUtils.business_reject_reason("2023-12-01", "RATE_CHANGE", "")
        assert result == "Missing new_rate for RATE_CHANGE"
        
        result = DataTransformationUtils.business_reject_reason("2023-12-01", "RATE_CHANGE", None)
        assert result == "Missing new_rate for RATE_CHANGE"
    
    def test_business_reject_reason_valid_data(self):
        """Test business validation with valid data"""
        result = DataTransformationUtils.business_reject_reason("2023-12-01", "RATE_CHANGE", "3.5")
        assert result is None
        
        result = DataTransformationUtils.business_reject_reason("2023-12-01", "TERM_CHANGE", "")
        assert result is None
    
    def test_render_xml_complete_template(self):
        """Test XML rendering with complete template"""
        template = "<loan id='{{LOAN_ID}}' event='{{EVENT_ID}}' date='{{EFFECTIVE_DATE}}' type='{{AMENDMENT_TYPE}}' rate='{{NEW_RATE}}' term='{{NEW_TERM_MONTHS}}'></loan>"
        
        result = DataTransformationUtils.render_xml(
            template, "LOAN123", "EVT456", "2023-12-01", "RATE_CHANGE", "3.5", "360"
        )
        
        expected = "<loan id='LOAN123' event='EVT456' date='2023-12-01' type='RATE_CHANGE' rate='3.5' term='360'></loan>"
        assert result == expected
    
    def test_render_xml_null_values(self):
        """Test XML rendering with null values"""
        template = "<loan id='{{LOAN_ID}}' event='{{EVENT_ID}}'></loan>"
        
        result = DataTransformationUtils.render_xml(
            template, None, None, None, None, None, None
        )
        
        expected = "<loan id='' event=''></loan>"
        assert result == expected
    
    def test_render_xml_null_template(self):
        """Test XML rendering with null template"""
        result = DataTransformationUtils.render_xml(
            None, "LOAN123", "EVT456", "2023-12-01", "RATE_CHANGE", "3.5", "360"
        )
        
        assert result == ""

@pytest.fixture
def spark_session():
    """Create Spark session for testing"""
    spark = SparkSession.builder \
        .appName("TestMortgageAmendmentProcessor") \
        .master("local[*]") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .getOrCreate()
    
    yield spark
    spark.stop()

@pytest.fixture
def test_config():
    """Create test configuration"""
    return Config("20231201_120000")

@pytest.fixture
def test_logger():
    """Create test logger"""
    return DataTransformationUtils.setup_logging()

@pytest.fixture
def processor(spark_session, test_config, test_logger):
    """Create MortgageAmendmentProcessor instance for testing"""
    return MortgageAmendmentProcessor(test_config, spark_session, test_logger)

class TestMortgageAmendmentProcessor:
    """Test cases for MortgageAmendmentProcessor class"""
    
    def test_processor_initialization(self, processor, test_config, test_logger):
        """Test processor initialization"""
        assert processor.config == test_config
        assert processor.logger == test_logger
        assert processor.utils is not None
        assert processor.schemas is not None
        assert processor.json_find_string_value_udf is not None
        assert processor.json_find_scalar_value_udf is not None
        assert processor.business_reject_reason_udf is not None
        assert processor.render_xml_udf is not None
    
    def test_ingest_landing_file(self, processor, spark_session):
        """Test landing file ingestion"""
        # Create test data
        test_data = [
            ("key1", 0, 100, "2023-12-01T10:00:00Z", '{"event_id": "evt1", "loan_id": "loan1"}'),
            ("key2", 1, 101, "2023-12-01T10:01:00Z", '{"event_id": "evt2", "loan_id": "loan2"}')
        ]
        
        schema = SchemaDefinitions.get_kafka_event_schema()
        test_df = spark_session.createDataFrame(test_data, schema)
        
        # Mock the file read operation
        with patch.object(processor.spark.read, 'csv', return_value=test_df):
            result_df = processor.ingest_landing_file()
            
            assert result_df.count() == 2
            assert result_df.columns == ["kafka_key", "kafka_partition", "kafka_offset", "ingest_ts", "payload_json"]
    
    def test_extract_event_metadata(self, processor, spark_session):
        """Test event metadata extraction"""
        # Create test data
        test_data = [
            ("key1", 0, 100, "2023-12-01T10:00:00Z", 
             '{"event_id": "evt1", "event_type": "MORTGAGE_AMENDMENT", "event_ts": "2023-12-01T09:00:00Z", "loan_id": "loan1", "source_system": "LOS"}')
        ]
        
        schema = SchemaDefinitions.get_kafka_event_schema()
        landing_df = spark_session.createDataFrame(test_data, schema)
        
        result_df = processor.extract_event_metadata(landing_df)
        
        assert result_df.count() == 1
        expected_columns = ["event_id", "event_type", "event_ts", "loan_id", "source_system", 
                          "kafka_key", "kafka_partition", "kafka_offset", "ingest_ts", "payload_json"]
        assert result_df.columns == expected_columns
    
    def test_persist_raw_events(self, processor, spark_session):
        """Test raw events persistence"""
        # Create test data
        test_data = [
            ("evt1", "MORTGAGE_AMENDMENT", "2023-12-01T09:00:00Z", "loan1", "LOS",
             "key1", 0, 100, "2023-12-01T10:00:00Z", '{"test": "data"}')
        ]
        
        schema = SchemaDefinitions.get_event_meta_schema()
        event_meta_df = spark_session.createDataFrame(test_data, schema)
        
        # Mock the write operation
        with patch.object(event_meta_df.write, 'csv') as mock_write:
            processor.persist_raw_events(event_meta_df)
            mock_write.assert_called_once()
    
    def test_validate_schema_and_split_valid_records(self, processor, spark_session):
        """Test schema validation with valid records"""
        # Create test data with valid records
        test_data = [
            ("evt1", "MORTGAGE_AMENDMENT", "2023-12-01T09:00:00Z", "loan1", "LOS",
             "key1", 0, 100, "2023-12-01T10:00:00Z", '{"test": "data"}')
        ]
        
        schema = SchemaDefinitions.get_event_meta_schema()
        event_meta_df = spark_session.createDataFrame(test_data, schema)
        
        schema_reject_df, valid_event_meta_df = processor.validate_schema_and_split(event_meta_df)
        
        assert schema_reject_df.count() == 0
        assert valid_event_meta_df.count() == 1
    
    def test_validate_schema_and_split_invalid_records(self, processor, spark_session):
        """Test schema validation with invalid records"""
        # Create test data with invalid records
        test_data = [
            ("", "MORTGAGE_AMENDMENT", "2023-12-01T09:00:00Z", "loan1", "LOS",
             "key1", 0, 100, "2023-12-01T10:00:00Z", '{"test": "data"}'),  # Missing event_id
            ("evt2", "INVALID_TYPE", "2023-12-01T09:00:00Z", "loan2", "LOS",
             "key2", 1, 101, "2023-12-01T10:01:00Z", '{"test": "data"}'),  # Invalid event_type
            ("evt3", "MORTGAGE_AMENDMENT", "2023-12-01T09:00:00Z", "", "LOS",
             "key3", 2, 102, "2023-12-01T10:02:00Z", '{"test": "data"}')   # Missing loan_id
        ]
        
        schema = SchemaDefinitions.get_event_meta_schema()
        event_meta_df = spark_session.createDataFrame(test_data, schema)
        
        schema_reject_df, valid_event_meta_df = processor.validate_schema_and_split(event_meta_df)
        
        assert schema_reject_df.count() == 3
        assert valid_event_meta_df.count() == 0
        
        # Check reject reasons
        reject_reasons = [row.reason for row in schema_reject_df.collect()]
        assert "Missing event_id" in reject_reasons
        assert "Unexpected event_type" in reject_reasons
        assert "Missing loan_id" in reject_reasons
    
    def test_canonicalize_amendment_data(self, processor, spark_session):
        """Test amendment data canonicalization"""
        # Create test data
        test_data = [
            ("evt1", "MORTGAGE_AMENDMENT", "2023-12-01T09:00:00Z", "loan1", "LOS",
             "key1", 0, 100, "2023-12-01T10:00:00Z", 
             '{"amendment_type": "RATE_CHANGE", "effective_date": "2023-12-01", "prior_rate": 4.0, "new_rate": 3.5, "prior_term_months": 360, "new_term_months": 360}')
        ]
        
        schema = SchemaDefinitions.get_event_meta_schema()
        valid_event_meta_df = spark_session.createDataFrame(test_data, schema)
        
        result_df = processor.canonicalize_amendment_data(valid_event_meta_df)
        
        assert result_df.count() == 1
        expected_columns = ["event_id", "event_type", "event_ts", "loan_id", "source_system",
                          "kafka_key", "kafka_partition", "kafka_offset", "ingest_ts", "payload_json",
                          "amendment_type", "effective_date", "prior_rate", "new_rate", 
                          "prior_term_months", "new_term_months"]
        assert result_df.columns == expected_columns
    
    def test_load_template_data(self, processor, spark_session):
        """Test template data loading"""
        # Create test template data
        test_data = [
            ("MORTGAGE_AMENDMENT", "<loan id='{{LOAN_ID}}' event='{{EVENT_ID}}'></loan>")
        ]
        
        schema = SchemaDefinitions.get_template_record_schema()
        test_df = spark_session.createDataFrame(test_data, schema)
        
        # Mock the file read operation
        with patch.object(processor.spark.read, 'csv', return_value=test_df):
            result_df = processor.load_template_data()
            
            assert result_df.count() == 1
            assert result_df.columns == ["template_key", "template_text"]
    
    def test_join_with_template(self, processor, spark_session):
        """Test joining canonical data with template"""
        # Create canonical data
        canonical_data = [
            ("evt1", "MORTGAGE_AMENDMENT", "2023-12-01T09:00:00Z", "loan1", "LOS",
             "key1", 0, 100, "2023-12-01T10:00:00Z", '{"test": "data"}',
             "RATE_CHANGE", "2023-12-01", "4.0", "3.5", "360", "360")
        ]
        
        canonical_schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("event_ts", StringType(), True),
            StructField("loan_id", StringType(), True),
            StructField("source_system", StringType(), True),
            StructField("kafka_key", StringType(), True),
            StructField("kafka_partition", IntegerType(), True),
            StructField("kafka_offset", LongType(), True),
            StructField("ingest_ts", StringType(), True),
            StructField("payload_json", StringType(), True),
            StructField("amendment_type", StringType(), True),
            StructField("effective_date", StringType(), True),
            StructField("prior_rate", StringType(), True),
            StructField("new_rate", StringType(), True),
            StructField("prior_term_months", StringType(), True),
            StructField("new_term_months", StringType(), True)
        ])
        
        canonical_df = spark_session.createDataFrame(canonical_data, canonical_schema)
        
        # Create template data
        template_data = [("MORTGAGE_AMENDMENT", "<loan id='{{LOAN_ID}}'></loan>")]
        template_schema = SchemaDefinitions.get_template_record_schema()
        template_df = spark_session.createDataFrame(template_data, template_schema)
        
        result_df = processor.join_with_template(canonical_df, template_df)
        
        assert result_df.count() == 1
        assert "template_key" in result_df.columns
        assert "template_text" in result_df.columns
    
    def test_business_validate_and_render_xml_valid_data(self, processor, spark_session):
        """Test business validation and XML rendering with valid data"""
        # Create test data with template
        test_data = [
            ("evt1", "MORTGAGE_AMENDMENT", "2023-12-01T09:00:00Z", "loan1", "LOS",
             "key1", 0, 100, "2023-12-01T10:00:00Z", '{"test": "data"}',
             "RATE_CHANGE", "2023-12-01", "4.0", "3.5", "360", "360",
             "MORTGAGE_AMENDMENT", "<loan id='{{LOAN_ID}}' event='{{EVENT_ID}}'></loan>")
        ]
        
        schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("event_ts", StringType(), True),
            StructField("loan_id", StringType(), True),
            StructField("source_system", StringType(), True),
            StructField("kafka_key", StringType(), True),
            StructField("kafka_partition", IntegerType(), True),
            StructField("kafka_offset", LongType(), True),
            StructField("ingest_ts", StringType(), True),
            StructField("payload_json", StringType(), True),
            StructField("amendment_type", StringType(), True),
            StructField("effective_date", StringType(), True),
            StructField("prior_rate", StringType(), True),
            StructField("new_rate", StringType(), True),
            StructField("prior_term_months", StringType(), True),
            StructField("new_term_months", StringType(), True),
            StructField("template_key", StringType(), True),
            StructField("template_text", StringType(), True)
        ])
        
        canonical_with_template_df = spark_session.createDataFrame(test_data, schema)
        
        business_reject_df, xml_out_df = processor.business_validate_and_render_xml(canonical_with_template_df)
        
        assert business_reject_df.count() == 0
        assert xml_out_df.count() == 1
        assert xml_out_df.columns == ["event_id", "loan_id", "xml_text"]
    
    def test_business_validate_and_render_xml_invalid_data(self, processor, spark_session):
        """Test business validation with invalid data"""
        # Create test data with missing effective_date
        test_data = [
            ("evt1", "MORTGAGE_AMENDMENT", "2023-12-01T09:00:00Z", "loan1", "LOS",
             "key1", 0, 100, "2023-12-01T10:00:00Z", '{"test": "data"}',
             "RATE_CHANGE", "", "4.0", "3.5", "360", "360",  # Missing effective_date
             "MORTGAGE_AMENDMENT", "<loan id='{{LOAN_ID}}'></loan>")
        ]
        
        schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("event_ts", StringType(), True),
            StructField("loan_id", StringType(), True),
            StructField("source_system", StringType(), True),
            StructField("kafka_key", StringType(), True),
            StructField("kafka_partition", IntegerType(), True),
            StructField("kafka_offset", LongType(), True),
            StructField("ingest_ts", StringType(), True),
            StructField("payload_json", StringType(), True),
            StructField("amendment_type", StringType(), True),
            StructField("effective_date", StringType(), True),
            StructField("prior_rate", StringType(), True),
            StructField("new_rate", StringType(), True),
            StructField("prior_term_months", StringType(), True),
            StructField("new_term_months", StringType(), True),
            StructField("template_key", StringType(), True),
            StructField("template_text", StringType(), True)
        ])
        
        canonical_with_template_df = spark_session.createDataFrame(test_data, schema)
        
        business_reject_df, xml_out_df = processor.business_validate_and_render_xml(canonical_with_template_df)
        
        assert business_reject_df.count() == 1
        assert xml_out_df.count() == 0
        
        # Check reject reason
        reject_row = business_reject_df.collect()[0]
        assert reject_row.reject_type == "BUSINESS"
        assert reject_row.reason == "Missing effective_date"
    
    def test_write_outputs(self, processor, spark_session):
        """Test output writing"""
        # Create test reject data
        schema_reject_data = [
            ("SCHEMA", "Missing event_id", "", "loan1", "2023-12-01T09:00:00Z", "LOS", 0, 100, "2023-12-01T10:00:00Z", '{"test": "data"}')
        ]
        
        business_reject_data = [
            ("BUSINESS", "Missing effective_date", "evt1", "loan1", "2023-12-01T09:00:00Z", "LOS", 0, 100, "2023-12-01T10:00:00Z", "")
        ]
        
        xml_out_data = [
            ("evt1", "loan1", "<loan id='loan1' event='evt1'></loan>")
        ]
        
        reject_schema = SchemaDefinitions.get_reject_record_schema()
        xml_schema = SchemaDefinitions.get_xml_out_record_schema()
        
        schema_reject_df = spark_session.createDataFrame(schema_reject_data, reject_schema)
        business_reject_df = spark_session.createDataFrame(business_reject_data, reject_schema)
        xml_out_df = spark_session.createDataFrame(xml_out_data, xml_schema)
        
        # Mock the write operations
        with patch.object(schema_reject_df.union(business_reject_df).write, 'csv') as mock_reject_write, \
             patch.object(xml_out_df.write, 'csv') as mock_xml_write:
            
            processor.write_outputs(schema_reject_df, business_reject_df, xml_out_df)
            
            mock_reject_write.assert_called_once()
            mock_xml_write.assert_called_once()

class TestEdgeCases:
    """Test edge cases and error scenarios"""
    
    def test_empty_dataframe_processing(self, processor, spark_session):
        """Test processing with empty dataframes"""
        # Create empty dataframe
        schema = SchemaDefinitions.get_kafka_event_schema()
        empty_df = spark_session.createDataFrame([], schema)
        
        result_df = processor.extract_event_metadata(empty_df)
        assert result_df.count() == 0
    
    def test_malformed_json_handling(self, spark_session):
        """Test handling of malformed JSON"""
        malformed_json = '{"event_id": "evt1", "incomplete"'
        
        result = DataTransformationUtils.json_find_string_value(malformed_json, "event_id")
        assert result == "evt1"  # Should still extract valid parts
        
        result = DataTransformationUtils.json_find_string_value(malformed_json, "incomplete")
        assert result == ""  # Should return empty for incomplete fields
    
    def test_special_characters_in_json(self, spark_session):
        """Test handling of special characters in JSON"""
        json_with_special_chars = '{"loan_id": "LOAN-123_ABC", "event_id": "EVT@456#789"}'
        
        result = DataTransformationUtils.json_find_string_value(json_with_special_chars, "loan_id")
        assert result == "LOAN-123_ABC"
        
        result = DataTransformationUtils.json_find_string_value(json_with_special_chars, "event_id")
        assert result == "EVT@456#789"
    
    def test_xml_template_with_special_characters(self):
        """Test XML rendering with special characters"""
        template = "<loan id='{{LOAN_ID}}' note='Special chars: &lt;&gt;&amp;'></loan>"
        
        result = DataTransformationUtils.render_xml(
            template, "LOAN&123", "EVT<456>", "2023-12-01", "RATE_CHANGE", "3.5", "360"
        )
        
        expected = "<loan id='LOAN&123' note='Special chars: &lt;&gt;&amp;'></loan>"
        assert result == expected

class TestPerformanceScenarios:
    """Test performance-related scenarios"""
    
    def test_large_dataset_schema_validation(self, processor, spark_session):
        """Test schema validation with larger dataset"""
        # Create larger test dataset
        test_data = []
        for i in range(1000):
            test_data.append((
                f"evt{i}", "MORTGAGE_AMENDMENT", "2023-12-01T09:00:00Z", f"loan{i}", "LOS",
                f"key{i}", i % 10, 100 + i, "2023-12-01T10:00:00Z", f'{{"test": "data{i}"}}'
            ))
        
        schema = SchemaDefinitions.get_event_meta_schema()
        event_meta_df = spark_session.createDataFrame(test_data, schema)
        
        schema_reject_df, valid_event_meta_df = processor.validate_schema_and_split(event_meta_df)
        
        assert schema_reject_df.count() == 0
        assert valid_event_meta_df.count() == 1000
    
    def test_broadcast_join_with_template(self, processor, spark_session):
        """Test broadcast join functionality with template data"""
        # Create canonical data
        canonical_data = [(f"evt{i}", "MORTGAGE_AMENDMENT", "2023-12-01T09:00:00Z", f"loan{i}", "LOS",
                          f"key{i}", i % 10, 100 + i, "2023-12-01T10:00:00Z", f'{{"test": "data{i}"}}',
                          "RATE_CHANGE", "2023-12-01", "4.0", "3.5", "360", "360") for i in range(100)]
        
        canonical_schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("event_ts", StringType(), True),
            StructField("loan_id", StringType(), True),
            StructField("source_system", StringType(), True),
            StructField("kafka_key", StringType(), True),
            StructField("kafka_partition", IntegerType(), True),
            StructField("kafka_offset", LongType(), True),
            StructField("ingest_ts", StringType(), True),
            StructField("payload_json", StringType(), True),
            StructField("amendment_type", StringType(), True),
            StructField("effective_date", StringType(), True),
            StructField("prior_rate", StringType(), True),
            StructField("new_rate", StringType(), True),
            StructField("prior_term_months", StringType(), True),
            StructField("new_term_months", StringType(), True)
        ])
        
        canonical_df = spark_session.createDataFrame(canonical_data, canonical_schema)
        
        # Create small template data (suitable for broadcast)
        template_data = [("MORTGAGE_AMENDMENT", "<loan id='{{LOAN_ID}}'></loan>")]
        template_schema = SchemaDefinitions.get_template_record_schema()
        template_df = spark_session.createDataFrame(template_data, template_schema)
        
        result_df = processor.join_with_template(canonical_df, template_df)
        
        assert result_df.count() == 100
        assert "template_text" in result_df.columns

if __name__ == "__main__":
    # Run tests with pytest
    pytest.main(["-v", __file__])
