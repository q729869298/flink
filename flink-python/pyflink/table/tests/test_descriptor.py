################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
import os

from pyflink.table.table_descriptor import (FileSystem, OldCsv, Rowtime, Schema, Kafka,
                                            Elasticsearch)
from pyflink.table.table_schema import TableSchema
from pyflink.table.table_sink import CsvTableSink
from pyflink.table.types import DataTypes
from pyflink.testing.test_case_utils import (PyFlinkTestCase, PyFlinkStreamTableTestCase,
                                             PyFlinkBatchTableTestCase)


class FileSystemDescriptorTests(PyFlinkTestCase):

    def test_path(self):
        file_system = FileSystem()

        file_system = file_system.path("/test.csv")

        properties = file_system.to_properties()
        expected = {'connector.property-version': '1',
                    'connector.type': 'filesystem',
                    'connector.path': '/test.csv'}
        assert properties == expected


class KafkaDescriptorTests(PyFlinkTestCase):

    def test_version(self):
        kafka = Kafka()

        kafka = kafka.version("0.11")

        properties = kafka.to_properties()
        expected = {'connector.version': '0.11',
                    'connector.type': 'kafka',
                    'connector.property-version': '1'}
        assert properties == expected

    def test_topic(self):
        kafka = Kafka()

        kafka = kafka.topic("topic1")

        properties = kafka.to_properties()
        expected = {'connector.type': 'kafka',
                    'connector.topic': 'topic1',
                    'connector.property-version': '1'}
        assert properties == expected

    def test_properties(self):
        kafka = Kafka()

        kafka = kafka.properties({"zookeeper.connect": "localhost:2181",
                                  "bootstrap.servers": "localhost:9092"})

        properties = kafka.to_properties()
        expected = {'connector.type': 'kafka',
                    'connector.properties.0.key': 'zookeeper.connect',
                    'connector.properties.0.value': 'localhost:2181',
                    'connector.properties.1.key': 'bootstrap.servers',
                    'connector.properties.1.value': 'localhost:9092',
                    'connector.property-version': '1'}
        assert properties == expected

    def test_property(self):
        kafka = Kafka()

        kafka = kafka.property("group.id", "testGroup")

        properties = kafka.to_properties()
        expected = {'connector.type': 'kafka',
                    'connector.properties.0.key': 'group.id',
                    'connector.properties.0.value': 'testGroup',
                    'connector.property-version': '1'}
        assert properties == expected

    def test_start_from_earliest(self):
        kafka = Kafka()

        kafka = kafka.start_from_earliest()

        properties = kafka.to_properties()
        expected = {'connector.type': 'kafka',
                    'connector.startup-mode': 'earliest-offset',
                    'connector.property-version': '1'}
        assert properties == expected

    def test_start_from_latest(self):
        kafka = Kafka()

        kafka = kafka.start_from_latest()

        properties = kafka.to_properties()
        expected = {'connector.type': 'kafka',
                    'connector.startup-mode': 'latest-offset',
                    'connector.property-version': '1'}
        assert properties == expected

    def test_start_from_group_offsets(self):
        kafka = Kafka()

        kafka = kafka.start_from_group_offsets()

        properties = kafka.to_properties()
        expected = {'connector.type': 'kafka',
                    'connector.startup-mode': 'group-offsets',
                    'connector.property-version': '1'}
        assert properties == expected

    def test_start_from_specific_offsets(self):
        kafka = Kafka()

        kafka = kafka.start_from_specific_offsets({1: 220, 3: 400})

        properties = kafka.to_properties()
        expected = {'connector.startup-mode': 'specific-offsets',
                    'connector.specific-offsets.0.partition': '1',
                    'connector.specific-offsets.0.offset': '220',
                    'connector.specific-offsets.1.partition': '3',
                    'connector.specific-offsets.1.offset': '400',
                    'connector.type': 'kafka',
                    'connector.property-version': '1'}
        assert properties == expected

    def test_start_from_specific_offset(self):
        kafka = Kafka()

        kafka = kafka.start_from_specific_offset(3, 300)

        properties = kafka.to_properties()
        expected = {'connector.startup-mode': 'specific-offsets',
                    'connector.specific-offsets.0.partition': '3',
                    'connector.specific-offsets.0.offset': '300',
                    'connector.type': 'kafka',
                    'connector.property-version': '1'}
        assert properties == expected

    def test_sink_partitioner_fixed(self):
        kafka = Kafka()

        kafka = kafka.sink_partitioner_fixed()

        properties = kafka.to_properties()
        expected = {'connector.sink-partitioner': 'fixed',
                    'connector.type': 'kafka',
                    'connector.property-version': '1'}
        assert properties == expected

    def test_sink_partitioner_custom(self):
        kafka = Kafka()

        kafka = kafka.sink_partitioner_custom(
            "org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner")

        properties = kafka.to_properties()
        expected = {'connector.sink-partitioner': 'custom',
                    'connector.sink-partitioner-class':
                        'org.apache.flink.streaming.connectors.kafka.partitioner.'
                        'FlinkFixedPartitioner',
                    'connector.type': 'kafka',
                    'connector.property-version': '1'}
        assert properties == expected

    def test_sink_partitioner_round_robin(self):
        kafka = Kafka()

        kafka = kafka.sink_partitioner_round_robin()

        properties = kafka.to_properties()
        expected = {'connector.sink-partitioner': 'round-robin',
                    'connector.type': 'kafka',
                    'connector.property-version': '1'}
        assert properties == expected


class ElasticsearchDescriptorTest(PyFlinkTestCase):

    def test_version(self):
        elasticsearch = Elasticsearch()

        elasticsearch = elasticsearch.version("6")

        properties = elasticsearch.to_properties()
        expected = {'connector.type': 'elasticsearch',
                    'connector.version': '6',
                    'connector.property-version': '1'}
        assert properties == expected

    def test_host(self):
        elasticsearch = Elasticsearch()

        elasticsearch = elasticsearch.host("localhost", 9200, "http")

        properties = elasticsearch.to_properties()
        expected = {'connector.hosts.0.hostname': 'localhost',
                    'connector.hosts.0.port': '9200',
                    'connector.hosts.0.protocol': 'http',
                    'connector.type': 'elasticsearch',
                    'connector.property-version': '1'}
        assert properties == expected

    def test_index(self):
        elasticsearch = Elasticsearch()

        elasticsearch = elasticsearch.index("MyUsers")

        properties = elasticsearch.to_properties()
        expected = {'connector.index': 'MyUsers',
                    'connector.type': 'elasticsearch',
                    'connector.property-version': '1'}
        assert properties == expected

    def test_document_type(self):
        elasticsearch = Elasticsearch()

        elasticsearch = elasticsearch.document_type("user")

        properties = elasticsearch.to_properties()
        expected = {'connector.document-type': 'user',
                    'connector.type': 'elasticsearch',
                    'connector.property-version': '1'}
        assert properties == expected

    def test_key_delimiter(self):
        elasticsearch = Elasticsearch()

        elasticsearch = elasticsearch.key_delimiter("$")

        properties = elasticsearch.to_properties()
        expected = {'connector.key-delimiter': '$',
                    'connector.type': 'elasticsearch',
                    'connector.property-version': '1'}
        assert properties == expected

    def test_key_null_literal(self):
        elasticsearch = Elasticsearch()

        elasticsearch = elasticsearch.key_null_literal("n/a")

        properties = elasticsearch.to_properties()
        expected = {'connector.key-null-literal': 'n/a',
                    'connector.type': 'elasticsearch',
                    'connector.property-version': '1'}
        assert properties == expected

    def test_failure_handler_fail(self):
        elasticsearch = Elasticsearch()

        elasticsearch = elasticsearch.failure_handler_fail()

        properties = elasticsearch.to_properties()
        expected = {'connector.failure-handler': 'fail',
                    'connector.type': 'elasticsearch',
                    'connector.property-version': '1'}
        assert properties == expected

    def test_failure_handler_ignore(self):
        elasticsearch = Elasticsearch()

        elasticsearch = elasticsearch.failure_handler_ignore()

        properties = elasticsearch.to_properties()
        expected = {'connector.failure-handler': 'ignore',
                    'connector.type': 'elasticsearch',
                    'connector.property-version': '1'}
        assert properties == expected

    def test_failure_handler_retry_rejected(self):
        elasticsearch = Elasticsearch()

        elasticsearch = elasticsearch.failure_handler_retry_rejected()

        properties = elasticsearch.to_properties()
        expected = {'connector.failure-handler': 'retry-rejected',
                    'connector.type': 'elasticsearch',
                    'connector.property-version': '1'}
        assert properties == expected

    def test_failure_handler_custom(self):
        elasticsearch = Elasticsearch()

        elasticsearch = elasticsearch.failure_handler_custom(
            "org.apache.flink.streaming.connectors.elasticsearch.util.IgnoringFailureHandler")

        properties = elasticsearch.to_properties()
        expected = {'connector.failure-handler': 'custom',
                    'connector.failure-handler-class':
                        'org.apache.flink.streaming.connectors.elasticsearch.util.'
                        'IgnoringFailureHandler',
                    'connector.type': 'elasticsearch',
                    'connector.property-version': '1'}
        assert properties == expected

    def test_disable_flush_on_checkpoint(self):
        elasticsearch = Elasticsearch()

        elasticsearch = elasticsearch.disable_flush_on_checkpoint()

        properties = elasticsearch.to_properties()
        expected = {'connector.flush-on-checkpoint': 'false',
                    'connector.type': 'elasticsearch',
                    'connector.property-version': '1'}
        assert properties == expected

    def test_bulk_flush_max_actions(self):
        elasticsearch = Elasticsearch()

        elasticsearch = elasticsearch.bulk_flush_max_actions(42)

        properties = elasticsearch.to_properties()
        expected = {'connector.bulk-flush.max-actions': '42',
                    'connector.type': 'elasticsearch',
                    'connector.property-version': '1'}
        assert properties == expected

    def test_bulk_flush_max_size(self):
        elasticsearch = Elasticsearch()

        elasticsearch = elasticsearch.bulk_flush_max_size("42 mb")

        properties = elasticsearch.to_properties()
        expected = {'connector.bulk-flush.max-size': '44040192 bytes',
                    'connector.type': 'elasticsearch',
                    'connector.property-version': '1'}

        assert properties == expected

    def test_bulk_flush_interval(self):
        elasticsearch = Elasticsearch()

        elasticsearch = elasticsearch.bulk_flush_interval(2000)

        properties = elasticsearch.to_properties()
        expected = {'connector.bulk-flush.interval': '2000',
                    'connector.type': 'elasticsearch',
                    'connector.property-version': '1'}
        assert properties == expected

    def test_bulk_flush_backoff_exponential(self):
        elasticsearch = Elasticsearch()

        elasticsearch = elasticsearch.bulk_flush_backoff_exponential()

        properties = elasticsearch.to_properties()
        expected = {'connector.bulk-flush.backoff.type': 'exponential',
                    'connector.type': 'elasticsearch',
                    'connector.property-version': '1'}
        assert properties == expected

    def test_bulk_flush_backoff_constant(self):
        elasticsearch = Elasticsearch()

        elasticsearch = elasticsearch.bulk_flush_backoff_constant()

        properties = elasticsearch.to_properties()
        expected = {'connector.bulk-flush.backoff.type': 'constant',
                    'connector.type': 'elasticsearch',
                    'connector.property-version': '1'}
        assert properties == expected

    def test_bulk_flush_backoff_max_retries(self):
        elasticsearch = Elasticsearch()

        elasticsearch = elasticsearch.bulk_flush_backoff_max_retries(3)

        properties = elasticsearch.to_properties()
        expected = {'connector.bulk-flush.backoff.max-retries': '3',
                    'connector.type': 'elasticsearch',
                    'connector.property-version': '1'}
        assert properties == expected

    def test_bulk_flush_backoff_delay(self):
        elasticsearch = Elasticsearch()

        elasticsearch = elasticsearch.bulk_flush_backoff_delay(30000)

        properties = elasticsearch.to_properties()
        expected = {'connector.bulk-flush.backoff.delay': '30000',
                    'connector.type': 'elasticsearch',
                    'connector.property-version': '1'}
        assert properties == expected

    def test_connection_max_retry_timeout(self):
        elasticsearch = Elasticsearch()

        elasticsearch = elasticsearch.connection_max_retry_timeout(3000)

        properties = elasticsearch.to_properties()
        expected = {'connector.connection-max-retry-timeout': '3000',
                    'connector.type': 'elasticsearch',
                    'connector.property-version': '1'}
        assert properties == expected

    def test_connection_path_prefix(self):
        elasticsearch = Elasticsearch()

        elasticsearch = elasticsearch.connection_path_prefix("/v1")

        properties = elasticsearch.to_properties()
        expected = {'connector.connection-path-prefix': '/v1',
                    'connector.type': 'elasticsearch',
                    'connector.property-version': '1'}
        assert properties == expected


class OldCsvDescriptorTests(PyFlinkTestCase):

    def test_field_delimiter(self):
        csv = OldCsv()

        csv = csv.field_delimiter("|")

        properties = csv.to_properties()
        expected = {'format.field-delimiter': '|',
                    'format.type': 'csv',
                    'format.property-version': '1'}
        assert properties == expected

    def test_line_delimiter(self):
        csv = OldCsv()

        csv = csv.line_delimiter(";")

        expected = {'format.type': 'csv',
                    'format.property-version': '1',
                    'format.line-delimiter': ';'}

        properties = csv.to_properties()
        assert properties == expected

    def test_ignore_parse_errors(self):
        csv = OldCsv()

        csv = csv.ignore_parse_errors()

        properties = csv.to_properties()
        expected = {'format.ignore-parse-errors': 'true',
                    'format.type': 'csv',
                    'format.property-version': '1'}
        assert properties == expected

    def test_quote_character(self):
        csv = OldCsv()

        csv = csv.quote_character("*")

        properties = csv.to_properties()
        expected = {'format.quote-character': '*',
                    'format.type': 'csv',
                    'format.property-version': '1'}
        assert properties == expected

    def test_comment_prefix(self):
        csv = OldCsv()

        csv = csv.comment_prefix("#")

        properties = csv.to_properties()
        expected = {'format.comment-prefix': '#',
                    'format.type': 'csv',
                    'format.property-version': '1'}
        assert properties == expected

    def test_ignore_first_line(self):
        csv = OldCsv()

        csv = csv.ignore_first_line()

        properties = csv.to_properties()
        expected = {'format.ignore-first-line': 'true',
                    'format.type': 'csv',
                    'format.property-version': '1'}
        assert properties == expected

    def test_field(self):
        csv = OldCsv()

        csv.field("a", DataTypes.BIGINT())
        csv.field("b", DataTypes.STRING())
        csv.field("c", "SQL_TIMESTAMP")

        properties = csv.to_properties()
        expected = {'format.fields.0.name': 'a',
                    'format.fields.0.type': 'BIGINT',
                    'format.fields.1.name': 'b',
                    'format.fields.1.type': 'VARCHAR',
                    'format.fields.2.name': 'c',
                    'format.fields.2.type': 'SQL_TIMESTAMP',
                    'format.type': 'csv',
                    'format.property-version': '1'}
        assert properties == expected

    def test_schema(self):
        csv = OldCsv()
        schema = TableSchema(["a", "b"], [DataTypes.INT(), DataTypes.STRING()])

        csv = csv.schema(schema)

        properties = csv.to_properties()
        expected = {'format.fields.0.name': 'a',
                    'format.fields.0.type': 'INT',
                    'format.fields.1.name': 'b',
                    'format.fields.1.type': 'VARCHAR',
                    'format.type': 'csv',
                    'format.property-version': '1'}

        assert properties == expected


class RowTimeDescriptorTests(PyFlinkTestCase):

    def test_timestamps_from_field(self):
        rowtime = Rowtime()

        rowtime = rowtime.timestamps_from_field("rtime")

        properties = rowtime.to_properties()
        expect = {'rowtime.timestamps.type': 'from-field', 'rowtime.timestamps.from': 'rtime'}
        assert properties == expect

    def test_timestamps_from_source(self):
        rowtime = Rowtime()

        rowtime = rowtime.timestamps_from_source()

        properties = rowtime.to_properties()
        expect = {'rowtime.timestamps.type': 'from-source'}
        assert properties == expect

    def test_timestamps_from_extractor(self):
        rowtime = Rowtime()

        rowtime = rowtime.timestamps_from_extractor(
            "org.apache.flink.table.descriptors.RowtimeTest$CustomExtractor")

        properties = rowtime.to_properties()
        expect = {'rowtime.timestamps.type': 'custom',
                  'rowtime.timestamps.class':
                  'org.apache.flink.table.descriptors.RowtimeTest$CustomExtractor',
                  'rowtime.timestamps.serialized':
                  'rO0ABXNyAD5vcmcuYXBhY2hlLmZsaW5rLnRhYmxlLmRlc2NyaXB0b3JzLlJvd3RpbWVUZXN0JEN1c3R'
                  'vbUV4dHJhY3RvcoaChjMg55xwAgABTAAFZmllbGR0ABJMamF2YS9sYW5nL1N0cmluZzt4cgA-b3JnLm'
                  'FwYWNoZS5mbGluay50YWJsZS5zb3VyY2VzLnRzZXh0cmFjdG9ycy5UaW1lc3RhbXBFeHRyYWN0b3Jf1'
                  'Y6piFNsGAIAAHhwdAACdHM'}
        assert properties == expect

    def test_watermarks_periodic_ascending(self):
        rowtime = Rowtime()

        rowtime = rowtime.watermarks_periodic_ascending()

        properties = rowtime.to_properties()
        expect = {'rowtime.watermarks.type': 'periodic-ascending'}
        assert properties == expect

    def test_watermarks_periodic_bounded(self):
        rowtime = Rowtime()

        rowtime = rowtime.watermarks_periodic_bounded(1000)

        properties = rowtime.to_properties()
        expect = {'rowtime.watermarks.type': 'periodic-bounded',
                  'rowtime.watermarks.delay': '1000'}
        assert properties == expect

    def test_watermarks_from_source(self):
        rowtime = Rowtime()

        rowtime = rowtime.watermarks_from_source()

        properties = rowtime.to_properties()
        expect = {'rowtime.watermarks.type': 'from-source'}
        assert properties == expect

    def test_watermarks_from_strategy(self):
        rowtime = Rowtime()

        rowtime = rowtime.watermarks_from_strategy(
            "org.apache.flink.table.descriptors.RowtimeTest$CustomAssigner")

        properties = rowtime.to_properties()
        expect = {'rowtime.watermarks.type': 'custom',
                  'rowtime.watermarks.class':
                  'org.apache.flink.table.descriptors.RowtimeTest$CustomAssigner',
                  'rowtime.watermarks.serialized':
                  'rO0ABXNyAD1vcmcuYXBhY2hlLmZsaW5rLnRhYmxlLmRlc2NyaXB0b3JzLlJvd3RpbWVUZXN0JEN1c3R'
                  'vbUFzc2lnbmVyeDcuDvfbu0kCAAB4cgBHb3JnLmFwYWNoZS5mbGluay50YWJsZS5zb3VyY2VzLndtc3'
                  'RyYXRlZ2llcy5QdW5jdHVhdGVkV2F0ZXJtYXJrQXNzaWduZXKBUc57oaWu9AIAAHhyAD1vcmcuYXBhY'
                  '2hlLmZsaW5rLnRhYmxlLnNvdXJjZXMud21zdHJhdGVnaWVzLldhdGVybWFya1N0cmF0ZWd53nt-g2OW'
                  'aT4CAAB4cA'}
        assert properties == expect


class SchemaDescriptorTests(PyFlinkTestCase):

    def test_field(self):
        schema = Schema()

        schema = schema\
            .field("int_field", DataTypes.INT())\
            .field("long_field", DataTypes.BIGINT())\
            .field("string_field", DataTypes.STRING())\
            .field("timestamp_field", DataTypes.TIMESTAMP())\
            .field("time_field", DataTypes.TIME())\
            .field("date_field", DataTypes.DATE())\
            .field("double_field", DataTypes.DOUBLE())\
            .field("float_field", DataTypes.FLOAT())\
            .field("byte_field", DataTypes.TINYINT())\
            .field("short_field", DataTypes.SMALLINT())\
            .field("boolean_field", DataTypes.BOOLEAN())

        properties = schema.to_properties()
        expected = {'schema.0.name': 'int_field',
                    'schema.0.type': 'INT',
                    'schema.1.name': 'long_field',
                    'schema.1.type': 'BIGINT',
                    'schema.2.name': 'string_field',
                    'schema.2.type': 'VARCHAR',
                    'schema.3.name': 'timestamp_field',
                    'schema.3.type': 'TIMESTAMP',
                    'schema.4.name': 'time_field',
                    'schema.4.type': 'TIME',
                    'schema.5.name': 'date_field',
                    'schema.5.type': 'DATE',
                    'schema.6.name': 'double_field',
                    'schema.6.type': 'DOUBLE',
                    'schema.7.name': 'float_field',
                    'schema.7.type': 'FLOAT',
                    'schema.8.name': 'byte_field',
                    'schema.8.type': 'TINYINT',
                    'schema.9.name': 'short_field',
                    'schema.9.type': 'SMALLINT',
                    'schema.10.name': 'boolean_field',
                    'schema.10.type': 'BOOLEAN'}
        assert properties == expected

    def test_field_in_string(self):
        schema = Schema()

        schema = schema\
            .field("int_field", 'INT')\
            .field("long_field", 'BIGINT')\
            .field("string_field", 'VARCHAR')\
            .field("timestamp_field", 'SQL_TIMESTAMP')\
            .field("time_field", 'SQL_TIME')\
            .field("date_field", 'SQL_DATE')\
            .field("double_field", 'DOUBLE')\
            .field("float_field", 'FLOAT')\
            .field("byte_field", 'TINYINT')\
            .field("short_field", 'SMALLINT')\
            .field("boolean_field", 'BOOLEAN')

        properties = schema.to_properties()
        expected = {'schema.0.name': 'int_field',
                    'schema.0.type': 'INT',
                    'schema.1.name': 'long_field',
                    'schema.1.type': 'BIGINT',
                    'schema.2.name': 'string_field',
                    'schema.2.type': 'VARCHAR',
                    'schema.3.name': 'timestamp_field',
                    'schema.3.type': 'SQL_TIMESTAMP',
                    'schema.4.name': 'time_field',
                    'schema.4.type': 'SQL_TIME',
                    'schema.5.name': 'date_field',
                    'schema.5.type': 'SQL_DATE',
                    'schema.6.name': 'double_field',
                    'schema.6.type': 'DOUBLE',
                    'schema.7.name': 'float_field',
                    'schema.7.type': 'FLOAT',
                    'schema.8.name': 'byte_field',
                    'schema.8.type': 'TINYINT',
                    'schema.9.name': 'short_field',
                    'schema.9.type': 'SMALLINT',
                    'schema.10.name': 'boolean_field',
                    'schema.10.type': 'BOOLEAN'}
        assert properties == expected

    def test_from_origin_field(self):
        schema = Schema()

        schema = schema\
            .field("int_field", DataTypes.INT())\
            .field("long_field", DataTypes.BIGINT()).from_origin_field("origin_field_a")\
            .field("string_field", DataTypes.STRING())

        properties = schema.to_properties()
        expected = {'schema.0.name': 'int_field',
                    'schema.0.type': 'INT',
                    'schema.1.name': 'long_field',
                    'schema.1.type': 'BIGINT',
                    'schema.1.from': 'origin_field_a',
                    'schema.2.name': 'string_field',
                    'schema.2.type': 'VARCHAR'}
        assert properties == expected

    def test_proctime(self):
        schema = Schema()

        schema = schema\
            .field("int_field", DataTypes.INT())\
            .field("ptime", DataTypes.BIGINT()).proctime()\
            .field("string_field", DataTypes.STRING())

        properties = schema.to_properties()
        expected = {'schema.0.name': 'int_field',
                    'schema.0.type': 'INT',
                    'schema.1.name': 'ptime',
                    'schema.1.type': 'BIGINT',
                    'schema.1.proctime': 'true',
                    'schema.2.name': 'string_field',
                    'schema.2.type': 'VARCHAR'}
        assert properties == expected

    def test_rowtime(self):
        schema = Schema()

        schema = schema\
            .field("int_field", DataTypes.INT())\
            .field("long_field", DataTypes.BIGINT())\
            .field("rtime", DataTypes.BIGINT())\
            .rowtime(
                Rowtime().timestamps_from_field("long_field").watermarks_periodic_bounded(5000))\
            .field("string_field", DataTypes.STRING())

        properties = schema.to_properties()
        print(properties)
        expected = {'schema.0.name': 'int_field',
                    'schema.0.type': 'INT',
                    'schema.1.name': 'long_field',
                    'schema.1.type': 'BIGINT',
                    'schema.2.name': 'rtime',
                    'schema.2.type': 'BIGINT',
                    'schema.2.rowtime.timestamps.type': 'from-field',
                    'schema.2.rowtime.timestamps.from': 'long_field',
                    'schema.2.rowtime.watermarks.type': 'periodic-bounded',
                    'schema.2.rowtime.watermarks.delay': '5000',
                    'schema.3.name': 'string_field',
                    'schema.3.type': 'VARCHAR'}
        assert properties == expected

    def test_schema(self):
        schema = Schema()
        table_schema = TableSchema(["a", "b"], [DataTypes.INT(), DataTypes.STRING()])

        schema = schema.schema(table_schema)

        properties = schema.to_properties()
        expected = {'schema.0.name': 'a',
                    'schema.0.type': 'INT',
                    'schema.1.name': 'b',
                    'schema.1.type': 'VARCHAR'}
        assert properties == expected


class AbstractTableDescriptorTests(object):

    def test_with_format(self):
        descriptor = self.t_env.connect(FileSystem())

        descriptor = descriptor.with_format(OldCsv().field("a", "INT"))

        properties = descriptor.to_properties()

        expected = {'format.type': 'csv',
                    'format.property-version': '1',
                    'format.fields.0.name': 'a',
                    'format.fields.0.type': 'INT',
                    'connector.property-version': '1',
                    'connector.type': 'filesystem'}
        assert properties == expected

    def test_with_schema(self):
        descriptor = self.t_env.connect(FileSystem())

        descriptor = descriptor.with_format(OldCsv()).with_schema(Schema().field("a", "INT"))

        properties = descriptor.to_properties()
        expected = {'schema.0.name': 'a',
                    'schema.0.type': 'INT',
                    'format.type': 'csv',
                    'format.property-version': '1',
                    'connector.type': 'filesystem',
                    'connector.property-version': '1'}
        assert properties == expected

    def test_register_table_sink(self):
        source_path = os.path.join(self.tempdir + '/streaming.csv')
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING()]
        data = [(1, "Hi", "Hello"), (2, "Hello", "Hello")]
        csv_source = self.prepare_csv_source(source_path, data, field_types, field_names)
        t_env = self.t_env
        t_env.register_table_source("source", csv_source)
        # connect sink
        sink_path = os.path.join(self.tempdir + '/streaming2.csv')
        if os.path.isfile(sink_path):
            os.remove(sink_path)

        t_env.connect(FileSystem().path(sink_path))\
             .with_format(OldCsv()
                          .field_delimiter(',')
                          .field("a", DataTypes.INT())
                          .field("b", DataTypes.STRING())
                          .field("c", DataTypes.STRING()))\
             .with_schema(Schema()
                          .field("a", DataTypes.INT())
                          .field("b", DataTypes.STRING())
                          .field("c", DataTypes.STRING()))\
             .register_table_sink("sink")
        t_env.scan("source") \
             .select("a + 1, b, c") \
             .insert_into("sink")
        t_env.execute()

        with open(sink_path, 'r') as f:
            lines = f.read()
            assert lines == '2,Hi,Hello\n' + "3,Hello,Hello\n"

    def test_register_table_source(self):
        source_path = os.path.join(self.tempdir + '/streaming.csv')
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING()]
        data = [(1, "Hi", "Hello"), (2, "Hello", "Hello")]
        self.prepare_csv_source(source_path, data, field_types, field_names)
        t_env = self.t_env
        sink_path = os.path.join(self.tempdir + '/streaming2.csv')
        if os.path.isfile(sink_path):
            os.remove(sink_path)
        t_env.register_table_sink(
            "sink",
            field_names, field_types, CsvTableSink(sink_path))

        # connect source
        t_env.connect(FileSystem().path(source_path))\
             .with_format(OldCsv()
                          .field_delimiter(',')
                          .field("a", DataTypes.INT())
                          .field("b", DataTypes.STRING())
                          .field("c", DataTypes.STRING()))\
             .with_schema(Schema()
                          .field("a", DataTypes.INT())
                          .field("b", DataTypes.STRING())
                          .field("c", DataTypes.STRING()))\
             .register_table_source("source")
        t_env.scan("source") \
             .select("a + 1, b, c") \
             .insert_into("sink")
        t_env.execute()

        with open(sink_path, 'r') as f:
            lines = f.read()
            assert lines == '2,Hi,Hello\n' + '3,Hello,Hello\n'

    def test_register_table_source_and_sink(self):
        source_path = os.path.join(self.tempdir + '/streaming.csv')
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING()]
        data = [(1, "Hi", "Hello"), (2, "Hello", "Hello")]
        self.prepare_csv_source(source_path, data, field_types, field_names)
        sink_path = os.path.join(self.tempdir + '/streaming2.csv')
        if os.path.isfile(sink_path):
            os.remove(sink_path)
        t_env = self.t_env

        t_env.connect(FileSystem().path(source_path))\
             .with_format(OldCsv()
                          .field_delimiter(',')
                          .field("a", DataTypes.INT())
                          .field("b", DataTypes.STRING())
                          .field("c", DataTypes.STRING()))\
             .with_schema(Schema()
                          .field("a", DataTypes.INT())
                          .field("b", DataTypes.STRING())
                          .field("c", DataTypes.STRING()))\
             .register_table_source_and_sink("source")
        t_env.connect(FileSystem().path(sink_path))\
             .with_format(OldCsv()
                          .field_delimiter(',')
                          .field("a", DataTypes.INT())
                          .field("b", DataTypes.STRING())
                          .field("c", DataTypes.STRING()))\
             .with_schema(Schema()
                          .field("a", DataTypes.INT())
                          .field("b", DataTypes.STRING())
                          .field("c", DataTypes.STRING()))\
             .register_table_source_and_sink("sink")
        t_env.scan("source") \
             .select("a + 1, b, c") \
             .insert_into("sink")
        t_env.execute()

        with open(sink_path, 'r') as f:
            lines = f.read()
            assert lines == '2,Hi,Hello\n' + "3,Hello,Hello\n"


class StreamTableDescriptorTests(PyFlinkStreamTableTestCase, AbstractTableDescriptorTests):

    def test_in_append_mode(self):
        descriptor = self.t_env.connect(FileSystem())

        descriptor = descriptor\
            .with_format(OldCsv())\
            .in_append_mode()

        properties = descriptor.to_properties()
        expected = {'update-mode': 'append',
                    'format.type': 'csv',
                    'format.property-version': '1',
                    'connector.property-version': '1',
                    'connector.type': 'filesystem'}
        assert properties == expected

    def test_in_retract_mode(self):
        descriptor = self.t_env.connect(FileSystem())

        descriptor = descriptor \
            .with_format(OldCsv()) \
            .in_retract_mode()

        properties = descriptor.to_properties()
        expected = {'update-mode': 'retract',
                    'format.type': 'csv',
                    'format.property-version': '1',
                    'connector.property-version': '1',
                    'connector.type': 'filesystem'}
        assert properties == expected

    def test_in_upsert_mode(self):
        descriptor = self.t_env.connect(FileSystem())

        descriptor = descriptor \
            .with_format(OldCsv()) \
            .in_upsert_mode()

        properties = descriptor.to_properties()
        expected = {'update-mode': 'upsert',
                    'format.type': 'csv',
                    'format.property-version': '1',
                    'connector.property-version': '1',
                    'connector.type': 'filesystem'}
        assert properties == expected


class BatchTableDescriptorTests(PyFlinkBatchTableTestCase, AbstractTableDescriptorTests):
    pass


class StreamDescriptorEndToEndTests(PyFlinkStreamTableTestCase):

    def test_end_to_end(self):
        source_path = os.path.join(self.tempdir + '/streaming.csv')
        with open(source_path, 'w') as f:
            lines = 'a,b,c\n' + \
                    '1,hi,hello\n' + \
                    '#comments\n' + \
                    "error line\n" + \
                    '2,"hi,world!",hello\n'
            f.write(lines)
            f.close()
        sink_path = os.path.join(self.tempdir + '/streaming2.csv')
        t_env = self.t_env
        # connect source
        t_env.connect(FileSystem().path(source_path))\
             .with_format(OldCsv()
                          .field_delimiter(',')
                          .line_delimiter("\n")
                          .ignore_parse_errors()
                          .quote_character('"')
                          .comment_prefix("#")
                          .ignore_first_line()
                          .field("a", "INT")
                          .field("b", "VARCHAR")
                          .field("c", "VARCHAR"))\
             .with_schema(Schema()
                          .field("a", "INT")
                          .field("b", "VARCHAR")
                          .field("c", "VARCHAR"))\
             .in_append_mode()\
             .register_table_source("source")
        # connect sink
        t_env.connect(FileSystem().path(sink_path))\
             .with_format(OldCsv()
                          .field_delimiter(',')
                          .field("a", DataTypes.INT())
                          .field("b", DataTypes.STRING())
                          .field("c", DataTypes.STRING()))\
             .with_schema(Schema()
                          .field("a", DataTypes.INT())
                          .field("b", DataTypes.STRING())
                          .field("c", DataTypes.STRING()))\
             .register_table_sink("sink")

        t_env.scan("source") \
             .select("a + 1, b, c") \
             .insert_into("sink")
        t_env.execute()

        with open(sink_path, 'r') as f:
            lines = f.read()
            assert lines == '2,hi,hello\n' + '3,hi,world!,hello\n'


if __name__ == '__main__':
    import unittest

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
