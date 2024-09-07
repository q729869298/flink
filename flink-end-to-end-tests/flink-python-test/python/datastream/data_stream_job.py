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

from typing import Any

from pyflink.common import Duration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import TimestampAssigner, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaOffsetsInitializer, \
    KafkaRecordSerializationSchema
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.datastream.functions import KeyedProcessFunction

from functions import MyKeySelector


def python_data_stream_example():
    env = StreamExecutionEnvironment.get_execution_environment()
    # Set the parallelism to be one to make sure that all data including fired timer and normal data
    # are processed by the same worker and the collected result would be in order which is good for
    # assertion.
    env.set_parallelism(1)
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

    type_info = Types.ROW_NAMED(['createTime', 'orderId', 'payAmount', 'payPlatform', 'provinceId'],
                                [Types.LONG(), Types.LONG(), Types.DOUBLE(), Types.INT(),
                                 Types.INT()])
    json_row_schema = JsonRowDeserializationSchema.builder().type_info(type_info).build()

    source = KafkaSource.builder() \
        .set_bootstrap_servers('localhost:9092') \
        .set_group_id('pyflink-e2e-source') \
        .set_topic_pattern("timer-stream-source") \
        .set_value_only_deserializer(json_row_schema) \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .build()

    record_serializer = KafkaRecordSerializationSchema.builder() \
        .set_topic('timer-stream-sink') \
        .set_value_serialization_schema(SimpleStringSchema()) \
        .build()
    sink = KafkaSink.builder() \
        .set_bootstrap_servers('localhost:9092') \
        .set_record_serializer(record_serializer) \
        .build()

    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5))\
        .with_timestamp_assigner(KafkaRowTimestampAssigner())

    ds = env.from_source(source, watermark_strategy, source_name="kafka source")
    ds.key_by(MyKeySelector(), key_type=Types.LONG()) \
        .process(MyProcessFunction(), output_type=Types.STRING()) \
        .sink_to(sink)
    env.execute_async("test data stream timer")


class MyProcessFunction(KeyedProcessFunction):

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        result = "Current key: {}, orderId: {}, payAmount: {}, timestamp: {}".format(
            str(ctx.get_current_key()), str(value[1]), str(value[2]), str(ctx.timestamp()))
        yield result
        current_watermark = ctx.timer_service().current_watermark()
        ctx.timer_service().register_event_time_timer(current_watermark + 1500)

    def on_timer(self, timestamp, ctx: 'KeyedProcessFunction.OnTimerContext'):
        yield "On timer timestamp: " + str(timestamp)


class KafkaRowTimestampAssigner(TimestampAssigner):

    def extract_timestamp(self, value: Any, record_timestamp: int) -> int:
        return int(value[0])


if __name__ == '__main__':
    python_data_stream_example()
