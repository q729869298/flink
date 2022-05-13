/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.firehose.table;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.connector.base.table.AsyncSinkConnectorOptions;

/** Options for the Kinesis firehose connector. */
@PublicEvolving
public class KinesisFirehoseConnectorOptions extends AsyncSinkConnectorOptions {

    public static final ConfigOption<String> DELIVERY_STREAM =
            ConfigOptions.key("delivery-stream")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Name of the Kinesis Firehose delivery stream backing this table.");

    public static final ConfigOption<Boolean> SINK_FAIL_ON_ERROR =
            ConfigOptions.key("sink.fail-on-error")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Optional fail on error value for kinesis Firehose sink, default is false");
}
