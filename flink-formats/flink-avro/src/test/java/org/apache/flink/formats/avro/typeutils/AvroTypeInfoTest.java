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

package org.apache.flink.formats.avro.typeutils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeInformationTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.formats.avro.generated.Address;
import org.apache.flink.formats.avro.generated.User;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link AvroTypeInfo}. */
public class AvroTypeInfoTest extends TypeInformationTestBase<AvroTypeInfo<?>> {

    @Override
    protected AvroTypeInfo<?>[] getTestData() {
        return new AvroTypeInfo<?>[] {
            new AvroTypeInfo<>(Address.class), new AvroTypeInfo<>(User.class),
        };
    }

    @Test
    void testAvroByDefault() {
        final TypeSerializer<User> serializer =
                new AvroTypeInfo<>(User.class).createSerializer(new ExecutionConfig());
        assertThat(serializer).isInstanceOf(AvroSerializer.class);
    }
}
