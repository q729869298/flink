/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.mongodb.sink;

/**
 * A {@link RuntimeException} wrapper indicating the exception was thrown from the Mongodb Async
 * Sink.
 */
class MongodbAsyncException extends RuntimeException {

    public MongodbAsyncException(final String message) {
        super(message);
    }

    public MongodbAsyncException(final String message, final Throwable cause) {
        super(message, cause);
    }

    /**
     * When the flag {@code failOnError} is set in {@link MongodbAsyncWriter}, this exception is
     * raised as soon as any exception occurs when Mongodb is written to.
     */
    static class MongodbAsyncFailFastException extends MongodbAsyncException {

        public MongodbAsyncFailFastException() {
            super(
                    "Encountered an exception while persisting records, not retrying due to {failOnError} being set.");
        }

        public MongodbAsyncFailFastException(final Throwable cause) {
            super(
                    "Encountered an exception while persisting records, not retrying due to {failOnError} being set.",
                    cause);
        }
    }
}
