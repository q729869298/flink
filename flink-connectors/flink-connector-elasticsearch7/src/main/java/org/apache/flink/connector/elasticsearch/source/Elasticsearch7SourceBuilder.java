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

package org.apache.flink.connector.elasticsearch.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.connector.elasticsearch.common.NetworkClientConfig;
import org.apache.flink.connector.elasticsearch.source.reader.Elasticsearch7SearchHitDeserializationSchema;
import org.apache.flink.util.Preconditions;

import org.apache.http.HttpHost;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The @builder class for {@link Elasticsearch7Source}.
 *
 * <p>The following example shows the minimum setup to create a ElasticsearchSource that reads the
 * String values from an Elasticsearch index.
 *
 * <pre>{@code
 * Elasticsearch7Source<String> source = Elasticsearch7Source.<String>builder()
 *     .setHosts(new HttpHost("localhost:9200"))
 *     .setIndexName("my-index")
 *     .setDeserializationSchema(new Elasticsearch7SearchHitDeserializationSchema<String>() {
 *          @Override
 *          public void deserialize(SearchHit record, Collector<String> out) {
 *              out.collect(record.getSourceAsString());
 *          }
 *
 *          @Override
 *          public TypeInformation<String> getProducedType() {
 *              return TypeInformation.of(String.class);
 *          }
 *      })
 *     .build();
 * }</pre>
 *
 * <p>The ElasticsearchSource runs in a {@link Boundedness#BOUNDED} mode and stops when the entire
 * index has been read.
 *
 * <p>Check the Java docs of each individual methods to learn more about the settings to build a
 * ElasticsearchSource.
 */
@PublicEvolving
public class Elasticsearch7SourceBuilder<OUT> {

    private Duration pitKeepAlive = Duration.ofMinutes(5);
    private int numberOfSearchSlices = 2;
    private String indexName;
    private Elasticsearch7SearchHitDeserializationSchema<OUT> deserializationSchema;

    private List<HttpHost> hosts;
    private String username;
    private String password;
    private String connectionPathPrefix;
    private Integer connectionTimeout;
    private Integer socketTimeout;
    private Integer connectionRequestTimeout;

    Elasticsearch7SourceBuilder() {}

    private boolean isGreaterOrEqual(Duration d1, Duration d2) {
        return ((d1.compareTo(d2) >= 0));
    }

    /**
     * Sets the keep alive duration for the Point-in-Time (PIT). The PIT is required to a snapshot
     * of the data in the index, to avoid reading the same data on subsequent search calls. If the
     * PIT has expired the source will not be able to read data from this snapshot again. This also
     * means that recovering from a checkpoint whose PIT has expired is not possible.
     *
     * @param pitKeepAlive duration of the PIT keep alive
     * @return this builder
     */
    public Elasticsearch7SourceBuilder<OUT> setPitKeepAlive(Duration pitKeepAlive) {
        checkNotNull(pitKeepAlive);
        checkArgument(
                isGreaterOrEqual(pitKeepAlive, Duration.ofMinutes(5)),
                "PIT keep alive should be at least 5 minutes.");
        this.pitKeepAlive = pitKeepAlive;
        return this;
    }

    /**
     * Sets the number of search slices to be used to read the index. The number of search slices
     * should be a multiple of the number of shards in your Elasticsearch cluster.
     *
     * @param numberOfSearchSlices the number of search slices
     * @return this builder
     */
    public Elasticsearch7SourceBuilder<OUT> setNumberOfSearchSlices(int numberOfSearchSlices) {
        checkArgument(numberOfSearchSlices >= 2, "Number of search slices must be at least 2.");
        this.numberOfSearchSlices = numberOfSearchSlices;
        return this;
    }

    /**
     * Sets the name of the Elasticsearch index to be read.
     *
     * @param indexName name of the Elasticsearch index
     * @return this builder
     */
    public Elasticsearch7SourceBuilder<OUT> setIndexName(String indexName) {
        checkNotNull(indexName);
        this.indexName = indexName;
        return this;
    }

    /**
     * Sets the {@link Elasticsearch7SearchHitDeserializationSchema} for the Elasticsearch source.
     * The given schema will be used to deserialize {@link org.elasticsearch.search.SearchHit}s into
     * the output type.
     *
     * @param deserializationSchema the deserialization schema to use
     * @return this builder
     */
    public Elasticsearch7SourceBuilder<OUT> setDeserializationSchema(
            Elasticsearch7SearchHitDeserializationSchema<OUT> deserializationSchema) {
        checkNotNull(deserializationSchema);
        this.deserializationSchema = deserializationSchema;
        return this;
    }

    /**
     * Sets the hosts where the Elasticsearch cluster nodes are reachable.
     *
     * @param hosts http addresses describing the node locations
     * @return this builder
     */
    public Elasticsearch7SourceBuilder<OUT> setHosts(HttpHost... hosts) {
        checkNotNull(hosts);
        checkState(hosts.length > 0, "Hosts cannot be empty.");
        Arrays.stream(hosts).forEach(Preconditions::checkNotNull);
        this.hosts = Arrays.asList(hosts);
        return this;
    }

    /**
     * Sets the username used to authenticate the connection with the Elasticsearch cluster.
     *
     * @param username of the Elasticsearch cluster user
     * @return this builder
     */
    public Elasticsearch7SourceBuilder<OUT> setConnectionUsername(String username) {
        checkNotNull(username);
        this.username = username;
        return this;
    }

    /**
     * Sets the password used to authenticate the connection with the Elasticsearch cluster.
     *
     * @param password of the Elasticsearch cluster user
     * @return this builder
     */
    public Elasticsearch7SourceBuilder<OUT> setConnectionPassword(String password) {
        checkNotNull(password);
        this.password = password;
        return this;
    }

    /**
     * Sets a prefix which used for every REST communication to the Elasticsearch cluster.
     *
     * @param prefix for the communication
     * @return this builder
     */
    public Elasticsearch7SourceBuilder<OUT> setConnectionPathPrefix(String prefix) {
        checkNotNull(prefix);
        this.connectionPathPrefix = prefix;
        return this;
    }

    /**
     * Sets the timeout for requesting the connection to the Elasticsearch cluster from the
     * connection manager.
     *
     * @param connectionRequestTimeout timeout for the connection request
     * @return this builder
     */
    public Elasticsearch7SourceBuilder<OUT> setConnectionRequestTimeout(
            int connectionRequestTimeout) {
        checkState(
                connectionRequestTimeout >= 0,
                "Connection request timeout must be larger than or equal to 0.");
        this.connectionRequestTimeout = connectionRequestTimeout;
        return this;
    }

    /**
     * Sets the timeout for establishing a connection to the Elasticsearch cluster.
     *
     * @param connectionTimeout timeout for the connection
     * @return this builder
     */
    public Elasticsearch7SourceBuilder<OUT> setConnectionTimeout(int connectionTimeout) {
        checkState(connectionTimeout >= 0, "Connection timeout must be larger than or equal to 0.");
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    /**
     * Sets the timeout for waiting for data or, put differently, a maximum period inactivity
     * between two consecutive data packets.
     *
     * @param socketTimeout timeout for the socket
     * @return this builder
     */
    public Elasticsearch7SourceBuilder<OUT> setSocketTimeout(int socketTimeout) {
        checkState(socketTimeout >= 0, "Socket timeout must be larger than or equal to 0.");
        this.socketTimeout = socketTimeout;
        return this;
    }

    private void checkRequiredParameters() {
        checkNotNull(hosts);
        checkArgument(!hosts.isEmpty(), "Hosts cannot be empty.");
        hosts.forEach(Preconditions::checkNotNull);
        checkArgument(!indexName.isEmpty(), "Index name cannot be empty.");
        checkArgument(numberOfSearchSlices >= 2, "Number of search slices must be at least 2.");
        checkArgument(
                isGreaterOrEqual(pitKeepAlive, Duration.ofMinutes(5)),
                "PIT keep alive should be at least 5 minutes.");
        checkNotNull(deserializationSchema, "Deserialization schema is required but not provided.");
    }

    /** Builds the {@link Elasticsearch7Source} using this preconfigured builder. */
    public Elasticsearch7Source<OUT> build() {
        checkRequiredParameters();

        ClosureCleaner.clean(
                deserializationSchema, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);

        NetworkClientConfig networkClientConfig =
                new NetworkClientConfig(
                        username,
                        password,
                        connectionPathPrefix,
                        connectionRequestTimeout,
                        connectionTimeout,
                        socketTimeout);

        Elasticsearch7SourceConfiguration sourceConfiguration =
                new Elasticsearch7SourceConfiguration(
                        hosts, indexName, numberOfSearchSlices, pitKeepAlive);

        return new Elasticsearch7Source<>(
                deserializationSchema, sourceConfiguration, networkClientConfig);
    }
}
