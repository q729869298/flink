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

package org.apache.flink.connectors.kinesis.table.utils;

import org.apache.flink.connector.aws.config.AWSConfigConstants;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/** Unit tests for {@link AWSOptionsUtils}. */
public class AWSOptionsUtilTest {

    @Test
    public void testAWSKeyMapper() {
        AWSOptionsUtils awsOptionsUtils = new AWSOptionsUtils(getDefaultAWSConfigurations());
        Map<String, String> actualMappedProperties = awsOptionsUtils.getProcessedResolvedOptions();
        Map<String, String> expectedProperties = getDefaultExpectedAWSConfigurations();
        Assertions.assertThat(actualMappedProperties).isEqualTo(expectedProperties);
    }

    @Test
    public void testAWSKeySelectionAndMapping() {
        Map<String, String> resolvedTableOptions = getDefaultAWSConfigurations();
        resolvedTableOptions.put("non.aws.key1", "value1");
        resolvedTableOptions.put("non.aws.key2", "value2");
        resolvedTableOptions.put("non.aws.key3", "value3");
        resolvedTableOptions.put("non.aws.key4", "value4");
        AWSOptionsUtils awsOptionsUtils = new AWSOptionsUtils(resolvedTableOptions);
        Map<String, String> actualMappedProperties = awsOptionsUtils.getProcessedResolvedOptions();
        Map<String, String> expectedProperties = getDefaultExpectedAWSConfigurations();
        Assertions.assertThat(actualMappedProperties).isEqualTo(expectedProperties);
    }

    @Test
    public void testGoodAWSProperties() {
        AWSOptionsUtils awsOptionsUtils = new AWSOptionsUtils(getDefaultAWSConfigurations());
        Properties expectedProperties = new Properties();
        expectedProperties.putAll(getDefaultExpectedAWSConfigurations());
        Properties actualProperties = awsOptionsUtils.getValidatedConfigurations();
        Assertions.assertThat(actualProperties).isEqualTo(expectedProperties);
    }

    @Test
    public void testBadAWSRegion() {
        Map<String, String> defaultProperties = getDefaultAWSConfigurations();
        defaultProperties.put("aws.region", "invalid-aws-region");
        AWSOptionsUtils awsOptionsUtils = new AWSOptionsUtils(defaultProperties);
        Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(awsOptionsUtils::getValidatedConfigurations)
                .withMessageContaining("Invalid AWS region set in config.");
    }

    @Test
    public void testMissingAWSCredentials() {
        Map<String, String> defaultProperties = getDefaultAWSConfigurations();
        defaultProperties.remove("aws.credentials.basic.accesskeyid");
        AWSOptionsUtils awsOptionsUtils = new AWSOptionsUtils(defaultProperties);
        Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(awsOptionsUtils::getValidatedConfigurations)
                .withMessageContaining(
                        String.format(
                                "Please set values for AWS Access Key ID ('%s') "
                                        + "and Secret Key ('%s') when using the BASIC AWS credential provider type.",
                                AWSConfigConstants.AWS_ACCESS_KEY_ID,
                                AWSConfigConstants.AWS_SECRET_ACCESS_KEY));
    }

    @Test
    public void testInvalidTrustAllCertificatesOption() {
        Map<String, String> defaultProperties = getDefaultAWSConfigurations();
        defaultProperties.put("aws.trust.all.certificates", "invalid-boolean");
        AWSOptionsUtils awsOptionsUtils = new AWSOptionsUtils(defaultProperties);
        Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(awsOptionsUtils::getValidatedConfigurations)
                .withMessageContaining(
                        String.format(
                                "Invalid %s value, must be a boolean.",
                                AWSConfigConstants.TRUST_ALL_CERTIFICATES));
    }

    private static Map<String, String> getDefaultAWSConfigurations() {
        Map<String, String> defaultAWSConfigurations = new HashMap<String, String>();
        defaultAWSConfigurations.put("aws.region", "us-west-2");
        defaultAWSConfigurations.put("aws.credentials.provider", "BASIC");
        defaultAWSConfigurations.put("aws.credentials.basic.accesskeyid", "ververicka");
        defaultAWSConfigurations.put(
                "aws.credentials.basic.secretkey", "SuperSecretSecretSquirrel");
        defaultAWSConfigurations.put("aws.trust.all.certificates", "true");
        return defaultAWSConfigurations;
    }

    private static Map<String, String> getDefaultExpectedAWSConfigurations() {
        Map<String, String> defaultExpectedAWSConfigurations = new HashMap<String, String>();
        defaultExpectedAWSConfigurations.put("aws.region", "us-west-2");
        defaultExpectedAWSConfigurations.put("aws.credentials.provider", "BASIC");
        defaultExpectedAWSConfigurations.put(
                "aws.credentials.provider.basic.accesskeyid", "ververicka");
        defaultExpectedAWSConfigurations.put(
                "aws.credentials.provider.basic.secretkey", "SuperSecretSecretSquirrel");
        defaultExpectedAWSConfigurations.put("aws.trust.all.certificates", "true");
        return defaultExpectedAWSConfigurations;
    }
}
