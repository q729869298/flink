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

package org.apache.flink.table.factories;

import org.apache.flink.table.api.NoMatchingTableFactoryException;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.factories.utils.TestLegacyCatalogFactory;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.factories.utils.TestLegacyCatalogFactory.CATALOG_TYPE_TEST_LEGACY;
import static org.junit.Assert.assertEquals;

/**
 * Tests for testing external catalog discovery using {@link TableFactoryService}. The tests assume
 * the catalog factory {@link CatalogFactory} is registered.
 *
 * @deprecated These tests are for the legacy factory stack
 */
@Deprecated
public class CatalogFactoryServiceTest {
    @Test
    public void testValidProperties() {
        Map<String, String> props = properties();

        assertEquals(
                TableFactoryService.find(CatalogFactory.class, props).getClass(),
                TestLegacyCatalogFactory.class);
    }

    @Test(expected = NoMatchingTableFactoryException.class)
    public void testInvalidContext() {
        Map<String, String> props = properties();
        props.put(CommonCatalogOptions.CATALOG_TYPE.key(), "unknown-catalog-type");
        TableFactoryService.find(CatalogFactory.class, props);
    }

    @Test
    public void testDifferentContextVersion() {
        Map<String, String> props = properties();
        props.put(FactoryUtil.PROPERTY_VERSION.key(), "2");

        // the catalog should still be found
        assertEquals(
                TableFactoryService.find(CatalogFactory.class, props).getClass(),
                TestLegacyCatalogFactory.class);
    }

    @Test(expected = NoMatchingTableFactoryException.class)
    public void testUnsupportedProperty() {
        Map<String, String> props = properties();
        props.put("unknown-property", "/new/path");
        TableFactoryService.find(CatalogFactory.class, props);
    }

    private Map<String, String> properties() {
        Map<String, String> properties = new HashMap<>();

        properties.put(CommonCatalogOptions.CATALOG_TYPE.key(), CATALOG_TYPE_TEST_LEGACY);
        properties.put(FactoryUtil.PROPERTY_VERSION.key(), "1");
        return properties;
    }
}
