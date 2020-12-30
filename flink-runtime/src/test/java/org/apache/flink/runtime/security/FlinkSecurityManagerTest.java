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

package org.apache.flink.runtime.security;

import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.runtime.UserSystemExitException;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.security.Permission;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.core.testutils.CommonTestUtils.assertThrows;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for {@code FlinkUserSecurityManager}. */
public class FlinkSecurityManagerTest extends TestLogger {

    private static final int TEST_EXIT_CODE = 123;
    SecurityManager originalSecurityManager;
    FlinkSecurityManager flinkSecurityManager;

    @Before
    public void setUp() {
        originalSecurityManager = System.getSecurityManager();
    }

    @After
    public void tearDown() {
        System.setSecurityManager(originalSecurityManager);
    }

    @Test(expected = UserSystemExitException.class)
    public void testThrowUserExit() {
        flinkSecurityManager =
                new FlinkSecurityManager(ClusterOptions.UserSystemExitMode.THROW, null);
        flinkSecurityManager.monitorUserSystemExit();
        flinkSecurityManager.checkExit(TEST_EXIT_CODE);
    }

    @Test
    public void testToggleUserExit() {
        flinkSecurityManager =
                new FlinkSecurityManager(ClusterOptions.UserSystemExitMode.THROW, null);
        flinkSecurityManager.checkExit(TEST_EXIT_CODE);
        flinkSecurityManager.monitorUserSystemExit();
        try {
            flinkSecurityManager.checkExit(TEST_EXIT_CODE);
            fail();
        } catch (UserSystemExitException ignored) {
        }
        flinkSecurityManager.unmonitorUserSystemExit();
        flinkSecurityManager.checkExit(TEST_EXIT_CODE);
    }

    @Test
    public void testPerThreadThrowUserExit() throws Exception {
        flinkSecurityManager =
                new FlinkSecurityManager(ClusterOptions.UserSystemExitMode.THROW, null);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        // Async thread test before enabling monitoring ensures it does not throw while prestarting
        // worker thread, which is to be unmonitored and tested after enabling monitoring enabled.
        CompletableFuture<Void> future =
                CompletableFuture.runAsync(
                        () -> flinkSecurityManager.checkExit(TEST_EXIT_CODE), executorService);
        future.get();
        flinkSecurityManager.monitorUserSystemExit();
        try {
            flinkSecurityManager.checkExit(TEST_EXIT_CODE);
            fail();
        } catch (UserSystemExitException ignored) {
        }
        // This threaded exit should be allowed as thread is not spawned while monitor is enabled.
        future =
                CompletableFuture.runAsync(
                        () -> flinkSecurityManager.checkExit(TEST_EXIT_CODE), executorService);
        future.get();
    }

    @Test
    public void testInheritedThrowUserExit() throws Exception {
        flinkSecurityManager =
                new FlinkSecurityManager(ClusterOptions.UserSystemExitMode.THROW, null);
        flinkSecurityManager.monitorUserSystemExit();
        try {
            flinkSecurityManager.checkExit(TEST_EXIT_CODE);
            fail();
        } catch (UserSystemExitException ignored) {
        }
        CheckedThread thread =
                new CheckedThread() {
                    @Override
                    public void go() {
                        try {
                            flinkSecurityManager.checkExit(TEST_EXIT_CODE);
                            fail();
                        } catch (UserSystemExitException ignored) {
                        } catch (Throwable t) {
                            fail();
                        }
                    }
                };
        thread.start();
        thread.sync();
    }

    @Test
    public void testLogUserExit() {
        // Log mode enables monitor but only logging allowing exit, hence not expecting exception.
        // NOTE - Do not specifically test warning logging.
        flinkSecurityManager =
                new FlinkSecurityManager(ClusterOptions.UserSystemExitMode.LOG, null);
        flinkSecurityManager.monitorUserSystemExit();
        flinkSecurityManager.checkExit(TEST_EXIT_CODE);
    }

    @Test
    public void testExitBehaviorChanged() {
        AtomicInteger exitStatus = new AtomicInteger(0);
        flinkSecurityManager =
                new FlinkSecurityManager(
                        ClusterOptions.UserSystemExitMode.DISABLED,
                        status -> exitStatus.set(status));
        flinkSecurityManager.checkExit(TEST_EXIT_CODE);
        assertThat(exitStatus.get(), is(TEST_EXIT_CODE));
    }

    @Test
    public void testExitBehaviorChangedWithExistingSecurityManager() {
        TestExitSecurityManager existingSecurityManager = new TestExitSecurityManager();
        System.setSecurityManager(existingSecurityManager);
        AtomicInteger customExitExecuted = new AtomicInteger(0);
        flinkSecurityManager =
                new FlinkSecurityManager(
                        ClusterOptions.UserSystemExitMode.DISABLED, customExitExecuted::set);

        flinkSecurityManager.checkExit(TEST_EXIT_CODE);
        assertThat(existingSecurityManager.getExitStatus(), is(TEST_EXIT_CODE));
        assertThat(customExitExecuted.get(), is(TEST_EXIT_CODE));
    }

    @Test(expected = UserSystemExitException.class)
    public void testExitBehaviorUnchangeOnThrowingUserExit() {
        flinkSecurityManager =
                new FlinkSecurityManager(ClusterOptions.UserSystemExitMode.THROW, status -> fail());
        flinkSecurityManager.monitorUserSystemExit();
        flinkSecurityManager.checkExit(TEST_EXIT_CODE);
    }

    @Test
    public void testDisabledConfiguration() {
        // Default case (no provided option) - allowing everything, so null security manager is
        // expected.
        Configuration configuration = new Configuration();
        flinkSecurityManager = FlinkSecurityManager.fromConfiguration(configuration);
        assertNull(flinkSecurityManager);

        // Disabled case (same as default)
        configuration.set(
                ClusterOptions.INTERCEPT_USER_SYSTEM_EXIT,
                ClusterOptions.UserSystemExitMode.DISABLED);
        flinkSecurityManager = FlinkSecurityManager.fromConfiguration(configuration);
        assertNull(flinkSecurityManager);

        // No halt (same as default)
        configuration.set(ClusterOptions.HALT_ON_SYSTEM_EXIT, false);
        flinkSecurityManager = FlinkSecurityManager.fromConfiguration(configuration);
        assertNull(flinkSecurityManager);
    }

    @Test
    public void testLogConfiguration() {
        // Enabled - log case (logging as warning but allowing exit)
        Configuration configuration = new Configuration();
        configuration.set(
                ClusterOptions.INTERCEPT_USER_SYSTEM_EXIT, ClusterOptions.UserSystemExitMode.LOG);
        flinkSecurityManager = FlinkSecurityManager.fromConfiguration(configuration);
        assertNotNull(flinkSecurityManager);
        assertFalse(flinkSecurityManager.userSystemExitMonitored());
        flinkSecurityManager.monitorUserSystemExit();
        assertTrue(flinkSecurityManager.userSystemExitMonitored());
        flinkSecurityManager.checkExit(TEST_EXIT_CODE);
        flinkSecurityManager.unmonitorUserSystemExit();
        assertFalse(flinkSecurityManager.userSystemExitMonitored());
    }

    @Test
    public void testThrowConfiguration() {
        // Enabled - throw case (disallowing by throwing exception)
        Configuration configuration = new Configuration();
        configuration.set(
                ClusterOptions.INTERCEPT_USER_SYSTEM_EXIT, ClusterOptions.UserSystemExitMode.THROW);
        flinkSecurityManager = FlinkSecurityManager.fromConfiguration(configuration);
        assertNotNull(flinkSecurityManager);
        assertFalse(flinkSecurityManager.userSystemExitMonitored());
        flinkSecurityManager.monitorUserSystemExit();
        assertTrue(flinkSecurityManager.userSystemExitMonitored());
        try {
            flinkSecurityManager.checkExit(TEST_EXIT_CODE);
            fail();
        } catch (UserSystemExitException ignored) {
        }
        flinkSecurityManager.unmonitorUserSystemExit();
        assertFalse(flinkSecurityManager.userSystemExitMonitored());

        // Test for disabled test to check if exit is still allowed (fromConfiguration gives null
        // since currently
        // there is only one option to have a valid security manager, so test with constructor).
        flinkSecurityManager =
                new FlinkSecurityManager(ClusterOptions.UserSystemExitMode.DISABLED, null);
        flinkSecurityManager.monitorUserSystemExit();
        assertTrue(flinkSecurityManager.userSystemExitMonitored());
        flinkSecurityManager.checkExit(TEST_EXIT_CODE);
    }

    @Test
    public void testHaltConfiguration() {
        // Halt as forceful shutdown replacing graceful system exit
        Configuration configuration = new Configuration();
        configuration.set(ClusterOptions.HALT_ON_SYSTEM_EXIT, true);
        flinkSecurityManager = FlinkSecurityManager.fromConfiguration(configuration);
        assertNotNull(flinkSecurityManager);
    }

    @Test(expected = NullPointerException.class)
    public void testInvalidConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set(ClusterOptions.INTERCEPT_USER_SYSTEM_EXIT, null);
        flinkSecurityManager = FlinkSecurityManager.fromConfiguration(configuration);
    }

    @Test
    public void testExistingSecurityManagerRespected() {
        // Don't set the following security manager directly to system, which makes test hang.
        SecurityManager originalSecurityManager =
                new SecurityManager() {
                    @Override
                    public void checkPermission(Permission perm) {
                        throw new SecurityException("not allowed");
                    }
                };
        flinkSecurityManager =
                new FlinkSecurityManager(
                        ClusterOptions.UserSystemExitMode.DISABLED,
                        status -> Assert.fail(),
                        originalSecurityManager);

        assertThrows(
                "not allowed",
                SecurityException.class,
                () -> {
                    flinkSecurityManager.checkExit(TEST_EXIT_CODE);
                    return null;
                });
    }

    @Test
    public void testRegistrationNotAllowedByExistingSecurityManager() {
        Configuration configuration = new Configuration();
        configuration.set(
                ClusterOptions.INTERCEPT_USER_SYSTEM_EXIT, ClusterOptions.UserSystemExitMode.THROW);

        System.setSecurityManager(
                new SecurityManager() {

                    private boolean fired;

                    @Override
                    public void checkPermission(Permission perm) {
                        if (!fired && perm.getName().equals("setSecurityManager")) {
                            try {
                                throw new SecurityException("not allowed");
                            } finally {
                                // Allow removing this manager again
                                fired = true;
                            }
                        }
                    }
                });

        assertThrows(
                "Could not register security manager",
                IllegalConfigurationException.class,
                () -> {
                    FlinkSecurityManager.setFromConfiguration(configuration);
                    return null;
                });
    }

    @Test(expected = UserSystemExitException.class)
    public void testMultiSecurityManagersWithSetFirstAndMonitored() {
        Configuration configuration = new Configuration();

        configuration.set(
                ClusterOptions.INTERCEPT_USER_SYSTEM_EXIT, ClusterOptions.UserSystemExitMode.THROW);
        configuration.set(ClusterOptions.HALT_ON_SYSTEM_EXIT, false);

        FlinkSecurityManager.setFromConfiguration(configuration);

        TestExitSecurityManager newSecurityManager = new TestExitSecurityManager();
        System.setSecurityManager(newSecurityManager);

        FlinkSecurityManager.monitorUserSystemExitForCurrentThread();
        newSecurityManager.checkExit(TEST_EXIT_CODE);
        assertThat(newSecurityManager.getExitStatus(), is(TEST_EXIT_CODE));
    }

    @Test(expected = UserSystemExitException.class)
    public void testMultiSecurityManagersWithSetLastAndMonitored() {
        Configuration configuration = new Configuration();

        configuration.set(
                ClusterOptions.INTERCEPT_USER_SYSTEM_EXIT, ClusterOptions.UserSystemExitMode.THROW);
        configuration.set(ClusterOptions.HALT_ON_SYSTEM_EXIT, false);

        TestExitSecurityManager oldSecurityManager = new TestExitSecurityManager();
        System.setSecurityManager(oldSecurityManager);

        FlinkSecurityManager.setFromConfiguration(configuration);

        FlinkSecurityManager.monitorUserSystemExitForCurrentThread();
        System.getSecurityManager().checkExit(TEST_EXIT_CODE);
        assertNull(oldSecurityManager.getExitStatus());
    }

    @Test
    public void testMultiSecurityManagersWithSetFirstAndUnmonitored() {
        Configuration configuration = new Configuration();

        configuration.set(
                ClusterOptions.INTERCEPT_USER_SYSTEM_EXIT, ClusterOptions.UserSystemExitMode.THROW);
        configuration.set(ClusterOptions.HALT_ON_SYSTEM_EXIT, false);

        FlinkSecurityManager.setFromConfiguration(configuration);

        TestExitSecurityManager newSecurityManager = new TestExitSecurityManager();
        System.setSecurityManager(newSecurityManager);

        newSecurityManager.checkExit(TEST_EXIT_CODE);
        assertThat(newSecurityManager.getExitStatus(), is(TEST_EXIT_CODE));
    }

    @Test
    public void testMultiSecurityManagersWithSetLastAndUnmonitored() {
        Configuration configuration = new Configuration();

        configuration.set(
                ClusterOptions.INTERCEPT_USER_SYSTEM_EXIT, ClusterOptions.UserSystemExitMode.THROW);
        configuration.set(ClusterOptions.HALT_ON_SYSTEM_EXIT, false);

        TestExitSecurityManager oldSecurityManager = new TestExitSecurityManager();
        System.setSecurityManager(oldSecurityManager);

        FlinkSecurityManager.setFromConfiguration(configuration);

        System.getSecurityManager().checkExit(TEST_EXIT_CODE);
        assertThat(oldSecurityManager.getExitStatus(), is(TEST_EXIT_CODE));
    }

    private class TestExitSecurityManager extends SecurityManager {
        private SecurityManager originalSecurityManager;
        private Integer exitStatus;

        public TestExitSecurityManager() {
            originalSecurityManager = System.getSecurityManager();
        }

        @Override
        public void checkExit(int status) {
            exitStatus = status;
            if (originalSecurityManager != null) {
                originalSecurityManager.checkExit(status);
            }
        }

        @Override
        public void checkPermission(Permission perm) {}

        public Integer getExitStatus() {
            return exitStatus;
        }
    }
}
