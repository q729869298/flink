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

package org.apache.flink.runtime.security.token.hadoop;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.security.token.DelegationTokenListener;
import org.apache.flink.runtime.security.token.DelegationTokenManager;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.apache.hadoop.security.Credentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.apache.flink.configuration.SecurityOptions.KERBEROS_TOKENS_RENEWAL_RETRY_BACKOFF;
import static org.apache.flink.configuration.SecurityOptions.KERBEROS_TOKENS_RENEWAL_TIME_RATIO;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Manager for delegation tokens in a Flink cluster.
 *
 * <p>When delegation token renewal is enabled, this manager will make sure long-running apps can
 * run without interruption while accessing secured services. It periodically logs in to the KDC
 * with user-provided credentials, and contacts all the configured secure services to obtain
 * delegation tokens to be distributed to the rest of the application.
 */
@Internal
public class KerberosDelegationTokenManager implements DelegationTokenManager {

    private static final Logger LOG = LoggerFactory.getLogger(KerberosDelegationTokenManager.class);

    private final Configuration configuration;

    private final double tokensRenewalTimeRatio;

    private final long renewalRetryBackoffPeriod;

    private final KerberosLoginProvider kerberosLoginProvider;

    @VisibleForTesting final Map<String, HadoopDelegationTokenProvider> delegationTokenProviders;

    @Nullable private final ScheduledExecutor scheduledExecutor;

    @Nullable private final ExecutorService ioExecutor;

    private final Object tokensUpdateFutureLock = new Object();

    @GuardedBy("tokensUpdateFutureLock")
    @Nullable
    private ScheduledFuture<?> tokensUpdateFuture;

    @Nullable private DelegationTokenListener delegationTokenListener;

    public KerberosDelegationTokenManager(
            Configuration configuration,
            @Nullable ScheduledExecutor scheduledExecutor,
            @Nullable ExecutorService ioExecutor) {
        this.configuration = checkNotNull(configuration, "Flink configuration must not be null");
        this.tokensRenewalTimeRatio = configuration.get(KERBEROS_TOKENS_RENEWAL_TIME_RATIO);
        this.renewalRetryBackoffPeriod =
                configuration.get(KERBEROS_TOKENS_RENEWAL_RETRY_BACKOFF).toMillis();
        this.kerberosLoginProvider = new KerberosLoginProvider(configuration);
        this.delegationTokenProviders = loadProviders();
        this.scheduledExecutor = scheduledExecutor;
        this.ioExecutor = ioExecutor;
    }

    private Map<String, HadoopDelegationTokenProvider> loadProviders() {
        LOG.info("Loading delegation token providers");

        ServiceLoader<HadoopDelegationTokenProvider> serviceLoader =
                ServiceLoader.load(HadoopDelegationTokenProvider.class);

        Map<String, HadoopDelegationTokenProvider> providers = new HashMap<>();
        for (HadoopDelegationTokenProvider provider : serviceLoader) {
            try {
                if (isProviderEnabled(provider.serviceName())) {
                    provider.init(configuration);
                    LOG.info(
                            "Delegation token provider {} loaded and initialized",
                            provider.serviceName());
                    providers.put(provider.serviceName(), provider);
                } else {
                    LOG.info(
                            "Delegation token provider {} is disabled so not loaded",
                            provider.serviceName());
                }
            } catch (Exception | NoClassDefFoundError e) {
                LOG.error(
                        "Failed to initialize delegation token provider {}",
                        provider.serviceName(),
                        e);
                if (!(e instanceof NoClassDefFoundError)) {
                    throw new FlinkRuntimeException(e);
                }
            }
        }

        LOG.info("Delegation token providers loaded successfully");

        return providers;
    }

    @VisibleForTesting
    boolean isProviderEnabled(String serviceName) {
        return configuration.getBoolean(
                String.format("security.kerberos.token.provider.%s.enabled", serviceName), true);
    }

    @VisibleForTesting
    boolean isProviderLoaded(String serviceName) {
        return delegationTokenProviders.containsKey(serviceName);
    }

    /**
     * Obtains new tokens in a one-time fashion and leaves it up to the caller to distribute them.
     */
    @Override
    public void obtainDelegationTokens(Credentials credentials) throws Exception {
        LOG.info("Obtaining delegation tokens");
        obtainDelegationTokensAndGetNextRenewal(credentials);
        LOG.info("Delegation tokens obtained successfully");
    }

    protected Optional<Long> obtainDelegationTokensAndGetNextRenewal(Credentials credentials) {
        Optional<Long> nextRenewal =
                delegationTokenProviders.values().stream()
                        .map(
                                provider -> {
                                    try {
                                        Optional<Long> nr = Optional.empty();
                                        if (provider.delegationTokensRequired()) {
                                            LOG.debug(
                                                    "Obtaining delegation token for service {}",
                                                    provider.serviceName());
                                            nr = provider.obtainDelegationTokens(credentials);
                                            LOG.debug(
                                                    "Obtained delegation token for service {} successfully",
                                                    provider.serviceName());
                                        } else {
                                            LOG.debug(
                                                    "Service {} does not need to obtain delegation token",
                                                    provider.serviceName());
                                        }
                                        return nr;
                                    } catch (Exception e) {
                                        LOG.error(
                                                "Failed to obtain delegation token for provider {}",
                                                provider.serviceName(),
                                                e);
                                        throw new FlinkRuntimeException(e);
                                    }
                                })
                        .flatMap(nr -> nr.map(Stream::of).orElseGet(Stream::empty))
                        .min(Long::compare);

        HadoopDelegationTokenUpdater.dumpAllTokens(credentials);

        return nextRenewal;
    }

    /**
     * Creates a re-occurring task which obtains new tokens and automatically distributes them to
     * task managers.
     */
    @Override
    public void start(DelegationTokenListener delegationTokenListener) throws Exception {
        checkNotNull(scheduledExecutor, "Scheduled executor must not be null");
        checkNotNull(ioExecutor, "IO executor must not be null");
        this.delegationTokenListener =
                checkNotNull(delegationTokenListener, "Delegation token listener must not be null");
        synchronized (tokensUpdateFutureLock) {
            checkState(tokensUpdateFuture == null, "Manager is already started");
        }

        if (!kerberosLoginProvider.isLoginPossible()) {
            LOG.info("Renewal is NOT possible, skipping to start renewal task");
            return;
        }

        startTokensUpdate();
    }

    @VisibleForTesting
    void startTokensUpdate() {
        try {
            LOG.info("Starting tokens update task");
            Credentials credentials = new Credentials();
            Optional<Long> nextRenewal = obtainDelegationTokensAndGetNextRenewal(credentials);

            if (credentials.numberOfTokens() > 0) {
                byte[] credentialsBytes = HadoopDelegationTokenConverter.serialize(credentials);

                HadoopDelegationTokenUpdater.addCurrentUserCredentials(credentialsBytes);

                LOG.info("Notifying listener about new tokens");
                checkNotNull(delegationTokenListener, "Listener must not be null");
                delegationTokenListener.onNewTokensObtained(credentialsBytes);
                LOG.info("Listener notified successfully");
            } else {
                LOG.warn("No tokens obtained so skipping listener notification");
            }

            if (nextRenewal.isPresent()) {
                long renewalDelay =
                        calculateRenewalDelay(Clock.systemDefaultZone(), nextRenewal.get());
                synchronized (tokensUpdateFutureLock) {
                    tokensUpdateFuture =
                            scheduledExecutor.schedule(
                                    () -> ioExecutor.execute(this::startTokensUpdate),
                                    renewalDelay,
                                    TimeUnit.MILLISECONDS);
                }
                LOG.info("Tokens update task started with {} ms delay", renewalDelay);
            } else {
                LOG.warn(
                        "Tokens update task not started because either no tokens obtained or none of the tokens specified its renewal date");
            }
        } catch (InterruptedException e) {
            // Ignore, may happen if shutting down.
            LOG.debug("Interrupted", e);
        } catch (Exception e) {
            synchronized (tokensUpdateFutureLock) {
                tokensUpdateFuture =
                        scheduledExecutor.schedule(
                                () -> ioExecutor.execute(this::startTokensUpdate),
                                renewalRetryBackoffPeriod,
                                TimeUnit.MILLISECONDS);
            }
            LOG.warn(
                    "Failed to update tokens, will try again in {} ms",
                    renewalRetryBackoffPeriod,
                    e);
        }
    }

    @VisibleForTesting
    void stopTokensUpdate() {
        synchronized (tokensUpdateFutureLock) {
            if (tokensUpdateFuture != null) {
                tokensUpdateFuture.cancel(true);
                tokensUpdateFuture = null;
            }
        }
    }

    @VisibleForTesting
    long calculateRenewalDelay(Clock clock, long nextRenewal) {
        long now = clock.millis();
        long renewalDelay = Math.round(tokensRenewalTimeRatio * (nextRenewal - now));
        LOG.debug(
                "Calculated delay on renewal is {}, based on next renewal {} and the ratio {}, and current time {}",
                renewalDelay,
                nextRenewal,
                tokensRenewalTimeRatio,
                now);
        return renewalDelay;
    }

    /** Stops re-occurring token obtain task. */
    @Override
    public void stop() {
        LOG.info("Stopping credential renewal");

        stopTokensUpdate();

        LOG.info("Stopped credential renewal");
    }
}
