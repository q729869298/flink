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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/** Tests for {@link TieredStorageMemoryManagerImpl}. */
class TieredStorageMemoryManagerImplTest {

    private static final int NETWORK_BUFFER_SIZE = 1024;

    private static final int NUM_TOTAL_BUFFERS = 1000;

    private static final float NUM_BUFFERS_TRIGGER_FLUSH_RATIO = 0.6f;

    private NetworkBufferPool globalPool;

    private List<BufferBuilder> requestedBuffers;

    private CompletableFuture<Void> hasReclaimBufferFinished;

    private int reclaimBufferCounter;

    @BeforeEach
    void before() {
        globalPool = new NetworkBufferPool(NUM_TOTAL_BUFFERS, NETWORK_BUFFER_SIZE);
        requestedBuffers = new ArrayList<>();
        hasReclaimBufferFinished = new CompletableFuture<>();
        reclaimBufferCounter = 0;
    }

    @AfterEach
    void after() {
        globalPool.destroy();
    }

    @Test
    void testRequestAndRecycleBuffers() throws IOException {
        int numBuffers = 1;

        BufferPool bufferPool = globalPool.createBufferPool(numBuffers, numBuffers);
        TieredStorageMemoryManagerImpl storageMemoryManager =
                createStorageMemoryManager(
                        bufferPool,
                        Collections.singletonList(new TieredStorageMemorySpec(this, 0)));
        assertThat(bufferPool.bestEffortGetNumOfUsedBuffers()).isZero();
        BufferBuilder builder = storageMemoryManager.requestBufferBlocking(this);
        assertThat(bufferPool.bestEffortGetNumOfUsedBuffers()).isOne();
        recycleBufferBuilder(builder);
        assertThat(bufferPool.bestEffortGetNumOfUsedBuffers()).isZero();
        storageMemoryManager.release();
    }

    @Test
    void testGetMaxNonReclaimableBuffers() throws IOException {
        int numBuffers = 10;
        int numExclusive = 5;

        TieredStorageMemoryManagerImpl storageMemoryManager =
                createStorageMemoryManager(
                        numBuffers,
                        Collections.singletonList(new TieredStorageMemorySpec(this, numExclusive)));

        List<BufferBuilder> requestedBuffers = new ArrayList<>();
        for (int i = 1; i <= numBuffers; i++) {
            requestedBuffers.add(storageMemoryManager.requestBufferBlocking(this));
            assertThat(storageMemoryManager.getMaxNonReclaimableBuffers(this))
                    .isEqualTo(numBuffers);
            int numExpectedAvailable = numBuffers - i;
            assertThat(
                            storageMemoryManager.getMaxNonReclaimableBuffers(this)
                                    - storageMemoryManager.numOwnerRequestedBuffer(this))
                    .isEqualTo(numExpectedAvailable);
        }

        requestedBuffers.forEach(TieredStorageMemoryManagerImplTest::recycleBufferBuilder);
        storageMemoryManager.release();
    }

    @Test
    void testNumMaxNonReclaimableWhenOtherUseLessThanGuaranteed() throws IOException {
        int numBuffers = 10;
        int numExclusive = 4;

        List<TieredStorageMemorySpec> storageMemorySpecs = new ArrayList<>();
        Object otherUser = new Object();
        storageMemorySpecs.add(new TieredStorageMemorySpec(this, 0));
        storageMemorySpecs.add(new TieredStorageMemorySpec(otherUser, numExclusive));
        TieredStorageMemoryManagerImpl storageMemoryManager =
                createStorageMemoryManager(numBuffers, storageMemorySpecs);

        List<BufferBuilder> requestedBuffers = new ArrayList<>();
        assertThat(storageMemoryManager.getMaxNonReclaimableBuffers(this))
                .isEqualTo(numBuffers - numExclusive);
        for (int i = 1; i <= numBuffers; i++) {
            requestedBuffers.add(storageMemoryManager.requestBufferBlocking(this));
            assertThat(storageMemoryManager.getMaxNonReclaimableBuffers(this))
                    .isEqualTo(numBuffers - numExclusive);
            int numExpectedAvailable = numBuffers - i - numExclusive;
            assertThat(
                            storageMemoryManager.getMaxNonReclaimableBuffers(this)
                                    - storageMemoryManager.numOwnerRequestedBuffer(this))
                    .isEqualTo(numExpectedAvailable);
        }

        requestedBuffers.forEach(TieredStorageMemoryManagerImplTest::recycleBufferBuilder);
        storageMemoryManager.release();
    }

    @Test
    void testNumMaxNonReclaimableWhenOtherUseMoreThanGuaranteed() throws IOException {
        int numBuffers = 10;
        int numExclusive = 4;

        List<TieredStorageMemorySpec> storageMemorySpecs = new ArrayList<>();
        Object otherUser = new Object();
        storageMemorySpecs.add(new TieredStorageMemorySpec(this, 0));
        storageMemorySpecs.add(new TieredStorageMemorySpec(otherUser, numExclusive));
        TieredStorageMemoryManagerImpl storageMemoryManager =
                createStorageMemoryManager(numBuffers, storageMemorySpecs);

        int numRequestedByOtherUser = numExclusive + 1;
        for (int i = 0; i < numRequestedByOtherUser; i++) {
            requestedBuffers.add(storageMemoryManager.requestBufferBlocking(otherUser));
        }

        assertThat(storageMemoryManager.getMaxNonReclaimableBuffers(this))
                .isEqualTo(numBuffers - numRequestedByOtherUser);
        for (int i = 1; i <= numBuffers - numRequestedByOtherUser; i++) {
            requestedBuffers.add(storageMemoryManager.requestBufferBlocking(this));
            assertThat(storageMemoryManager.getMaxNonReclaimableBuffers(this))
                    .isEqualTo(numBuffers - numRequestedByOtherUser);
            int numExpectedAvailable = numBuffers - i - numRequestedByOtherUser;
            assertThat(
                            storageMemoryManager.getMaxNonReclaimableBuffers(this)
                                    - storageMemoryManager.numOwnerRequestedBuffer(this))
                    .isEqualTo(numExpectedAvailable);
        }
        assertThat(storageMemoryManager.numOwnerRequestedBuffer(this))
                .isEqualTo(numBuffers - numRequestedByOtherUser);
        assertThat(storageMemoryManager.numOwnerRequestedBuffer(otherUser))
                .isEqualTo(numRequestedByOtherUser);

        requestedBuffers.forEach(TieredStorageMemoryManagerImplTest::recycleBufferBuilder);
        storageMemoryManager.release();
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testTriggerReclaimBuffers() throws IOException {
        int numBuffers = 5;

        TieredStorageMemoryManagerImpl storageMemoryManager =
                createStorageMemoryManager(
                        numBuffers,
                        Collections.singletonList(new TieredStorageMemorySpec(this, 0)));
        storageMemoryManager.listenBufferReclaimRequest(this::onBufferReclaimRequest);

        int numBuffersBeforeTriggerReclaim = (int) (numBuffers * NUM_BUFFERS_TRIGGER_FLUSH_RATIO);
        for (int i = 0; i < numBuffersBeforeTriggerReclaim; i++) {
            requestedBuffers.add(storageMemoryManager.requestBufferBlocking(this));
        }

        assertThat(reclaimBufferCounter).isZero();
        assertThat(requestedBuffers).hasSize(numBuffersBeforeTriggerReclaim);
        requestedBuffers.add(storageMemoryManager.requestBufferBlocking(this));
        assertThatFuture(hasReclaimBufferFinished).eventuallySucceeds();
        assertThat(reclaimBufferCounter).isOne();
        assertThat(requestedBuffers).hasSize(1);
        recycleRequestedBuffers();

        storageMemoryManager.release();
    }

    @Test
    void testTransferBufferOwnership() throws IOException {
        TieredStorageMemoryManagerImpl memoryManager =
                createStorageMemoryManager(
                        1, Collections.singletonList(new TieredStorageMemorySpec(this, 0)));
        BufferBuilder bufferBuilder = memoryManager.requestBufferBlocking(this);
        assertThat(memoryManager.numOwnerRequestedBuffer(this)).isOne();

        BufferConsumer bufferConsumer = bufferBuilder.createBufferConsumerFromBeginning();
        Buffer buffer = bufferConsumer.build();
        bufferBuilder.close();
        bufferConsumer.close();
        Object newOwner = new Object();
        memoryManager.transferBufferOwnership(this, newOwner, buffer);
        assertThat(memoryManager.numOwnerRequestedBuffer(this)).isZero();
        assertThat(memoryManager.numOwnerRequestedBuffer(newOwner)).isOne();
        buffer.recycleBuffer();
        assertThat(memoryManager.numOwnerRequestedBuffer(newOwner)).isZero();
    }

    @Test
    void testCanNotTransferOwnershipForEvent() throws IOException {
        TieredStorageMemoryManagerImpl memoryManager =
                createStorageMemoryManager(
                        1, Collections.singletonList(new TieredStorageMemorySpec(this, 0)));
        BufferConsumer bufferConsumer =
                BufferBuilderTestUtils.createEventBufferConsumer(1, Buffer.DataType.EVENT_BUFFER);
        Buffer buffer = bufferConsumer.build();
        bufferConsumer.close();
        assertThatThrownBy(() -> memoryManager.transferBufferOwnership(this, new Object(), buffer))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testReleaseBeforeRecyclingBuffers() throws IOException {
        int numBuffers = 5;

        TieredStorageMemoryManagerImpl storageMemoryManager =
                createStorageMemoryManager(
                        numBuffers,
                        Collections.singletonList(new TieredStorageMemorySpec(this, 0)));
        requestedBuffers.add(storageMemoryManager.requestBufferBlocking(this));
        assertThatThrownBy(storageMemoryManager::release).isInstanceOf(IllegalStateException.class);
        recycleRequestedBuffers();
        storageMemoryManager.release();
    }

    @Test
    void testLeakingBuffers() throws IOException {
        int numBuffers = 10;

        TieredStorageMemoryManagerImpl storageMemoryManager =
                createStorageMemoryManager(
                        numBuffers,
                        Collections.singletonList(new TieredStorageMemorySpec(this, 0)));

        requestedBuffers.add(storageMemoryManager.requestBufferBlocking(this));
        assertThatThrownBy(storageMemoryManager::release)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Leaking buffers");
        recycleRequestedBuffers();
        storageMemoryManager.release();
    }

    public void onBufferReclaimRequest() {
        reclaimBufferCounter++;
        recycleRequestedBuffers();
        hasReclaimBufferFinished.complete(null);
    }

    private void recycleRequestedBuffers() {
        requestedBuffers.forEach(
                builder -> {
                    BufferConsumer bufferConsumer = builder.createBufferConsumer();
                    Buffer buffer = bufferConsumer.build();
                    buffer.getRecycler().recycle(buffer.getMemorySegment());
                });
        requestedBuffers.clear();
    }

    private TieredStorageMemoryManagerImpl createStorageMemoryManager(
            int numBuffersInBufferPool, List<TieredStorageMemorySpec> storageMemorySpecs)
            throws IOException {
        BufferPool bufferPool =
                globalPool.createBufferPool(numBuffersInBufferPool, numBuffersInBufferPool);
        return createStorageMemoryManager(bufferPool, storageMemorySpecs);
    }

    private TieredStorageMemoryManagerImpl createStorageMemoryManager(
            BufferPool bufferPool, List<TieredStorageMemorySpec> storageMemorySpecs) {
        TieredStorageMemoryManagerImpl storageProducerMemoryManager =
                new TieredStorageMemoryManagerImpl(NUM_BUFFERS_TRIGGER_FLUSH_RATIO, true);
        storageProducerMemoryManager.setup(bufferPool, storageMemorySpecs);
        return storageProducerMemoryManager;
    }

    private static void recycleBufferBuilder(BufferBuilder bufferBuilder) {
        BufferConsumer bufferConsumer = bufferBuilder.createBufferConsumer();
        Buffer buffer = bufferConsumer.build();
        NetworkBuffer networkBuffer =
                new NetworkBuffer(
                        buffer.getMemorySegment(), buffer.getRecycler(), buffer.getDataType());
        networkBuffer.recycleBuffer();
    }
}
