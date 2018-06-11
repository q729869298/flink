/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.heap;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.AbstractKeyGroupPartitionedSnapshot;
import org.apache.flink.runtime.state.AbstractKeyGroupPartitioner;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;

/**
 * This class represents the snapshot of a {@link CopyOnWriteStateTable} and has a role in operator state checkpointing. Besides
 * holding the {@link CopyOnWriteStateTable}s internal entries at the time of the snapshot, this class is also responsible for
 * preparing and writing the state in the process of checkpointing.
 * <p>
 * IMPORTANT: Please notice that snapshot integrity of entries in this class rely on proper copy-on-write semantics
 * through the {@link CopyOnWriteStateTable} that created the snapshot object, but all objects in this snapshot must be considered
 * as READ-ONLY!. The reason is that the objects held by this class may or may not be deep copies of original objects
 * that may still used in the {@link CopyOnWriteStateTable}. This depends for each entry on whether or not it was subject to
 * copy-on-write operations by the {@link CopyOnWriteStateTable}. Phrased differently: the {@link CopyOnWriteStateTable} provides
 * copy-on-write isolation for this snapshot, but this snapshot does not isolate modifications from the
 * {@link CopyOnWriteStateTable}!
 *
 * @param <K> type of key
 * @param <N> type of namespace
 * @param <S> type of state
 */
@Internal
public class CopyOnWriteStateTableSnapshot<K, N, S>
		extends AbstractStateTableSnapshot<K, N, S, CopyOnWriteStateTable<K, N, S>> {

	/**
	 * Version of the {@link CopyOnWriteStateTable} when this snapshot was created. This can be used to release the snapshot.
	 */
	private final int snapshotVersion;

	/**
	 * The state table entries, as by the time this snapshot was created. Objects in this array may or may not be deep
	 * copies of the current entries in the {@link CopyOnWriteStateTable} that created this snapshot. This depends for each entry
	 * on whether or not it was subject to copy-on-write operations by the {@link CopyOnWriteStateTable}.
	 */
	@Nonnull
	private final CopyOnWriteStateTable.StateTableEntry<K, N, S>[] snapshotData;

	/** The number of (non-null) entries in snapshotData. */
	@Nonnegative
	private final int numberOfEntriesInSnapshotData;

	/**
	 * A local duplicate of the table's key serializer.
	 */
	@Nonnull
	private final TypeSerializer<K> localKeySerializer;

	/**
	 * A local duplicate of the table's namespace serializer.
	 */
	@Nonnull
	private final TypeSerializer<N> localNamespaceSerializer;

	/**
	 * A local duplicate of the table's state serializer.
	 */
	@Nonnull
	private final TypeSerializer<S> localStateSerializer;

	/**
	 * Result of partitioning the snapshot by key-group. This is lazily created in the process of writing this snapshot
	 * to an output as part of checkpointing.
	 */
	@Nullable
	private PartitionedStateTableSnapshot partitionedStateTableSnapshot;

	/**
	 * Creates a new {@link CopyOnWriteStateTableSnapshot}.
	 *
	 * @param owningStateTable the {@link CopyOnWriteStateTable} for which this object represents a snapshot.
	 */
	CopyOnWriteStateTableSnapshot(CopyOnWriteStateTable<K, N, S> owningStateTable) {

		super(owningStateTable);
		this.snapshotData = owningStateTable.snapshotTableArrays();
		this.snapshotVersion = owningStateTable.getStateTableVersion();
		this.numberOfEntriesInSnapshotData = owningStateTable.size();


		// We create duplicates of the serializers for the async snapshot, because TypeSerializer
		// might be stateful and shared with the event processing thread.
		this.localKeySerializer = owningStateTable.keyContext.getKeySerializer().duplicate();
		this.localNamespaceSerializer = owningStateTable.metaInfo.getNamespaceSerializer().duplicate();
		this.localStateSerializer = owningStateTable.metaInfo.getStateSerializer().duplicate();

		this.partitionedStateTableSnapshot = null;
	}

	/**
	 * Returns the internal version of the {@link CopyOnWriteStateTable} when this snapshot was created. This value must be used to
	 * tell the {@link CopyOnWriteStateTable} when to release this snapshot.
	 */
	int getSnapshotVersion() {
		return snapshotVersion;
	}

	/**
	 * Partitions the snapshot data by key-group. The algorithm first builds a histogram for the distribution of keys
	 * into key-groups. Then, the histogram is accumulated to obtain the boundaries of each key-group in an array.
	 * Last, we use the accumulated counts as write position pointers for the key-group's bins when reordering the
	 * entries by key-group. This operation is lazily performed before the first writing of a key-group.
	 * <p>
	 * As a possible future optimization, we could perform the repartitioning in-place, using a scheme similar to the
	 * cuckoo cycles in cuckoo hashing. This can trade some performance for a smaller memory footprint.
	 */
	@Nonnull
	@SuppressWarnings("unchecked")
	@Override
	public KeyGroupPartitionedSnapshot partitionByKeyGroup() {

		if (partitionedStateTableSnapshot == null) {

			final InternalKeyContext<K> keyContext = owningStateTable.keyContext;
			final KeyGroupRange keyGroupRange = keyContext.getKeyGroupRange();
			final int numberOfKeyGroups = keyContext.getNumberOfKeyGroups();

			StateTableKeyGroupPartitioner<K, N, S> keyGroupPartitioner = new StateTableKeyGroupPartitioner<>(
				snapshotData,
				numberOfEntriesInSnapshotData,
				keyGroupRange,
				numberOfKeyGroups);

			partitionedStateTableSnapshot = new PartitionedStateTableSnapshot(keyGroupPartitioner.partitionByKeyGroup());
		}

		return partitionedStateTableSnapshot;
	}

	@Override
	public void release() {
		owningStateTable.releaseSnapshot(this);
	}

	/**
	 * Returns true iff the given state table is the owner of this snapshot object.
	 */
	boolean isOwner(CopyOnWriteStateTable<K, N, S> stateTable) {
		return stateTable == owningStateTable;
	}

	/**
	 * This class is the implementation of {@link AbstractKeyGroupPartitioner} for {@link CopyOnWriteStateTable}.
	 *
	 * @param <K> type of key.
	 * @param <N> type of namespace.
	 * @param <S> type of state value.
	 */
	@VisibleForTesting
	protected static final class StateTableKeyGroupPartitioner<K, N, S>
		extends AbstractKeyGroupPartitioner<CopyOnWriteStateTable.StateTableEntry<K, N, S>> {

		@Nonnull
		private final CopyOnWriteStateTable.StateTableEntry<K, N, S>[] snapshotData;

		@Nonnull
		private final CopyOnWriteStateTable.StateTableEntry<K, N, S>[] flattenedData;

		@SuppressWarnings("unchecked")
		StateTableKeyGroupPartitioner(
			@Nonnull CopyOnWriteStateTable.StateTableEntry<K, N, S>[] snapshotData,
			@Nonnegative int stateTableSize,
			@Nonnull KeyGroupRange keyGroupRange,
			@Nonnegative int totalKeyGroups) {

			super(stateTableSize, keyGroupRange, totalKeyGroups);
			this.snapshotData = snapshotData;
			this.flattenedData = new CopyOnWriteStateTable.StateTableEntry[numberOfElements];
		}

		@Override
		protected void reportAllElementKeyGroups() {
			// In this step we i) 'flatten' the linked list of entries to a second array and ii) report key-groups.
			int flattenIndex = 0;
			for (CopyOnWriteStateTable.StateTableEntry<K, N, S> entry : snapshotData) {
				while (null != entry) {
					final int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(entry.key, totalKeyGroups);
					reportKeyGroupOfElementAtIndex(flattenIndex, keyGroup);
					flattenedData[flattenIndex++] = entry;
					entry = entry.next;
				}
			}
		}

		@Nonnull
		@Override
		protected Object extractKeyFromElement(CopyOnWriteStateTable.StateTableEntry<K, N, S> element) {
			return element.getKey();
		}

		@Nonnull
		@Override
		protected CopyOnWriteStateTable.StateTableEntry<K, N, S>[] getPartitioningInput() {
			return flattenedData;
		}

		@Nonnull
		@Override
		protected CopyOnWriteStateTable.StateTableEntry<K, N, S>[] getPartitioningOutput() {
			return snapshotData;
		}
	}

	/**
	 * This class represents a {@link org.apache.flink.runtime.state.StateSnapshot.KeyGroupPartitionedSnapshot} for
	 * {@link CopyOnWriteStateTable}.
	 */
	private final class PartitionedStateTableSnapshot
		extends AbstractKeyGroupPartitionedSnapshot<CopyOnWriteStateTable.StateTableEntry<K, N, S>> {

		public PartitionedStateTableSnapshot(
			@Nonnull AbstractKeyGroupPartitioner.PartitioningResult<CopyOnWriteStateTable.StateTableEntry<K, N, S>> partitioningResult) {
			super(partitioningResult);
		}

		@Override
		protected void writeElement(
			@Nonnull CopyOnWriteStateTable.StateTableEntry<K, N, S> element,
			@Nonnull DataOutputView dov) throws IOException {
			localNamespaceSerializer.serialize(element.namespace, dov);
			localKeySerializer.serialize(element.key, dov);
			localStateSerializer.serialize(element.state, dov);
		}
	}
}
