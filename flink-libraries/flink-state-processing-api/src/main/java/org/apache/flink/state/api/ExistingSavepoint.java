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

package org.apache.flink.state.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.state.api.input.BroadcastStateInputFormat;
import org.apache.flink.state.api.input.KeyedStateInputFormat;
import org.apache.flink.state.api.input.ListStateInputFormat;
import org.apache.flink.state.api.input.UnionStateInputFormat;
import org.apache.flink.state.api.runtime.OperatorIDGenerator;
import org.apache.flink.state.api.runtime.metadata.SavepointMetadata;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An existing savepoint.
 */
@PublicEvolving
@SuppressWarnings("WeakerAccess")
public class ExistingSavepoint extends WritableSavepoint<ExistingSavepoint> {
	private final ExecutionEnvironment env;

	private final SavepointMetadata metadata;

	private final StateBackend stateBackend;

	ExistingSavepoint(ExecutionEnvironment env, SavepointMetadata metadata, StateBackend stateBackend) throws IOException {
		Preconditions.checkNotNull(env, "The execution environment must not be null");
		Preconditions.checkNotNull(metadata, "The savepoint metadata must not be null");
		Preconditions.checkNotNull(stateBackend, "The state backend must not be null");

		this.env = env;
		this.metadata = metadata;
		this.stateBackend = stateBackend;
	}

	/**
	 * Read operator {@code ListState} from a {@code Savepoint}.
	 * @param uid The uid of the operator.
	 * @param name The (unique) name for the state.
	 * @param typeInfo The type of the elements in the state.
	 * @param <T> The type of the values that are in the list state.
	 * @return A {@code DataSet} representing the elements in state.
	 * @throws IOException If the savepoint path is invalid or the uid does not exist.
	 */
	public <T> DataSet<T> readListState(String uid, String name, TypeInformation<T> typeInfo) throws IOException {
		OperatorState operatorState = metadata.getOperatorState(uid);
		ListStateDescriptor<T> descriptor = new ListStateDescriptor<>(name, typeInfo);
		ListStateInputFormat<T> inputFormat = new ListStateInputFormat<>(operatorState, descriptor);
		return env.createInput(inputFormat, typeInfo);
	}

	/**
	 * Read operator {@code ListState} from a {@code Savepoint} when a
	 * custom serializer was used; e.g., a different serializer than the
	 * one returned by {@code TypeInformation#createSerializer}.
	 * @param uid The uid of the operator.
	 * @param name The (unique) name for the state.
	 * @param typeInfo The type of the elements in the state.
	 * @param serializer The serializer used to write the elements into state.
	 * @param <T> The type of the values that are in the list state.
	 * @return A {@code DataSet} representing the elements in state.
	 * @throws IOException If the savepoint path is invalid or the uid does not exist.
	 */
	public <T> DataSet<T> readListState(
		String uid,
		String name,
		TypeInformation<T> typeInfo,
		TypeSerializer<T> serializer) throws IOException {

		OperatorState operatorState = metadata.getOperatorState(uid);
		ListStateDescriptor<T> descriptor = new ListStateDescriptor<>(name, serializer);
		ListStateInputFormat<T> inputFormat = new ListStateInputFormat<>(operatorState, descriptor);
		return env.createInput(inputFormat, typeInfo);
	}

	/**
	 * Read operator {@code UnionState} from a {@code Savepoint}.
	 * @param uid The uid of the operator.
	 * @param name The (unique) name for the state.
	 * @param typeInfo The type of the elements in the state.
	 * @param <T> The type of the values that are in the union state.
	 * @return A {@code DataSet} representing the elements in state.
	 * @throws IOException If the savepoint path is invalid or the uid does not exist.
	 */
	public <T> DataSet<T> readUnionState(String uid, String name, TypeInformation<T> typeInfo) throws IOException {
		OperatorState operatorState = metadata.getOperatorState(uid);
		ListStateDescriptor<T> descriptor = new ListStateDescriptor<>(name, typeInfo);
		UnionStateInputFormat<T> inputFormat = new UnionStateInputFormat<>(operatorState, descriptor);
		return env.createInput(inputFormat, typeInfo);
	}

	/**
	 * Read operator {@code UnionState} from a {@code Savepoint} when a
	 * custom serializer was used; e.g., a different serializer than the
	 * one returned by {@code TypeInformation#createSerializer}.
	 * @param uid The uid of the operator.
	 * @param name The (unique) name for the state.
	 * @param typeInfo The type of the elements in the state.
	 * @param serializer The serializer used to write the elements into state.
	 * @param <T> The type of the values that are in the union state.
	 * @return A {@code DataSet} representing the elements in state.
	 * @throws IOException If the savepoint path is invalid or the uid does not exist.
	 */
	public <T> DataSet<T> readUnionState(
		String uid,
		String name,
		TypeInformation<T> typeInfo,
		TypeSerializer<T> serializer) throws IOException {

		OperatorState operatorState = metadata.getOperatorState(uid);
		ListStateDescriptor<T> descriptor = new ListStateDescriptor<>(name, serializer);
		UnionStateInputFormat<T> inputFormat = new UnionStateInputFormat<>(operatorState, descriptor);
		return env.createInput(inputFormat, typeInfo);
	}

	/**
	 * Read operator {@code BroadcastState} from a {@code Savepoint}.
	 * @param uid The uid of the operator.
	 * @param name The (unique) name for the state.
	 * @param keyTypeInfo The type information for the keys in the state.
	 * @param valueTypeInfo The type information for the values in the state.
	 * @param <K> The type of keys in state.
	 * @param <V> The type of values in state.
	 * @return A {@code DataSet} of key-value pairs from state.
	 * @throws IOException If the savepoint does not contain the specified uid.
	 */
	public <K, V> DataSet<Tuple2<K, V>> readBroadcastState(
		String uid,
		String name,
		TypeInformation<K> keyTypeInfo,
		TypeInformation<V> valueTypeInfo) throws IOException {

		OperatorState operatorState = metadata.getOperatorState(uid);
		MapStateDescriptor<K, V> descriptor = new MapStateDescriptor<>(name, keyTypeInfo, valueTypeInfo);
		BroadcastStateInputFormat<K, V> inputFormat = new BroadcastStateInputFormat<>(operatorState, descriptor);
		return env.createInput(inputFormat, new TupleTypeInfo<>(keyTypeInfo, valueTypeInfo));
	}

	/**
	 * Read operator {@code BroadcastState} from a {@code Savepoint}
	 * when a custom serializer was used; e.g., a different serializer
	 * than the one returned by {@code TypeInformation#createSerializer}.
	 * @param uid The uid of the operator.
	 * @param name The (unique) name for the state.
	 * @param keyTypeInfo The type information for the keys in the state.
	 * @param valueTypeInfo The type information for the values in the state.
	 * @param keySerializer The type serializer used to write keys into the state.
	 * @param valueSerializer The type serializer used to write values into the state.
	 * @param <K> The type of keys in state.
	 * @param <V> The type of values in state.
	 * @return A {@code DataSet} of key-value pairs from state.
	 * @throws IOException If the savepoint path is invalid or the uid does not exist.
	 */
	public <K, V> DataSet<Tuple2<K, V>> readBroadcastState(
		String uid,
		String name,
		TypeInformation<K> keyTypeInfo,
		TypeInformation<V> valueTypeInfo,
		TypeSerializer<K> keySerializer,
		TypeSerializer<V> valueSerializer) throws IOException {

		OperatorState operatorState = metadata.getOperatorState(uid);
		MapStateDescriptor<K, V> descriptor = new MapStateDescriptor<>(name, keySerializer, valueSerializer);
		BroadcastStateInputFormat<K, V> inputFormat = new BroadcastStateInputFormat<>(operatorState, descriptor);
		return env.createInput(inputFormat, new TupleTypeInfo<>(keyTypeInfo, valueTypeInfo));
	}

	/*
	 * Read keyed state from an operator in a {@code Savepoint}.
	 * @param uid The uid of the operator.
	 * @param function The {@link KeyedStateReaderFunction} that is called for each key in state.
	 * @param <K> The type of the key in state.
	 * @param <OUT> The output type of the transform function.
	 * @return A {@code DataSet} of objects read from keyed state.
	 */
	public <K, OUT> DataSet<OUT> readKeyedState(String uid, KeyedStateReaderFunction<K, OUT> function) throws IOException {

		TypeInformation<K> keyTypeInfo;
		TypeInformation<OUT> outType;

		try {
			keyTypeInfo = TypeExtractor.createTypeInfo(
				KeyedStateReaderFunction.class,
				function.getClass(), 0, null, null);
		} catch (InvalidTypesException e) {
			throw new InvalidProgramException(
				"The key type of the KeyedStateReaderFunction could not be automatically determined. Please use " +
					"Savepoint#readKeyedState(String, KeyedStateReaderFunction, TypeInformation, TypeInformation) instead.", e);
		}

		try {
			outType = TypeExtractor.getUnaryOperatorReturnType(
				function,
				KeyedStateReaderFunction.class,
				0,
				1,
				TypeExtractor.NO_INDEX,
				keyTypeInfo,
				Utils.getCallLocationName(),
				false);
		} catch (InvalidTypesException e) {
			throw new InvalidProgramException(
				"The output type of the KeyedStateReaderFunction could not be automatically determined. Please use " +
					"Savepoint#readKeyedState(String, KeyedStateReaderFunction, TypeInformation, TypeInformation) instead.", e);
		}

		return readKeyedState(uid, function, keyTypeInfo, outType);
	}

	/**
	 * Read keyed state from an operator in a {@code Savepoint}.
	 * @param uid The uid of the operator.
	 * @param function The {@link KeyedStateReaderFunction} that is called for each key in state.
	 * @param keyTypeInfo The type information of the key in state.
	 * @param outTypeInfo The type information of the output of the transform reader function.
	 * @param <K> The type of the key in state.
	 * @param <OUT> The output type of the transform function.
	 * @return A {@code DataSet} of objects read from keyed state.
	 */
	public <K, OUT> DataSet<OUT> readKeyedState(
		String uid,
		KeyedStateReaderFunction<K, OUT> function,
		TypeInformation<K> keyTypeInfo,
		TypeInformation<OUT> outTypeInfo) throws IOException {

		OperatorState operatorState = metadata.getOperatorState(uid);
		KeyedStateInputFormat<K, OUT> inputFormat = new KeyedStateInputFormat<>(
			operatorState,
			stateBackend,
			keyTypeInfo,
			function);

		return env.createInput(inputFormat, outTypeInfo);
	}

	@Override
	public void write(String path) {
		Path savepointPath = new Path(path);

		Set<OperatorID> droppedIDs = droppedOperators
			.stream()
			.map(OperatorIDGenerator::fromUid)
			.collect(Collectors.toSet());

		List<OperatorState> remainingOperators = metadata
			.getOperatorStates()
			.stream()
			.filter(operator -> !droppedIDs.contains(operator.getOperatorID()))
			.collect(Collectors.toList());

		Set<OperatorID> existingOperatorIDs = remainingOperators
			.stream()
			.map(OperatorState::getOperatorID)
			.collect(Collectors.toSet());

		for (String uid : transformations.keySet()) {
			if (existingOperatorIDs.contains(OperatorIDGenerator.fromUid(uid))) {
				throw new IllegalArgumentException("The base savepoint " + path + " already contains uid " + uid + ". All uid's must be unique");
			}
		}

		DataSet<OperatorState> existingOperators = env
			.fromCollection(remainingOperators)
			.name(metadata.toString());

		write(savepointPath, transformations, stateBackend, metadata, existingOperators);
	}
}
