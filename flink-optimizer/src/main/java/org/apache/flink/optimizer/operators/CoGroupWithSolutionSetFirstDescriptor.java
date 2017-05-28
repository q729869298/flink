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

package org.apache.flink.optimizer.operators;

import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.optimizer.CompilerException;
import org.apache.flink.optimizer.dag.TwoInputNode;
import org.apache.flink.optimizer.dataproperties.LocalProperties;
import org.apache.flink.optimizer.dataproperties.RequestedLocalProperties;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.DualInputPlanNode;
import org.apache.flink.optimizer.util.Utils;
import org.apache.flink.runtime.operators.DriverStrategy;

import java.util.Collections;
import java.util.List;

/**
 * 
 */
public class CoGroupWithSolutionSetFirstDescriptor extends CoGroupDescriptor {
	
	public CoGroupWithSolutionSetFirstDescriptor(FieldList keys1, FieldList keys2) {
		super(keys1, keys2);
	}
	
	@Override
	protected List<LocalPropertiesPair> createPossibleLocalProperties() {
		RequestedLocalProperties none = new RequestedLocalProperties();
		RequestedLocalProperties sort = new RequestedLocalProperties(Utils.createOrdering(this.keys2));
		return Collections.singletonList(new LocalPropertiesPair(none, sort));
	}

	@Override
	public DualInputPlanNode instantiate(Channel in1, Channel in2, TwoInputNode node) {
		boolean[] inputOrders = in2.getLocalProperties().getOrdering() == null ? null : in2.getLocalProperties().getOrdering().getFieldSortDirections();

		if (inputOrders == null || inputOrders.length < this.keys2.size()) {
			throw new CompilerException("BUG: The input strategy does not sufficiently describe the sort orders for a CoGroup operator.");
		} else if (inputOrders.length > this.keys2.size()) {
			boolean[] tmp = new boolean[this.keys2.size()];
			System.arraycopy(inputOrders, 0, tmp, 0, tmp.length);
			inputOrders = tmp;
		}

		return new DualInputPlanNode(node, "CoGroup ("+node.getOperator().getName()+")", in1, in2, DriverStrategy.CO_GROUP, this.keys1, this.keys2, inputOrders);
	}

	@Override
	public boolean areCoFulfilled(RequestedLocalProperties requested1, RequestedLocalProperties requested2,
			LocalProperties produced1, LocalProperties produced2)
	{
		return true;
	}

	@Override
	public LocalProperties computeLocalProperties(LocalProperties in1, LocalProperties in2) {
		return in2;
	}
}
