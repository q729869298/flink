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

package org.apache.flink.runtime.instance;

import org.apache.flink.runtime.jobmanager.slots.SlotOwner;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.AbstractID;
import org.apache.flink.api.common.JobID;

import javax.annotation.Nullable;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class represents a shared slot. A shared slot can have multiple
 * {@link SimpleSlot} instances within itself. This allows to
 * schedule multiple tasks simultaneously to the same resource. Sharing a resource with multiple
 * tasks is crucial for simple pipelined / streamed execution, where both the sender and the receiver
 * are typically active at the same time.
 *
 * <p><b>IMPORTANT:</b> This class contains no synchronization. Thus, the caller has to guarantee proper
 * synchronization. In the current implementation, all concurrently modifying operations are
 * passed through a {@link SlotSharingGroupAssignment} object which is responsible for
 * synchronization.
 */
public class SharedSlot extends Slot {

	/** The assignment group os shared slots that manages the availability and release of the slots */
	private final SlotSharingGroupAssignment assignmentGroup;

	/** The set os sub-slots allocated from this shared slot */
	private final Set<Slot> subSlots;


	/**
	 * Creates a new shared slot that has no parent (is a root slot) and does not belong to any task group.
	 * This constructor is used to create a slot directly from an instance. 
	 * 
	 * @param jobID The ID of the job that the slot is created for.
	 * @param owner The component from which this slot is allocated.
	 * @param location The location info of the TaskManager where the slot was allocated from
	 * @param slotNumber The number of the slot.
	 * @param taskManagerActorGateway The actor gateway to communicate with the TaskManager   
	 * @param assignmentGroup The assignment group that this shared slot belongs to.
	 */
	public SharedSlot(
			JobID jobID, SlotOwner owner, TaskManagerLocation location, int slotNumber,
			ActorGateway taskManagerActorGateway,
			SlotSharingGroupAssignment assignmentGroup) {

		this(jobID, owner, location, slotNumber, taskManagerActorGateway, assignmentGroup, null, null);
	}

	/**
	 * Creates a new shared slot that has is a sub-slot of the given parent shared slot, and that belongs
	 * to the given task group.
	 * 
	 * @param jobID The ID of the job that the slot is created for.
	 * @param owner The component from which this slot is allocated.
	 * @param location The location info of the TaskManager where the slot was allocated from
	 * @param slotNumber The number of the slot.
	 * @param taskManagerActorGateway The actor gateway to communicate with the TaskManager   
	 * @param assignmentGroup The assignment group that this shared slot belongs to.
	 * @param parent The parent slot of this slot.
	 * @param groupId The assignment group of this slot.
	 */
	public SharedSlot(
			JobID jobID, SlotOwner owner, TaskManagerLocation location, int slotNumber,
			ActorGateway taskManagerActorGateway,
			SlotSharingGroupAssignment assignmentGroup,
			@Nullable SharedSlot parent, @Nullable AbstractID groupId) {

		super(jobID, owner, location, slotNumber, taskManagerActorGateway, parent, groupId);

		this.assignmentGroup = checkNotNull(assignmentGroup);
		this.subSlots = new HashSet<Slot>();
	}

	// ------------------------------------------------------------------------
	//  Properties
	// ------------------------------------------------------------------------

	@Override
	public int getNumberLeaves() {
		while (true) {
			try {
				int result = 0;
				for (Slot slot: subSlots){
					result += slot.getNumberLeaves();
				}
				return result;
			}
			catch (ConcurrentModificationException e) {
				// ignore and retry
			}
		}
	}

	/**
	 * Checks whether this slot is a root slot that has not yet added any child slots.
	 * 
	 * @return True, if this slot is a root slot and has not yet added any children, false otherwise.
	 */
	public boolean isRootAndEmpty() {
		return getParent() == null && subSlots.isEmpty();
	}

	/**
	 * Checks whether this shared slot has any sub slots.
	 * 
	 * @return True, if the shared slot has sub slots, false otherwise.
	 */
	public boolean hasChildren() {
		return subSlots.size() > 0;
	}
	
	@Override
	public void releaseSlot() {
		assignmentGroup.releaseSharedSlot(this);
		
		if (!(isReleased() && subSlots.isEmpty())) {
			throw new IllegalStateException("Bug: SharedSlot is not empty and released after call to releaseSlot()");
		}
	}

	/**
	 * Gets the set of all slots allocated as sub-slots of this shared slot.
	 *
	 * @return All sub-slots allocated from this shared slot.
	 */
	Set<Slot> getSubSlots() {
		return subSlots;
	}

	// ------------------------------------------------------------------------
	//  INTERNAL : TO BE CALLED ONLY BY THE assignmentGroup - Allocating sub-slots
	// ------------------------------------------------------------------------

	/**
	 * Creates a new sub slot if the slot is not dead, yet. This method should only be called from
	 * the assignment group instance to guarantee synchronization.
	 * 
	 * <b>NOTE:</b> This method is not synchronized and must only be called from
	 *              the slot's assignment group.
	 *
	 * @param groupId The ID to identify tasks which can be deployed in this sub slot.
	 * @return The new sub slot if the shared slot is still alive, otherwise null.
	 */
	SimpleSlot allocateSubSlot(AbstractID groupId) {
		if (isAlive()) {
			SimpleSlot slot = new SimpleSlot(
					getJobID(), getOwner(), getTaskManagerLocation(), subSlots.size(), 
					getTaskManagerActorGateway(), this, groupId);
			subSlots.add(slot);
			return slot;
		}
		else {
			return null;
		}
	}

	/**
	 * Creates a new sub slot if the slot is not dead, yet. This method should only be called from
	 * the assignment group instance to guarantee synchronization.
	 * 
	 * NOTE: This method should only be called from the slot's assignment group.
	 *
	 * @param groupId The ID to identify tasks which can be deployed in this sub slot.
	 * @return The new sub slot if the shared slot is still alive, otherwise null.
	 */
	SharedSlot allocateSharedSlot(AbstractID groupId){
		if (isAlive()) {
			SharedSlot slot = new SharedSlot(
					getJobID(), getOwner(), getTaskManagerLocation(), subSlots.size(), 
					getTaskManagerActorGateway(), assignmentGroup, this, groupId);
			subSlots.add(slot);
			return slot;
		}
		else {
			return null;
		}
	}

	// ------------------------------------------------------------------------
	//  INTERNAL : TO BE CALLED ONLY BY THE assignmentGroup - releasing slots
	// ------------------------------------------------------------------------
	
	/**
	 * Disposes the given sub slot. This method is called by the child simple slot to tell this
	 * shared slot to release it.
	 *
	 * The releasing process itself is done by the {@link SlotSharingGroupAssignment}, which controls
	 * all the modifications in this shared slot.
	 *
	 * NOTE: This method must not modify the shared slot directly !!!
	 *
	 * @param slot The sub-slot which shall be removed from the shared slot.
	 */
	void releaseChild(SimpleSlot slot) {
		assignmentGroup.releaseSimpleSlot(slot);
	}
	
	/**
	 * Removes the given slot from this shared slot. This method Should only be called
	 * through this shared slot's {@link SlotSharingGroupAssignment}
	 *
	 * @param slot slot to be removed from the set of sub slots.
	 * @return Number of remaining sub slots
	 */
	int removeDisposedChildSlot(Slot slot) {
		if (!slot.isReleased() || !subSlots.remove(slot)) {
			throw new IllegalArgumentException();
		}
		return subSlots.size();
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return "Shared " + super.toString();
	}
}