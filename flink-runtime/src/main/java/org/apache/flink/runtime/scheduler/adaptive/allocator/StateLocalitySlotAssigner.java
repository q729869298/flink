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

package org.apache.flink.runtime.scheduler.adaptive.allocator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan.SlotAssignment;
import org.apache.flink.runtime.scheduler.adaptive.allocator.JobAllocationsInformation.VertexAllocationInformation;
import org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingSlotAllocator.ExecutionSlotSharingGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.apache.flink.runtime.scheduler.adaptive.allocator.DefaultSlotAssigner.createExecutionSlotSharingGroups;
import static org.apache.flink.util.Preconditions.checkState;

/** A {@link SlotAssigner} that assigns slots based on the number of local key groups. */
@Internal
public class StateLocalitySlotAssigner implements SlotAssigner {

    @Override
    public Collection<SlotAssignment> assignSlots(
            JobInformation jobInformation,
            Collection<? extends SlotInfo> freeSlots,
            VertexParallelism vertexParallelism,
            JobAllocationsInformation previousAllocations) {
        checkState(
                freeSlots.size() >= jobInformation.getSlotSharingGroups().size(),
                "Not enough slots to allocate all the slot sharing groups (have: %s, need: %s)",
                freeSlots.size(),
                jobInformation.getSlotSharingGroups().size());

        final List<ExecutionSlotSharingGroup> allGroups = new ArrayList<>();
        for (SlotSharingGroup slotSharingGroup : jobInformation.getSlotSharingGroups()) {
            allGroups.addAll(createExecutionSlotSharingGroups(vertexParallelism, slotSharingGroup));
        }
        final Map<JobVertexID, Integer> parallelism = getParallelism(allGroups);
        final PriorityQueue<AllocationScore> scores =
                calculateScores(jobInformation, previousAllocations, allGroups, parallelism);

        final Map<String, ExecutionSlotSharingGroup> groupsById =
                allGroups.stream().collect(toMap(ExecutionSlotSharingGroup::getId, identity()));
        final Map<AllocationID, SlotInfo> slotsById =
                selectSlotsInMinimalTaskExecutors(freeSlots, allGroups, scores).stream()
                        .collect(toMap(SlotInfo::getAllocationId, identity()));
        AllocationScore score;
        final Collection<SlotAssignment> assignments = new ArrayList<>();
        while ((score = scores.poll()) != null) {
            if (slotsById.containsKey(score.getAllocationId())
                    && groupsById.containsKey(score.getGroupId())) {
                assignments.add(
                        new SlotAssignment(
                                slotsById.remove(score.getAllocationId()),
                                groupsById.remove(score.getGroupId())));
            }
        }
        // Distribute the remaining slots with no score
        Iterator<? extends SlotInfo> remainingSlots = slotsById.values().iterator();
        for (ExecutionSlotSharingGroup group : groupsById.values()) {
            checkState(
                    remainingSlots.hasNext(),
                    "No slots available for group %s (%s more in total). This is likely a bug.",
                    group,
                    groupsById.size());
            assignments.add(new SlotAssignment(remainingSlots.next(), group));
            remainingSlots.remove();
        }

        return assignments;
    }

    @Override
    public List<TaskManagerLocation> sortPrioritizedTaskExecutors(
            Collection<? extends SlotInfo> slots, Collection<AllocationScore> scores) {
        Map<TaskManagerLocation, ? extends Set<? extends SlotInfo>> slotsByTaskExecutor =
                SlotAssigner.getSlotsPerTaskExecutor(slots);
        final Map<AllocationID, TaskManagerLocation> allocIdToTaskExecutor =
                slots.stream()
                        .collect(
                                Collectors.toMap(
                                        SlotInfo::getAllocationId,
                                        SlotInfo::getTaskManagerLocation));

        Map<TaskManagerLocation, Long> scorePerTaskExecutor =
                getScorePerTaskExecutor(scores, slotsByTaskExecutor, allocIdToTaskExecutor);
        return slotsByTaskExecutor.keySet().stream()
                .sorted(
                        (left, right) -> {
                            int diff =
                                    Integer.compare(
                                            slotsByTaskExecutor.get(right).size(),
                                            slotsByTaskExecutor.get(left).size());
                            return diff != 0
                                    ? diff
                                    : Long.compare(
                                            scorePerTaskExecutor.getOrDefault(right, 0L),
                                            scorePerTaskExecutor.getOrDefault(left, 0L));
                        })
                .collect(Collectors.toList());
    }

    @Nonnull
    private PriorityQueue<AllocationScore> calculateScores(
            JobInformation jobInformation,
            JobAllocationsInformation previousAllocations,
            List<ExecutionSlotSharingGroup> allGroups,
            Map<JobVertexID, Integer> parallelism) {
        // PQ orders the pairs (allocationID, groupID) by score, decreasing
        // the score is computed as the potential amount of state that would reside locally
        final PriorityQueue<AllocationScore> scores =
                new PriorityQueue<>(Comparator.reverseOrder());
        for (ExecutionSlotSharingGroup group : allGroups) {
            scores.addAll(calculateScore(group, parallelism, jobInformation, previousAllocations));
        }
        return scores;
    }

    private static Map<JobVertexID, Integer> getParallelism(
            List<ExecutionSlotSharingGroup> groups) {
        final Map<JobVertexID, Integer> parallelism = new HashMap<>();
        for (ExecutionSlotSharingGroup group : groups) {
            for (ExecutionVertexID evi : group.getContainedExecutionVertices()) {
                parallelism.merge(evi.getJobVertexId(), 1, Integer::sum);
            }
        }
        return parallelism;
    }

    public Collection<AllocationScore> calculateScore(
            ExecutionSlotSharingGroup group,
            Map<JobVertexID, Integer> parallelism,
            JobInformation jobInformation,
            JobAllocationsInformation previousAllocations) {
        final Map<AllocationID, Long> score = new HashMap<>();
        for (ExecutionVertexID evi : group.getContainedExecutionVertices()) {
            final KeyGroupRange kgr =
                    KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(
                            jobInformation
                                    .getVertexInformation(evi.getJobVertexId())
                                    .getMaxParallelism(),
                            parallelism.get(evi.getJobVertexId()),
                            evi.getSubtaskIndex());
            previousAllocations
                    .getAllocations(evi.getJobVertexId())
                    .forEach(
                            allocation -> {
                                long value = estimateSize(kgr, allocation);
                                if (value > 0) {
                                    score.merge(allocation.getAllocationID(), value, Long::sum);
                                }
                            });
        }

        return score.entrySet().stream()
                .map(e -> new AllocationScore(group.getId(), e.getKey(), e.getValue()))
                .collect(Collectors.toList());
    }

    private static long estimateSize(
            KeyGroupRange newRange, VertexAllocationInformation allocation) {
        KeyGroupRange oldRange = allocation.getKeyGroupRange();
        if (allocation.stateSizeInBytes * oldRange.getNumberOfKeyGroups() == 0) {
            return 0L;
        }
        // round up to 1
        long keyGroupSize =
                allocation.stateSizeInBytes
                        / Math.min(allocation.stateSizeInBytes, oldRange.getNumberOfKeyGroups());
        int numberOfKeyGroups = oldRange.getIntersection(newRange).getNumberOfKeyGroups();
        return numberOfKeyGroups * keyGroupSize;
    }

    private static Map<TaskManagerLocation, Long> getScorePerTaskExecutor(
            Collection<AllocationScore> scores,
            Map<TaskManagerLocation, ? extends Set<? extends SlotInfo>> slotsByTaskExecutor,
            Map<AllocationID, TaskManagerLocation> allocIdToTaskExecutor) {
        Map<TaskManagerLocation, Long> scorePerTaskExecutor =
                new HashMap<>(slotsByTaskExecutor.size());
        for (AllocationScore allocScore : scores) {
            final TaskManagerLocation tml = allocIdToTaskExecutor.get(allocScore.getAllocationId());
            if (Objects.nonNull(tml)) {
                scorePerTaskExecutor.compute(
                        tml,
                        (rid, oldVal) ->
                                Objects.isNull(oldVal)
                                        ? allocScore.getScore()
                                        : oldVal + allocScore.getScore());
            }
        }
        return scorePerTaskExecutor;
    }
}
