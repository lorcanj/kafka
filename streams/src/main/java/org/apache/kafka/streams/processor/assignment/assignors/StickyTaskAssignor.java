/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.processor.assignment.assignors;

import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.assignment.ApplicationState;
import org.apache.kafka.streams.processor.assignment.KafkaStreamsAssignment;
import org.apache.kafka.streams.processor.assignment.KafkaStreamsAssignment.AssignedTask;
import org.apache.kafka.streams.processor.assignment.KafkaStreamsState;
import org.apache.kafka.streams.processor.assignment.ProcessId;
import org.apache.kafka.streams.processor.assignment.TaskAssignmentUtils;
import org.apache.kafka.streams.processor.assignment.TaskAssignmentUtils.RackAwareOptimizationParams;
import org.apache.kafka.streams.processor.assignment.TaskAssignor;
import org.apache.kafka.streams.processor.assignment.TaskInfo;
import org.apache.kafka.streams.processor.assignment.TaskTopicPartition;
import org.apache.kafka.streams.processor.internals.assignment.RackAwareTaskAssignor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableMap;


public class StickyTaskAssignor implements TaskAssignor {
    private static final Logger LOG = LoggerFactory.getLogger(StickyTaskAssignor.class);

    public static final int DEFAULT_STICKY_TRAFFIC_COST = 1;
    public static final int DEFAULT_STICKY_NON_OVERLAP_COST = 10;

    private final boolean mustPreserveActiveTaskAssignment;

    public StickyTaskAssignor() {
        this(false);
    }

    public StickyTaskAssignor(final boolean mustPreserveActiveTaskAssignment) {
        this.mustPreserveActiveTaskAssignment = mustPreserveActiveTaskAssignment;
    }

    @Override
    public TaskAssignment assign(final ApplicationState applicationState) {
        final Map<ProcessId, KafkaStreamsState> clients = applicationState.kafkaStreamsStates(false);
        final Map<TaskId, ProcessId> previousActiveAssignment = mapPreviousActiveTasks(clients);
        final Map<TaskId, Set<ProcessId>> previousStandbyAssignment = mapPreviousStandbyTasks(clients);
        final AssignmentState assignmentState = new AssignmentState(applicationState, clients,
            previousActiveAssignment, previousStandbyAssignment);

        assignActive(applicationState, clients.values(), assignmentState, this.mustPreserveActiveTaskAssignment);
        optimizeActive(applicationState, assignmentState);
        assignStandby(applicationState, assignmentState);
        optimizeStandby(applicationState, assignmentState);

        final Map<ProcessId, KafkaStreamsAssignment> finalAssignments = assignmentState.newAssignments;
        if (mustPreserveActiveTaskAssignment && !finalAssignments.isEmpty()) {
            // We set the followup deadline for only one of the clients.
            final ProcessId clientId = finalAssignments.entrySet().iterator().next().getKey();
            final KafkaStreamsAssignment previousAssignment = finalAssignments.get(clientId);
            finalAssignments.put(clientId, previousAssignment.withFollowupRebalance(Instant.ofEpochMilli(0)));
        }

        return new TaskAssignment(finalAssignments.values());
    }

    private void optimizeActive(final ApplicationState applicationState,
                                final AssignmentState assignmentState) {
        if (mustPreserveActiveTaskAssignment) {
            return;
        }

        final Map<ProcessId, KafkaStreamsAssignment> currentAssignments = assignmentState.newAssignments;

        final RackAwareOptimizationParams statefulTaskParams = RackAwareOptimizationParams.of(applicationState)
            .withTrafficCostOverride(
                applicationState.assignmentConfigs().rackAwareTrafficCost().orElse(DEFAULT_STICKY_TRAFFIC_COST)
            )
            .withNonOverlapCostOverride(
                applicationState.assignmentConfigs().rackAwareNonOverlapCost().orElse(DEFAULT_STICKY_NON_OVERLAP_COST)
            )
            .forStatefulTasks();
        TaskAssignmentUtils.optimizeRackAwareActiveTasks(statefulTaskParams, currentAssignments);

        TaskAssignmentUtils.optimizeRackAwareActiveTasks(
            RackAwareOptimizationParams.of(applicationState)
                .forStatelessTasks()
                .withTrafficCostOverride(RackAwareTaskAssignor.STATELESS_TRAFFIC_COST)
                .withNonOverlapCostOverride(RackAwareTaskAssignor.STATELESS_NON_OVERLAP_COST),
            currentAssignments
        );
        assignmentState.processOptimizedAssignments(currentAssignments);
    }

    private void optimizeStandby(final ApplicationState applicationState, final AssignmentState assignmentState) {
        if (applicationState.assignmentConfigs().numStandbyReplicas() <= 0) {
            return;
        }

        if (mustPreserveActiveTaskAssignment) {
            return;
        }

        final Map<ProcessId, KafkaStreamsAssignment> assignments = assignmentState.newAssignments;

        final RackAwareOptimizationParams optimizationParams = RackAwareOptimizationParams.of(applicationState)
            .withTrafficCostOverride(
                applicationState.assignmentConfigs().rackAwareTrafficCost().orElse(DEFAULT_STICKY_TRAFFIC_COST)
            )
            .withNonOverlapCostOverride(
                applicationState.assignmentConfigs().rackAwareNonOverlapCost().orElse(DEFAULT_STICKY_NON_OVERLAP_COST)
            );
        TaskAssignmentUtils.optimizeRackAwareStandbyTasks(optimizationParams, assignments);
        assignmentState.processOptimizedAssignments(assignments);
    }

    private static void assignActive(final ApplicationState applicationState,
                                     final Collection<KafkaStreamsState> clients,
                                     final AssignmentState assignmentState,
                                     final boolean mustPreserveActiveTaskAssignment) {
        //final int totalCapacity = computeTotalProcessingThreads(clients);
        final Set<TaskId> allTaskIds = applicationState.allTasks().keySet();
        // final int taskCount = allTaskIds.size();
        // goal for total equality
        // final int activeTasksPerThread = taskCount / totalCapacity;
        final Set<TaskId> unassigned = new HashSet<>(allTaskIds);

        // first try and re-assign existing active tasks to clients that previously had
        // the same active task
        for (final TaskId taskId : assignmentState.previousActiveAssignment.keySet()) {
            if (allTaskIds.contains(taskId)) {
                final ProcessId previousClientForTask = assignmentState.previousActiveAssignment.get(taskId);
                if (mustPreserveActiveTaskAssignment || assignmentState.hasRoomForActiveTask(previousClientForTask, taskId)) {
                    assignmentState.finalizeAssignment(taskId, previousClientForTask, AssignedTask.Type.ACTIVE);
                    assignmentState.updateClientWeightMap(previousClientForTask, taskId);
                    unassigned.remove(taskId);

                }
            }
        }

        // try and assign any remaining unassigned tasks to clients that previously
        // have seen the task.
        // might be a bit inefficient how this is being done
        for (final Iterator<TaskId> iterator = unassigned.iterator(); iterator.hasNext(); ) {
            final TaskId taskId = iterator.next();
            final Set<ProcessId> previousClientsForStandbyTask = assignmentState.previousStandbyAssignment.getOrDefault(taskId, new HashSet<>());
            for (final ProcessId client: previousClientsForStandbyTask) {
                // want to cache negative result of hasRoomForActiveTask as it is leading to
                // a reduction in performance
                if (assignmentState.hasRoomForActiveTask(client, taskId)) {
                    assignmentState.finalizeAssignment(taskId, client, AssignedTask.Type.ACTIVE);
                    assignmentState.updateClientWeightMap(client, taskId);
                    iterator.remove();
                    break;
                }
            }
        }

        // changed order the unassigned tasks are sorted
        // might change the ordering
        // also double check taskInputPartitionCount
//        final List<TaskId> sortedTasks = assignmentState.taskInputPartitionCount.entrySet()
//                .stream().filter(entry -> unassigned.contains(entry.getKey()))
//                .sorted(Map.Entry.<TaskId, Integer>comparingByValue().reversed())
//                .map(Map.Entry::getKey)
//                .collect(Collectors.toList());
        // changed this but might be worth checking what is best for the assignment
        final List<TaskId> sortedTasks = new ArrayList<>(unassigned);
        Collections.sort(sortedTasks);

        final Set<ProcessId> candidateClients = clients.stream()
                .map(KafkaStreamsState::processId)
                .collect(Collectors.toSet());
        for (final TaskId taskId : sortedTasks) {
            final ProcessId bestClient = assignmentState.findBestClientForTask(taskId, candidateClients);
            assignmentState.finalizeAssignment(taskId, bestClient, AssignedTask.Type.ACTIVE);
            assignmentState.updateClientWeightMap(bestClient, taskId);
        }
    }

    private static void assignStandby(final ApplicationState applicationState,
                                      final AssignmentState assignmentState) {
        final Set<TaskInfo> statefulTasks = applicationState.allTasks().values().stream()
            .filter(taskInfo -> taskInfo.topicPartitions().stream().anyMatch(TaskTopicPartition::isChangelog))
            .collect(Collectors.toSet());
        final int numStandbyReplicas = applicationState.assignmentConfigs().numStandbyReplicas();
        for (final TaskInfo task : statefulTasks) {
            for (int i = 0; i < numStandbyReplicas; i++) {
                final Set<ProcessId> candidateClients = assignmentState.findClientsWithoutAssignedTask(task.id());
                if (candidateClients.isEmpty()) {
                    LOG.warn("Unable to assign {} of {} standby tasks for task [{}]. " +
                             "There is not enough available capacity. You should " +
                             "increase the number of threads and/or application instances " +
                             "to maintain the requested number of standby replicas.",
                        numStandbyReplicas - i,
                        numStandbyReplicas, task.id());
                    break;
                }

                final ProcessId bestClient = assignmentState.findBestClientForTask(task.id(), candidateClients);
                assignmentState.finalizeAssignment(task.id(), bestClient, AssignedTask.Type.STANDBY);
                assignmentState.updateClientWeightMap(bestClient, task.id());
            }
        }
    }

    private static Map<TaskId, ProcessId> mapPreviousActiveTasks(final Map<ProcessId, KafkaStreamsState> clients) {
        final Map<TaskId, ProcessId> previousActiveTasks = new HashMap<>();
        for (final KafkaStreamsState client : clients.values()) {
            for (final TaskId taskId : client.previousActiveTasks()) {
                previousActiveTasks.put(taskId, client.processId());
            }
        }
        return previousActiveTasks;
    }

    private static Map<TaskId, Set<ProcessId>> mapPreviousStandbyTasks(final Map<ProcessId, KafkaStreamsState> clients) {
        final Map<TaskId, Set<ProcessId>> previousStandbyTasks = new HashMap<>();
        for (final KafkaStreamsState client : clients.values()) {
            for (final TaskId taskId : client.previousStandbyTasks()) {
                previousStandbyTasks.computeIfAbsent(taskId, k -> new HashSet<>());
                previousStandbyTasks.get(taskId).add(client.processId());
            }
        }
        return previousStandbyTasks;
    }

    private static int computeTotalProcessingThreads(final Collection<KafkaStreamsState> clients) {
        int count = 0;
        for (final KafkaStreamsState client : clients) {
            count += client.numProcessingThreads();
        }
        return count;
    }

    private static class AssignmentState {
        private final Map<ProcessId, KafkaStreamsState> clients;
        private final Map<TaskId, ProcessId> previousActiveAssignment;
        private final Map<TaskId, Set<ProcessId>> previousStandbyAssignment;
        private final Map<TaskId, Integer> taskInputPartitionCount;
        private final Map<ProcessId, Integer> currentClientWeight;
        private final double fairPartitionsPerClientThread;
        private final int averageTaskWeight;
        private final Set<ProcessId> processFull;

        private final TaskPairs taskPairs;

        private Map<TaskId, Set<ProcessId>> newTaskLocations;
        private Map<ProcessId, KafkaStreamsAssignment> newAssignments;

        private AssignmentState(final ApplicationState applicationState,
                                final Map<ProcessId, KafkaStreamsState> clients,
                                final Map<TaskId, ProcessId> previousActiveAssignment,
                                final Map<TaskId, Set<ProcessId>> previousStandbyAssignment) {
            this.clients = clients;
            this.previousActiveAssignment = unmodifiableMap(previousActiveAssignment);
            this.previousStandbyAssignment = unmodifiableMap(previousStandbyAssignment);
            this.processFull = new HashSet<>();
            this.currentClientWeight = new HashMap<>();
            this.taskInputPartitionCount = calculateInputPartitionsPerTask(applicationState.allTasks());

            // task weight is partition count
            final int totalPartitionCount = this.taskInputPartitionCount.values().stream().mapToInt(Integer::intValue).sum();
            final int totalNumberOfThreads = clients.values().stream().mapToInt(KafkaStreamsState::numProcessingThreads).sum();
            // average partitions per thread
            this.fairPartitionsPerClientThread = (double) totalPartitionCount / totalNumberOfThreads;

            final int taskCount = applicationState.allTasks().size();
            final int safeTaskCount = taskCount == 0 ? 1 : taskCount;

            this.averageTaskWeight = Math.max(Math.floorDiv(totalPartitionCount, safeTaskCount), 1);

            final int maxPairs = taskCount * (taskCount - 1) / 2;
            this.taskPairs = new TaskPairs(maxPairs);

            // this.averageTaskWeight = Math.min(Math.floorDiv(this.taskInputPartitionCount.values().stream().mapToInt(Integer::intValue).sum(), taskCount), 1);

            this.newTaskLocations = previousActiveAssignment.keySet().stream()
                .collect(Collectors.toMap(Function.identity(), taskId -> new HashSet<>()));
            this.newAssignments = clients.values().stream().collect(Collectors.toMap(
                KafkaStreamsState::processId,
                state -> KafkaStreamsAssignment.of(state.processId(), new HashSet<>())
            ));
        }

        private void finalizeAssignment(final TaskId taskId, final ProcessId client, final AssignedTask.Type type) {
            final Set<TaskId> newAssignmentsForClient = newAssignments.get(client).tasks().keySet();
            taskPairs.addPairs(taskId, newAssignmentsForClient);

            newAssignments.get(client).assignTask(new AssignedTask(taskId, type));
            newTaskLocations.computeIfAbsent(taskId, k -> new HashSet<>()).add(client);
        }

        private void processOptimizedAssignments(final Map<ProcessId, KafkaStreamsAssignment> optimizedAssignments) {
            final Map<TaskId, Set<ProcessId>> newTaskLocations = new HashMap<>();

            for (final Map.Entry<ProcessId, KafkaStreamsAssignment> entry : optimizedAssignments.entrySet()) {
                final ProcessId processId = entry.getKey();
                final Set<AssignedTask> assignedTasks = new HashSet<>(optimizedAssignments.get(processId).tasks().values());

                for (final AssignedTask task : assignedTasks) {
                    newTaskLocations.computeIfAbsent(task.id(), k -> new HashSet<>()).add(processId);
                }
            }

            this.newTaskLocations = newTaskLocations;
            this.newAssignments = optimizedAssignments;
        }

        private boolean hasRoomForActiveTask(final ProcessId processId, final TaskId taskId) {
            if (processFull.contains(processId)) {
                return false;
            }

            final int capacity = clients.get(processId).numProcessingThreads();
            final var newActiveTaskCount = newAssignments.computeIfAbsent(processId, k -> KafkaStreamsAssignment.of(processId, new HashSet<>()))
                .tasks().values()
                .stream().filter(assignedTask -> assignedTask.type() == AssignedTask.Type.ACTIVE)
                .collect(Collectors.toSet());

            if (newActiveTaskCount.size() < capacity) {
                return false;
            }

            final int currentClientPartitionSize = newActiveTaskCount.stream().mapToInt(task -> taskInputPartitionCount.getOrDefault(task.id(), 1)).sum();
            final int addedTaskWeight = taskInputPartitionCount.getOrDefault(taskId, 1);

            final boolean hasRoom = currentClientPartitionSize + addedTaskWeight < fairPartitionsPerClientThread * capacity + averageTaskWeight;
            if (!hasRoom) {
                this.processFull.add(processId);
            }
            return hasRoom;
        }

        private ProcessId findBestClientForTask(final TaskId taskId, final Set<ProcessId> clientsWithin) {
            if (clientsWithin.size() == 1) {
                return clientsWithin.iterator().next();
            }

            final ProcessId previousClient = findLeastLoadedClientWithPreviousActiveOrStandbyTask(
                taskId, clientsWithin);
            if (previousClient == null) {
                return findLeastLoadedClient(taskId, clientsWithin);
            }

            // this needs to check if balance with the addition of that extra thing
            if (shouldBalanceLoad(previousClient)) {
                final ProcessId standby = findLeastLoadedClientWithPreviousStandbyTask(taskId, clientsWithin);
                if (standby == null || shouldBalanceLoad(standby)) {
                    return findLeastLoadedClient(taskId, clientsWithin);
                }
                return standby;
            }
            return previousClient;
        }

        private Set<ProcessId> findClientsWithoutAssignedTask(final TaskId taskId) {
            final Set<ProcessId> unavailableClients = newTaskLocations.get(taskId);
            return clients.values().stream()
                .map(KafkaStreamsState::processId)
                .filter(o -> !unavailableClients.contains(o))
                .collect(Collectors.toSet());
        }
        
        private double clientLoad(final ProcessId processId) {
            final int capacity = clients.get(processId).numProcessingThreads();
            final double totalTaskCount = newAssignments.get(processId).tasks().size();
            return totalTaskCount / capacity;
        }

        private double clientLoadPartitions(final ProcessId processId) {
            final int capacity = clients.get(processId).numProcessingThreads();
            final double totalPartitionCount = this.currentClientWeight.getOrDefault(processId, 0);
            return totalPartitionCount / capacity;
        }

        private Map<TaskId, Integer> calculateInputPartitionsPerTask(final Map<TaskId, TaskInfo> map) {
            final Map<TaskId, Integer> taskPartitionCount = new HashMap<>();
            for (final Map.Entry<TaskId, TaskInfo> entry : map.entrySet()) {
                int inputPartitionCount = 0;
                for (final TaskTopicPartition partition : entry.getValue().topicPartitions()) {

                    if (partition.isChangelog())
                        continue;
                    inputPartitionCount++;
                }
                taskPartitionCount.put(entry.getKey(), Math.max(1, inputPartitionCount));
            }
            for (final var thing : taskPartitionCount.entrySet()) {
                System.out.println(thing.getValue());
            }
            return taskPartitionCount;
        }

        private final void updateClientWeightMap(final ProcessId client, final TaskId taskId) {
            this.currentClientWeight.merge(client, taskInputPartitionCount.getOrDefault(taskId, 1), Integer::sum);
        }

        private ProcessId findLeastLoadedClient(final TaskId taskId, final Set<ProcessId> clientIds) {
            ProcessId leastLoaded = null;
            double minLoad = Double.MAX_VALUE;

            ProcessId leastLoadedTEST = null;
            double minLoadTEST = Double.MAX_VALUE;

            for (final ProcessId processId : clientIds) {
                final double thisClientLoad = clientLoadPartitions(processId);
                if (thisClientLoad == 0) {
                    return processId;
                }

                if (leastLoaded == null || thisClientLoad < minLoad) {
                    final Set<TaskId> assignedTasks = newAssignments.get(processId).tasks().values()
                        .stream().map(AssignedTask::id).collect(Collectors.toSet());
                    if (taskPairs.hasNewPair(taskId, assignedTasks)) {
                        leastLoaded = processId;
                        minLoad = thisClientLoad;
                    }
                }

                // track absolute minimum separately
                if (thisClientLoad < minLoadTEST) {
                    minLoadTEST = thisClientLoad;
                    leastLoadedTEST = processId;
                }
            }

            if (leastLoaded != null) {
                return leastLoaded;
            }

            return leastLoadedTEST;
        }

        private ProcessId findLeastLoadedClientWithPreviousActiveOrStandbyTask(final TaskId taskId,
                                                                               final Set<ProcessId> clientsWithin) {
            final ProcessId previous = previousActiveAssignment.get(taskId);
            if (previous != null && clientsWithin.contains(previous)) {
                return previous;
            }
            return findLeastLoadedClientWithPreviousStandbyTask(taskId, clientsWithin);
        }

        private ProcessId findLeastLoadedClientWithPreviousStandbyTask(final TaskId taskId,
                                                                       final Set<ProcessId> clientsWithin) {
            final Set<ProcessId> ids = previousStandbyAssignment.getOrDefault(taskId, new HashSet<>());
            final HashSet<ProcessId> constrainTo = new HashSet<>(ids);
            constrainTo.retainAll(clientsWithin);
            return findLeastLoadedClient(taskId, constrainTo);
        }

        private boolean shouldBalanceLoad(final ProcessId client) {
            final double thisClientLoad = clientLoad(client);
            // System.out.println(this.averageTaskWeight);
            // System.out.println(client);
            // System.out.println("CHECK BALANCING");

            // want to keep this and so also want to keep the clientLoad also
            if (thisClientLoad < 1) {
                return false;
            }

            final double thisClientLoadPartition = clientLoadPartitions(client);
            final int clientCapacity = clients.get(client).numProcessingThreads();
            return thisClientLoadPartition >= fairPartitionsPerClientThread * clientCapacity;
        }
    }

    private static class TaskPairs {
        private final Set<TaskPair> pairs;
        private final int maxPairs;

        public TaskPairs(final int maxPairs) {
            this.maxPairs = maxPairs;
            this.pairs = new HashSet<>(maxPairs);
        }

        public boolean hasNewPair(final TaskId task1,
                                  final Set<TaskId> taskIds) {
            if (pairs.size() == maxPairs) {
                return false;
            }
            for (final TaskId taskId : taskIds) {
                if (!pairs.contains(pair(task1, taskId))) {
                    return true;
                }
            }
            return false;
        }

        public void addPairs(final TaskId taskId, final Set<TaskId> assigned) {
            for (final TaskId id : assigned) {
                pairs.add(pair(id, taskId));
            }
        }

        public TaskPair pair(final TaskId task1, final TaskId task2) {
            if (task1.compareTo(task2) < 0) {
                return new TaskPair(task1, task2);
            }
            return new TaskPair(task2, task1);
        }
    }

    private static class TaskPair {
        private final TaskId task1;
        private final TaskId task2;

        TaskPair(final TaskId task1, final TaskId task2) {
            this.task1 = task1;
            this.task2 = task2;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final TaskPair pair = (TaskPair) o;
            return Objects.equals(task1, pair.task1) &&
                   Objects.equals(task2, pair.task2);
        }

        @Override
        public int hashCode() {
            return Objects.hash(task1, task2);
        }
    }
}
