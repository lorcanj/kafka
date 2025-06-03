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
package org.apache.kafka.streams.processor.internals.assignment;

import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.assignment.ApplicationState;
import org.apache.kafka.streams.processor.assignment.KafkaStreamsAssignment;
import org.apache.kafka.streams.processor.assignment.KafkaStreamsState;
import org.apache.kafka.streams.processor.assignment.ProcessId;
import org.apache.kafka.streams.processor.assignment.TaskAssignmentUtils;
import org.apache.kafka.streams.processor.assignment.TaskAssignor;
import org.apache.kafka.streams.processor.internals.Task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.kafka.common.utils.Utils.diff;
import static org.apache.kafka.streams.processor.assignment.KafkaStreamsAssignment.AssignedTask.Type.ACTIVE;
import static org.apache.kafka.streams.processor.assignment.KafkaStreamsAssignment.AssignedTask.Type.STANDBY;
import static org.apache.kafka.streams.processor.internals.assignment.TaskMovement.assignActiveTaskMovements;
import static org.apache.kafka.streams.processor.internals.assignment.TaskMovement.assignStandbyTaskMovements;


public class HighAvailabilityAssignor implements TaskAssignor {
    private static final Logger log = LoggerFactory.getLogger(HighAvailabilityAssignor.class);
    public static final int DEFAULT_HIGH_AVAILABILITY_TRAFFIC_COST = 10;
    public static final int DEFAULT_HIGH_AVAILABILITY_NON_OVERLAP_COST = 1;

    @Override
    public TaskAssignment assign(final ApplicationState applicationState) {
        // KafkaStreamsState - immutable input
        // therefore need another class to use to hold the current data during re-balance
        // i.e. HighAvailabilityClientState
        final Map<ProcessId, KafkaStreamsState> clients = applicationState.kafkaStreamsStates(false);
        // Lorcan
        // this can probably just be done at the end
        final HighAvailabilityAssignor.AssignmentState assignmentState = new HighAvailabilityAssignor.AssignmentState(applicationState, clients);
        // HighAvailabilityClientState need these created for each ClientState
        final SortedSet<TaskId> statefulTasks = applicationState.allTasks().entrySet().stream().filter(entry -> entry.getValue().isStateful())
                .map(Map.Entry::getKey).collect(Collectors.toCollection(TreeSet::new));


        // assignStateful tasks updates newAssignments
        assignActiveStatefulTasks(applicationState, assignmentState, statefulTasks, clients.values());
        optimiseActiveStatefulTasks(applicationState, assignmentState);


        // should use the mapProcessToClientStateRebalanceDTO, but at this point the map we want to use is stale
        // first update mapProcessToClientStateRebalanceDTO
        resetAndUpdateDTOMapActiveTasks(assignmentState);

        // now create the clientStatesOld
        // TODO: check if is correct
        final TreeMap<ProcessId, ClientState> clientStatesOLD = translateToLegacyClientStateMap(clients, assignmentState);

        // the below needs the old ClientStates, as I'm still using the old assignor
        assignStandbyReplicaTasks(applicationState, assignmentState, statefulTasks, clients.values(), clientStatesOLD);

        // after this, DTO and clientStateOLD are both stale
        optimizeStandbyTasks(applicationState, assignmentState);

        // need to update DTO and clientStateOLD with the standby assignments in new assignments
        // TODO: might not be fully correct re ClientState
        updateDTOAndClientStateStandbys(assignmentState, clientStatesOLD);

        final AtomicInteger remainingWarmupReplicas = new AtomicInteger(applicationState.assignmentConfigs().maxWarmupReplicas());

        final Map<TaskId, SortedSet<ProcessId>> tasksToCaughtUpClients = AssignmentState.tasksToCaughtUpClients(
                statefulTasks,
                assignmentState.mapProcessToClientStateRebalanceDTO,
                applicationState
        );

        final Map<TaskId, SortedSet<ProcessId>> tasksToClientByLag = AssignmentState.tasksToClientByLag(statefulTasks, assignmentState.mapProcessToClientStateRebalanceDTO, applicationState);

        // We temporarily need to know which standby tasks were intended as warmups
        // for active tasks, so that we don't move them (again) when we plan standby
        // task movements. We can then immediately treat warmups exactly the same as
        // hot-standby replicas, so we just track it right here as metadata, rather
        // than add "warmup" assignments to ClientState, for example.
        final Map<ProcessId, Set<TaskId>> warmups = new TreeMap<>();

        // Lorcan
        final int neededActiveTaskMovements = assignActiveTaskMovements(
                tasksToCaughtUpClients,
                tasksToClientByLag,
                // might be wrong as stale data
                clientStatesOLD,
                warmups,
                remainingWarmupReplicas
        );

        final int neededStandbyTaskMovements = assignStandbyTaskMovements(
                tasksToCaughtUpClients,
                tasksToClientByLag,
                // might be wrong as stale data
                clientStatesOLD,
                remainingWarmupReplicas,
                warmups
        );

        // by this point clientStatesOLD is the most up to date for active and standby tasks
        // the above 2 function calls does further balancing, so need to update again
        reorderDTOAndAssignmentState(assignmentState, clientStatesOLD);
        // after reorderDTOAndAssignmentState, newAssignments, DTO and clientState are all up to date

        assignStatelessActiveTasks(applicationState, assignmentState, diff(TreeSet::new, applicationState.allTasks().keySet(), statefulTasks));
        //
        optimizeStatelessTasks(applicationState, assignmentState);

        final Map<ProcessId, KafkaStreamsAssignment> finalAssignments = assignmentState.newAssignments;

        final boolean probingRebalanceNeeded = neededActiveTaskMovements + neededStandbyTaskMovements > 0;

        if (probingRebalanceNeeded && !finalAssignments.isEmpty()) {
            // We set the followup deadline for only one of the clients.
            final ProcessId clientId = finalAssignments.entrySet().iterator().next().getKey();
            final KafkaStreamsAssignment previousAssignment = finalAssignments.get(clientId);
            // taken from StickyTaskAssignor
            finalAssignments.put(clientId, previousAssignment.withFollowupRebalance(Instant.ofEpochMilli(0)));
        }

        // TODO: probably just remove this
        // final TreeMap<ProcessId, KafkaStreamsState> clientStates = new TreeMap<>(clients);
        // log.info("Decided on assignment: {} with {} followup probing rebalance.", clientStates, probingRebalanceNeeded ? "" : " no");

        return new TaskAssignment(finalAssignments.values());
    }

    private static void reorderDTOAndAssignmentState(final AssignmentState assignmentState, final Map<ProcessId, ClientState> clientStateMap) {

        for (final var clientMap : clientStateMap.entrySet()) {
            final HighAvailabilityClientState currentAssignment = assignmentState.mapProcessToClientStateRebalanceDTO.get(clientMap.getKey());
            currentAssignment.assignedActiveTasks.taskIds = new HashSet<>(clientMap.getValue().activeTasks());
            currentAssignment.assignedStandbyTasks.taskIds = new HashSet<>(clientMap.getValue().standbyTasks());

            // want to update below
            // below is wrong as I'm updating the clientState and not the KafkaStreamsAssignment

            // final KafkaStreamsAssignment currentStreamsAssignment = assignmentState.newAssignments.get(clientMap.getKey());
            // up to date assigned tasks for given processId from ClientState
            final Set<TaskId> activeAssignedTaskIds = clientMap.getValue().activeTasks();
            final Set<TaskId> standbyAssignedTaskIds = clientMap.getValue().standbyTasks();

            assignmentState.newAssignments.clear();

            for (final var blah : activeAssignedTaskIds) {
                assignmentState.finalizeAssignment(blah, clientMap.getKey(), KafkaStreamsAssignment.AssignedTask.Type.ACTIVE);
            }

            for (final var blah : standbyAssignedTaskIds) {
                assignmentState.finalizeAssignment(blah, clientMap.getKey(), KafkaStreamsAssignment.AssignedTask.Type.STANDBY);
            }

//            for (var blah : currentStreamsAssignment.tasks().entrySet()) {
//            // switch (blah.getValue().type())
//                // for stale assignments, if taskId not in clientStateMap, want to unassign it
//                if (!activeAssignedTaskIds.contains(blah.getKey())) {
//                    clientMap.getValue().unassignActive(blah.getKey());
//                } else {
//                    // intersection means it's already correctly assigned
//                    activeAssignedTaskIds.remove(blah.getKey());
//                }
//                if (!standbyAssignedTaskIds.contains(blah.getKey())) {
//                    clientMap.getValue().unassignStandby(blah.getKey());
//                } else {
//                    // intersection means it's already correctly assigned
//                    standbyAssignedTaskIds.remove(blah.getKey());
//                }
//            }
//            // now have all the taskIds, use that to get task and assign it
//            for (var blah : activeAssignedTaskIds) {
//                // is the task active or standby?
//                clientMap.getValue().assignActive(blah);
//            }
//            for (var blah : standbyAssignedTaskIds) {
//                clientMap.getValue().assignStandby(blah);
//            }
        }
    }

    // TODO: good first attempt
    private TreeMap<ProcessId, ClientState> translateToLegacyClientStateMap(
            final Map<ProcessId, KafkaStreamsState> newStates,
            final AssignmentState assignmentState) {

        final TreeMap<ProcessId, ClientState> legacyClientStates = new TreeMap<>();

        for (final Map.Entry<ProcessId, KafkaStreamsState> entry : newStates.entrySet()) {
            final ProcessId processId = entry.getKey();
            final KafkaStreamsState newState = entry.getValue(); // The NEW state object

            final ClientState legacyClientState = new ClientState(processId, entry.getValue().numProcessingThreads());

            // add previous active and standby tasks to the clientState
            legacyClientState.addPreviousActiveTasks(newState.previousActiveTasks());
            legacyClientState.addPreviousStandbyTasks(newState.previousStandbyTasks());

            // 5. Copy other fields (HostInfo, RackID) if legacy ClientState has them and legacy logic needs them.
            // legacyClientState.setHostInfo(newState.hostInfo());
            // newState.rackId().ifPresent(legacyClientState::setRackId); // Handle Optional if needed

            // 6. Put the populated legacy state into the map
            legacyClientStates.put(processId, legacyClientState);
        }

        // assign active and standby tasks to correctly populate the ClientState map
        // this is super ugly and quite expensive to do
        for (final var thing : assignmentState.mapProcessToClientStateRebalanceDTO.entrySet()) {
            final ClientState clientStateToUpdate = legacyClientStates.get(thing.getKey());
            if (clientStateToUpdate != null) {
                for (final var taskId : thing.getValue().activeTasks()) {
                    clientStateToUpdate.assignActive(taskId);
                }
                for (final var taskId : thing.getValue().standbyTasks()) {
                    clientStateToUpdate.assignStandby(taskId);
                }
            }
        }

        return legacyClientStates;
    }

    // TODO: not sure if ClientState object is being fully correctly updated
    private void updateDTOAndClientStateStandbys(final AssignmentState assignmentState, final Map<ProcessId, ClientState> clientStateMap) {

        // for each processId, want all of the assigned standby tasks, then assign then to the DTO
        // and the clientStates map
        for (final var thing : assignmentState.newAssignments.values()) {
            final var t = thing.tasks().entrySet().stream()
                    .filter(b -> b.getValue().type() == STANDBY)
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet());

            final HighAvailabilityClientState currentHAAState = assignmentState.mapProcessToClientStateRebalanceDTO.get(thing.processId());

            currentHAAState.assignedStandbyTasks.setTaskIds(t);

            // Lorcan
            // below is awful but necessary atm
            final ClientState currentClientState = clientStateMap.get(thing.processId());
            for (final TaskId taskId : currentClientState.standbyTasks()) {
                currentClientState.unassignStandby(taskId);
            }

            for (final TaskId taskId : t) {
                currentClientState.assignStandby(taskId);
            }
        }

//        for (var thing : assignmentState.)
//        for (var thing : assignmentState.mapProcessToClientStateRebalanceDTO.entrySet()) {
//            ProcessId currentProcessId = thing.getKey();
//            thing.getValue()
//        }
    }

    // when do I need to update this?
    // after rackOptimisation?
    private void resetAndUpdateDTOMapActiveTasks(final AssignmentState assignmentState) {
        for (final var thing : assignmentState.mapProcessToClientStateRebalanceDTO.entrySet()) {
            thing.getValue().assignedActiveTasks.taskIds.clear();

            final Set<TaskId> activeTaskIds = assignmentState.newAssignments.get(thing.getKey()).tasks().entrySet()
                    .stream()
                    .filter(task -> task.getValue().type() == ACTIVE)
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet());
            thing.getValue().assignedActiveTasks.setTaskIds(activeTaskIds);
        }
    }

    // loop over mapProcessToClientStateRebalanceDTO rather than clientStateMap might be wrong
    private static void resetAndUpdateDTOMapStandbyTasks(final AssignmentState assignmentState, final Map<ProcessId, ClientState> clientStateMap) {
        for (final var thing : assignmentState.mapProcessToClientStateRebalanceDTO.entrySet()) {
            thing.getValue().assignedStandbyTasks.taskIds.clear();

            final Set<TaskId> standbyTaskIds = clientStateMap.get(thing.getKey()).standbyTasks();
            thing.getValue().assignedStandbyTasks.setTaskIds(standbyTaskIds);
        }
    }

    private static void updateDTOMapAndClientStateStandbyTasks(final AssignmentState assignmentState, final Map<ProcessId, ClientState> clientStateMap) {
        // TODO lorcan update
        for (final var thing : assignmentState.newAssignments.entrySet()) {
            final Set<TaskId> standbyTasks = thing.getValue().tasks().entrySet()
                    .stream()
                    .filter(input -> input.getValue().type() == STANDBY)
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet());

            assignmentState.mapProcessToClientStateRebalanceDTO.get(thing.getKey()).assignedStandbyTasks.taskIds.clear();
            assignmentState.mapProcessToClientStateRebalanceDTO.get(thing.getKey()).assignedStandbyTasks.setTaskIds(standbyTasks);

            // TODO: need to complete this
            final ClientState clientState = clientStateMap.get(thing.getKey());
            standbyTasks.forEach(clientState::assignStandby);
        }

        for (final var thing : assignmentState.mapProcessToClientStateRebalanceDTO.entrySet()) {
            thing.getValue().assignedStandbyTasks.taskIds.clear();

            final Set<TaskId> standbyTaskIds = assignmentState.newAssignments.get(thing.getKey()).tasks().entrySet()
                    .stream()
                    .filter(task -> task.getValue().type() == STANDBY)
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet());
            thing.getValue().assignedStandbyTasks.setTaskIds(standbyTaskIds);
        }
    }

    // here we want to loop in a similar way to the original method
    private static void assignActiveStatefulTasks(final ApplicationState applicationState,
                                                  final AssignmentState assignmentState,
                                                  final SortedSet<TaskId> statefulTasks,
                                                  final Collection<KafkaStreamsState> clients) {

        // think the below is initially just round robin
        Iterator<HighAvailabilityClientState> highAvailabilityClientStateIterator = null;
        for (final TaskId task : statefulTasks) {
            if (highAvailabilityClientStateIterator == null || !highAvailabilityClientStateIterator.hasNext()) {
                highAvailabilityClientStateIterator = assignmentState.mapProcessToClientStateRebalanceDTO.values().iterator();
            }
            highAvailabilityClientStateIterator.next().assignActive(task);
        }

        // Lorcan
        // commented out the below because want to use the HAA client object
//        // ugly but might be it
//        final List<TaskId> sortedStatefulTasks = new ArrayList<>(statefulTasks);
//        Collections.sort(sortedStatefulTasks);
//        final SortedSet<ProcessId> candidateClients = (SortedSet<ProcessId>) clients.stream()
//                .map(KafkaStreamsState::processId)
//                .collect(Collectors.toSet());
//        Iterator<ProcessId> consumerClientIdIterator = null;
//        for (final TaskId task : sortedStatefulTasks) {
//            if (consumerClientIdIterator == null || !consumerClientIdIterator.hasNext()) {
//                consumerClientIdIterator = candidateClients.iterator();
//            }
//
//            assignmentState.finalizeAssignment(task, consumerClientIdIterator.next(), KafkaStreamsAssignment.AssignedTask.Type.ACTIVE);
//        }

        balanceTasksOverThreads(
                assignmentState.mapProcessToClientStateRebalanceDTO,
                HighAvailabilityClientState::activeTasks,
                HighAvailabilityClientState::unassignActive,
                HighAvailabilityClientState::assignActive,
                (source, destination) -> true
        );

        //
        populateNewActiveAssignments(assignmentState);

        // at this point will want to populate the processId map to KafkaStreamsState to then use the Utils stuff for rack optimisation
        // using the assignmentStateDTO need to create/ populate the assignmentState.newAssignments
    }

    private static void optimizeStandbyTasks(final ApplicationState applicationState, final AssignmentState assignmentState) {
        if (applicationState.assignmentConfigs().numStandbyReplicas() <= 0) {
            return;
        }

        final Map<ProcessId, KafkaStreamsAssignment> assignments = assignmentState.newAssignments;

        final TaskAssignmentUtils.RackAwareOptimizationParams optimizationParams = TaskAssignmentUtils.RackAwareOptimizationParams.of(applicationState)
                .withTrafficCostOverride(
                        applicationState.assignmentConfigs().rackAwareTrafficCost().orElse(DEFAULT_HIGH_AVAILABILITY_TRAFFIC_COST)
                )
                .withNonOverlapCostOverride(
                        applicationState.assignmentConfigs().rackAwareNonOverlapCost().orElse(DEFAULT_HIGH_AVAILABILITY_NON_OVERLAP_COST)
                );
        TaskAssignmentUtils.optimizeRackAwareStandbyTasks(optimizationParams, assignments);
        // by this point they are optimised
        assignmentState.newAssignments = assignments;
    }

    // might need to split this, not 100% sure though
    private void optimiseActiveStatefulTasks(final ApplicationState applicationState,
                                final AssignmentState assignmentState) {

        final Map<ProcessId, KafkaStreamsAssignment> assignments = assignmentState.newAssignments;

        final TaskAssignmentUtils.RackAwareOptimizationParams statefulTaskParams = TaskAssignmentUtils.RackAwareOptimizationParams.of(applicationState)
                .withTrafficCostOverride(
                        applicationState.assignmentConfigs().rackAwareTrafficCost().orElse(DEFAULT_HIGH_AVAILABILITY_TRAFFIC_COST)
                )
                .withNonOverlapCostOverride(
                        applicationState.assignmentConfigs().rackAwareNonOverlapCost().orElse(DEFAULT_HIGH_AVAILABILITY_NON_OVERLAP_COST)
                )
                .forStatefulTasks();
        TaskAssignmentUtils.optimizeRackAwareActiveTasks(statefulTaskParams, assignments);

        // by this point they are optimised
        assignmentState.newAssignments = assignments;
    }

    private void optimizeStatelessTasks(final ApplicationState applicationState, final AssignmentState assignmentState) {

        final Map<ProcessId, KafkaStreamsAssignment> assignments = assignmentState.newAssignments;

        TaskAssignmentUtils.optimizeRackAwareActiveTasks(
                TaskAssignmentUtils.RackAwareOptimizationParams.of(applicationState)
                        .forStatelessTasks()
                        .withTrafficCostOverride(RackAwareTaskAssignor.STATELESS_TRAFFIC_COST)
                        .withNonOverlapCostOverride(RackAwareTaskAssignor.STATELESS_NON_OVERLAP_COST),
                assignments
        );
        assignmentState.newAssignments = assignments;
    }

    private static void populateNewStatelessActiveAssignments(final AssignmentState assignmentState, final Map<ProcessId, Set<TaskId>> statelessTasksMap) {
        for (final var thing : statelessTasksMap.entrySet()) {
            for (final var blah : thing.getValue()) {
                assignmentState.finalizeAssignment(blah, thing.getKey(), KafkaStreamsAssignment.AssignedTask.Type.ACTIVE);
            }
        }
    }

    private static void populateNewActiveAssignments(final AssignmentState assignmentState) {
        for (final var thing : assignmentState.mapProcessToClientStateRebalanceDTO.entrySet()) {
            // probably will want to make generic
            for (final var blah : thing.getValue().activeTasks()) {
                assignmentState.finalizeAssignment(blah, thing.getKey(), KafkaStreamsAssignment.AssignedTask.Type.ACTIVE);
            }
        }
    }

    private static void populateNewStandbyAssignments(final AssignmentState assignmentState) {
        for (final var thing : assignmentState.mapProcessToClientStateRebalanceDTO.entrySet()) {
            for (final var blah : thing.getValue().standbyTasks()) {
                assignmentState.finalizeAssignment(blah, thing.getKey(), STANDBY);
            }
        }
    }

    private static void assignStatelessActiveTasks(final ApplicationState applicationState,
                                                   final AssignmentState assignmentState,
                                                   final Iterable<TaskId> statelessTasks) {

        final ConstrainedPrioritySet statelessActiveTaskClientsByTaskLoad = new ConstrainedPrioritySet(
                (client, task) -> true,
                client -> assignmentState.mapProcessToClientStateRebalanceDTO.get(client).activeTaskLoad()
        );
        statelessActiveTaskClientsByTaskLoad.offerAll(assignmentState.mapProcessToClientStateRebalanceDTO.keySet());
        final Map<ProcessId, Set<TaskId>> statelessTasksMap = new HashMap<>();
        // Lorcan, not sure about this treeset
        // final SortedSet<TaskId> = new TreeSet<>();
        for (final TaskId task : statelessTasks) {
            // sortedTasks.add(task);
            final ProcessId client = statelessActiveTaskClientsByTaskLoad.poll(task);
            final HighAvailabilityClientState state = assignmentState.mapProcessToClientStateRebalanceDTO.get(client);
            statelessTasksMap.computeIfAbsent(client, k -> new HashSet<>()).add(task);
            state.assignActive(task);
            statelessActiveTaskClientsByTaskLoad.offer(client);
        }

        // might be wrong as not 100% sure if can use this for the stateless active tasks
        // Lorcan
        // TODO: check as not sure if this is right for stateless tasks
        // now using statless specific mapper
        populateNewStatelessActiveAssignments(assignmentState, statelessTasksMap);
    }

    private static void assignStandbyReplicaTasks(final ApplicationState applicationState,
                                                  final AssignmentState assignmentState,
                                                  final SortedSet<TaskId> statefulTasks,
                                                  final Collection<KafkaStreamsState> clients,
                                                  final TreeMap<ProcessId, ClientState> clientStatesOLD) {

        if (applicationState.assignmentConfigs().numStandbyReplicas() == 0) {
            return;
        }

        final StandbyTaskAssignor standbyTaskAssignor = StandbyTaskAssignorFactory.create(applicationState.assignmentConfigs(), null);

        standbyTaskAssignor.assign(clientStatesOLD, applicationState.allTasks().keySet(), statefulTasks, applicationState.assignmentConfigs());

        // above has assigned using the ClientState map
        // so now the most up to date assignment for standby tasks is in clientStatesOLD

        // shouldn't this use clientStatesOLD to update the state?
        // Yes
        // TODO: the below is wrong, should update using the clientStates map
        resetAndUpdateDTOMapStandbyTasks(assignmentState, clientStatesOLD);

        // now clientStatesOLD and mapProcessToClientStateRebalanceDTO are up to date
        balanceTasksOverThreadsClientState(
                assignmentState.mapProcessToClientStateRebalanceDTO,
                HighAvailabilityClientState::standbyTasks,
                HighAvailabilityClientState::unassignStandby,
                HighAvailabilityClientState::assignStandby,
                standbyTaskAssignor::isAllowedTaskMovement,
                clientStatesOLD
        );
        // from this point now mapProcessToClientStateRebalanceDTO is up to date
        populateNewStandbyAssignments(assignmentState);
        // from this point now assignmentState.newAssignments is up to date too
    }

    // ugly but might work
    // currently uses a mix of HAAClientState
    // and ClientState because it uses taskMovementAttemptPredicate which is from the StandbyAssignor
    private static void balanceTasksOverThreadsClientState(final SortedMap<ProcessId, HighAvailabilityClientState> clientStates,
                                                final Function<HighAvailabilityClientState, Set<TaskId>> currentAssignmentAccessor,
                                                final BiConsumer<HighAvailabilityClientState, TaskId> taskUnassignor,
                                                final BiConsumer<HighAvailabilityClientState, TaskId> taskAssignor,
                                                final BiPredicate<ClientState, ClientState> taskMovementAttemptPredicate,
                                                final SortedMap<ProcessId, ClientState> clientStatesOLD) {
        boolean keepBalancing = true;
        while (keepBalancing) {
            keepBalancing = false;
            for (final Map.Entry<ProcessId, HighAvailabilityClientState> sourceEntry : clientStates.entrySet()) {
                final ProcessId sourceClient = sourceEntry.getKey();
                final HighAvailabilityClientState sourceClientAssignmentState = sourceEntry.getValue();
                // Lorcan, not 100% sure about this
                final ClientState sourceClientState = clientStatesOLD.get(sourceEntry.getKey());

                for (final Map.Entry<ProcessId, HighAvailabilityClientState> destinationEntry : clientStates.entrySet()) {
                    final ProcessId destinationClient = destinationEntry.getKey();
                    final HighAvailabilityClientState destinationClientAssignmentState = destinationEntry.getValue();
                    if (sourceClient.equals(destinationClient)) {
                        continue;
                    }
                    final ClientState destinationclientState = clientStatesOLD.get(destinationEntry.getKey());

                    final Set<TaskId> sourceTasks = new TreeSet<>(currentAssignmentAccessor.apply(sourceClientAssignmentState));
                    final Iterator<TaskId> sourceIterator = sourceTasks.iterator();
                    // re-ordered these
                    while (sourceIterator.hasNext() && AssignmentState.shouldMoveATask(sourceClientAssignmentState, destinationClientAssignmentState)) {
                        final TaskId taskToMove = sourceIterator.next();
                        final boolean canMove = !destinationClientAssignmentState.hasAssignedTask(taskToMove)
                                // When ClientTagAwareStandbyTaskAssignor is used, we need to make sure that
                                // sourceClient tags matches destinationClient tags.
                                && taskMovementAttemptPredicate.test(sourceClientState, destinationclientState);
                        if (canMove) {
                            taskUnassignor.accept(sourceClientAssignmentState, taskToMove);
                            taskAssignor.accept(destinationClientAssignmentState, taskToMove);
                            keepBalancing = true;
                        }
                    }
                }
            }
        }
    }

    private static void balanceTasksOverThreads(final SortedMap<ProcessId, HighAvailabilityClientState> clientStates,
                                                final Function<HighAvailabilityClientState, Set<TaskId>> currentAssignmentAccessor,
                                                final BiConsumer<HighAvailabilityClientState, TaskId> taskUnassignor,
                                                final BiConsumer<HighAvailabilityClientState, TaskId> taskAssignor,
                                                final BiPredicate<HighAvailabilityClientState, HighAvailabilityClientState> taskMovementAttemptPredicate) {
        boolean keepBalancing = true;
        while (keepBalancing) {
            keepBalancing = false;
            for (final Map.Entry<ProcessId, HighAvailabilityClientState> sourceEntry : clientStates.entrySet()) {
                final ProcessId sourceClient = sourceEntry.getKey();
                final HighAvailabilityClientState sourceClientAssignmentState = sourceEntry.getValue();

                for (final Map.Entry<ProcessId, HighAvailabilityClientState> destinationEntry : clientStates.entrySet()) {
                    final ProcessId destinationClient = destinationEntry.getKey();
                    final HighAvailabilityClientState destinationClientAssignmentState = destinationEntry.getValue();
                    if (sourceClient.equals(destinationClient)) {
                        continue;
                    }

                    final Set<TaskId> sourceTasks = new TreeSet<>(currentAssignmentAccessor.apply(sourceClientAssignmentState));
                    final Iterator<TaskId> sourceIterator = sourceTasks.iterator();
                    while (sourceIterator.hasNext() && AssignmentState.shouldMoveATask(sourceClientAssignmentState, destinationClientAssignmentState)) {
                        final TaskId taskToMove = sourceIterator.next();
                        final boolean canMove = !destinationClientAssignmentState.hasAssignedTask(taskToMove)
                                                // When ClientTagAwareStandbyTaskAssignor is used, we need to make sure that
                                                // sourceClient tags matches destinationClient tags.
                                                && taskMovementAttemptPredicate.test(sourceClientAssignmentState, destinationClientAssignmentState);
                        if (canMove) {
                            taskUnassignor.accept(sourceClientAssignmentState, taskToMove);
                            taskAssignor.accept(destinationClientAssignmentState, taskToMove);
                            keepBalancing = true;
                        }
                    }
                }
            }
        }
    }

    // should only have a single assignmentState
    public static class AssignmentState {
        private Map<ProcessId, KafkaStreamsAssignment> newAssignments;
        private final SortedMap<ProcessId, HighAvailabilityClientState> mapProcessToClientStateRebalanceDTO;

        // do I need assigned active and assigned standby
        AssignmentState(final ApplicationState applicationState,
                        final Map<ProcessId, KafkaStreamsState> clients) {

            // Lorcan
            this.newAssignments = clients.values().stream().collect(Collectors.toMap(
                    KafkaStreamsState::processId,
                    state -> KafkaStreamsAssignment.of(state.processId(), new HashSet<>())
            ));

            // Lorcan
            // If there are duplicates, just take the current value
            // might be wrong
            this.mapProcessToClientStateRebalanceDTO = clients.values().stream().collect(Collectors.toMap(
                    KafkaStreamsState::processId,
                    state -> new HighAvailabilityClientState(state.processId(), state.numProcessingThreads()),
                    (existingValue, newValue) -> existingValue,
                    TreeMap::new
            ));
        }

        // TODO: might need to update this
        // need this to update mapProcessToClientStateRebalanceDTO
        // need the below to then create this from the other map
        private void finalizeAssignment(final TaskId taskId, final ProcessId client, final KafkaStreamsAssignment.AssignedTask.Type type) {
            // currently null pointer but will just want to update this at the end
            // want to check if has been assigned already because passing in the whole thing for standby currently which I think is wrong
            newAssignments.get(client).assignTask(new KafkaStreamsAssignment.AssignedTask(taskId, type));
        }

        private static boolean shouldMoveATask(final HighAvailabilityClientState sourceClientState, final HighAvailabilityClientState destinationClientState) {
            final double skew = sourceClientState.assignedTaskLoad() - destinationClientState.assignedTaskLoad();

            if (skew <= 0) {
                return false;
            }

            final double proposedAssignedTasksPerStreamThreadAtDestination =
                    (destinationClientState.assignedTaskCount() + 1.0) / destinationClientState.capacity();
            final double proposedAssignedTasksPerStreamThreadAtSource =
                    (sourceClientState.assignedTaskCount() - 1.0) / sourceClientState.capacity();
            final double proposedSkew = proposedAssignedTasksPerStreamThreadAtSource - proposedAssignedTasksPerStreamThreadAtDestination;
            if (proposedSkew < 0) {
                // then the move would only create an imbalance in the other direction.
                return false;
            }
            // we should only move a task if doing so would actually improve the skew.
            return proposedSkew < skew;
        }

        private static Map<TaskId, SortedSet<ProcessId>> tasksToCaughtUpClients(final Set<TaskId> statefulTasks,
                                                                                final Map<ProcessId, HighAvailabilityClientState> clientStates,
                                                                                final ApplicationState applicationState) {

            final long acceptableRecoveryLag = applicationState.assignmentConfigs().acceptableRecoveryLag();
            final Map<TaskId, SortedSet<ProcessId>> taskToCaughtUpClients = new HashMap<>();

            for (final TaskId task : statefulTasks) {
                final TreeSet<ProcessId> caughtUpClients = new TreeSet<>();
                for (final Map.Entry<ProcessId, HighAvailabilityClientState> clientEntry : clientStates.entrySet()) {
                    final ProcessId client = clientEntry.getKey();
                    // not sure if this should be true or false;
                    // not sure if this is correct but it might be
                    final long taskLag = applicationState.kafkaStreamsStates(false).get(client).lagFor(task);
                    // final long taskLag = clientEntry.getValue().lagFor(task);
                    if (activeRunning(taskLag) || unbounded(acceptableRecoveryLag) || acceptable(acceptableRecoveryLag, taskLag)) {
                        caughtUpClients.add(client);
                    }
                }
                taskToCaughtUpClients.put(task, caughtUpClients);
            }
            return taskToCaughtUpClients;
        }

        private static Map<TaskId, SortedSet<ProcessId>> tasksToClientByLag(final Set<TaskId> statefulTasks,
                                                                            final Map<ProcessId, HighAvailabilityClientState> clientStates,
                                                                            final ApplicationState applicationState) {
            final Map<TaskId, SortedSet<ProcessId>> tasksToClientByLag = new HashMap<>();
            for (final TaskId task : statefulTasks) {
                final SortedSet<ProcessId> clientLag = new TreeSet<>(Comparator.<ProcessId>comparingLong(a ->
                        // not 100% sure about the below
                        applicationState.kafkaStreamsStates(false).get(a).lagFor(task)).thenComparing(a -> a));
                clientLag.addAll(clientStates.keySet());
                tasksToClientByLag.put(task, clientLag);
            }
            return tasksToClientByLag;
        }

        private static boolean unbounded(final long acceptableRecoveryLag) {
            return acceptableRecoveryLag == Long.MAX_VALUE;
        }

        private static boolean acceptable(final long acceptableRecoveryLag, final long taskLag) {
            return taskLag >= 0 && taskLag <= acceptableRecoveryLag;
        }

        private static boolean activeRunning(final long taskLag) {
            return taskLag == Task.LATEST_OFFSET;
        }
    }

    // used to hold the data during the assignment
    static class AssignmentClientStateTask {
        // TODO: not updating consumerToTaskIds in mapping, might be wrong
        // Check if need to change
        private final Map<String, Set<TaskId>> consumerToTaskIds;
        private Set<TaskId> taskIds;

        AssignmentClientStateTask(final Set<TaskId> taskIds,
                                  final Map<String, Set<TaskId>> consumerToTaskIds) {
            this.taskIds = taskIds;
            this.consumerToTaskIds = consumerToTaskIds;
        }

        void setTaskIds(final Set<TaskId> clientToTaskIds) {
            taskIds = clientToTaskIds;
        }

        Set<TaskId> taskIds() {
            return taskIds;
        }

        Map<String, Set<TaskId>> consumerToTaskIds() {
            return consumerToTaskIds;
        }
    }

    // this should be per each client instance
    // but not contain everything that the ClientState holds
    static class HighAvailabilityClientState {
        // these should be for the specific clientState new object not this overall one

        // not sure if I need the complexity of these classes
        private final AssignmentClientStateTask assignedStandbyTasks = new AssignmentClientStateTask(new TreeSet<>(), new TreeMap<>());
        private final AssignmentClientStateTask assignedActiveTasks = new AssignmentClientStateTask(new TreeSet<>(), new TreeMap<>());

        // Lorcan
        // not sure if I need this as might be in the application state
        private final int capacity;
        // need this to map this object to the ClientState objects
        private final ProcessId processId;

        public HighAvailabilityClientState(final ProcessId processId, final int capacity) {
            this.processId = processId;
            this.capacity = capacity;
        }

        boolean hasAssignedTask(final TaskId taskId) {
            return assignedActiveTasks.taskIds().contains(taskId) || assignedStandbyTasks.taskIds().contains(taskId);
        }

        private int activeTaskCount() {
            return assignedActiveTasks.taskIds().size();
        }
        private int standbyTaskCount() {
            return assignedStandbyTasks.taskIds().size();
        }
        private int assignedTaskCount() {
            return activeTaskCount() + standbyTaskCount();
        }
        private double assignedTaskLoad() {
            return ((double) assignedTaskCount()) / capacity;
        }

        private int capacity() {
            return capacity;
        }

        public Set<TaskId> activeTasks() {
            return assignedActiveTasks.taskIds();
        }

        public Set<TaskId> standbyTasks() {
            return assignedStandbyTasks.taskIds();
        }

        public void assignActive(final TaskId task) {
            assertNotAssigned(task);
            assignedActiveTasks.taskIds().add(task);
        }

        public void unassignActive(final TaskId task) {
            final Set<TaskId> taskIds = assignedActiveTasks.taskIds();
            if (!taskIds.contains(task)) {
                throw new IllegalArgumentException("Tried to unassign active task " + task + ", but it is not currently assigned: " + this);
            }
            taskIds.remove(task);
        }

        private void assertNotAssigned(final TaskId task) {
            if (assignedStandbyTasks.taskIds().contains(task) || assignedActiveTasks.taskIds().contains(task)) {
                throw new IllegalArgumentException("Tried to assign task " + task + ", but it is already assigned: " + this);
            }
        }

        public void assignStandby(final TaskId task) {
            assertNotAssigned(task);
            assignedStandbyTasks.taskIds().add(task);
        }

        void unassignStandby(final TaskId task) {
            final Set<TaskId> taskIds = assignedStandbyTasks.taskIds();
            if (!taskIds.contains(task)) {
                throw new IllegalArgumentException("Tried to unassign standby task " + task + ", but it is not currently assigned: " + this);
            }
            taskIds.remove(task);
        }

        double activeTaskLoad() {
            return ((double) activeTaskCount()) / capacity;
        }

        // should have methods to just set the whole set of task ids to be that, but a bit dangerous because doing without the check

//        void setAllActiveTasks(Set<TaskId> newActiveTasks) {
//            assignedActiveTasks.taskIds
//        }

//        /**
//         * Returns the total lag across all logged stores in the task. Equal to the end offset sum if this client
//         * did not have any state for this task on disk.
//         *
//         * @return end offset sum - offset sum
//         *          Task.LATEST_OFFSET if this was previously an active running task on this client
//         */
//        public long lagFor(final TaskId task) {
//            final Long totalLag = taskLagTotals.get(task);
//            if (totalLag == null) {
//                throw new IllegalStateException("Tried to lookup lag for unknown task " + task);
//            }
//            return totalLag;
//        }
    }
}
