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

import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.assignment.ApplicationState;
import org.apache.kafka.streams.processor.assignment.AssignmentConfigs;
import org.apache.kafka.streams.processor.assignment.KafkaStreamsAssignment;
import org.apache.kafka.streams.processor.assignment.KafkaStreamsState;
import org.apache.kafka.streams.processor.assignment.ProcessId;
import org.apache.kafka.streams.processor.assignment.TaskAssignmentUtils;
import org.apache.kafka.streams.processor.assignment.TaskAssignor;
import org.apache.kafka.streams.processor.assignment.TaskAssignor.TaskAssignment;
import org.apache.kafka.streams.processor.assignment.TaskInfo;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.processor.assignment.KafkaStreamsAssignment.AssignedTask.Type.ACTIVE;
import static org.apache.kafka.streams.processor.assignment.KafkaStreamsAssignment.AssignedTask.Type.STANDBY;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.EMPTY_RACK_AWARE_ASSIGNMENT_TAGS;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.PID_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.PID_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.PID_3;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_3;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_4;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_5;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_2_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_2_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_2_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_3_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_3_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_3_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.verifyStandbySatisfyRackReplicaKafka;
import static org.apache.kafka.streams.processor.internals.assignment.TaskAssignmentUtilsTest.mkStreamState;
import static org.apache.kafka.streams.processor.internals.assignment.TaskAssignmentUtilsTest.mkTaskInfo;
import static org.apache.kafka.streams.processor.internals.assignment.TaskAssignmentUtilsTest.processId;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class CustomHighAvailabilityAssignorTest {

    private TaskAssignor assignor;

    @BeforeEach
    public void setUp() {
        assignor = new HighAvailabilityAssignor();
    }

    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY,
    })
    public void shouldAssignOneActiveTaskToEachProcessWhenTaskCountSameAsProcessCount(final String rackAwareStrategy) {
        final Map<ProcessId, KafkaStreamsState> streamStates = mkMap(
                mkStreamState(1, 1, Optional.empty()),
                mkStreamState(2, 1, Optional.empty()),
                mkStreamState(3, 1, Optional.empty())
        );
        final Map<TaskId, TaskInfo> tasks = mkMap(
                mkTaskInfo(TASK_0_0, false),
                mkTaskInfo(TASK_0_1, false),
                mkTaskInfo(TASK_0_2, false)
        );

        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks, rackAwareStrategy);
        for (final KafkaStreamsAssignment assignment : assignments.values()) {
            assertThat(assignment.tasks().size(), equalTo(1));
        }
    }

    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY,
    })
    public void shouldAssignTopicGroupIdEvenlyAcrossClientsWithNoStandByTasks(final String rackAwareStrategy) {
        final Map<ProcessId, KafkaStreamsState> streamStates = mkMap(
                mkStreamState(1, 2, Optional.empty()),
                mkStreamState(2, 2, Optional.empty()),
                mkStreamState(3, 2, Optional.empty())
        );
        final Map<TaskId, TaskInfo> tasks = mkMap(
                mkTaskInfo(TASK_1_0, false),
                mkTaskInfo(TASK_1_1, false),
                mkTaskInfo(TASK_2_2, false),
                mkTaskInfo(TASK_2_0, false),
                mkTaskInfo(TASK_2_1, false),
                mkTaskInfo(TASK_1_2, false)
        );

        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks, rackAwareStrategy);
        assertActiveTaskTopicGroupIdsEvenlyDistributed(assignments);
    }

    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY,
    })
    public void shouldAssignTopicGroupIdEvenlyAcrossClientsWithStandByTasks(final String rackAwareStrategy) {
        final Map<ProcessId, KafkaStreamsState> streamStates = mkMap(
                mkStreamState(1, 2, Optional.empty()),
                mkStreamState(2, 2, Optional.empty()),
                mkStreamState(3, 2, Optional.empty())
        );

        final Map<TaskId, TaskInfo> tasks = mkMap(
                mkTaskInfo(TASK_2_0, false),
                mkTaskInfo(TASK_1_1, false),
                mkTaskInfo(TASK_1_2, false),
                mkTaskInfo(TASK_1_0, false),
                mkTaskInfo(TASK_2_1, false),
                mkTaskInfo(TASK_2_2, false)
        );
        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks, 1, rackAwareStrategy);
        assertActiveTaskTopicGroupIdsEvenlyDistributed(assignments);
    }

    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY,
    })
    public void shouldAssignBasedOnCapacity(final String rackAwareStrategy) {
        final Map<TaskId, TaskInfo> tasks = mkMap(
                mkTaskInfo(TASK_0_0, false),
                mkTaskInfo(TASK_0_1, false),
                mkTaskInfo(TASK_0_2, false)
        );
        final Map<ProcessId, KafkaStreamsState> streamStates = mkMap(
                mkStreamState(1, 1, Optional.empty()),
                mkStreamState(2, 2, Optional.empty())
        );
        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks, rackAwareStrategy);
        assertThat(assignments.get(processId(1)).tasks().size(), equalTo(1));
        assertThat(assignments.get(processId(2)).tasks().size(), equalTo(2));
    }

    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY,
    })
    public void shouldAssignTasksEvenlyWithUnequalTopicGroupSizes(final String rackAwareStrategy) {
        final Map<TaskId, TaskInfo> tasks = mkMap(
                mkTaskInfo(TASK_1_0, false),
                mkTaskInfo(TASK_0_0, false),
                mkTaskInfo(TASK_0_1, false),
                mkTaskInfo(TASK_0_2, false),
                mkTaskInfo(TASK_0_3, false),
                mkTaskInfo(TASK_0_4, false),
                mkTaskInfo(TASK_0_5, false)
        );

        final Map<ProcessId, KafkaStreamsState> streamStates = mkMap(
                mkStreamState(1, 1, Optional.empty(), Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3, TASK_0_4, TASK_0_5, TASK_1_0), Set.of()),
                mkStreamState(2, 1, Optional.empty())
        );

        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks, rackAwareStrategy);
        final Set<TaskId> client1Tasks = assignments.get(processId(1)).tasks().values().stream()
                .filter(t -> t.type() == ACTIVE)
                .map(KafkaStreamsAssignment.AssignedTask::id)
                .collect(Collectors.toSet());
        final Set<TaskId> client2Tasks = assignments.get(processId(2)).tasks().values().stream()
                .filter(t -> t.type() == ACTIVE)
                .map(KafkaStreamsAssignment.AssignedTask::id)
                .collect(Collectors.toSet());

        final Set<TaskId> allTasks = tasks.keySet();

        // one client should get 3 tasks and the other should have 4
        assertThat(
                (client1Tasks.size() == 3 && client2Tasks.size() == 4) ||
                        (client1Tasks.size() == 4 && client2Tasks.size() == 3),
                is(true));
        allTasks.removeAll(client1Tasks);
        // client2 should have all the remaining tasks not assigned to client 1
        assertThat(client2Tasks, equalTo(allTasks));
    }

    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY,
    })
    public void shouldAssignAtLeastOneTaskToEachClientIfPossible(final String rackAwareStrategy) {
        final Map<TaskId, TaskInfo> tasks = mkMap(
                mkTaskInfo(TASK_0_0, false),
                mkTaskInfo(TASK_0_1, false),
                mkTaskInfo(TASK_0_2, false)
        );

        final Map<ProcessId, KafkaStreamsState> streamStates = mkMap(
                mkStreamState(1, 3, Optional.empty()),
                mkStreamState(2, 1, Optional.empty()),
                mkStreamState(3, 1, Optional.empty())
        );

        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks, rackAwareStrategy);
        assertThat(activeTasks(assignments, 1).size(), is(1));
        assertThat(activeTasks(assignments, 2).size(), is(1));
        assertThat(activeTasks(assignments, 3).size(), is(1));
    }

    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY,
    })
    public void shouldAssignEachActiveTaskToOneClientWhenMoreClientsThanTasks(final String rackAwareStrategy) {
        final Map<TaskId, TaskInfo> tasks = mkMap(
                mkTaskInfo(TASK_0_0, false),
                mkTaskInfo(TASK_0_1, false),
                mkTaskInfo(TASK_0_2, false)
        );

        final Map<ProcessId, KafkaStreamsState> streamStates = mkMap(
                mkStreamState(1, 1, Optional.empty()),
                mkStreamState(2, 1, Optional.empty()),
                mkStreamState(3, 1, Optional.empty()),
                mkStreamState(4, 1, Optional.empty()),
                mkStreamState(5, 1, Optional.empty()),
                mkStreamState(6, 1, Optional.empty())
        );

        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks, rackAwareStrategy);
        final List<KafkaStreamsAssignment.AssignedTask> allTasks = allTasks(assignments);
        assertThat(allTasks.stream().filter(t -> t.type() == ACTIVE).map(KafkaStreamsAssignment.AssignedTask::id).collect(
                Collectors.toSet()), equalTo(Set.of(TASK_0_0, TASK_0_1, TASK_0_2)));
        assertThat(allTasks.stream().filter(t -> t.type() == STANDBY).map(KafkaStreamsAssignment.AssignedTask::id).collect(
                Collectors.toSet()), equalTo(Set.of()));

        final int clientsWithATask = assignments.values().stream().mapToInt(assignment -> assignment.tasks().isEmpty() ? 0 : 1).sum();
        assertThat(clientsWithATask, greaterThanOrEqualTo(3));
    }


    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY,
    })
    public void shouldAssignMoreTasksToClientWithMoreCapacity(final String rackAwareStrategy) {
        final Map<TaskId, TaskInfo> tasks = mkMap(
                mkTaskInfo(TASK_0_0, false),
                mkTaskInfo(TASK_0_1, false),
                mkTaskInfo(TASK_0_2, false),
                mkTaskInfo(TASK_1_0, false),
                mkTaskInfo(TASK_1_1, false),
                mkTaskInfo(TASK_1_2, false),
                mkTaskInfo(TASK_2_0, false),
                mkTaskInfo(TASK_2_1, false),
                mkTaskInfo(TASK_2_2, false),
                mkTaskInfo(TASK_3_0, false),
                mkTaskInfo(TASK_3_1, false),
                mkTaskInfo(TASK_3_2, false)
        );

        final Map<ProcessId, KafkaStreamsState> streamStates = mkMap(
                mkStreamState(1, 1, Optional.empty()),
                mkStreamState(2, 2, Optional.empty())
        );

        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks, rackAwareStrategy);
        assertThat(activeTasks(assignments, 1).size(), equalTo(4));
        assertThat(activeTasks(assignments, 2).size(), equalTo(8));
    }

    static Stream<Arguments> parameter() {
        return Stream.of(
                Arguments.of(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE, false, 1),
                Arguments.of(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC, true, 1),
                Arguments.of(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY, true, 4)
        );
    }

    private AssignmentConfigs getConfigWithoutStandbys(final String rackAwareStrategy) {
        return new AssignmentConfigs(
                /*acceptableRecoveryLag*/ 100L,
                /*maxWarmupReplicas*/ 2,
                /*numStandbyReplicas*/ 0,
                /*probingRebalanceIntervalMs*/ 60 * 1000L,
                /*rackAwareAssignmentTags*/ EMPTY_RACK_AWARE_ASSIGNMENT_TAGS,
                null,
                null,
                rackAwareStrategy
        );
    }

    private AssignmentConfigs getConfigWithStandbys(final String rackAwareStrategy) {
        return getConfigWithStandbys(1, rackAwareStrategy);
    }

    private AssignmentConfigs getConfigWithStandbys(final int replicaNum, final String rackAwareStrategy) {
        return new AssignmentConfigs(
                /*acceptableRecoveryLag*/ 100L,
                /*maxWarmupReplicas*/ 2,
                /*numStandbyReplicas*/ replicaNum,
                /*probingRebalanceIntervalMs*/ 60 * 1000L,
                /*rackAwareAssignmentTags*/ EMPTY_RACK_AWARE_ASSIGNMENT_TAGS,
                null,
                null,
                rackAwareStrategy
        );
    }

    // Below taken from HAATask test file
    // Lorcan
    // TODO: update the assertions
    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldBeStickyForActiveAndStandbyTasksWhileWarmingUp(final String rackAwareStrategy) {

        final Set<TaskId> allTaskIds = Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_1_0, TASK_1_1, TASK_1_2, TASK_2_0, TASK_2_1, TASK_2_2);

        // 1. Create AssignmentConfigs
        final AssignmentConfigs configs = new AssignmentConfigs(
                11L, // acceptableRecoveryLag
                2,   // maxWarmupReplicas
                1,   // numStandbyReplicas
                60_000L, // probingRebalanceIntervalMs
                Collections.emptyList(), // rackAwareAssignmentTags
                OptionalInt.empty(),     // Corrected: Was Optional.empty() which might infer incorrectly
                OptionalInt.empty(),     // Corrected: Was Optional.empty()
                rackAwareStrategy
        );

        // 2. Create Map<TaskId, TaskInfo> - All tasks are stateful
        final Map<TaskId, TaskInfo> tasks = allTaskIds.stream()
                .collect(Collectors.toMap(
                        Function.identity(),
                        taskId -> mkTaskInfo(taskId, true).getValue() // true for stateful
                ));

        // 3. Create Map<ProcessId, KafkaStreamsState>
        final Map<ProcessId, KafkaStreamsState> clientStatesMap = new HashMap<>();
        final int capacity = 1; // All clients have capacity 1 (1 thread)

        // Client 1 (PID_1): Was all active, 0 lag
        final Map<TaskId, Long> lags1 = allTaskIds.stream().collect(Collectors.toMap(Function.identity(), t -> 0L));
        clientStatesMap.put(PID_1, mkStreamState(
                        1,                              // ProcessId (assuming your PID_1 is compatible with int, or the actual mkStreamState takes ProcessId)
                        capacity,                          // int
                        Optional.empty(),                  // Optional<String> for rackId
                        allTaskIds,                        // Set<TaskId> for previous active
                        emptySet(),                        // Set<TaskId> for previous standby
                        Collections.emptyMap(),            // Map<String, String> for client tags (added this, as it's in signature 3)
                        Optional.of(lags1)                 // Optional<Map<TaskId, Long>> for taskLags
                ).getValue()
        );

        // Client 2 (PID_2): Was all standby, lag 10
        final Map<TaskId, Long> lags2 = allTaskIds.stream().collect(Collectors.toMap(Function.identity(), t -> 10L));
        clientStatesMap.put(PID_2, mkStreamState(
                        2,                          // ProcessId (assuming your PID_1 is compatible with int, or the actual mkStreamState takes ProcessId)
                        capacity,                          // int
                        Optional.empty(),                  // Optional<String> for rackId
                        allTaskIds,                        // Set<TaskId> for previous active
                        emptySet(),            // Set<TaskId> for previous standby
                        Collections.emptyMap(),            // Map<String, String> for client tags (added this, as it's in signature 3)
                        Optional.of(lags2)                 // Optional<Map<TaskId, Long>> for taskLags
                ).getValue()
        );

        // Client 3 (PID_3): New/empty, max lag
        final Map<TaskId, Long> lags3 = allTaskIds.stream().collect(Collectors.toMap(Function.identity(), t -> Long.MAX_VALUE));
        clientStatesMap.put(PID_3, mkStreamState(
                        3,                          // ProcessId (assuming your PID_1 is compatible with int, or the actual mkStreamState takes ProcessId)
                        capacity,                          // int
                        Optional.empty(),                  // Optional<String> for rackId
                        allTaskIds,                        // Set<TaskId> for previous active
                        emptySet(),            // Set<TaskId> for previous standby
                        Collections.emptyMap(),            // Map<String, String> for client tags (added this, as it's in signature 3)
                        Optional.of(lags3)                 // Optional<Map<TaskId, Long>> for taskLags
                ).getValue()
        );

        // 4. Create ApplicationState
        final ApplicationState applicationState = new TaskAssignmentUtilsTest.TestApplicationState(
                configs,
                clientStatesMap,
                tasks
        );

        // 5. Instantiate and Call your New HighAvailabilityAssignor
        final TaskAssignor assignor = new HighAvailabilityAssignor(); // Your new HAA
        final TaskAssignment taskAssignment = assignor.assign(applicationState);

        // 6. Adapt Assertions
        final Map<ProcessId, KafkaStreamsAssignment> assignmentsByProcessId =
                taskAssignment.assignment().stream()
                        .collect(Collectors.toMap(KafkaStreamsAssignment::processId, Function.identity()));

        final KafkaStreamsAssignment assignment1 = assignmentsByProcessId.get(PID_1);
        assertThat("PID_1 assignment should exist", assignment1, notNullValue());
        assertThat(assignment1.tasks().size(), is(allTaskIds.size()));

        final KafkaStreamsAssignment assignment2 = assignmentsByProcessId.get(PID_2);
        assertThat("PID_2 assignment should exist", assignment2, notNullValue());
        assertThat(assignment2.tasks().size(), is(allTaskIds.size()));

        final KafkaStreamsAssignment assignment3 = assignmentsByProcessId.get(PID_3);
        assertThat("PID_3 assignment should exist", assignment3, notNullValue());
        assertThat(assignment3.tasks().size(), is(2));

        // Assert that a probing rebalance IS scheduled
        final boolean probingSignaled = taskAssignment.assignment().stream()
                .anyMatch(ka -> ka.followupRebalanceDeadline().isPresent() &&
                        ka.followupRebalanceDeadline().get().equals(Instant.ofEpochMilli(0)));
        assertThat("Probing rebalance should be signaled", probingSignaled, is(true));

        verifyStandbySatisfyRackReplicaKafka(applicationState, allTaskIds, assignmentsByProcessId, null, true, null);
    }

    // update  below

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldSkipWarmupsWhenAcceptableLagIsMax(final String rackAwareStrategy) {
        final Set<TaskId> allTaskIds = Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_1_0, TASK_1_1, TASK_1_2, TASK_2_0, TASK_2_1, TASK_2_2);

        final AssignmentConfigs configs = new AssignmentConfigs(
                Long.MAX_VALUE,
                1,
                1,
                60_000L,
                Collections.emptyList(),
                OptionalInt.empty(),
                OptionalInt.empty(),
                rackAwareStrategy
        );

        // TODO: double check the tasks

        // 2. Create Map<TaskId, TaskInfo> - All tasks are stateful
        final Map<TaskId, TaskInfo> tasks = allTaskIds.stream()
                .collect(Collectors.toMap(
                        Function.identity(),
                        taskId -> mkTaskInfo(taskId, true).getValue() // true for stateful
                ));

        // 3. Create Map<ProcessId, KafkaStreamsState>
        final Map<ProcessId, KafkaStreamsState> clientStatesMap = new HashMap<>();
        final int capacity = 1; // All clients have capacity 1 (1 thread)

        // Client 1 (PID_1): Was all active, 0 lag
        final Map<TaskId, Long> lags1 = allTaskIds.stream().collect(Collectors.toMap(Function.identity(), t -> 0L));

        clientStatesMap.put(PID_1, mkStreamState(
                        1,                              // ProcessId (assuming your PID_1 is compatible with int, or the actual mkStreamState takes ProcessId)
                        capacity,                          // int
                        Optional.empty(),                  // Optional<String> for rackId
                        allTaskIds,                        // Set<TaskId> for previous active
                        emptySet(),                        // Set<TaskId> for previous standby
                        Collections.emptyMap(),            // Map<String, String> for client tags (added this, as it's in signature 3)
                        Optional.of(lags1)                 // Optional<Map<TaskId, Long>> for taskLags
                ).getValue()
        );

        // Client 2 (PID_2): Was all standby, lag 10
        final Map<TaskId, Long> lags2 = allTaskIds.stream().collect(Collectors.toMap(Function.identity(), t -> Long.MAX_VALUE));
        clientStatesMap.put(PID_2, mkStreamState(
                        2,                          // ProcessId (assuming your PID_1 is compatible with int, or the actual mkStreamState takes ProcessId)
                        capacity,                          // int
                        Optional.empty(),                  // Optional<String> for rackId
                        allTaskIds,                        // Set<TaskId> for previous active
                        emptySet(),            // Set<TaskId> for previous standby
                        Collections.emptyMap(),            // Map<String, String> for client tags (added this, as it's in signature 3)
                        Optional.of(lags2)                 // Optional<Map<TaskId, Long>> for taskLags
                ).getValue()
        );

        // Client 3 (PID_3): New/empty, max lag
        final Map<TaskId, Long> lags3 = allTaskIds.stream().collect(Collectors.toMap(Function.identity(), t -> Long.MAX_VALUE));
        clientStatesMap.put(PID_3, mkStreamState(
                        3,                             // ProcessId (assuming your PID_1 is compatible with int, or the actual mkStreamState takes ProcessId)
                        capacity,                          // int
                        Optional.empty(),                  // Optional<String> for rackId
                        allTaskIds,                        // Set<TaskId> for previous active
                        emptySet(),                        // Set<TaskId> for previous standby
                        Collections.emptyMap(),            // Map<String, String> for client tags (added this, as it's in signature 3)
                        Optional.of(lags3)                 // Optional<Map<TaskId, Long>> for taskLags
                ).getValue()
        );

        // 4. Create ApplicationState
        final ApplicationState applicationState = new TaskAssignmentUtilsTest.TestApplicationState(
                configs,
                clientStatesMap,
                tasks
        );

        // 5. Instantiate and Call your New HighAvailabilityAssignor
        final TaskAssignor assignor = new HighAvailabilityAssignor(); // Your new HAA
        final TaskAssignment taskAssignment = assignor.assign(applicationState);

        // 6. Adapt Assertions
        final Map<ProcessId, KafkaStreamsAssignment> assignmentsByProcessId =
                taskAssignment.assignment().stream()
                        .collect(Collectors.toMap(KafkaStreamsAssignment::processId, Function.identity()));

        final KafkaStreamsAssignment assignment1 = assignmentsByProcessId.get(PID_1);
        assertThat(assignment1.tasks().size(), is(6));

        final KafkaStreamsAssignment assignment2 = assignmentsByProcessId.get(PID_2);
        assertThat(assignment2.tasks().size(), is(6));

        final KafkaStreamsAssignment assignment3 = assignmentsByProcessId.get(PID_3);
        assertThat(assignment3.tasks().size(), is(6));

        final boolean probingSignaled = taskAssignment.assignment().stream()
                .anyMatch(ka -> ka.followupRebalanceDeadline().isPresent() &&
                        ka.followupRebalanceDeadline().get().equals(Instant.ofEpochMilli(0)));
        assertThat("Probing rebalance should be signaled", probingSignaled, is(false));
    }
    // above do

    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC
    })
    public void shouldAssignCorrectNumberOfStandbys(final String rackAwareStrategy) {
        // 1. Define the tasks and clients for the scenario
        final TaskId task00 = new TaskId(0, 0);
        final TaskId task01 = new TaskId(0, 1);
        final TaskId task02 = new TaskId(0, 2);
        final Set<TaskId> allTaskIds = Set.of(task00, task01, task02);

        final ProcessId pid1 = processId(1);
        final ProcessId pid2 = processId(2);
        final ProcessId pid3 = processId(3);
        final int capacity = 1;

        // 2. Set up AssignmentConfigs to request 2 standby replicas
        final AssignmentConfigs configs = new AssignmentConfigs(
                10000L, // acceptableRecoveryLag
                1,      // maxWarmupReplicas
                2,      // numStandbyReplicas
                60_000L,
                Collections.emptyList(),
                OptionalInt.empty(),
                OptionalInt.empty(),
                rackAwareStrategy
        );

        // 3. Define all tasks as stateful
        final Map<TaskId, TaskInfo> tasks = allTaskIds.stream()
                .collect(Collectors.toMap(
                        Function.identity(),
                        taskId -> mkTaskInfo(taskId, true).getValue() // true for stateful
                ));

        // 4. Set up the initial state: each client has one active task and NO previous standbys
        final Map<ProcessId, KafkaStreamsState> clientStatesMap = new HashMap<>();

        // All clients are fully caught-up (lag = 0) for all tasks
        final Optional<Map<TaskId, Long>> lags = Optional.of(allTaskIds.stream().collect(Collectors.toMap(Function.identity(), t -> 0L)));

        clientStatesMap.put(pid1, mkStreamState(1, capacity, Optional.empty(), Set.of(task00), Collections.emptySet(), Collections.emptyMap(), lags).getValue());
        clientStatesMap.put(pid2, mkStreamState(2, capacity, Optional.empty(), Set.of(task01), Collections.emptySet(), Collections.emptyMap(), lags).getValue());
        clientStatesMap.put(pid3, mkStreamState(3, capacity, Optional.empty(), Set.of(task02), Collections.emptySet(), Collections.emptyMap(), lags).getValue());

        // 5. Create the ApplicationState object
        final ApplicationState applicationState = new TaskAssignmentUtilsTest.TestApplicationState(
                configs,
                clientStatesMap,
                tasks
        );

        // 6. Instantiate and call your new HighAvailabilityAssignor
        final TaskAssignor assignor = new HighAvailabilityAssignor(); // Your new HAA
        final TaskAssignment taskAssignment = assignor.assign(applicationState);

        // 7. Assert the results
        final Map<ProcessId, KafkaStreamsAssignment> assignments = taskAssignment.assignment().stream()
                .collect(Collectors.toMap(KafkaStreamsAssignment::processId, Function.identity()));

        // Verify active tasks remain where they were (since everything is healthy)
        assertThat(activeTasks(assignments, 1), equalTo(Set.of(task00)));
        assertThat(activeTasks(assignments, 2), equalTo(Set.of(task01)));
        assertThat(activeTasks(assignments, 3), equalTo(Set.of(task02)));

        // Verify standby tasks are correctly placed
        // PID 1 has active 0_0, so it should get standbys for 0_1 and 0_2
        assertThat(standbyTasks(assignments, 1), equalTo(Set.of(task01, task02)));

        // PID 2 has active 0_1, so it should get standbys for 0_0 and 0_2
        assertThat(standbyTasks(assignments, 2), equalTo(Set.of(task00, task02)));

        // PID 3 has active 0_2, so it should get standbys for 0_0 and 0_1
        assertThat(standbyTasks(assignments, 3), equalTo(Set.of(task00, task01)));
    }

    private Set<TaskId> standbyTasks(final Map<ProcessId, KafkaStreamsAssignment> assignments,
                                     final int client) {
        final KafkaStreamsAssignment assignment = assignments.getOrDefault(processId(client), null);
        if (assignment == null) {
            return Set.of();
        }
        return assignment.tasks().values().stream().filter(t -> t.type() == STANDBY)
                .map(KafkaStreamsAssignment.AssignedTask::id)
                .collect(Collectors.toSet());
    }

    private List<KafkaStreamsAssignment.AssignedTask> allTasks(final Map<ProcessId, KafkaStreamsAssignment> assignments) {
        final List<KafkaStreamsAssignment.AssignedTask> allTasks = new ArrayList<>();
        assignments.values().forEach(assignment -> allTasks.addAll(assignment.tasks().values()));
        return allTasks;
    }

    private void assertActiveTaskTopicGroupIdsEvenlyDistributed(final Map<ProcessId, KafkaStreamsAssignment> assignments) {
        for (final KafkaStreamsAssignment assignment : assignments.values()) {
            final List<Integer> topicGroupIds = new ArrayList<>();
            final Set<TaskId> activeTasks = assignment.tasks().values().stream()
                    .map(KafkaStreamsAssignment.AssignedTask::id)
                    .collect(Collectors.toSet());
            for (final TaskId activeTask : activeTasks) {
                topicGroupIds.add(activeTask.subtopology());
            }
            Collections.sort(topicGroupIds);
            assertThat(topicGroupIds, equalTo(asList(1, 2)));
        }
    }

    private Map<ProcessId, KafkaStreamsAssignment> assign(final Map<ProcessId, KafkaStreamsState> streamStates,
                                                          final Map<TaskId, TaskInfo> tasks,
                                                          final String rackAwareStrategy) {
        return assign(streamStates, tasks, 0, rackAwareStrategy);
    }

    private Map<ProcessId, KafkaStreamsAssignment> assign(final Map<ProcessId, KafkaStreamsState> streamStates,
                                                          final Map<TaskId, TaskInfo> tasks,
                                                          final int numStandbys,
                                                          final String rackAwareStrategy) {
        return assign(streamStates, tasks, defaultAssignmentConfigs(numStandbys, rackAwareStrategy));
    }

    private Map<ProcessId, KafkaStreamsAssignment> assign(final Map<ProcessId, KafkaStreamsState> streamStates,
                                                          final Map<TaskId, TaskInfo> tasks,
                                                          final AssignmentConfigs assignmentConfigs) {
        final ApplicationState applicationState = new TaskAssignmentUtilsTest.TestApplicationState(
                assignmentConfigs,
                streamStates,
                tasks
        );
        final TaskAssignment taskAssignment = assignor.assign(applicationState);
        final TaskAssignor.AssignmentError assignmentError = TaskAssignmentUtils.validateTaskAssignment(applicationState, taskAssignment);
        assertThat(assignmentError, equalTo(TaskAssignor.AssignmentError.NONE));
        return indexAssignment(taskAssignment.assignment());
    }

    public AssignmentConfigs defaultAssignmentConfigs(final int numStandbys, final String rackAwareStrategy) {
        return new AssignmentConfigs(
                0L,
                1,
                numStandbys,
                60_000L,
                Collections.emptyList(),
                OptionalInt.empty(),
                OptionalInt.empty(),
                rackAwareStrategy
        );
    }

    private Set<TaskId> activeTasks(final Map<ProcessId, KafkaStreamsAssignment> assignments,
                                    final int client) {
        final KafkaStreamsAssignment assignment = assignments.getOrDefault(processId(client), null);
        if (assignment == null) {
            return Set.of();
        }
        return assignment.tasks().values().stream().filter(t -> t.type() == ACTIVE)
                .map(KafkaStreamsAssignment.AssignedTask::id)
                .collect(Collectors.toSet());
    }

    private Map<ProcessId, KafkaStreamsAssignment> indexAssignment(final Collection<KafkaStreamsAssignment> assignments) {
        return assignments.stream().collect(Collectors.toMap(KafkaStreamsAssignment::processId, assignment -> assignment));
    }
}