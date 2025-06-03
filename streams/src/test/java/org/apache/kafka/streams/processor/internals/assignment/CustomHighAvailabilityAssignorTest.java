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
import org.apache.kafka.streams.processor.assignment.assignors.StickyTaskAssignor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_2;
import static org.apache.kafka.streams.processor.internals.assignment.TaskAssignmentUtilsTest.mkStreamState;
import static org.apache.kafka.streams.processor.internals.assignment.TaskAssignmentUtilsTest.mkTaskInfo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class CustomHighAvailabilityAssignorTest {

    private TaskAssignor assignor;

    @BeforeEach
    public void setUp() {
        assignor = new StickyTaskAssignor();
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

    private Map<ProcessId, KafkaStreamsAssignment> indexAssignment(final Collection<KafkaStreamsAssignment> assignments) {
        return assignments.stream().collect(Collectors.toMap(KafkaStreamsAssignment::processId, assignment -> assignment));
    }
}