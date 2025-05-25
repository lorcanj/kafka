package org.apache.kafka.streams.processor.internals.assignment;

import com.sun.source.tree.Tree;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.assignment.ApplicationState;
import org.apache.kafka.streams.processor.assignment.KafkaStreamsAssignment;
import org.apache.kafka.streams.processor.assignment.KafkaStreamsState;
import org.apache.kafka.streams.processor.assignment.ProcessId;
import org.apache.kafka.streams.processor.assignment.TaskAssignor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableSet;


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
        final TreeMap<ProcessId, KafkaStreamsState> clientStates = new TreeMap<>(clients);
        // need the old clientStates because I want to use the existing processing for the standbyReplica stuff
        // to reduce the number of changes I need to make
        final TreeMap<ProcessId, ClientState> clientStatesOLD = translateToLegacyClientStateMap(clientStates);

        assignActiveStatefulTasks(applicationState, assignmentState, statefulTasks, clients.values());

        // TODO: need to check this is correct
        // the below needs the old ClientStates
        assignStandbyReplicaTasks(applicationState, assignmentState, statefulTasks, clients.values(), clientStatesOLD);




        return null;
    }

    // below not done properly
    private TreeMap<ProcessId, ClientState> translateToLegacyClientStateMap(
            final Map<ProcessId, KafkaStreamsState> newStates) {

        final TreeMap<ProcessId, ClientState> legacyClientStates = new TreeMap<>();

        for (final Map.Entry<ProcessId, KafkaStreamsState> entry : newStates.entrySet()) {
            final ProcessId processId = entry.getKey();
            final KafkaStreamsState newState = entry.getValue(); // The NEW state object

            // 1. Instantiate the LEGACY ClientState object. Adjust constructor as needed.
            final ClientState legacyState = new ClientState(processId, entry.getValue().numProcessingThreads()); // Use your actual LEGACY ClientState class!

            for (var task : newState.previousActiveTasks())
                legacyState.assignActive(task);

            for (var task : newState.previousStandbyTasks())
                legacyState.assignStandby(task);

            // 5. Copy other fields (HostInfo, RackID) if legacy ClientState has them and legacy logic needs them.
            // legacyState.setHostInfo(newState.hostInfo());
            // newState.rackId().ifPresent(legacyState::setRackId); // Handle Optional if needed

            // 6. Put the populated legacy state into the map
            legacyClientStates.put(processId, legacyState);
        }

        return legacyClientStates;
    }

    private static void assignActiveStatefulTasks(final ApplicationState applicationState,
                                                  final AssignmentState assignmentState,
                                                  final SortedSet<TaskId> statefulTasks,
                                                  final Collection<KafkaStreamsState> clients) {
        // ugly but might be it
        final List<TaskId> sortedStatefulTasks = new ArrayList<>(statefulTasks);
        Collections.sort(sortedStatefulTasks);
        final SortedSet<ProcessId> candidateClients = (SortedSet<ProcessId>) clients.stream()
                .map(KafkaStreamsState::processId)
                .collect(Collectors.toSet());
        Iterator<ProcessId> consumerClientIdIterator = null;
        for (final TaskId task : sortedStatefulTasks) {
            if (consumerClientIdIterator == null || !consumerClientIdIterator.hasNext()) {
                consumerClientIdIterator = candidateClients.iterator();
            }

            assignmentState.finalizeAssignment(task, consumerClientIdIterator.next(), KafkaStreamsAssignment.AssignedTask.Type.ACTIVE);
        }

        // TODO: balanceTasksOverThread need to add this
        balanceTasksOverThreads(

        );

        // at this point will want to populate the processId map to KafkaStreamsState to then use the Utils stuff for rack optimisation

        // TODO: add rack stuff
    }

    // problem
    // we are assigning with the KafkaStreamsState, but need more information
    // than this class contains
    // which is why I created the HighAvailabilityClientState to use as the class during the rebalance

    private static void assignStandbyReplicaTasks(final ApplicationState applicationState,
                                                  final AssignmentState assignmentState,
                                                  final SortedSet<TaskId> statefulTasks,
                                                  final Collection<KafkaStreamsState> clients,
                                                  final TreeMap<ProcessId, ClientState> clientStatesOLD,
                                                  final TreeMap<>) {

        if (applicationState.assignmentConfigs().numStandbyReplicas() == 0) {
            return;
        }

        final StandbyTaskAssignor standbyTaskAssignor = StandbyTaskAssignorFactory.create(applicationState.assignmentConfigs(), null);

        standbyTaskAssignor.assign(clientStatesOLD, applicationState.allTasks().keySet(), statefulTasks, applicationState.assignmentConfigs());

        balanceTasksOverThreads(
                assignmentState.newAssignments

        );










    }

    //
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
                    // re-order these
                    while (AssignmentState.shouldMoveATask(sourceClientAssignmentState, destinationClientAssignmentState) && sourceIterator.hasNext()) {
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

    // this has been taken from the stickyTaskAssignor
    // should probably only update this at the end

    // create this whole thing at the end because don't want to jump back and forth between DTOs

    // should only have a single assignmentState

    static class AssignmentState {
        // what is clients?
        private final Map<ProcessId, KafkaStreamsState> clients;
        private final Map<ProcessId, KafkaStreamsAssignment> newAssignments;
        private final Map<ProcessId, HighAvailabilityClientState> mapProcessToClientStateRebalanceDTO;

        // do I need assigned active and assigned standby
        AssignmentState(final ApplicationState applicationState,
                        final Map<ProcessId, KafkaStreamsState> clients) {

            this.clients = clients;
            // this should only be populated at the end
            this.newAssignments = null;

//                    clients.values().stream().collect(Collectors.toMap(
//                    KafkaStreamsState::processId,
//                    state -> KafkaStreamsAssignment.of(state.processId(), new HashSet<>())
//            ));

            // creates blank HAClientState objects so that we can start assigning and tracking the stuff
            this.mapProcessToClientStateRebalanceDTO = clients.values().stream().collect(Collectors.toMap(
                    KafkaStreamsState::processId,
                    state -> new HighAvailabilityClientState(state.processId())
            ));
            var thing = clients.values().stream().collect(Collectors.toMap(
                    KafkaStreamsState::processId,
                    state -> KafkaStreamsAssignment.of(state.processId(), new HashSet<>())
            ));
        }




        // TODO: might need to update this
        // need this to update mapProcessToClientStateRebalanceDTO
        // need the below to then create this from the other map
        private void finalizeAssignment(final TaskId taskId, final ProcessId client, final KafkaStreamsAssignment.AssignedTask.Type type) {
            // currently null pointer but will just want to update this at the end
            newAssignments.get(client).assignTask(new KafkaStreamsAssignment.AssignedTask(taskId, type));

            // not sure if I need the below, leave it out for the moment
            // newTaskLocations.computeIfAbsent(taskId, k -> new HashSet<>()).add(client);
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
    }

    // used to hold the data during the assignment
    // needed to create this
    static class AssignmentClientStateTask {
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
        private int capacity;

        public HighAvailabilityClientState (final ProcessId processId) {
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

        private int capacity() { return capacity; }

        public Set<TaskId> activeTasks() {
            return unmodifiableSet(assignedActiveTasks.taskIds());
        }

        public void assignActive(final TaskId task) {
            assertNotAssigned(task);
            assignedActiveTasks.taskIds().add(task);
        }

        // why creating a new reference?
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
    }
}
