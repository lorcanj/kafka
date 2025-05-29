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
import java.util.SortedSet;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableSet;
import static org.apache.kafka.common.utils.Utils.diff;
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
        final TreeMap<ProcessId, KafkaStreamsState> clientStates = new TreeMap<>(clients);
        // need the old clientStates because I want to use the existing processing for the standbyReplica stuff
        // to reduce the number of changes I need to make
        final TreeMap<ProcessId, ClientState> clientStatesOLD = translateToLegacyClientStateMap(clientStates);


        assignActiveStatefulTasks(applicationState, assignmentState, statefulTasks, clients.values());
        optimiseActiveStatefulTasks(applicationState, assignmentState);

        // the below needs the old ClientStates
        assignStandbyReplicaTasks(applicationState, assignmentState, statefulTasks, clients.values(), clientStatesOLD);
        optimizeStandbyTasks(applicationState, assignmentState);

        final AtomicInteger remainingWarmupReplicas = new AtomicInteger(applicationState.assignmentConfigs().maxWarmupReplicas());

        final Map<TaskId, SortedSet<ProcessId>> tasksToCaughtUpClients = AssignmentState.tasksToCaughtUpClients(
                statefulTasks,
                assignmentState.mapProcessToClientStateRebalanceDTO,
                // below is ugly
                applicationState.assignmentConfigs().acceptableRecoveryLag(),
                applicationState
        );

        final Map<TaskId, SortedSet<ProcessId>> tasksToClientByLag = AssignmentState.tasksToClientByLag(statefulTasks, assignmentState.mapProcessToClientStateRebalanceDTO, applicationState);

        // We temporarily need to know which standby tasks were intended as warmups
        // for active tasks, so that we don't move them (again) when we plan standby
        // task movements. We can then immediately treat warmups exactly the same as
        // hot-standby replicas, so we just track it right here as metadata, rather
        // than add "warmup" assignments to ClientState, for example.
        final Map<ProcessId, Set<TaskId>> warmups = new TreeMap<>();

        // TODO: need to update the clientState map as now stale
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
                clientStatesOLD,
                remainingWarmupReplicas,
                warmups
        );

        assignStatelessActiveTasks(applicationState, assignmentState, diff(TreeSet::new, applicationState.allTasks().keySet(), statefulTasks));
        optimizeStatelessTasks(applicationState, assignmentState);

        final Map<ProcessId, KafkaStreamsAssignment> finalAssignments = assignmentState.newAssignments;

        boolean probingRebalanceNeeded = neededActiveTaskMovements + neededStandbyTaskMovements > 0;

        if (probingRebalanceNeeded && !finalAssignments.isEmpty()) {
            // We set the followup deadline for only one of the clients.
            final ProcessId clientId = finalAssignments.entrySet().iterator().next().getKey();
            final KafkaStreamsAssignment previousAssignment = finalAssignments.get(clientId);
            // taken from StickyTaskAssignor
            finalAssignments.put(clientId, previousAssignment.withFollowupRebalance(Instant.ofEpochMilli(0)));
        }

        return new TaskAssignment(finalAssignments.values());
    }

    // TODO: need to complete this
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

    // here we want to loop in a similar way to the original method
    private static void assignActiveStatefulTasks(final ApplicationState applicationState,
                                                  final AssignmentState assignmentState,
                                                  final SortedSet<TaskId> statefulTasks,
                                                  final Collection<KafkaStreamsState> clients) {

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

        // TODO: balanceTasksOverThread need to add this
        balanceTasksOverThreads(
                assignmentState.mapProcessToClientStateRebalanceDTO,
                HighAvailabilityClientState::activeTasks,
                HighAvailabilityClientState::unassignActive,
                HighAvailabilityClientState::assignActive,
                (source, destination) -> true
        );

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

    private static void populateNewActiveAssignments(AssignmentState assignmentState) {
        for (var thing : assignmentState.mapProcessToClientStateRebalanceDTO.entrySet()) {
            // probably will want to make generic
            for (var blah : thing.getValue().activeTasks()) {
                assignmentState.finalizeAssignment(blah, thing.getKey(), KafkaStreamsAssignment.AssignedTask.Type.ACTIVE);
            }
        }
    }

    private static void populateNewStandbyAssignments(AssignmentState assignmentState) {
        for (var thing : assignmentState.mapProcessToClientStateRebalanceDTO.entrySet()) {
            // probably will want to make generic
            for (var blah : thing.getValue().standbyTasks()) {
                assignmentState.finalizeAssignment(blah, thing.getKey(), KafkaStreamsAssignment.AssignedTask.Type.STANDBY);
            }
        }
    }

    private static void assignStatelessActiveTasks(ApplicationState applicationState,
                                                   AssignmentState assignmentState,
                                                   Iterable<TaskId> statelessTasks) {

        final ConstrainedPrioritySet statelessActiveTaskClientsByTaskLoad = new ConstrainedPrioritySet(
                (client, task) -> true,
                client -> assignmentState.mapProcessToClientStateRebalanceDTO.get(client).   activeTaskLoad()
        );
        statelessActiveTaskClientsByTaskLoad.offerAll(assignmentState.mapProcessToClientStateRebalanceDTO.keySet());

        // Lorcan, not sure about this treeset
        // final SortedSet<TaskId> = new TreeSet<>();
        for (final TaskId task : statelessTasks) {
            // sortedTasks.add(task);
            final ProcessId client = statelessActiveTaskClientsByTaskLoad.poll(task);
            final HighAvailabilityClientState state = assignmentState.mapProcessToClientStateRebalanceDTO.get(client);
            state.assignActive(task);
            statelessActiveTaskClientsByTaskLoad.offer(client);
        }

        // might be wrong as not 100% sure if can use this for the stateless active tasks
        // Lorcan
        // TODO: check as not sure if this is right for stateless tasks
        // check this in the WIP
        populateNewActiveAssignments(assignmentState);

        final Map<ProcessId, KafkaStreamsAssignment> currentAssignments = assignmentState.newAssignments;

        final TaskAssignmentUtils.RackAwareOptimizationParams statefulTaskParams = TaskAssignmentUtils.RackAwareOptimizationParams.of(applicationState)
                .withTrafficCostOverride(
                        applicationState.assignmentConfigs().rackAwareTrafficCost().orElse(DEFAULT_HIGH_AVAILABILITY_TRAFFIC_COST)
                )
                .withNonOverlapCostOverride(
                        applicationState.assignmentConfigs().rackAwareNonOverlapCost().orElse(DEFAULT_HIGH_AVAILABILITY_NON_OVERLAP_COST)
                )
                .forStatelessTasks();
        TaskAssignmentUtils.optimizeRackAwareActiveTasks(statefulTaskParams, currentAssignments);

        TaskAssignmentUtils.optimizeRackAwareActiveTasks(
                TaskAssignmentUtils.RackAwareOptimizationParams.of(applicationState)
                        .forStatelessTasks()
                        .withTrafficCostOverride(RackAwareTaskAssignor.STATELESS_TRAFFIC_COST)
                        .withNonOverlapCostOverride(RackAwareTaskAssignor.STATELESS_NON_OVERLAP_COST),
                currentAssignments
        );
        assignmentState.newAssignments = currentAssignments;
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

        balanceTasksOverThreadsClientState(
                assignmentState.mapProcessToClientStateRebalanceDTO,
                HighAvailabilityClientState::standbyTasks,
                HighAvailabilityClientState::unassignStandby,
                HighAvailabilityClientState::assignStandby,
                // below uses the ClientState objects
                standbyTaskAssignor::isAllowedTaskMovement,
                clientStatesOLD
        );

        populateNewStandbyAssignments(assignmentState);

        // not sure about this for standby
        final Map<ProcessId, KafkaStreamsAssignment> currentAssignments = assignmentState.newAssignments;

        final TaskAssignmentUtils.RackAwareOptimizationParams optimizationParams = TaskAssignmentUtils.RackAwareOptimizationParams.of(applicationState)
                .withTrafficCostOverride(
                        applicationState.assignmentConfigs().rackAwareTrafficCost().orElse(DEFAULT_HIGH_AVAILABILITY_TRAFFIC_COST)
                )
                .withNonOverlapCostOverride(
                        applicationState.assignmentConfigs().rackAwareNonOverlapCost().orElse(DEFAULT_HIGH_AVAILABILITY_NON_OVERLAP_COST)
                );

        TaskAssignmentUtils.optimizeRackAwareStandbyTasks(optimizationParams, currentAssignments);
        assignmentState.newAssignments = currentAssignments;
    }

    // ugly but might work
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
        private Map<ProcessId, KafkaStreamsAssignment> newAssignments;
        private final SortedMap<ProcessId, HighAvailabilityClientState> mapProcessToClientStateRebalanceDTO;

        // do I need assigned active and assigned standby
        AssignmentState(final ApplicationState applicationState,
                        final Map<ProcessId, KafkaStreamsState> clients) {

            // Lorcan
            // this should only be properly populated at the end
            this.newAssignments = clients.values().stream().collect(Collectors.toMap(
                    KafkaStreamsState::processId,
                    state -> KafkaStreamsAssignment.of(state.processId(), new HashSet<>())
            ));

            // creates blank HAClientState objects so that we can start assigning and tracking the stuff
            // need sortedMap because of the balance threads function

            this.mapProcessToClientStateRebalanceDTO = clients.values().stream().collect(Collectors.toMap(
                    KafkaStreamsState::processId,
                    state -> new HighAvailabilityClientState(state.processId(), state.numProcessingThreads()),
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
                                                                                final long acceptableRecoveryLag,
                                                                                final ApplicationState applicationState) {
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
                                                                            ApplicationState applicationState) {
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

        // Lorcan
        // not sure if I need this as might be in the application state
        private final int capacity;
        // need this to map this object to the ClientState objects
        private final ProcessId processId;

        public HighAvailabilityClientState (final ProcessId processId, final int capacity) {
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

        private int capacity() { return capacity; }

        public Set<TaskId> activeTasks() {
            return unmodifiableSet(assignedActiveTasks.taskIds());
        }

        public Set<TaskId> standbyTasks() { return unmodifiableSet(assignedStandbyTasks.taskIds()); }

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

        double activeTaskLoad() {
            return ((double) activeTaskCount()) / capacity;
        }

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
