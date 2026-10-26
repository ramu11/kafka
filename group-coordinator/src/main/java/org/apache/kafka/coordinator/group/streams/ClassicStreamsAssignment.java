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
package org.apache.kafka.coordinator.group.streams;

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue.Subtopology;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue.TopicInfo;
import org.apache.kafka.coordinator.group.streams.topics.ConfiguredSubtopology;
import org.apache.kafka.coordinator.group.streams.topics.ConfiguredTopology;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Builds the classic consumer-protocol assignment expected by legacy Kafka Streams members.
 *
 * <p>The outer assignment contains the active input topic partitions. The bridge emits the negotiated legacy
 * AssignmentInfo version for the classic members. For metadata versions 7 and later, it includes the classic client's
 * next probing deadline for the selected classic member with warm-up work. The active task list is aligned one-to-one
 * with the outer partition list; a task ID is repeated for every
 * input partition belonging to that task. Standby and warm-up tasks include their input topic
 * partitions in the legacy standby-task map, and warm-up tasks are exposed as standby tasks.
 */
public final class ClassicStreamsAssignment {
    private static final int ASSIGNMENT_INFO_VERSION = 1;
    private static final long DEFAULT_PROBING_REBALANCE_INTERVAL_MS = 600_000L;

    private ClassicStreamsAssignment() { }

    /**
     * Creates a consumer-protocol assignment for a member of a migrated Streams group.
     *
     * @param group the current Streams group
     * @param metadataImage the broker metadata image used to validate topic partition availability
     * @param assignment the assignment for the member
     * @return a legacy consumer assignment with active partitions and version-1 Streams user data
     * @throws AssignmentNotReadyException if the group metadata is insufficient to produce a valid assignment
     * @throws SchemaException if a task cannot be represented by the legacy Streams task-ID format
     */
    public static Assignment toConsumerAssignment(
        StreamsGroup group,
        CoordinatorMetadataImage metadataImage,
        TasksTupleWithEpochs assignment
    ) {
        return toConsumerAssignment(group, metadataImage, assignment, null, 0L, false);
    }

    /**
     * Creates a classic assignment and, for the selected classic warm-up member, carries the next probing deadline.
     * The configured interval is read from the member's serialized classic subscription metadata. Older clients have
     * no interval field; in that case the documented Streams default is used.
     */
    public static Assignment toConsumerAssignment(
        StreamsGroup group,
        CoordinatorMetadataImage metadataImage,
        TasksTupleWithEpochs assignment,
        String classicProtocolMetadata,
        long currentTimeMs,
        boolean scheduleProbingRebalance
    ) {
        Objects.requireNonNull(group, "group cannot be null");
        Objects.requireNonNull(metadataImage, "metadataImage cannot be null");
        Objects.requireNonNull(assignment, "assignment cannot be null");

        final StreamsTopology topology = group.topology().orElseThrow(() ->
            new AssignmentNotReadyException("The Streams group has no topology."));
        final Map<String, Set<String>> inputTopicsBySubtopology = inputTopicsBySubtopology(group, topology);

        final List<TopicPartition> activePartitions = new ArrayList<>();
        final List<LegacyTaskId> activeTaskIds = new ArrayList<>();
        for (LegacyTaskId task : sortedTasks(assignment.activeTasks())) {
            final List<TopicPartition> taskPartitions = inputPartitionsForTask(
                task, inputTopicsBySubtopology, metadataImage);
            for (TopicPartition partition : taskPartitions) {
                activePartitions.add(partition);
                activeTaskIds.add(task);
            }
        }

        if (new HashSet<>(activePartitions).size() != activePartitions.size()) {
            throw new AssignmentNotReadyException("The active Streams assignment contains duplicate topic partitions.");
        }

        final SortedMap<LegacyTaskId, List<TopicPartition>> standbyTasks = new TreeMap<>();
        for (LegacyTaskId task : sortedTasks(assignment.standbyAndWarmupTasks())) {
            standbyTasks.put(task, inputPartitionsForTask(task, inputTopicsBySubtopology, metadataImage));
        }

        final ClassicStreamsMemberMetadata.ProbingRebalanceConfiguration probingConfiguration =
            decodeProbingRebalanceConfiguration(classicProtocolMetadata);
        final AssignmentInfoVersions assignmentInfoVersions = assignmentInfoVersions(group, classicProtocolMetadata);
        final int assignmentInfoVersion = assignmentInfoVersions.usedVersion();
        final int commonlySupportedVersion = assignmentInfoVersions.commonlySupportedVersion();
        long nextRebalanceMs = Long.MAX_VALUE;
        if (scheduleProbingRebalance && !assignment.warmupTasks().isEmpty() && assignmentInfoVersion >= 7) {
            long intervalMs = probingConfiguration.probingRebalanceIntervalMs();
            if (intervalMs <= 0) intervalMs = DEFAULT_PROBING_REBALANCE_INTERVAL_MS;
            if (currentTimeMs <= Long.MAX_VALUE - intervalMs) {
                nextRebalanceMs = currentTimeMs + intervalMs;
            }
        }

        return new Assignment(activePartitions, ByteBuffer.wrap(encodeAssignmentInfo(
            activeTaskIds, standbyTasks, assignmentInfoVersion, commonlySupportedVersion, nextRebalanceMs)));
    }

    private static Map<String, Set<String>> inputTopicsBySubtopology(StreamsGroup group, StreamsTopology topology) {
        final Optional<ConfiguredTopology> configuredTopology = group.configuredTopology()
            .filter(configured -> configured.topologyEpoch() == topology.topologyEpoch());
        if (configuredTopology.isPresent()) {
            final ConfiguredTopology configured = configuredTopology.get();
            if (!configured.isReady()) {
                throw new AssignmentNotReadyException("The configured Streams topology is not ready.");
            }
            final SortedMap<String, ConfiguredSubtopology> configuredSubtopologies = configured.subtopologies()
                .orElseThrow(() -> new AssignmentNotReadyException("The configured Streams subtopologies are unavailable."));
            if (!configuredSubtopologies.keySet().equals(topology.subtopologies().keySet())) {
                throw new AssignmentNotReadyException("The configured topology does not match the current topology.");
            }

            final Map<String, Set<String>> result = new TreeMap<>();
            configuredSubtopologies.forEach((subtopologyId, subtopology) -> {
                final Set<String> inputTopics = new TreeSet<>(subtopology.sourceTopics());
                inputTopics.addAll(subtopology.repartitionSourceTopics().keySet());
                result.put(subtopologyId, Collections.unmodifiableSet(inputTopics));
            });
            return result;
        }

        final Map<String, Set<String>> result = new TreeMap<>();
        for (Map.Entry<String, Subtopology> entry : topology.subtopologies().entrySet()) {
            final Subtopology subtopology = entry.getValue();
            if (!subtopology.sourceTopicRegex().isEmpty()) {
                throw new AssignmentNotReadyException(
                    "The configured topology is required to resolve source-topic regular expressions.");
            }
            final Set<String> inputTopics = new TreeSet<>(subtopology.sourceTopics());
            subtopology.repartitionSourceTopics().stream().map(TopicInfo::name).forEach(inputTopics::add);
            result.put(entry.getKey(), Collections.unmodifiableSet(inputTopics));
        }
        return result;
    }

    private static List<TopicPartition> inputPartitionsForTask(
        LegacyTaskId task,
        Map<String, Set<String>> inputTopicsBySubtopology,
        CoordinatorMetadataImage metadataImage
    ) {
        final String subtopologyId = Integer.toString(task.subtopologyId());
        final Set<String> inputTopics = inputTopicsBySubtopology.get(subtopologyId);
        if (inputTopics == null) {
            throw new AssignmentNotReadyException("No topology metadata exists for subtopology " + subtopologyId + ".");
        }
        if (inputTopics.isEmpty()) {
            throw new AssignmentNotReadyException("Subtopology " + subtopologyId + " has no resolved input topics.");
        }

        final List<TopicPartition> partitions = new ArrayList<>();
        for (String topic : inputTopics) {
            final int partitionCount = metadataImage.topicMetadata(topic)
                .map(CoordinatorMetadataImage.TopicMetadata::partitionCount)
                .orElseThrow(() -> new AssignmentNotReadyException(
                    "Topic metadata is unavailable for Streams input topic " + topic + "."));
            if (task.partition() < partitionCount) {
                partitions.add(new TopicPartition(topic, task.partition()));
            }
        }
        if (partitions.isEmpty()) {
            throw new AssignmentNotReadyException(
                "No input topic partition exists for Streams task " + task.subtopologyId() + "_" + task.partition() + ".");
        }
        return partitions;
    }

    private static List<LegacyTaskId> sortedTasks(Map<String, Set<Integer>> tasks) {
        return tasks.entrySet().stream()
            .flatMap(entry -> entry.getValue().stream().map(partition ->
                new LegacyTaskId(parseSubtopologyId(entry.getKey()), requireValidPartition(partition))))
            .sorted()
            .collect(Collectors.toList());
    }

    private static int requireValidPartition(Integer partition) {
        if (partition == null || partition < 0) {
            throw new SchemaException("Legacy Streams assignments require non-negative task partition IDs.");
        }
        return partition;
    }

    private static int parseSubtopologyId(String subtopologyId) {
        try {
            final int parsed = Integer.parseInt(subtopologyId);
            if (parsed < 0 || !Integer.toString(parsed).equals(subtopologyId)) {
                throw new NumberFormatException("not a canonical non-negative integer");
            }
            return parsed;
        } catch (NumberFormatException exception) {
            throw new SchemaException(
                "Legacy Streams assignments require canonical numeric subtopology IDs: " + subtopologyId,
                exception);
        }
    }

    private static byte[] encodeAssignmentInfo(
        List<LegacyTaskId> activeTaskIds,
        SortedMap<LegacyTaskId, List<TopicPartition>> standbyTasks,
        int assignmentInfoVersion,
        int commonlySupportedVersion,
        long nextRebalanceMs
    ) {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeInt(assignmentInfoVersion);
            if (assignmentInfoVersion >= 3) {
                output.writeInt(commonlySupportedVersion);
            }

            // The active-task list is aligned one-to-one with the outer assignment's partitions.
            output.writeInt(activeTaskIds.size());
            for (LegacyTaskId task : activeTaskIds) {
                writeAssignmentTaskId(output, task, assignmentInfoVersion);
            }

            // The legacy standby map carries both standby and warm-up input partitions.
            output.writeInt(standbyTasks.size());
            for (Map.Entry<LegacyTaskId, List<TopicPartition>> entry : standbyTasks.entrySet()) {
                writeAssignmentTaskId(output, entry.getKey(), assignmentInfoVersion);
                output.writeInt(entry.getValue().size());
                for (TopicPartition partition : entry.getValue()) {
                    output.writeUTF(partition.topic());
                    output.writeInt(partition.partition());
                }
            }

            if (assignmentInfoVersion == 2 || assignmentInfoVersion == 3) {
                // AssignmentInfo v2-v3 encode the active partitions-by-host map directly.
                output.writeInt(0);
            } else if (assignmentInfoVersion == 4) {
                output.writeInt(0); // partitions by host
                output.writeInt(0); // assignment error code
            } else if (assignmentInfoVersion == 5) {
                output.writeInt(0); // topic dictionary
                output.writeInt(0); // partitions by host
                output.writeInt(0); // assignment error code
            } else if (assignmentInfoVersion >= 6) {
                // AssignmentInfo v6+ uses a shared topic dictionary followed by active and standby host maps.
                // The migration bridge has no classic host metadata to report, so each structure is empty.
                output.writeInt(0); // topic dictionary
                output.writeInt(0); // partitions by host
                output.writeInt(0); // standby partitions by host
                output.writeInt(0); // assignment error code
                if (assignmentInfoVersion >= 7) {
                    output.writeLong(nextRebalanceMs);
                }
            }
            output.flush();
            return bytes.toByteArray();
        } catch (IOException exception) {
            // ByteArrayOutputStream does not throw IOExceptions, but keep the encoding contract explicit.
            throw new IllegalStateException("Could not encode legacy Streams assignment.", exception);
        }
    }

    private static void writeAssignmentTaskId(DataOutputStream output, LegacyTaskId task, int version) throws IOException {
        output.writeInt(task.subtopologyId());
        output.writeInt(task.partition());
        if (version >= 10) {
            // Named topologies are unsupported on the classic bridge; v10+ still encodes the empty topology name.
            output.writeInt(0);
        }
    }

    private record AssignmentInfoVersions(int usedVersion, int commonlySupportedVersion) { }

    private static AssignmentInfoVersions assignmentInfoVersions(
        StreamsGroup group,
        String fallbackProtocolMetadata
    ) {
        int minUsedVersion = Integer.MAX_VALUE;
        int minSupportedVersion = Integer.MAX_VALUE;
        for (StreamsGroupMember member : group.members().values()) {
            if (member.classicSessionTimeoutMs() == null || member.classicSessionTimeoutMs() <= 0 ||
                member.classicProtocolMetadata() == null || member.classicProtocolMetadata().isBlank()) {
                continue;
            }
            final ClassicStreamsMemberMetadata.ProbingRebalanceConfiguration configuration =
                decodeProbingRebalanceConfiguration(member.classicProtocolMetadata());
            minUsedVersion = Math.min(minUsedVersion, configuration.version());
            if (configuration.version() >= 3 && configuration.latestSupportedVersion() >= 3) {
                minSupportedVersion = Math.min(minSupportedVersion, configuration.latestSupportedVersion());
            }
        }

        // Direct unit callers and groups with no persisted classic metadata use the current member as the fallback.
        if (minUsedVersion == Integer.MAX_VALUE) {
            final ClassicStreamsMemberMetadata.ProbingRebalanceConfiguration fallback =
                decodeProbingRebalanceConfiguration(fallbackProtocolMetadata);
            minUsedVersion = fallback.version();
            minSupportedVersion = fallback.latestSupportedVersion() >= 3
                ? fallback.latestSupportedVersion()
                : Integer.MAX_VALUE;
        }

        final int commonlySupportedVersion = minUsedVersion >= 3
            ? (minSupportedVersion == Integer.MAX_VALUE ? minUsedVersion : minSupportedVersion)
            : -1;
        return new AssignmentInfoVersions(minUsedVersion, commonlySupportedVersion);
    }

    private static ClassicStreamsMemberMetadata.ProbingRebalanceConfiguration decodeProbingRebalanceConfiguration(
        String classicProtocolMetadata
    ) {
        if (classicProtocolMetadata == null || classicProtocolMetadata.isBlank()) {
            return new ClassicStreamsMemberMetadata.ProbingRebalanceConfiguration(1, -1, -1L);
        }
        try {
            final byte[] encodedSubscription = Base64.getDecoder().decode(classicProtocolMetadata);
            final ByteBuffer userData = ConsumerProtocol.deserializeSubscription(ByteBuffer.wrap(encodedSubscription)).userData();
            if (userData == null) {
                return new ClassicStreamsMemberMetadata.ProbingRebalanceConfiguration(1, -1, -1L);
            }
            return ClassicStreamsMemberMetadata.decodeProbingRebalanceConfiguration(userData);
        } catch (RuntimeException exception) {
            // Some pre-migration records/tests have no decodable classic user data; keep the v1 fallback safe.
            return new ClassicStreamsMemberMetadata.ProbingRebalanceConfiguration(1, -1, -1L);
        }
    }

    private record LegacyTaskId(int subtopologyId, int partition) implements Comparable<LegacyTaskId> {
        @Override
        public int compareTo(LegacyTaskId other) {
            final int subtopologyComparison = Integer.compare(subtopologyId, other.subtopologyId);
            return subtopologyComparison != 0 ? subtopologyComparison : Integer.compare(partition, other.partition);
        }
    }

    /** Indicates that current group/topic metadata is not sufficient to build a valid legacy assignment yet. */
    public static final class AssignmentNotReadyException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public AssignmentNotReadyException(String message) {
            super(message);
        }
    }
}
