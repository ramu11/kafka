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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.SubscriptionInfoData;
import org.apache.kafka.common.message.SubscriptionInfoData.PartitionToOffsetSum;
import org.apache.kafka.common.message.SubscriptionInfoData.TaskOffsetSum;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.generated.StreamsGroupMemberMetadataValue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * The Streams-specific metadata embedded in a classic-protocol member subscription.
 *
 * @param processId The Streams process ID.
 * @param userEndpoint The user endpoint, or {@code null} when not configured.
 * @param ownedTasks The member's previously active and standby tasks.
 * @param clientTags The client tags supported by this subscription version.
 * @param taskOffsetSums The available offset sums, keyed by subtopology and partition.
 * @param taskEndOffsetSums Known end-offset sums; absent entries mean the end offset is unknown.
 */
public record ClassicStreamsMemberMetadata(
    String processId,
    String userEndpoint,
    TasksTuple ownedTasks,
    Map<String, String> clientTags,
    Map<String, Map<Integer, Long>> taskOffsetSums,
    Map<String, Map<Integer, Long>> taskEndOffsetSums
) {
    private static final int MIN_VERSION_OFFSET_SUM_SUBSCRIPTION = 7;
    private static final int MIN_NAMED_TOPOLOGY_VERSION = 10;
    private static final int MIN_VERSION_TASK_END_OFFSET_SUM_SUBSCRIPTION = 12;
    private static final long LATEST_OFFSET = -2L;
    private static final long UNKNOWN_OFFSET_SUM = -3L;

    /** Backward-compatible constructor for call sites without end-offset telemetry. */
    public ClassicStreamsMemberMetadata(
        String processId,
        String userEndpoint,
        TasksTuple ownedTasks,
        Map<String, String> clientTags,
        Map<String, Map<Integer, Long>> taskOffsetSums
    ) {
        this(processId, userEndpoint, ownedTasks, clientTags, taskOffsetSums, Map.of());
    }

    /**
     * Creates an immutable representation of the decoded legacy metadata.
     *
     * @param processId The Streams process ID.
     * @param userEndpoint The user endpoint, or {@code null} when not configured.
     * @param ownedTasks The member's previously active and standby tasks.
     * @param clientTags The client tags supported by this subscription version.
     * @param taskOffsetSums The available offset sums, keyed by subtopology and partition.
     * @param taskEndOffsetSums Known end-offset sums; absent entries mean the end offset is unknown.
     */
    public ClassicStreamsMemberMetadata {
        Objects.requireNonNull(processId, "processId cannot be null");
        Objects.requireNonNull(ownedTasks, "ownedTasks cannot be null");
        clientTags = Collections.unmodifiableMap(new HashMap<>(clientTags));
        ownedTasks = new TasksTuple(
            immutableTaskMap(ownedTasks.activeTasks()),
            immutableTaskMap(ownedTasks.standbyTasks()),
            immutableTaskMap(ownedTasks.warmupTasks())
        );
        final Map<String, Map<Integer, Long>> immutableOffsetSums = new HashMap<>();
        taskOffsetSums.forEach((subtopology, offsets) ->
            immutableOffsetSums.put(subtopology, Collections.unmodifiableMap(new HashMap<>(offsets)))
        );
        taskOffsetSums = Collections.unmodifiableMap(immutableOffsetSums);
        final Map<String, Map<Integer, Long>> immutableEndOffsetSums = new HashMap<>();
        taskEndOffsetSums.forEach((subtopology, offsets) ->
            immutableEndOffsetSums.put(subtopology, Collections.unmodifiableMap(new HashMap<>(offsets)))
        );
        taskEndOffsetSums = Collections.unmodifiableMap(immutableEndOffsetSums);
    }

    /**
     * The protocol version and the legacy client's probing interval, where present.
     * Versions before 12 have no interval field and therefore return -1 for the interval.
     */
    public record ProbingRebalanceConfiguration(
        int version,
        int latestSupportedVersion,
        long probingRebalanceIntervalMs
    ) { }

    /**
     * Reads only the protocol-version fields needed to reconstruct a classic assignment deadline.
     * The caller must pass the user-data bytes from ConsumerProtocol.Subscription, not the outer subscription.
     */
    public static ProbingRebalanceConfiguration decodeProbingRebalanceConfiguration(ByteBuffer userData) {
        Objects.requireNonNull(userData, "userData cannot be null");
        final ByteBuffer buffer = userData.duplicate();
        if (buffer.remaining() < Integer.BYTES) {
            throw new SchemaException("Classic Streams subscription metadata is too short to contain a version.");
        }

        final int version = buffer.getInt(buffer.position());
        if (version < 1) {
            throw new SchemaException("Invalid classic Streams subscription metadata version: " + version);
        }
        if (version > SubscriptionInfoData.HIGHEST_SUPPORTED_VERSION) {
            throw new UnsupportedVersionException(
                "Classic Streams subscription metadata version " + version + " is not supported by this broker."
            );
        }

        final SubscriptionInfoData data = new SubscriptionInfoData(new ByteBufferAccessor(buffer), (short) version);
        return new ProbingRebalanceConfiguration(
            version,
            data.latestSupportedVersion(),
            data.probingRebalanceIntervalMs()
        );
    }

    /**
     * Decodes the Streams metadata stored in a classic member's ConsumerProtocol subscription.
    /**
     * Decodes the Streams metadata stored in a classic member's ConsumerProtocol subscription.
     *
     * @param userData The user-data byte buffer from the classic member subscription.
     * @return The decoded metadata.
     * @throws SchemaException if the metadata is malformed or uses named topologies.
     * @throws UnsupportedVersionException if its version is newer than this broker supports.
     */
    public static ClassicStreamsMemberMetadata decode(ByteBuffer userData) {
        Objects.requireNonNull(userData, "userData cannot be null");
        final ByteBuffer buffer = userData.duplicate();
        if (buffer.remaining() < Integer.BYTES) {
            throw new SchemaException("Classic Streams subscription metadata is too short to contain a version.");
        }

        final int version = buffer.getInt(buffer.position());
        if (version < 1) {
            throw new SchemaException("Invalid classic Streams subscription metadata version: " + version);
        }
        if (version > SubscriptionInfoData.HIGHEST_SUPPORTED_VERSION) {
            throw new UnsupportedVersionException(
                "Classic Streams subscription metadata version " + version + " is not supported by this broker."
            );
        }

        final SubscriptionInfoData data = new SubscriptionInfoData(new ByteBufferAccessor(buffer), (short) version);
        final Uuid encodedProcessId = data.processId();
        if (encodedProcessId == null) {
            throw new SchemaException("Classic Streams subscription metadata has no process ID.");
        }
        final String processId = new UUID(
            encodedProcessId.getMostSignificantBits(),
            encodedProcessId.getLeastSignificantBits()
        ).toString();
        final byte[] encodedEndpoint = data.userEndPoint();
        final String userEndpoint = encodedEndpoint == null || encodedEndpoint.length == 0
            ? null
            : new String(encodedEndpoint, StandardCharsets.UTF_8);

        final DecodedTasks decodedTasks = decodeTasks(data, version);
        final Map<String, Map<Integer, Long>> taskEndOffsetSums = decodeTaskEndOffsetSums(data, version);

        final Map<String, String> clientTags = new HashMap<>();
        if (data.clientTags() != null) {
            data.clientTags().forEach(tag -> clientTags.put(
                new String(tag.key(), StandardCharsets.UTF_8),
                new String(tag.value(), StandardCharsets.UTF_8)
            ));
        }

        return new ClassicStreamsMemberMetadata(
            processId,
            userEndpoint,
            decodedTasks.ownedTasks(),
            clientTags,
            decodedTasks.taskOffsetSums(),
            taskEndOffsetSums
        );
    }

    private static Map<String, Map<Integer, Long>> decodeTaskEndOffsetSums(
        SubscriptionInfoData data,
        int version
    ) {
        if (version < MIN_VERSION_TASK_END_OFFSET_SUM_SUBSCRIPTION || data.taskEndOffsetSums() == null) {
            return Map.of();
        }

        final Map<String, Map<Integer, Long>> taskEndOffsetSums = new HashMap<>();
        for (TaskOffsetSum endOffsetSum : data.taskEndOffsetSums()) {
            if (endOffsetSum.namedTopology() != null) {
                throw new SchemaException("Named topologies cannot be migrated to Streams groups.");
            }

            final long value = endOffsetSum.offsetSum();
            // Long.MAX_VALUE is the StateUpdater's unknown/overflow sentinel, not a usable end offset.
            if (value < 0L || value == Long.MAX_VALUE) {
                continue;
            }

            final String subtopologyId = Integer.toString(endOffsetSum.topicGroupId());
            final Map<Integer, Long> byPartition = taskEndOffsetSums.computeIfAbsent(
                subtopologyId,
                ignored -> new HashMap<>()
            );
            if (byPartition.putIfAbsent(endOffsetSum.partition(), value) != null) {
                throw new SchemaException("Classic Streams subscription contains duplicate task end-offset sums.");
            }
        }
        return taskEndOffsetSums;
    }

    private record DecodedTasks(
        TasksTuple ownedTasks,
        Map<String, Map<Integer, Long>> taskOffsetSums
    ) { }

    private static DecodedTasks decodeTasks(SubscriptionInfoData data, int version) {
        final Map<String, Set<Integer>> activeTasks = new HashMap<>();
        final Map<String, Set<Integer>> standbyTasks = new HashMap<>();
        final Map<String, Map<Integer, Long>> taskOffsetSums = new HashMap<>();
        if (version < MIN_VERSION_OFFSET_SUM_SUBSCRIPTION) {
            decodeLegacyTasks(data, activeTasks, standbyTasks, taskOffsetSums);
        } else {
            decodeOffsetSumTasks(data, version, activeTasks, standbyTasks, taskOffsetSums);
        }
        return new DecodedTasks(new TasksTuple(activeTasks, standbyTasks, Map.of()), taskOffsetSums);
    }

    private static void decodeLegacyTasks(
        SubscriptionInfoData data,
        Map<String, Set<Integer>> activeTasks,
        Map<String, Set<Integer>> standbyTasks,
        Map<String, Map<Integer, Long>> taskOffsetSums
    ) {
        data.prevTasks().forEach(task -> addTask(
            activeTasks, standbyTasks, taskOffsetSums,
            task.topicGroupId(), task.partition(), LATEST_OFFSET
        ));
        data.standbyTasks().forEach(task -> addTask(
            activeTasks, standbyTasks, taskOffsetSums,
            task.topicGroupId(), task.partition(), UNKNOWN_OFFSET_SUM
        ));
    }

    private static void decodeOffsetSumTasks(
        SubscriptionInfoData data,
        int version,
        Map<String, Set<Integer>> activeTasks,
        Map<String, Set<Integer>> standbyTasks,
        Map<String, Map<Integer, Long>> taskOffsetSums
    ) {
        for (TaskOffsetSum task : data.taskOffsetSums()) {
            if (version >= MIN_NAMED_TOPOLOGY_VERSION) {
                decodeNamedTopologyTask(task, activeTasks, standbyTasks, taskOffsetSums);
            } else {
                decodePartitionOffsetSums(task, activeTasks, standbyTasks, taskOffsetSums);
            }
        }
    }

    private static void decodeNamedTopologyTask(
        TaskOffsetSum task,
        Map<String, Set<Integer>> activeTasks,
        Map<String, Set<Integer>> standbyTasks,
        Map<String, Map<Integer, Long>> taskOffsetSums
    ) {
        if (task.namedTopology() != null) {
            throw new SchemaException("Named topologies cannot be migrated to Streams groups.");
        }
        addTask(
            activeTasks, standbyTasks, taskOffsetSums,
            task.topicGroupId(), task.partition(), task.offsetSum()
        );
    }

    private static void decodePartitionOffsetSums(
        TaskOffsetSum task,
        Map<String, Set<Integer>> activeTasks,
        Map<String, Set<Integer>> standbyTasks,
        Map<String, Map<Integer, Long>> taskOffsetSums
    ) {
        for (PartitionToOffsetSum partition : task.partitionToOffsetSum()) {
            addTask(
                activeTasks, standbyTasks, taskOffsetSums,
                task.topicGroupId(), partition.partition(), partition.offsetSum()
            );
        }
    }

    /**
     * Converts this legacy member metadata into the Streams-group representation used during online migration.
     * The classic group's generation is retained as the current member epoch and active-task assignment epoch.
     *
     * @param memberId The classic member ID.
     * @param instanceId The classic member's static instance ID, or {@code null} for a dynamic member.
     * @param rackId The member's rack ID, or {@code null} if not provided.
     * @param clientId The Kafka client ID.
     * @param clientHost The Kafka client host.
     * @param rebalanceTimeoutMs The member's rebalance timeout.
     * @param memberEpoch The classic group generation ID.
     * @param topologyEpoch The topology epoch supplied by the joining Streams-protocol member.
     * @return The Streams-group member representation.
     */
    public StreamsGroupMember toStreamsGroupMember(
        String memberId,
        String instanceId,
        String rackId,
        String clientId,
        String clientHost,
        int rebalanceTimeoutMs,
        int memberEpoch,
        int topologyEpoch
    ) {
        final Map<String, Map<Integer, Integer>> activeTasksWithEpochs = new HashMap<>();
        ownedTasks.activeTasks().forEach((subtopologyId, partitions) -> {
            final Map<Integer, Integer> partitionsWithEpochs = new HashMap<>();
            partitions.forEach(partition -> partitionsWithEpochs.put(partition, memberEpoch));
            activeTasksWithEpochs.put(subtopologyId, partitionsWithEpochs);
        });
        return new StreamsGroupMember.Builder(memberId)
            .setMemberEpoch(memberEpoch)
            .setPreviousMemberEpoch(Math.max(0, memberEpoch - 1))
            .setState(MemberState.STABLE)
            .setInstanceId(instanceId)
            .setRackId(rackId)
            .setClientId(clientId)
            .setClientHost(clientHost)
            .setRebalanceTimeoutMs(rebalanceTimeoutMs)
            .setTopologyEpoch(topologyEpoch)
            .setProcessId(processId)
            .setUserEndpoint(toEndpoint(userEndpoint))
            .setClientTags(clientTags)
            .setAssignedTasks(new TasksTupleWithEpochs(activeTasksWithEpochs, ownedTasks.standbyTasks(), ownedTasks.warmupTasks()))
            .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
            .build();
    }

    private static StreamsGroupMemberMetadataValue.Endpoint toEndpoint(String endpoint) {
        if (endpoint == null || endpoint.isBlank()) {
            return null;
        }
        try {
            final String host = Utils.getHost(endpoint);
            final Integer port = Utils.getPort(endpoint);
            if (host == null || host.isBlank() || port == null) {
                throw new SchemaException("Malformed classic Streams user endpoint: " + endpoint);
            }
            return new StreamsGroupMemberMetadataValue.Endpoint()
                .setHost(host)
                .setPort(port);
        } catch (NumberFormatException exception) {
            throw new SchemaException("Malformed classic Streams user endpoint: " + endpoint, exception);
        }
    }

    private static void addTask(
        Map<String, Set<Integer>> activeTasks,
        Map<String, Set<Integer>> standbyTasks,
        Map<String, Map<Integer, Long>> taskOffsetSums,
        int topicGroupId,
        int partition,
        long offsetSum
    ) {
        final String subtopologyId = Integer.toString(topicGroupId);
        final Map<String, Set<Integer>> tasks = offsetSum == LATEST_OFFSET ? activeTasks : standbyTasks;
        tasks.computeIfAbsent(subtopologyId, __ -> new HashSet<>()).add(partition);
        taskOffsetSums.computeIfAbsent(subtopologyId, __ -> new HashMap<>()).put(partition, offsetSum);
    }

    private static Map<String, Set<Integer>> immutableTaskMap(Map<String, Set<Integer>> tasks) {
        final Map<String, Set<Integer>> immutableTasks = new HashMap<>();
        tasks.forEach((subtopology, partitions) ->
            immutableTasks.put(subtopology, Collections.unmodifiableSet(new HashSet<>(partitions)))
        );
        return Collections.unmodifiableMap(immutableTasks);
    }
}
