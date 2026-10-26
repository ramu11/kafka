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
package org.apache.kafka.coordinator.group;

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.SubscriptionInfoData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.KRaftCoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.group.generated.StreamsGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue.Subtopology;
import org.apache.kafka.coordinator.group.streams.ClassicStreamsAssignment;
import org.apache.kafka.coordinator.group.streams.MemberState;
import org.apache.kafka.coordinator.group.streams.StreamsGroup;
import org.apache.kafka.coordinator.group.streams.StreamsGroupBuilder;
import org.apache.kafka.coordinator.group.streams.StreamsGroupMember;
import org.apache.kafka.coordinator.group.streams.StreamsTopology;
import org.apache.kafka.coordinator.group.streams.TasksTuple;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Base64;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.coordinator.group.StreamsGroupTestUtil.streamsGroupMemberBuilderWithDefaults;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClassicStreamsGroupProtocolBridgeTest {
    @Test
    void shouldTranslateJoinGroupAndSyncGroupForMigratedClassicStreamsMember() throws Exception {
        final String groupId = "classic-streams-bridge";
        final String memberId = "classic-member";
        final int assignmentEpoch = 2;
        final TasksTuple targetAssignment = new TasksTuple(
            Map.of("0", Set.of(1)),
            Map.of("1", Set.of(2)),
            Map.of("2", Set.of(3))
        );
        final StreamsGroupMember member = streamsGroupMemberBuilderWithDefaults(memberId)
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(0)
            .setTopologyEpoch(-1)
            .setClassicSessionTimeoutMs(5000)
            .setClassicProtocolMetadata("migration-metadata-placeholder")
            .build();
        final StreamsTopology topology = new StreamsTopology(0, Map.of(
            "0", new Subtopology().setSubtopologyId("0").setSourceTopics(List.of("input-topic")),
            "1", new Subtopology().setSubtopologyId("1").setSourceTopics(List.of("input-topic-1")),
            "2", new Subtopology().setSubtopologyId("2").setSourceTopics(List.of("input-topic-2"))
        ));
        final CoordinatorMetadataImage metadataImage = new KRaftCoordinatorMetadataImage(new MetadataImageBuilder()
            .addTopic(Uuid.randomUuid(), "input-topic", 2)
            .addTopic(Uuid.randomUuid(), "input-topic-1", 4)
            .addTopic(Uuid.randomUuid(), "input-topic-2", 4)
            .build());
        final GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withMetadataImage(metadataImage)
            .withStreamsGroup(new StreamsGroupBuilder(groupId, assignmentEpoch)
                .withTopology(topology)
                .withValidatedTopologyEpoch(0)
                .withMember(member)
                .withTargetAssignment(memberId, targetAssignment)
                .withTargetAssignmentEpoch(assignmentEpoch))
            .build();

        final SubscriptionInfoData subscriptionInfo = new SubscriptionInfoData()
            .setVersion(12)
            .setLatestSupportedVersion(12)
            .setProcessId(Uuid.randomUuid())
            .setUserEndPoint(new byte[0])
            .setTaskOffsetSums(List.of(new SubscriptionInfoData.TaskOffsetSum()
                .setTopicGroupId(1)
                .setPartition(2)
                .setOffsetSum(123L)
                .setNamedTopology(null)))
            .setTaskEndOffsetSums(List.of(
                new SubscriptionInfoData.TaskOffsetSum()
                    .setTopicGroupId(1)
                    .setPartition(2)
                    .setOffsetSum(456L)
                    .setNamedTopology(null),
                new SubscriptionInfoData.TaskOffsetSum()
                    .setTopicGroupId(2)
                    .setPartition(3)
                    .setOffsetSum(Long.MAX_VALUE)
                    .setNamedTopology(null)
            ))
            .setClientTags(List.of())
            .setProbingRebalanceIntervalMs(123_456L);
        final ByteBuffer userData = MessageUtil.toByteBufferAccessor(subscriptionInfo, (short) 12).buffer();
        final byte[] subscription = ConsumerProtocol.serializeSubscription(
            new Subscription(List.of("input-topic", "input-topic-1", "input-topic-2"), userData)
        ).array();
        final JoinGroupRequestData.JoinGroupRequestProtocolCollection protocols =
            new JoinGroupRequestData.JoinGroupRequestProtocolCollection(1);
        protocols.add(new JoinGroupRequestData.JoinGroupRequestProtocol()
            .setName("stream")
            .setMetadata(subscription));

        final JoinGroupRequestData joinRequest = new JoinGroupRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId)
            .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
            .setProtocols(protocols)
            .setSessionTimeoutMs(5000)
            .setRebalanceTimeoutMs(10000);
        final GroupMetadataManagerTestContext.JoinResult join = context.sendClassicGroupJoin(joinRequest, true, true);
        assertFalse(join.joinFuture.isDone());
        join.appendFuture.complete(null);
        final JoinGroupResponseData joinResponse = join.joinFuture.get();

        assertEquals(Errors.NONE.code(), joinResponse.errorCode());
        assertEquals(ConsumerProtocol.PROTOCOL_TYPE, joinResponse.protocolType());
        assertEquals("stream", joinResponse.protocolName());
        assertEquals(assignmentEpoch, joinResponse.generationId());
        assertEquals(memberId, joinResponse.leader());
        assertTrue(joinResponse.skipAssignment());

        final GroupMetadataManagerTestContext.SyncResult sync = context.sendClassicGroupSync(
            new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
                .withGroupId(groupId)
                .withMemberId(memberId)
                .withGenerationId(assignmentEpoch)
                .withProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                .withProtocolName("stream")
                .build()
        );
        assertTrue(sync.syncFuture.isDone());
        assertEquals(Errors.NONE.code(), sync.syncFuture.get().errorCode());
        assertEquals(ConsumerProtocol.PROTOCOL_TYPE, sync.syncFuture.get().protocolType());
        assertEquals("stream", sync.syncFuture.get().protocolName());
        final Assignment decodedAssignment = ConsumerProtocol.deserializeAssignment(
            ByteBuffer.wrap(sync.syncFuture.get().assignment()));
        assertEquals(List.of(new TopicPartition("input-topic", 1)), decodedAssignment.partitions());
        try (DataInputStream input = new DataInputStream(
            new ByteArrayInputStream(toByteArray(decodedAssignment.userData())))) {
            // The v12 subscription negotiates AssignmentInfo v12, not the old v1-only layout.
            assertEquals(12, input.readInt()); // Used assignment metadata version.
            assertEquals(12, input.readInt()); // Commonly supported assignment metadata version.

            assertEquals(1, input.readInt()); // Active task count.
            assertEquals(0, input.readInt()); // Active task subtopology.
            assertEquals(1, input.readInt()); // Active task partition.
            assertEquals(0, input.readInt()); // Empty named-topology field (v10+).

            assertEquals(2, input.readInt()); // Standby plus warm-up task count.
            assertEquals(1, input.readInt()); // First task subtopology.
            assertEquals(2, input.readInt()); // First task partition.
            assertEquals(0, input.readInt()); // Empty named-topology field (v10+).
            assertEquals(1, input.readInt()); // First task input partition count.
            assertEquals("input-topic-1", input.readUTF());
            assertEquals(2, input.readInt());

            assertEquals(2, input.readInt()); // Second task subtopology.
            assertEquals(3, input.readInt()); // Second task partition.
            assertEquals(0, input.readInt()); // Empty named-topology field (v10+).
            assertEquals(1, input.readInt()); // Second task input partition count.
            assertEquals("input-topic-2", input.readUTF());
            assertEquals(3, input.readInt());

            // v6+ host metadata section: empty topic dictionary, active/standby host maps and error code.
            assertEquals(0, input.readInt()); // Topic dictionary size.
            assertEquals(0, input.readInt()); // Active partitions by host.
            assertEquals(0, input.readInt()); // Standby partitions by host.
            assertEquals(0, input.readInt()); // Assignment error code.
            input.readLong(); // nextRebalanceMs (Long.MAX_VALUE when no deadline is scheduled).
            assertEquals(0, input.available());
        }
        final StreamsGroup updatedGroup = context.groupMetadataManager.streamsGroup(groupId);
        assertEquals(assignmentEpoch, updatedGroup.getMemberOrThrow(memberId).memberEpoch());
        assertEquals(MemberState.STABLE, updatedGroup.getMemberOrThrow(memberId).state());
        assertEquals(5000, updatedGroup.getMemberOrThrow(memberId).classicSessionTimeoutMs());
        assertEquals(Base64.getEncoder().encodeToString(subscription),
            updatedGroup.getMemberOrThrow(memberId).classicProtocolMetadata());
        assertEquals(Map.of("1", Map.of(2, 123L)), updatedGroup.taskOffsets(memberId).taskOffsets(),
            "Known legacy Streams offset sums must reach the Streams assignor's transient telemetry.");
        assertEquals(Map.of("1", Map.of(2, 456L)), updatedGroup.taskOffsets(memberId).taskEndOffsets(),
            "Only the known end-offset sum actually reported by v12 classic metadata is forwarded.");
    }

    @Test
    void shouldNotAdvanceClassicMemberEpochBeforeOwnedTasksAreReleased() throws Exception {
        final String groupId = "classic-streams-bridge-revocation";
        final String memberId = "classic-member";
        final int assignmentEpoch = 2;
        final TasksTuple targetAssignment = new TasksTuple(
            Map.of("0", Set.of(1)),
            Map.of(),
            Map.of()
        );
        final StreamsGroupMember member = streamsGroupMemberBuilderWithDefaults(memberId)
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(0)
            .setTopologyEpoch(-1)
            .setAssignedTasks(new org.apache.kafka.coordinator.group.streams.TasksTupleWithEpochs(
                Map.of("0", Map.of(0, 1)), Map.of(), Map.of()))
            .setClassicSessionTimeoutMs(5000)
            .setClassicProtocolMetadata("migration-metadata-placeholder")
            .build();
        final StreamsTopology topology = new StreamsTopology(0, Map.of(
            "0", new Subtopology().setSubtopologyId("0").setSourceTopics(List.of("input-topic"))
        ));
        final CoordinatorMetadataImage metadataImage = new KRaftCoordinatorMetadataImage(new MetadataImageBuilder()
            .addTopic(Uuid.randomUuid(), "input-topic", 2)
            .build());
        final GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withMetadataImage(metadataImage)
            .withStreamsGroup(new StreamsGroupBuilder(groupId, assignmentEpoch)
                .withTopology(topology)
                .withValidatedTopologyEpoch(0)
                .withMember(member)
                .withTargetAssignment(memberId, targetAssignment)
                .withTargetAssignmentEpoch(assignmentEpoch))
            .build();

        final SubscriptionInfoData subscriptionInfo = new SubscriptionInfoData()
            .setVersion(11)
            .setLatestSupportedVersion(11)
            .setProcessId(Uuid.randomUuid())
            .setUserEndPoint(new byte[0])
            .setTaskOffsetSums(List.of(new SubscriptionInfoData.TaskOffsetSum()
                .setTopicGroupId(0)
                .setPartition(0)
                .setOffsetSum(-2L)
                .setNamedTopology(null)))
            .setClientTags(List.of());
        final ByteBuffer userData = MessageUtil.toByteBufferAccessor(subscriptionInfo, (short) 11).buffer();
        final byte[] subscription = ConsumerProtocol.serializeSubscription(
            new Subscription(List.of("input-topic"), userData)
        ).array();
        final JoinGroupRequestData.JoinGroupRequestProtocolCollection protocols =
            new JoinGroupRequestData.JoinGroupRequestProtocolCollection(1);
        protocols.add(new JoinGroupRequestData.JoinGroupRequestProtocol()
            .setName("stream")
            .setMetadata(subscription));

        final JoinGroupRequestData joinRequest = new JoinGroupRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId)
            .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
            .setProtocols(protocols)
            .setSessionTimeoutMs(5000)
            .setRebalanceTimeoutMs(10000);
        final GroupMetadataManagerTestContext.JoinResult join = context.sendClassicGroupJoin(joinRequest, true, true);
        assertFalse(join.joinFuture.isDone());
        join.appendFuture.complete(null);
        final JoinGroupResponseData joinResponse = join.joinFuture.get();

        assertEquals(Errors.NONE.code(), joinResponse.errorCode());
        assertEquals(1, joinResponse.generationId(), "The classic member epoch must not advance before revocation is acknowledged.");

        final StreamsGroup updatedGroup = context.groupMetadataManager.streamsGroup(groupId);
        final StreamsGroupMember updatedMember = updatedGroup.getMemberOrThrow(memberId);
        assertEquals(1, updatedMember.memberEpoch());
        assertEquals(MemberState.UNREVOKED_TASKS, updatedMember.state());
        assertEquals(Set.of(0), updatedMember.tasksPendingRevocation().activeTasksWithEpochs().get("0").keySet());
    }

    @Test
    void shouldTreatClassicStandbyAsWarmupWhenWaitingForWarmupRevocation() throws Exception {
        final String groupId = "classic-streams-bridge-warmup-revocation";
        final String memberId = "classic-member";
        final int assignmentEpoch = 2;
        final TasksTuple targetAssignment = new TasksTuple(
            Map.of("0", Set.of(1)),
            Map.of(),
            Map.of()
        );
        final StreamsGroupMember member = streamsGroupMemberBuilderWithDefaults(memberId)
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(0)
            .setTopologyEpoch(-1)
            .setAssignedTasks(new org.apache.kafka.coordinator.group.streams.TasksTupleWithEpochs(
                Map.of(), Map.of(), Map.of("0", Set.of(0))))
            .setClassicSessionTimeoutMs(5000)
            .setClassicProtocolMetadata("migration-metadata-placeholder")
            .build();
        final StreamsTopology topology = new StreamsTopology(0, Map.of(
            "0", new Subtopology().setSubtopologyId("0").setSourceTopics(List.of("input-topic"))
        ));
        final CoordinatorMetadataImage metadataImage = new KRaftCoordinatorMetadataImage(new MetadataImageBuilder()
            .addTopic(Uuid.randomUuid(), "input-topic", 2)
            .build());
        final GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withMetadataImage(metadataImage)
            .withStreamsGroup(new StreamsGroupBuilder(groupId, assignmentEpoch)
                .withTopology(topology)
                .withValidatedTopologyEpoch(0)
                .withMember(member)
                .withTargetAssignment(memberId, targetAssignment)
                .withTargetAssignmentEpoch(assignmentEpoch))
            .build();

        // Classic Streams has only active and standby task roles, so report warm-up task 0 as standby.
        final SubscriptionInfoData subscriptionInfo = new SubscriptionInfoData()
            .setVersion(11)
            .setLatestSupportedVersion(11)
            .setProcessId(Uuid.randomUuid())
            .setUserEndPoint(new byte[0])
            .setTaskOffsetSums(List.of(new SubscriptionInfoData.TaskOffsetSum()
                .setTopicGroupId(0)
                .setPartition(0)
                .setOffsetSum(-3L)
                .setNamedTopology(null)))
            .setClientTags(List.of());
        final ByteBuffer userData = MessageUtil.toByteBufferAccessor(subscriptionInfo, (short) 11).buffer();
        final byte[] subscription = ConsumerProtocol.serializeSubscription(
            new Subscription(List.of("input-topic"), userData)
        ).array();
        final JoinGroupRequestData.JoinGroupRequestProtocolCollection protocols =
            new JoinGroupRequestData.JoinGroupRequestProtocolCollection(1);
        protocols.add(new JoinGroupRequestData.JoinGroupRequestProtocol()
            .setName("stream")
            .setMetadata(subscription));

        final JoinGroupRequestData joinRequest = new JoinGroupRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId)
            .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
            .setProtocols(protocols)
            .setSessionTimeoutMs(5000)
            .setRebalanceTimeoutMs(10000);
        final GroupMetadataManagerTestContext.JoinResult join = context.sendClassicGroupJoin(joinRequest, true, true);
        assertFalse(join.joinFuture.isDone());
        join.appendFuture.complete(null);
        final JoinGroupResponseData joinResponse = join.joinFuture.get();

        assertEquals(Errors.NONE.code(), joinResponse.errorCode());
        assertEquals(1, joinResponse.generationId(),
            "The classic member epoch must not advance while its reported standby is a warm-up pending revocation.");

        final StreamsGroup updatedGroup = context.groupMetadataManager.streamsGroup(groupId);
        final StreamsGroupMember updatedMember = updatedGroup.getMemberOrThrow(memberId);
        assertEquals(1, updatedMember.memberEpoch());
        assertEquals(MemberState.UNREVOKED_TASKS, updatedMember.state());
        assertEquals(Set.of(0), updatedMember.tasksPendingRevocation().warmupTasks().get("0"));
    }

    @Test
    void shouldTriggerOnlyOneClassicProbingRebalancePerAssignmentEpoch() throws Exception {
        final String groupId = "classic-streams-bridge-probing";
        final int assignmentEpoch = 2;
        final StreamsGroupMember memberA = streamsGroupMemberBuilderWithDefaults("classic-a")
            .setMemberEpoch(assignmentEpoch)
            .setPreviousMemberEpoch(assignmentEpoch - 1)
            .setTopologyEpoch(-1)
            .setProcessId("process-a")
            .setAssignedTasks(new org.apache.kafka.coordinator.group.streams.TasksTupleWithEpochs(
                Map.of("0", Map.of(0, assignmentEpoch)), Map.of(), Map.of("0", Set.of(1))))
            .setClassicSessionTimeoutMs(5000)
            .setClassicProtocolMetadata("migration-metadata-placeholder")
            .build();
        final StreamsGroupMember memberB = streamsGroupMemberBuilderWithDefaults("classic-b")
            .setMemberEpoch(assignmentEpoch)
            .setPreviousMemberEpoch(assignmentEpoch - 1)
            .setTopologyEpoch(-1)
            .setProcessId("process-b")
            .setAssignedTasks(new org.apache.kafka.coordinator.group.streams.TasksTupleWithEpochs(
                Map.of("0", Map.of(1, assignmentEpoch)), Map.of(), Map.of()))
            .setClassicSessionTimeoutMs(5000)
            .setClassicProtocolMetadata("migration-metadata-placeholder")
            .build();
        // The native member also has a warm-up task, but the dedicated probe candidate must be a classic member
        // that itself holds warm-up work. The classic view of classic-a's warm-up task is a standby task.
        final StreamsGroupMember nativeMember = streamsGroupMemberBuilderWithDefaults("streams-c")
            .setMemberEpoch(assignmentEpoch)
            .setPreviousMemberEpoch(assignmentEpoch - 1)
            .setTopologyEpoch(0)
            .setProcessId("process-c")
            .setAssignedTasks(new org.apache.kafka.coordinator.group.streams.TasksTupleWithEpochs(
                Map.of(), Map.of(), Map.of("0", Set.of(2))))
            .build();

        final TasksTuple targetAssignmentA = new TasksTuple(Map.of("0", Set.of(0)), Map.of(), Map.of());
        final TasksTuple targetAssignmentB = new TasksTuple(Map.of("0", Set.of(1)), Map.of(), Map.of());
        final TasksTuple targetAssignmentC = new TasksTuple(Map.of(), Map.of("0", Set.of(0)), Map.of());
        final StreamsTopology topology = new StreamsTopology(0, Map.of(
            "0", new Subtopology().setSubtopologyId("0").setSourceTopics(List.of("input-topic"))
        ));
        final CoordinatorMetadataImage metadataImage = new KRaftCoordinatorMetadataImage(new MetadataImageBuilder()
            .addTopic(Uuid.randomUuid(), "input-topic", 3)
            .build());
        final GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withMetadataImage(metadataImage)
            .withStreamsGroup(new StreamsGroupBuilder(groupId, assignmentEpoch)
                .withTopology(topology)
                .withValidatedTopologyEpoch(0)
                .withMember(memberA)
                .withMember(memberB)
                .withMember(nativeMember)
                .withTargetAssignment("classic-a", targetAssignmentA)
                .withTargetAssignment("classic-b", targetAssignmentB)
                .withTargetAssignment("streams-c", targetAssignmentC)
                .withTargetAssignmentEpoch(assignmentEpoch))
            .build();

        final HeartbeatRequestData heartbeatA = new HeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId("classic-a")
            .setGenerationId(assignmentEpoch);
        final var firstProbe = context.sendClassicGroupHeartbeat(heartbeatA);
        assertEquals(Errors.REBALANCE_IN_PROGRESS.code(), firstProbe.response().errorCode());
        assertEquals(1, firstProbe.records().size(), "The first probe must persist its one-shot marker.");
        final StreamsGroupMemberMetadataValue persistedProbe =
            (StreamsGroupMemberMetadataValue) firstProbe.records().get(0).value().message();
        assertEquals(assignmentEpoch, persistedProbe.classicProbingRebalanceEpoch());
        assertTrue(persistedProbe.classicProbingRebalancePending());
        StreamsGroupMember updatedA = context.groupMetadataManager.streamsGroup(groupId).getMemberOrThrow("classic-a");
        assertEquals(assignmentEpoch, updatedA.classicProbingRebalanceEpoch());
        assertTrue(updatedA.classicProbingRebalancePending());

        final var repeatedProbe = context.sendClassicGroupHeartbeat(heartbeatA);
        assertEquals(Errors.REBALANCE_IN_PROGRESS.code(), repeatedProbe.response().errorCode());
        assertTrue(repeatedProbe.records().isEmpty(), "Repeated heartbeats must not create more probe records.");

        final var heartbeatB = context.sendClassicGroupHeartbeat(new HeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId("classic-b")
            .setGenerationId(assignmentEpoch));
        assertEquals(Errors.NONE.code(), heartbeatB.response().errorCode(),
            "Only a classic member that currently holds warm-up work should be selected.");

        // A successful JoinGroup refreshes classic Streams subscription metadata and completes this probe.
        final GroupMetadataManagerTestContext.JoinResult join = context.sendClassicGroupJoin(
            classicStreamsJoinRequest(groupId, "classic-a", 0), true, true);
        assertFalse(join.joinFuture.isDone());
        join.appendFuture.complete(null);
        assertEquals(Errors.NONE.code(), join.joinFuture.get().errorCode());
        updatedA = context.groupMetadataManager.streamsGroup(groupId).getMemberOrThrow("classic-a");
        assertEquals(assignmentEpoch, updatedA.classicProbingRebalanceEpoch());
        assertFalse(updatedA.classicProbingRebalancePending());

        final var afterJoin = context.sendClassicGroupHeartbeat(heartbeatA);
        assertEquals(Errors.NONE.code(), afterJoin.response().errorCode(),
            "The same assignment epoch must not create another immediate probe.");
    }

    @Test
    void shouldNotTriggerClassicProbingWhenWarmupTasksAreHeldOnlyByNativeMembers() {
        final String groupId = "classic-streams-bridge-native-warmup-only";
        final int assignmentEpoch = 2;
        final StreamsGroupMember classicMember = streamsGroupMemberBuilderWithDefaults("classic-a")
            .setMemberEpoch(assignmentEpoch)
            .setPreviousMemberEpoch(assignmentEpoch - 1)
            .setTopologyEpoch(-1)
            .setProcessId("process-a")
            .setAssignedTasks(new org.apache.kafka.coordinator.group.streams.TasksTupleWithEpochs(
                Map.of("0", Map.of(0, assignmentEpoch)), Map.of(), Map.of()))
            .setClassicSessionTimeoutMs(5000)
            .setClassicProtocolMetadata("migration-metadata-placeholder")
            .build();
        final StreamsGroupMember nativeMember = streamsGroupMemberBuilderWithDefaults("streams-c")
            .setMemberEpoch(assignmentEpoch)
            .setPreviousMemberEpoch(assignmentEpoch - 1)
            .setTopologyEpoch(0)
            .setProcessId("process-c")
            .setAssignedTasks(new org.apache.kafka.coordinator.group.streams.TasksTupleWithEpochs(
                Map.of(), Map.of(), Map.of("0", Set.of(1))))
            .build();
        final StreamsTopology topology = new StreamsTopology(0, Map.of(
            "0", new Subtopology().setSubtopologyId("0").setSourceTopics(List.of("input-topic"))
        ));
        final CoordinatorMetadataImage metadataImage = new KRaftCoordinatorMetadataImage(new MetadataImageBuilder()
            .addTopic(Uuid.randomUuid(), "input-topic", 2)
            .build());
        final GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withMetadataImage(metadataImage)
            .withStreamsGroup(new StreamsGroupBuilder(groupId, assignmentEpoch)
                .withTopology(topology)
                .withValidatedTopologyEpoch(0)
                .withMember(classicMember)
                .withMember(nativeMember)
                .withTargetAssignment("classic-a", new TasksTuple(Map.of("0", Set.of(0)), Map.of(), Map.of()))
                .withTargetAssignment("streams-c", new TasksTuple(Map.of("0", Set.of(1)), Map.of(), Map.of()))
                .withTargetAssignmentEpoch(assignmentEpoch))
            .build();

        final var heartbeat = context.sendClassicGroupHeartbeat(new HeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId("classic-a")
            .setGenerationId(assignmentEpoch));
        assertEquals(Errors.NONE.code(), heartbeat.response().errorCode());
        assertTrue(heartbeat.records().isEmpty(),
            "A native member's warm-up task must not independently select an unrelated classic member for probing.");
    }

    private static JoinGroupRequestData classicStreamsJoinRequest(String groupId, String memberId, int partitionId) {
        final SubscriptionInfoData subscriptionInfo = new SubscriptionInfoData()
            .setVersion(11)
            .setLatestSupportedVersion(11)
            .setProcessId(Uuid.randomUuid())
            .setUserEndPoint(new byte[0])
            .setTaskOffsetSums(List.of(new SubscriptionInfoData.TaskOffsetSum()
                .setTopicGroupId(0)
                .setPartition(partitionId)
                .setOffsetSum(-2L)
                .setNamedTopology(null)))
            .setClientTags(List.of());
        final ByteBuffer userData = MessageUtil.toByteBufferAccessor(subscriptionInfo, (short) 11).buffer();
        final byte[] subscription = ConsumerProtocol.serializeSubscription(
            new Subscription(List.of("input-topic"), userData)
        ).array();
        final JoinGroupRequestData.JoinGroupRequestProtocolCollection protocols =
            new JoinGroupRequestData.JoinGroupRequestProtocolCollection(1);
        protocols.add(new JoinGroupRequestData.JoinGroupRequestProtocol()
            .setName("stream")
            .setMetadata(subscription));
        return new JoinGroupRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId)
            .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
            .setProtocols(protocols)
            .setSessionTimeoutMs(5000)
            .setRebalanceTimeoutMs(10000);
    }

    @Test
    void shouldCarryClassicProbingDeadlineInAssignmentMetadata() throws Exception {
        final String groupId = "classic-streams-probing-deadline";
        final String memberId = "classic-member";
        final int assignmentEpoch = 1;
        final long nowMs = 10_000L;
        final long probingIntervalMs = 1_234L;
        final StreamsTopology topology = new StreamsTopology(0, Map.of(
            "0", new Subtopology().setSubtopologyId("0").setSourceTopics(List.of("input-topic"))
        ));
        final CoordinatorMetadataImage metadataImage = new KRaftCoordinatorMetadataImage(new MetadataImageBuilder()
            .addTopic(Uuid.randomUuid(), "input-topic", 2)
            .build());
        final TasksTuple targetAssignment = new TasksTuple(
            Map.of("0", Set.of(0)), Map.of(), Map.of("0", Set.of(1)));
        final org.apache.kafka.coordinator.group.streams.TasksTupleWithEpochs currentAssignment =
            new org.apache.kafka.coordinator.group.streams.TasksTupleWithEpochs(
                Map.of("0", Map.of(0, assignmentEpoch)), Map.of(), Map.of("0", Set.of(1)));
        final StreamsGroupMember member = streamsGroupMemberBuilderWithDefaults(memberId)
            .setMemberEpoch(assignmentEpoch)
            .setPreviousMemberEpoch(0)
            .setTopologyEpoch(-1)
            .setAssignedTasks(currentAssignment)
            .setClassicSessionTimeoutMs(5000)
            .build();
        final GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withMetadataImage(metadataImage)
            .withStreamsGroup(new StreamsGroupBuilder(groupId, assignmentEpoch)
                .withTopology(topology)
                .withValidatedTopologyEpoch(0)
                .withMember(member)
                .withTargetAssignment(memberId, targetAssignment)
                .withTargetAssignmentEpoch(assignmentEpoch))
            .build();
        final StreamsGroup group = context.groupMetadataManager.streamsGroup(groupId);

        final SubscriptionInfoData subscriptionInfo = new SubscriptionInfoData()
            .setVersion(12)
            .setLatestSupportedVersion(12)
            .setProcessId(Uuid.randomUuid())
            .setUserEndPoint(new byte[0])
            .setTaskOffsetSums(List.of())
            .setTaskEndOffsetSums(List.of())
            .setClientTags(List.of())
            .setProbingRebalanceIntervalMs(probingIntervalMs);
        final ByteBuffer userData = MessageUtil.toByteBufferAccessor(subscriptionInfo, (short) 12).buffer();
        final byte[] encodedSubscription = ConsumerProtocol.serializeSubscription(
            new Subscription(List.of("input-topic"), userData)
        ).array();
        final String classicProtocolMetadata = Base64.getEncoder().encodeToString(encodedSubscription);

        // Re-encoding the same assignment epoch on a later SyncGroup must re-arm the deadline.
        // That is the client-side equivalent of probing again if the warm-up task is still assigned.
        for (final long assignmentTimeMs : List.of(nowMs, nowMs + 50L)) {
            final org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Assignment legacyAssignment =
                ClassicStreamsAssignment.toConsumerAssignment(
                    group,
                    metadataImage,
                    currentAssignment,
                    classicProtocolMetadata,
                    assignmentTimeMs,
                    true
                );
            assertProbingDeadline(legacyAssignment, assignmentTimeMs + probingIntervalMs);
        }
    }

    private static void assertProbingDeadline(
        final org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Assignment assignment,
        final long expectedNextRebalanceMs
    ) throws Exception {
        try (DataInputStream input = new DataInputStream(
            new ByteArrayInputStream(toByteArray(assignment.userData())))) {
            assertEquals(12, input.readInt()); // AssignmentInfo version.
            assertEquals(12, input.readInt()); // Commonly supported version.
            assertEquals(1, input.readInt());  // Active task count.
            assertEquals(0, input.readInt());  // Active subtopology.
            assertEquals(0, input.readInt());  // Active partition.
            assertEquals(0, input.readInt());  // Empty topology name (v10+).
            assertEquals(1, input.readInt());  // Standby/warm-up task count.
            assertEquals(0, input.readInt());  // Warm-up subtopology.
            assertEquals(1, input.readInt());  // Warm-up partition.
            assertEquals(0, input.readInt());  // Empty topology name (v10+).
            assertEquals(1, input.readInt());  // Warm-up input partition count.
            assertEquals("input-topic", input.readUTF());
            assertEquals(1, input.readInt());
            assertEquals(0, input.readInt());  // Topic dictionary size.
            assertEquals(0, input.readInt());  // Partitions by host.
            assertEquals(0, input.readInt());  // Standby partitions by host.
            assertEquals(0, input.readInt());  // Assignment error code.
            assertEquals(expectedNextRebalanceMs, input.readLong());
            assertEquals(0, input.available());
        }
    }

    private static byte[] toByteArray(ByteBuffer data) {
        final ByteBuffer copy = data.duplicate();
        final byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return bytes;
    }
}
