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
import org.apache.kafka.common.message.SubscriptionInfoData.ClientTag;
import org.apache.kafka.common.message.SubscriptionInfoData.TaskId;
import org.apache.kafka.common.message.SubscriptionInfoData.TaskOffsetSum;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.protocol.types.SchemaException;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ClassicStreamsMemberMetadataTest {
    @Test
    void shouldDecodeLatestSubscriptionMetadata() {
        final UUID processId = UUID.fromString("c6f40ec4-a6b7-4d01-8f1f-7ad5782d1b4a");
        final SubscriptionInfoData data = new SubscriptionInfoData()
            .setVersion(11)
            .setLatestSupportedVersion(11)
            .setProcessId(new Uuid(processId.getMostSignificantBits(), processId.getLeastSignificantBits()))
            .setUserEndPoint("localhost:8080".getBytes(StandardCharsets.UTF_8))
            .setTaskOffsetSums(List.of(
                new TaskOffsetSum()
                    .setTopicGroupId(0)
                    .setPartition(1)
                    .setOffsetSum(-2L)
                    .setNamedTopology(null),
                new TaskOffsetSum()
                    .setTopicGroupId(2)
                    .setPartition(3)
                    .setOffsetSum(42L)
                    .setNamedTopology(null)
            ))
            .setClientTags(List.of(new ClientTag()
                .setKey("rack".getBytes(StandardCharsets.UTF_8))
                .setValue("west".getBytes(StandardCharsets.UTF_8))));

        final ByteBuffer encoded = MessageUtil.toByteBufferAccessor(data, (short) 11).buffer();
        final ClassicStreamsMemberMetadata decoded = ClassicStreamsMemberMetadata.decode(encoded);

        assertEquals(processId.toString(), decoded.processId());
        assertEquals("localhost:8080", decoded.userEndpoint());
        assertEquals(Map.of("0", Set.of(1)), decoded.ownedTasks().activeTasks());
        assertEquals(Map.of("2", Set.of(3)), decoded.ownedTasks().standbyTasks());
        assertEquals(Map.of("rack", "west"), decoded.clientTags());
        assertEquals(Map.of("0", Map.of(1, -2L), "2", Map.of(3, 42L)), decoded.taskOffsetSums());
    }

    @Test
    void shouldDecodeLegacySubscriptionMetadata() {
        final UUID processId = UUID.fromString("c6f40ec4-a6b7-4d01-8f1f-7ad5782d1b4a");
        final SubscriptionInfoData data = new SubscriptionInfoData()
            .setVersion(6)
            .setLatestSupportedVersion(11)
            .setProcessId(new Uuid(processId.getMostSignificantBits(), processId.getLeastSignificantBits()))
            .setPrevTasks(List.of(new TaskId().setTopicGroupId(1).setPartition(2)))
            .setStandbyTasks(List.of(new TaskId().setTopicGroupId(3).setPartition(4)));

        final ByteBuffer encoded = MessageUtil.toByteBufferAccessor(data, (short) 6).buffer();
        final ClassicStreamsMemberMetadata decoded = ClassicStreamsMemberMetadata.decode(encoded);

        assertEquals(Map.of("1", Set.of(2)), decoded.ownedTasks().activeTasks());
        assertEquals(Map.of("3", Set.of(4)), decoded.ownedTasks().standbyTasks());
        assertEquals(Map.of("1", Map.of(2, -2L), "3", Map.of(4, -3L)), decoded.taskOffsetSums());
    }

    @Test
    void shouldConvertToStreamsGroupMember() {
        final UUID processId = UUID.fromString("c6f40ec4-a6b7-4d01-8f1f-7ad5782d1b4a");
        final SubscriptionInfoData data = new SubscriptionInfoData()
            .setVersion(11)
            .setLatestSupportedVersion(11)
            .setProcessId(new Uuid(processId.getMostSignificantBits(), processId.getLeastSignificantBits()))
            .setUserEndPoint("localhost:8080".getBytes(StandardCharsets.UTF_8))
            .setTaskOffsetSums(List.of(
                new TaskOffsetSum()
                    .setTopicGroupId(0)
                    .setPartition(1)
                    .setOffsetSum(-2L)
                    .setNamedTopology(null),
                new TaskOffsetSum()
                    .setTopicGroupId(2)
                    .setPartition(3)
                    .setOffsetSum(42L)
                    .setNamedTopology(null)
            ))
            .setClientTags(List.of(new ClientTag()
                .setKey("rack".getBytes(StandardCharsets.UTF_8))
                .setValue("west".getBytes(StandardCharsets.UTF_8))));
        final ByteBuffer encoded = MessageUtil.toByteBufferAccessor(data, (short) 11).buffer();
        final ClassicStreamsMemberMetadata decoded = ClassicStreamsMemberMetadata.decode(encoded);

        final StreamsGroupMember member = decoded.toStreamsGroupMember(
            "member-1", "instance-1", "rack-east", "client-1", "127.0.0.1", 15000, 23, 0);

        assertEquals("member-1", member.memberId());
        assertEquals(23, member.memberEpoch());
        assertEquals(22, member.previousMemberEpoch());
        assertEquals(MemberState.STABLE, member.state());
        assertEquals("instance-1", member.instanceId().orElseThrow());
        assertEquals("rack-east", member.rackId().orElseThrow());
        assertEquals("client-1", member.clientId());
        assertEquals("127.0.0.1", member.clientHost());
        assertEquals(15000, member.rebalanceTimeoutMs());
        assertEquals(0, member.topologyEpoch());
        assertEquals(processId.toString(), member.processId());
        assertEquals("localhost", member.userEndpoint().orElseThrow().host());
        assertEquals(8080, member.userEndpoint().orElseThrow().port());
        assertEquals(Map.of("rack", "west"), member.clientTags());
        assertEquals(Map.of("0", Map.of(1, 23)), member.assignedTasks().activeTasksWithEpochs());
        assertEquals(Map.of("2", Set.of(3)), member.assignedTasks().standbyTasks());
        assertEquals(Map.of(), member.assignedTasks().warmupTasks());
        assertEquals(TasksTupleWithEpochs.EMPTY, member.tasksPendingRevocation());
    }

    @Test
    void shouldRejectUnsupportedMetadataVersion() {
        final int unsupportedVersion = SubscriptionInfoData.HIGHEST_SUPPORTED_VERSION + 1;
        final ByteBuffer encoded = ByteBuffer.allocate(Integer.BYTES * 2)
            .putInt(unsupportedVersion)
            .putInt(unsupportedVersion);
        encoded.flip();

        assertThrows(UnsupportedVersionException.class, () -> ClassicStreamsMemberMetadata.decode(encoded));
    }

    @Test
    void shouldRejectTruncatedMetadata() {
        assertThrows(SchemaException.class, () -> ClassicStreamsMemberMetadata.decode(ByteBuffer.allocate(2)));
    }

    @Test
    void shouldRejectNamedTopologies() {
        final SubscriptionInfoData data = new SubscriptionInfoData()
            .setVersion(11)
            .setLatestSupportedVersion(11)
            .setProcessId(new Uuid(0L, 1L))
            .setTaskOffsetSums(List.of(new TaskOffsetSum()
                .setTopicGroupId(0)
                .setPartition(1)
                .setOffsetSum(-2L)
                .setNamedTopology("named")));
        final ByteBuffer encoded = MessageUtil.toByteBufferAccessor(data, (short) 11).buffer();

        assertThrows(SchemaException.class, () -> ClassicStreamsMemberMetadata.decode(encoded));
    }
}
