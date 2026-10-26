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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.KRaftCoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue.Subtopology;
import org.apache.kafka.coordinator.group.streams.topics.ConfiguredInternalTopic;
import org.apache.kafka.coordinator.group.streams.topics.ConfiguredSubtopology;
import org.apache.kafka.coordinator.group.streams.topics.ConfiguredTopology;
import org.apache.kafka.timeline.SnapshotRegistry;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ClassicStreamsAssignmentTest {
    @Test
    void shouldEncodeOuterAssignmentAndCompatibleAssignmentInfo() throws IOException {
        final StreamsTopology topology = new StreamsTopology(0, Map.of(
            "0", new Subtopology().setSubtopologyId("0").setSourceTopics(List.of("input-a", "input-b")),
            "1", new Subtopology().setSubtopologyId("1").setSourceTopics(List.of("input-c")),
            "2", new Subtopology().setSubtopologyId("2").setSourceTopics(List.of("input-d"))
        ));
        final StreamsGroup group = streamsGroup(topology);
        final CoordinatorMetadataImage metadataImage = metadataImage(Map.of(
            "input-a", 2,
            "input-b", 1,
            "input-c", 4,
            "input-d", 4
        ));
        final TasksTupleWithEpochs tasks = new TasksTupleWithEpochs(
            Map.of("0", Map.of(0, 4)),
            Map.of("1", Set.of(2)),
            Map.of("2", Set.of(3))
        );

        final Assignment assignment = ClassicStreamsAssignment.toConsumerAssignment(group, metadataImage, tasks);
        final Assignment decoded = ConsumerProtocol.deserializeAssignment(ConsumerProtocol.serializeAssignment(assignment));
        assertEquals(List.of(new TopicPartition("input-a", 0), new TopicPartition("input-b", 0)), decoded.partitions());

        final byte[] userData = toByteArray(decoded.userData());
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(userData))) {
            assertEquals(1, input.readInt());

            // Active Task 0_0 is repeated once per assigned input partition.
            assertEquals(2, input.readInt());
            assertEquals(0, input.readInt());
            assertEquals(0, input.readInt());
            assertEquals(0, input.readInt());
            assertEquals(0, input.readInt());

            // Standby and warm-up tasks both carry the input partitions expected by the legacy assignor.
            assertEquals(2, input.readInt());
            assertEquals(1, input.readInt());
            assertEquals(2, input.readInt());
            assertEquals(1, input.readInt());
            assertEquals("input-c", input.readUTF());
            assertEquals(2, input.readInt());
            assertEquals(2, input.readInt());
            assertEquals(3, input.readInt());
            assertEquals(1, input.readInt());
            assertEquals("input-d", input.readUTF());
            assertEquals(3, input.readInt());
            assertEquals(0, input.available());
        }
    }

    @Test
    void shouldSkipInputTopicsThatDoNotHaveTheTaskPartition() throws IOException {
        final StreamsGroup group = streamsGroup(new StreamsTopology(0, Map.of(
            "0", new Subtopology().setSubtopologyId("0").setSourceTopics(List.of("input-a", "input-b"))
        )));
        final CoordinatorMetadataImage metadataImage = metadataImage(Map.of("input-a", 2, "input-b", 1));
        final TasksTupleWithEpochs tasks = new TasksTupleWithEpochs(Map.of("0", Map.of(1, 1)), Map.of(), Map.of());

        final Assignment assignment = ClassicStreamsAssignment.toConsumerAssignment(group, metadataImage, tasks);
        assertEquals(List.of(new TopicPartition("input-a", 1)), assignment.partitions());

        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(toByteArray(assignment.userData())))) {
            assertEquals(1, input.readInt());
            assertEquals(1, input.readInt());
            assertEquals(0, input.readInt());
            assertEquals(1, input.readInt());
            assertEquals(0, input.readInt());
            assertEquals(0, input.available());
        }
    }

    @Test
    void shouldUseConfiguredTopologyToResolveRegexAndRepartitionInputs() {
        final Subtopology subtopology = new Subtopology()
            .setSubtopologyId("0")
            .setSourceTopicRegex(List.of("input-.*"));
        final StreamsGroup group = streamsGroup(new StreamsTopology(0, Map.of("0", subtopology)));
        group.setConfiguredTopology(new ConfiguredTopology(
            0,
            0L,
            Optional.of(new TreeMap<>(Map.of("0", new ConfiguredSubtopology(
                2,
                Set.of("resolved-input"),
                Map.of("repartition-topic", new ConfiguredInternalTopic(
                    "repartition-topic", 2, Optional.empty(), Map.of())),
                Set.of(),
                Map.of()
            )))),
            Map.of(),
            Optional.empty()
        ));
        final CoordinatorMetadataImage metadataImage = metadataImage(Map.of("resolved-input", 2, "repartition-topic", 2));
        final TasksTupleWithEpochs tasks = new TasksTupleWithEpochs(Map.of("0", Map.of(1, 1)), Map.of(), Map.of());

        final Assignment assignment = ClassicStreamsAssignment.toConsumerAssignment(group, metadataImage, tasks);
        assertEquals(
            List.of(new TopicPartition("repartition-topic", 1), new TopicPartition("resolved-input", 1)),
            assignment.partitions());
    }

    @Test
    void shouldFailToBuildAssignmentWhenRegexTopologyHasNotBeenResolved() {
        final StreamsGroup group = streamsGroup(new StreamsTopology(0, Map.of(
            "0", new Subtopology().setSubtopologyId("0").setSourceTopicRegex(List.of("input-.*"))
        )));
        final CoordinatorMetadataImage metadataImage = metadataImage(Map.of("input-a", 1));
        final TasksTupleWithEpochs tasks = new TasksTupleWithEpochs(Map.of("0", Map.of(0, 1)), Map.of(), Map.of());

        assertThrows(ClassicStreamsAssignment.AssignmentNotReadyException.class,
            () -> ClassicStreamsAssignment.toConsumerAssignment(group, metadataImage, tasks));
    }

    @Test
    void shouldRejectNonNumericSubtopologyIds() {
        final StreamsGroup group = streamsGroup(new StreamsTopology(0, Map.of(
            "named", new Subtopology().setSubtopologyId("named").setSourceTopics(List.of("input"))
        )));
        final CoordinatorMetadataImage metadataImage = metadataImage(Map.of("input", 1));
        final TasksTupleWithEpochs tasks = new TasksTupleWithEpochs(Map.of("named", Map.of(0, 1)), Map.of(), Map.of());

        assertThrows(SchemaException.class,
            () -> ClassicStreamsAssignment.toConsumerAssignment(group, metadataImage, tasks));
    }

    private static StreamsGroup streamsGroup(StreamsTopology topology) {
        final LogContext logContext = new LogContext();
        final StreamsGroup group = new StreamsGroup(logContext, new SnapshotRegistry(logContext), "classic-streams-test");
        group.setTopology(topology);
        return group;
    }

    private static CoordinatorMetadataImage metadataImage(Map<String, Integer> partitionCounts) {
        final MetadataImageBuilder builder = new MetadataImageBuilder();
        partitionCounts.forEach((topic, count) -> builder.addTopic(Uuid.randomUuid(), topic, count));
        return new KRaftCoordinatorMetadataImage(builder.build());
    }

    private static byte[] toByteArray(ByteBuffer data) {
        final ByteBuffer copy = data.duplicate();
        final byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return bytes;
    }
}
