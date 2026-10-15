package com.example;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

/** Parallelism configuration and the keyed, re-timestamped Kafka sink records. */
class ScalingConfigTest {

    private static ParameterTool args(String... a) {
        return ParameterTool.fromArgs(a);
    }

    @Test
    void parallelismFromArgumentOrEnvironment() {
        assertNull(KafkaFlinkJob.resolveParallelism(args(), name -> null));
        assertEquals(3, KafkaFlinkJob.resolveParallelism(args(), name -> "FLINK_JOB_PARALLELISM".equals(name) ? "3" : null));
        // the program argument wins
        assertEquals(2, KafkaFlinkJob.resolveParallelism(args("--parallelism", "2"), name -> "8"));
        assertNull(KafkaFlinkJob.resolveParallelism(args(), name -> " "));
        assertThrows(IllegalArgumentException.class, () -> KafkaFlinkJob.resolveParallelism(args("--parallelism", "0"), n -> null));
        assertThrows(IllegalArgumentException.class, () -> KafkaFlinkJob.resolveParallelism(args("--parallelism", "x"), n -> null));
    }

    @Test
    void sinkRecordIsKeyedByUseridAndStampedWithWriteTime() {
        KeyedJsonSerializationSchema schema = new KeyedJsonSerializationSchema("processed-topic", () -> 1767268800123L);
        String json = "{\"userid\":\"S0001\",\"timestamp\":\"2026-01-01T12:00:00.000Z\"}";
        // the incoming element timestamp (raw-topic CreateTime) must not be copied
        ProducerRecord<byte[], byte[]> record = schema.serialize(Tuple2.of("S0001", json), null, 1L);
        assertEquals("processed-topic", record.topic());
        assertNull(record.partition());
        assertEquals(1767268800123L, record.timestamp());
        assertArrayEquals("S0001".getBytes(StandardCharsets.UTF_8), record.key());
        assertEquals(json, new String(record.value(), StandardCharsets.UTF_8));
    }

    @Test
    void processNodeKeepsUseridForTheKey() {
        ObjectNode out = KafkaFlinkJob.processNode(new ObjectMapper(), "{\"userid\":12345,\"pulse\":80}");
        assertEquals("12345", out.get("userid").asText());
        assertNull(KafkaFlinkJob.processNode(new ObjectMapper(), "{\"pulse\":80}"));
    }

    @Test
    void sinkProducerConfigCompresses() {
        assertEquals("lz4", KafkaFlinkJob.sinkProducerConfig().getProperty("compression.type"));
    }
}
