package com.example;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.util.function.LongSupplier;

/**
 * Writes {@code (key, json)} pairs to a Kafka topic: the key (the patient's {@code userid}) becomes the Kafka
 * message key, so all records of a patient land in the same partition and keep their order.
 *
 * <p>The Kafka record timestamp (CreateTime) is set to the wall-clock time at which the record is handed to
 * the Kafka producer. The default serializer would copy the timestamp of the incoming Kafka record instead
 * (the raw-topic CreateTime), which hides the processing time of this stage.</p>
 */
public class KeyedJsonSerializationSchema implements KafkaRecordSerializationSchema<Tuple2<String, String>> {

    private static final long serialVersionUID = 1L;

    private final String topic;
    private final SerializableClock clock;

    /** Serializable time source (tests use a fixed clock) */
    public interface SerializableClock extends LongSupplier, java.io.Serializable {
    }

    public KeyedJsonSerializationSchema(String topic) {
        this(topic, System::currentTimeMillis);
    }

    KeyedJsonSerializationSchema(String topic, SerializableClock clock) {
        this.topic = topic;
        this.clock = clock;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Tuple2<String, String> element, KafkaSinkContext context, Long timestamp) {
        byte[] key = element.f0 == null ? null : element.f0.getBytes(StandardCharsets.UTF_8);
        byte[] value = element.f1.getBytes(StandardCharsets.UTF_8);
        return new ProducerRecord<>(topic, null, clock.getAsLong(), key, value);
    }
}
