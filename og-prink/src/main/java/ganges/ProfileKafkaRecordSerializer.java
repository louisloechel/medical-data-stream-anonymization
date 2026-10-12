package ganges;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ganges.config.AnonymizationProfile;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.util.function.LongSupplier;

/**
 * Kafka record serializer of the Prink output.
 *
 * <ul>
 *     <li>value: the anonymized record as JSON ({@link ProfileTupleToJson})</li>
 *     <li>key: the data subject key <em>as released</em> (e.g. the salted hash of {@code userid} with role
 *     {@code mask}); no key if the profile drops it. The source identifier is never used as key.</li>
 *     <li>timestamp (CreateTime): wall-clock time at which the record is handed to the Kafka producer, so the
 *     latency of the Prink stage can be measured from topic dumps (the default would copy the input
 *     record's timestamp).</li>
 * </ul>
 */
public class ProfileKafkaRecordSerializer implements KafkaRecordSerializationSchema<Tuple> {

    private static final long serialVersionUID = 1L;

    /** Serializable time source (tests use a fixed clock) */
    public interface SerializableClock extends LongSupplier, java.io.Serializable {
    }

    private final String topic;
    private final String keyField;
    private final ProfileTupleToJson toJson;
    private final SerializableClock clock;

    public ProfileKafkaRecordSerializer(AnonymizationProfile profile) {
        this(profile, System::currentTimeMillis);
    }

    ProfileKafkaRecordSerializer(AnonymizationProfile profile, SerializableClock clock) {
        this.topic = profile.kafka.outputTopic;
        this.keyField = profile.dataSubjectKey;
        this.toJson = new ProfileTupleToJson(profile);
        this.clock = clock;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Tuple element, KafkaSinkContext context, Long timestamp) {
        ObjectNode json = toJson.toJson(element);
        JsonNode key = keyField == null ? null : json.get(keyField);
        byte[] keyBytes = key == null || key.isNull() ? null : key.asText().getBytes(StandardCharsets.UTF_8);
        return new ProducerRecord<>(topic, null, clock.getAsLong(), keyBytes,
                json.toString().getBytes(StandardCharsets.UTF_8));
    }
}
