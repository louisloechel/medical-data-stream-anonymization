package streamanon.anonymization;

import com.fasterxml.jackson.databind.node.ObjectNode;
import streamanon.anonymization.config.AnonymizationProfile;
import streamanon.anonymization.config.ProfileLoader;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Prink with parallelism > 1: keyBy(data subject) sends all records of a patient to one subtask, and every
 * subtask clusters only its own patients. The privacy guarantee per released cluster (>= k distinct patients,
 * l-diversity) must hold for every parallelism.
 */
class ParallelismTest {

    private static final List<String> QIS = Arrays.asList("ageBand", "sex", "postcode2", "ward", "icd10");
    private static final List<String> SENSITIVE = Arrays.asList("resp", "spo2", "bps", "pulse", "temp", "rhythm", "o2", "news2Total");

    private static AnonymizationProfile icuProfile() throws Exception {
        return ProfileLoader.load(ParameterTool.fromArgs(new String[]{"--profile", "icu-qi-v2"}),
                name -> "PRINK_MASK_SALT".equals(name) ? "synthetic-test-salt" : null);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 4})
    void everyReleasedClassHasAtLeastKPatients(int parallelism) throws Exception {
        AnonymizationProfile profile = icuProfile();
        int k = profile.privacy.k;
        List<String> input = ProfileJobTest.syntheticIcuRecords(60, 20);
        List<ObjectNode> output = ProfileJobTest.run(profile, input, parallelism);

        // every subtask holds back up to delta records when the bounded input ends
        assertTrue(output.size() >= input.size() - parallelism * profile.privacy.delta,
                "released " + output.size() + " of " + input.size());

        Map<String, Set<String>> patientsPerClass = new HashMap<>();
        for (ObjectNode record : output) {
            if (record.get("infoloss").asDouble() >= 1.0) continue; // suppressed (fully generalized)
            StringBuilder signature = new StringBuilder();
            for (String attribute : QIS) signature.append(record.get(attribute)).append('|');
            for (String attribute : SENSITIVE) signature.append(record.get(attribute)).append('|');
            patientsPerClass.computeIfAbsent(signature.toString(), x -> new HashSet<>()).add(record.get("userid").asText());
        }
        assertFalse(patientsPerClass.isEmpty(), "some clusters are released without suppression");
        for (Map.Entry<String, Set<String>> entry : patientsPerClass.entrySet()) {
            assertTrue(entry.getValue().size() >= k, "parallelism " + parallelism + ": equivalence class "
                    + entry.getKey() + " has only " + entry.getValue().size() + " patients");
        }
        System.out.printf(Locale.ROOT, "[ParallelismTest] parallelism=%d released=%d/%d mean infoloss=%.3f suppressed=%.1f%%%n",
                parallelism, output.size(), input.size(), meanInfoLoss(output), 100.0 * suppressedShare(output));
    }

    private static double meanInfoLoss(List<ObjectNode> output) {
        return output.stream().mapToDouble(r -> r.get("infoloss").asDouble()).average().orElse(Double.NaN);
    }

    private static double suppressedShare(List<ObjectNode> output) {
        return output.stream().filter(r -> r.get("infoloss").asDouble() >= 1.0).count() / (double) Math.max(1, output.size());
    }

    @Test
    void parallelismFromArgumentOrEnvironment() {
        assertEquals(1, AnonymizationJob.resolveParallelism(ParameterTool.fromArgs(new String[0]), n -> null));
        assertEquals(4, AnonymizationJob.resolveParallelism(ParameterTool.fromArgs(new String[0]),
                n -> "PRINK_PARALLELISM".equals(n) ? "4" : null));
        assertEquals(2, AnonymizationJob.resolveParallelism(ParameterTool.fromArgs(new String[]{"--parallelism", "2"}), n -> "4"));
        assertThrows(IllegalArgumentException.class,
                () -> AnonymizationJob.resolveParallelism(ParameterTool.fromArgs(new String[]{"--parallelism", "0"}), n -> null));
    }

    @Test
    void sinkRecordIsKeyedByReleasedKeyAndStampedWithWriteTime() throws Exception {
        AnonymizationProfile profile = icuProfile();
        ProfileKafkaRecordSerializer serializer = new ProfileKafkaRecordSerializer(profile, () -> 1767268800123L);
        String[] names = profile.outputNames();
        Tuple tuple = Tuple.newInstance(profile.outputArity());
        for (int i = 0; i < names.length; i++) {
            if ("userid".equals(names[i])) tuple.setField("a1b2c3d4e5f60718", i); // already masked by Prink
            if ("timestamp".equals(names[i])) tuple.setField("2026-01-01T12:00:00.000Z", i);
        }
        ProducerRecord<byte[], byte[]> record = serializer.serialize(tuple, null, 1L);
        assertEquals(profile.kafka.outputTopic, record.topic());
        assertEquals(1767268800123L, record.timestamp());
        assertEquals("a1b2c3d4e5f60718", new String(record.key(), StandardCharsets.UTF_8));
        assertTrue(new String(record.value(), StandardCharsets.UTF_8).contains("\"timestamp\":\"2026-01-01T12:00:00.000Z\""));

        // profile that drops the key: no Kafka key
        AnonymizationProfile dropped = icuProfile();
        dropped.fields.stream().filter(f -> f.name.equals("userid")).forEach(f -> {
            f.role = streamanon.anonymization.config.FieldSpec.Role.DROP;
            f.mask = null;
        });
        ProfileKafkaRecordSerializer noKey = new ProfileKafkaRecordSerializer(dropped, () -> 1L);
        assertNull(noKey.serialize(Tuple.newInstance(dropped.outputArity()), null, null).key());
    }
}
