package ganges;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ganges.config.AnonymizationProfile;
import ganges.config.ProfileLoader;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Runs the anonymization pipeline of {@link GangesEvaluation} (JSON -> profile tuple -> keyBy patient ->
 * CastleFunction -> JSON) on a local Flink mini cluster with a bounded input instead of Kafka.
 */
class ProfileJobTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int PATIENTS = 12;
    private static final int RECORDS_PER_PATIENT = 8;

    /** Synthetic records in the processed-topic schema; some values are missing or null */
    private static List<String> syntheticRecords() {
        List<String> records = new ArrayList<>();
        Random random = new Random(42);
        String[] icd = {"A00.0", "A01.1", "A15.3", "A20.1"};
        int recordId = 0;
        for (int round = 0; round < RECORDS_PER_PATIENT; round++) {
            for (int p = 0; p < PATIENTS; p++) {
                ObjectNode r = MAPPER.createObjectNode();
                r.put("recordid", Integer.toHexString(1000 + recordId++));
                r.put("userid", String.format("%07d", p));
                r.put("username", "Synthetic Person " + p);
                r.put("caseid", "CASE-" + p);
                if (p % 4 != 0) r.put("ward", "W" + (p % 3)); // old-pipeline style records have no ward
                r.put("resp", 12 + random.nextInt(10));
                r.put("bps", 100 + random.nextInt(50));
                if (p % 5 == 0) r.putNull("pulse"); else r.put("pulse", 55 + random.nextInt(50));
                r.put("spo2", 90 + random.nextInt(10));
                r.put("temp", 36.0 + random.nextInt(30) / 10.0);
                r.put("icd10", icd[p % icd.length]);
                r.put("respNEWS", random.nextInt(2));
                r.put("bpsNEWS", random.nextInt(2));
                r.put("pulseNEWS", 0);
                r.put("tempNEWS", random.nextInt(2));
                r.put("spo2NEWS", 0);
                r.put("timestamp", "2026-01-01T12:00:00.123Z");
                records.add(r.toString());
            }
        }
        return records;
    }

    private static List<ObjectNode> run(AnonymizationProfile profile, List<String> input) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<Tuple> anonymized = GangesEvaluation.anonymize(env, env.fromCollection(input), profile);
        ProfileTupleToJson toJson = new ProfileTupleToJson(profile);
        List<ObjectNode> output = new ArrayList<>();
        try (CloseableIterator<Tuple> it = anonymized.executeAndCollect(profile.evalDescription())) {
            while (it.hasNext()) output.add(toJson.toJson(it.next()));
        }
        return output;
    }

    @Test
    void vitalsSensitiveProfileEndToEnd() throws Exception {
        int k = 3;
        int delta = 20;
        AnonymizationProfile profile = ProfileLoader.load(ParameterTool.fromArgs(new String[]{
                "--profile", "vitals-sensitive", "--k", String.valueOf(k), "--l", "2", "--delta", String.valueOf(delta)}), name -> null);
        List<String> input = syntheticRecords();
        List<ObjectNode> output = run(profile, input);

        // all but at most delta tuples (still held by Prink when the bounded input ends) are released
        assertTrue(output.size() >= input.size() - delta, "released " + output.size() + " of " + input.size());

        Set<String> expectedKeys = new HashSet<>(Arrays.asList("recordid", "userid", "caseid", "ward", "icd10",
                "resp", "bps", "pulse", "spo2", "temp", "timestamp", "measured_at", "infoloss"));
        Map<String, Set<String>> patientsPerEquivalenceClass = new HashMap<>();
        for (ObjectNode record : output) {
            Set<String> keys = new HashSet<>();
            record.fieldNames().forEachRemaining(keys::add);
            assertEquals(expectedKeys, keys, "only configured, non-dropped fields are written");
            String serialized = record.toString();
            assertFalse(serialized.contains("Synthetic Person"), "dropped username leaked: " + serialized);
            assertFalse(serialized.contains("CASE-"), "masked caseid leaked: " + serialized);

            // released (not suppressed) records: every combination of generalized values covers >= k patients
            if (record.get("infoloss").asDouble() < 1.0) {
                StringBuilder signature = new StringBuilder();
                for (String attribute : Arrays.asList("ward", "icd10", "resp", "bps", "pulse", "spo2", "temp")) {
                    signature.append(record.get(attribute)).append('|');
                }
                patientsPerEquivalenceClass.computeIfAbsent(signature.toString(), s -> new HashSet<>()).add(record.get("userid").asText());
            }
        }
        assertFalse(patientsPerEquivalenceClass.isEmpty(), "some clusters are released without suppression");
        for (Map.Entry<String, Set<String>> entry : patientsPerEquivalenceClass.entrySet()) {
            assertTrue(entry.getValue().size() >= k, "equivalence class " + entry.getKey() + " has only " + entry.getValue().size() + " patients");
        }
    }

    @Test
    void thesisDefaultProfileEndToEnd() throws Exception {
        AnonymizationProfile profile = ProfileLoader.load(ParameterTool.fromArgs(new String[]{"--delta", "20"}), name -> null);
        List<String> input = syntheticRecords();
        List<ObjectNode> output = run(profile, input);
        assertTrue(output.size() >= input.size() - 20);
        for (JsonNode record : output) {
            assertEquals(14, record.size());
            String temp = record.get("temp").asText();
            assertTrue(temp.startsWith("(") && temp.contains("."), "temperature is aggregated as a double range: " + temp);
            assertTrue(record.get("userid").asText().matches("\\d{7}"));
        }
    }
}
