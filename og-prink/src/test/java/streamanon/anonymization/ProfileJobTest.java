package streamanon.anonymization;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import streamanon.anonymization.config.AnonymizationProfile;
import streamanon.anonymization.config.ProfileLoader;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Runs the anonymization pipeline of {@link AnonymizationJob} (JSON -> profile tuple -> keyBy patient ->
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

    private static final String[] RHYTHMS = {"Sinus-Rhythmus", "Sinus-Tachykardie", "Sinus-Bradykardie",
            "Afib Rhythmus", "HF unregelmäßig", "SV-Tachykardie", "Bigeminus", "Pacer-Rhythmus", "-?-"};

    /**
     * Synthetic records in the processed-topic schema v2 (ICU fields). Demographics are constant per patient;
     * some fields are missing or null. All values are made up.
     */
    static List<String> syntheticIcuRecords(int patients, int recordsPerPatient) {
        List<String> records = new ArrayList<>();
        Random random = new Random(7);
        String[] ageBands = {"40-49", "50-59", "60-69", "70-79", "80-89", "60-69"};
        String[] icd = {"J96.11", "J96.10", "J96.01", "I21.4", "A41.9", null};
        String[] wards = {"ICU-A", "ICU-B", "IMC-1"};
        int recordId = 0;
        for (int round = 0; round < recordsPerPatient; round++) {
            for (int p = 0; p < patients; p++) {
                ObjectNode r = MAPPER.createObjectNode();
                r.put("recordid", "r" + recordId++);
                r.put("userid", String.format("S%04d", p));
                r.put("username", "Synthetic Person " + p);
                r.put("userinitials", "SP");
                r.put("caseid", "CASE-" + p);
                r.put("ageBand", ageBands[p % ageBands.length]);
                if (p % 7 == 0) r.putNull("sex"); else r.put("sex", p % 2 == 0 ? "F" : "M");
                r.put("postcode2", String.valueOf(10 + p % 4));
                r.put("ward", wards[p % wards.length]);
                if (icd[p % icd.length] == null) r.putNull("icd10"); else r.put("icd10", icd[p % icd.length]);
                r.put("rhythm", RHYTHMS[random.nextInt(RHYTHMS.length)]);
                r.put("ectopy", "none");
                r.put("ectopyCount", random.nextInt(5));
                r.put("sqi", "Good");
                r.put("ventMode", "CPAP");
                boolean o2 = random.nextBoolean();
                r.put("o2", o2);
                r.put("acvpu", random.nextInt(10) == 0 ? "V" : "A");
                r.put("rass", -random.nextInt(3));
                r.put("gcs", 13 + random.nextInt(3));
                r.put("resp", 12 + random.nextInt(14));
                r.put("bps", 90 + random.nextInt(70));
                if (p % 5 == 0) r.putNull("pulse"); else r.put("pulse", 55 + random.nextInt(70));
                r.put("spo2", 88 + random.nextInt(12));
                r.put("temp", 36.0 + random.nextInt(30) / 10.0);
                r.put("respNEWS", 0);
                r.put("spo2NEWS", 1);
                r.put("o2NEWS", o2 ? 2 : 0);
                r.put("news2Partial", 3);
                r.put("news2Components", 7);
                r.put("news2Complete", round % 3 != 0);
                if (round % 3 == 0) r.putNull("news2Total"); else r.put("news2Total", random.nextInt(12));
                r.put("measured_at", "2026-01-01T00:00:00.000Z");
                r.put("timestamp", "2026-01-01T12:00:00.123Z");
                records.add(r.toString());
            }
        }
        return records;
    }

    private static List<ObjectNode> run(AnonymizationProfile profile, List<String> input) throws Exception {
        return run(profile, input, 1);
    }

    /** Runs the pipeline with the given parallelism of the keyed CastleFunction (the bounded source has parallelism 1) */
    static List<ObjectNode> run(AnonymizationProfile profile, List<String> input, int parallelism) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        DataStream<Tuple> anonymized = AnonymizationJob.anonymize(env, env.fromCollection(input), profile);
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
    void icuQiV2ProfileEndToEnd() throws Exception {
        // bundled profile with its defaults (k=5, l=2, delta=125)
        AnonymizationProfile profile = ProfileLoader.load(ParameterTool.fromArgs(new String[]{"--profile", "icu-qi-v2"}),
                name -> "PRINK_MASK_SALT".equals(name) ? "synthetic-test-salt" : null);
        int k = profile.privacy.k;
        int delta = profile.privacy.delta;
        assertEquals(5, k);
        List<String> input = syntheticIcuRecords(30, 20);
        List<ObjectNode> output = run(profile, input);
        assertTrue(output.size() >= input.size() - delta, "released " + output.size() + " of " + input.size());

        List<String> qis = Arrays.asList("ageBand", "sex", "postcode2", "ward", "icd10");
        List<String> sensitive = Arrays.asList("resp", "spo2", "bps", "pulse", "temp", "rhythm", "o2", "news2Total");
        Set<String> expectedKeys = new HashSet<>(Arrays.asList("recordid", "userid", "caseid", "timestamp", "infoloss"));
        expectedKeys.addAll(qis);
        expectedKeys.addAll(sensitive);

        Map<String, Set<String>> patientsPerEquivalenceClass = new HashMap<>();
        Set<String> generalizedRhythms = new HashSet<>();
        for (ObjectNode record : output) {
            Set<String> keys = new HashSet<>();
            record.fieldNames().forEachRemaining(keys::add);
            assertEquals(expectedKeys, keys, "only configured, non-dropped fields are written");
            String serialized = record.toString();
            assertFalse(serialized.contains("Synthetic Person"), "dropped username leaked: " + serialized);
            assertFalse(serialized.contains("CASE-"), "masked caseid leaked: " + serialized);
            assertFalse(record.get("userid").asText().startsWith("S0"), "userid is masked: " + serialized);
            assertFalse(record.has("measured_at"));
            if (!record.get("rhythm").isNull()) generalizedRhythms.add(record.get("rhythm").asText());
            String o2 = record.get("o2").asText();
            assertTrue(o2.equals("(0,0)") || o2.equals("(0,1)") || o2.equals("(1,1)"), "o2 is a range within [0,1]: " + o2);

            if (record.get("infoloss").asDouble() < 1.0) {
                StringBuilder signature = new StringBuilder();
                for (String attribute : qis) signature.append(record.get(attribute)).append('|');
                for (String attribute : sensitive) signature.append(record.get(attribute)).append('|');
                patientsPerEquivalenceClass.computeIfAbsent(signature.toString(), x -> new HashSet<>())
                        .add(record.get("userid").asText());
            }
        }
        assertFalse(patientsPerEquivalenceClass.isEmpty(), "some clusters are released without suppression");
        for (Map.Entry<String, Set<String>> entry : patientsPerEquivalenceClass.entrySet()) {
            assertTrue(entry.getValue().size() >= k, "equivalence class " + entry.getKey() + " has only " + entry.getValue().size() + " patients");
        }
        // rhythm labels are generalized within the rhythm hierarchy
        Set<String> rhythmNodes = new HashSet<>(Arrays.asList(RHYTHMS));
        rhythmNodes.addAll(Arrays.asList("Rhythm", "Supraventricular", "Sinus", "Atrial fibrillation or irregular",
                "Other SV", "Ventricular", "Ventricular ectopy pattern", "Paced", "Not analysable", "Unknown"));
        assertTrue(rhythmNodes.containsAll(generalizedRhythms), "unexpected rhythm values " + generalizedRhythms);
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
