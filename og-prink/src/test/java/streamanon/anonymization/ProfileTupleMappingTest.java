package streamanon.anonymization;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import streamanon.anonymization.config.AnonymizationProfile;
import streamanon.anonymization.config.FieldSpec;
import streamanon.anonymization.config.ProfileLoader;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/** JSON -> tuple mapping and tuple -> JSON serialization driven by an anonymization profile. */
class ProfileTupleMappingTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Record in the agreed processed-topic schema (synthetic values) */
    static final String NEW_RECORD = "{\"recordid\":\"a3f9c2e17b4d0e55\",\"userid\":\"0300001\",\"caseid\":\"C-123\","
            + "\"username\":\"Test Person\",\"userinitials\":\"TP\",\"ward\":\"W1\",\"resp\":18,\"bps\":120,"
            + "\"pulse\":80,\"spo2\":97,\"temp\":37.3,\"icd10\":\"A01.1\",\"timestamp\":\"2026-01-01T12:00:00.123Z\","
            + "\"measured_at\":\"2026-01-01T11:59:00.000Z\",\"respNEWS\":0,\"bpsNEWS\":0,\"pulseNEWS\":0,"
            + "\"tempNEWS\":0,\"spo2NEWS\":0,\"news2Partial\":0,\"news2Components\":[\"resp\",\"bps\"]}";

    /** Record of the old pipeline: no spo2, caseid, ward, ... */
    static final String OLD_RECORD = "{\"recordid\":\"65339\",\"userid\":\"0300001\",\"username\":\"Test Person\","
            + "\"resp\":13,\"respNEWS\":0,\"bps\":133,\"bpsNEWS\":0,\"pulse\":62,\"pulseNEWS\":0,\"temp\":37.3,"
            + "\"tempNEWS\":0,\"icd10\":\"A15.0\",\"timestamp\":\"2026-01-01T12:00:00.123Z\"}";

    private static AnonymizationProfile profile(String... args) throws IOException {
        return ProfileLoader.load(ParameterTool.fromArgs(args), name -> null);
    }

    private static Tuple map(AnonymizationProfile profile, String json) throws IOException {
        return new ProfileJsonToTuple(profile).toTuple(MAPPER.readTree(json));
    }

    private static Object field(AnonymizationProfile profile, Tuple tuple, String name) {
        List<FieldSpec> fields = profile.tupleFields();
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).name.equals(name)) return tuple.getField(i);
        }
        throw new AssertionError("field not in tuple: " + name);
    }

    @Test
    void thesisDefaultMapsTypes() throws IOException {
        AnonymizationProfile profile = profile();
        Tuple tuple = map(profile, OLD_RECORD);
        assertEquals(13, tuple.getArity());
        assertEquals("65339", field(profile, tuple, "recordid"));
        assertEquals("0300001", field(profile, tuple, "userid"), "leading zero of the patient number is kept");
        assertEquals(13, field(profile, tuple, "resp"));
        assertEquals(37.3, field(profile, tuple, "temp"), "temperature is not truncated");
        assertEquals("A15.0", field(profile, tuple, "icd10"));
    }

    @Test
    void missingAndNullFieldsBecomeNull() throws IOException {
        AnonymizationProfile profile = profile("--profile", "vitals-sensitive");
        Tuple old = map(profile, OLD_RECORD);
        assertNull(field(profile, old, "spo2"));
        assertNull(field(profile, old, "ward"));
        assertNull(field(profile, old, "caseid"));
        assertNull(field(profile, old, "measured_at"));
        assertEquals(62, field(profile, old, "pulse"));

        ObjectNode json = (ObjectNode) MAPPER.readTree(NEW_RECORD);
        json.putNull("pulse");
        json.putNull("icd10");
        json.putNull("temp");
        json.put("resp", "not a number");
        Tuple tuple = new ProfileJsonToTuple(profile).toTuple(json);
        assertNull(field(profile, tuple, "pulse"), "JSON null must not become 0");
        assertNull(field(profile, tuple, "icd10"), "JSON null must not become the string \"null\"");
        assertNull(field(profile, tuple, "temp"));
        assertNull(field(profile, tuple, "resp"), "unparsable values become null");
        assertEquals(97, field(profile, tuple, "spo2"));
    }

    @Test
    void valueConversion() {
        JsonNode text12 = MAPPER.getNodeFactory().textNode("12");
        assertEquals(12, ProfileJsonToTuple.parse(text12, FieldSpec.FieldType.INT));
        assertEquals(12L, ProfileJsonToTuple.parse(text12, FieldSpec.FieldType.LONG));
        assertEquals(12.0, ProfileJsonToTuple.parse(text12, FieldSpec.FieldType.DOUBLE));
        assertEquals(37, ProfileJsonToTuple.parse(MAPPER.getNodeFactory().numberNode(37.3), FieldSpec.FieldType.INT));
        assertEquals(37.3f, ProfileJsonToTuple.parse(MAPPER.getNodeFactory().numberNode(37.3), FieldSpec.FieldType.FLOAT));
        assertEquals("65339", ProfileJsonToTuple.parse(MAPPER.getNodeFactory().numberNode(65339), FieldSpec.FieldType.STRING));
        assertEquals("[\"a\",\"b\"]", ProfileJsonToTuple.parse(MAPPER.createArrayNode().add("a").add("b"), FieldSpec.FieldType.STRING));
        assertNull(ProfileJsonToTuple.parse(MAPPER.getNodeFactory().nullNode(), FieldSpec.FieldType.STRING));
        assertNull(ProfileJsonToTuple.parse(null, FieldSpec.FieldType.INT));
    }

    @Test
    void droppedFieldsAreNotPartOfTheTuple() throws IOException {
        AnonymizationProfile profile = profile("--profile", "news2-only");
        Set<String> tupleNames = new HashSet<>();
        for (FieldSpec f : profile.tupleFields()) tupleNames.add(f.name);
        for (String dropped : Arrays.asList("username", "resp", "bps", "pulse", "spo2", "temp", "news2Components")) {
            assertFalse(tupleNames.contains(dropped), dropped + " must not enter Prink");
        }
        assertTrue(tupleNames.contains("spo2NEWS"));
        assertTrue(tupleNames.contains("news2Partial"));
    }

    @Test
    void maskedValuesAreReplacedBeforeAnonymization() throws IOException {
        FieldSpec.MaskSpec hash = new FieldSpec.MaskSpec();
        hash.salt = "salt";
        hash.length = 16;
        Object masked = ProfileJsonToTuple.mask("C-123", hash);
        assertEquals(16, masked.toString().length());
        assertEquals(masked, ProfileJsonToTuple.mask("C-123", hash), "hash is deterministic");
        assertNotEquals(masked, ProfileJsonToTuple.mask("C-124", hash));
        FieldSpec.MaskSpec otherSalt = new FieldSpec.MaskSpec();
        otherSalt.salt = "other";
        otherSalt.length = 16;
        assertNotEquals(masked, ProfileJsonToTuple.mask("C-123", otherSalt));

        FieldSpec.MaskSpec constant = new FieldSpec.MaskSpec();
        constant.strategy = "constant";
        constant.value = "REDACTED";
        assertEquals("REDACTED", ProfileJsonToTuple.mask("C-123", constant));
        assertNull(ProfileJsonToTuple.mask(null, constant));

        AnonymizationProfile profile = profile("--profile", "vitals-sensitive");
        Object caseid = field(profile, map(profile, NEW_RECORD), "caseid");
        assertNotNull(caseid);
        assertNotEquals("C-123", caseid);
    }

    /** Drop and mask fields never reach the output, also not for the anonymized tuple of a dropped key. */
    @Test
    void dropAndMaskFieldsAreNeverEmitted() throws IOException {
        String yaml = "name: t\nprivacy: {l: 1}\n"
                + "fields:\n"
                + "  - {name: userid, role: drop}\n"            // key used by Prink but not released
                + "  - {name: caseid, role: mask, mask: {strategy: hash, salt: x}}\n"
                + "  - {name: username, role: drop}\n"
                + "  - {name: pulse, type: int, role: sensitive, generalizer: aggregation, bounds: [20, 220]}\n";
        AnonymizationProfile profile = ProfileLoader.parse(yaml, name -> null);
        ProfileLoader.validate(profile);
        assertEquals(3, profile.inputArity(), "dropped key stays inside the tuple for Prink");
        assertEquals(0, profile.keyPosition());

        Tuple tuple = map(profile, NEW_RECORD);
        // simulate Prink output: generalized pulse + infoloss
        Tuple out = Tuple.newInstance(profile.outputArity());
        for (int i = 0; i < tuple.getArity(); i++) out.setField(tuple.getField(i), i);
        out.setField(Tuple2.of(60, 90), 2);
        out.setField(0.1f, 3);

        ObjectNode json = new ProfileTupleToJson(profile).toJson(out);
        assertEquals(new HashSet<>(Arrays.asList("caseid", "pulse", "infoloss")), fieldNames(json));
        String serialized = new String(new ProfileTupleToJson(profile).serialize(out));
        for (String secret : Arrays.asList("0300001", "Test Person", "C-123")) {
            assertFalse(serialized.contains(secret), "output must not contain " + secret + ": " + serialized);
        }
        assertEquals("(60,90)", json.get("pulse").asText(), "generalized values are written as before");
    }

    @Test
    void outputUsesConfiguredNamesAndNullForMissingValues() throws IOException {
        AnonymizationProfile profile = profile();
        Tuple tuple = map(profile, "{\"userid\":\"0300001\"}");
        Tuple out = Tuple.newInstance(profile.outputArity());
        for (int i = 0; i < tuple.getArity(); i++) out.setField(tuple.getField(i), i);
        out.setField(0.5f, profile.outputArity() - 1);
        ObjectNode json = new ProfileTupleToJson(profile).toJson(out);
        assertEquals(new HashSet<>(Arrays.asList(profile.outputNames())), fieldNames(json));
        assertTrue(json.get("pulse").isNull());
        assertEquals("0300001", json.get("userid").asText());
        assertEquals("0.5", json.get("infoloss").asText());
    }

    @Test
    void flatMapSkipsInvalidRecords() throws IOException {
        AnonymizationProfile profile = profile();
        ProfileJsonToTuple mapper = new ProfileJsonToTuple(profile);
        List<Tuple> out = new ArrayList<>();
        Collector<Tuple> collector = new Collector<Tuple>() {
            @Override public void collect(Tuple record) { out.add(record); }
            @Override public void close() { }
        };
        mapper.flatMap("not json", collector);
        mapper.flatMap("[1,2]", collector);
        mapper.flatMap("{}", collector);                              // no data subject key
        mapper.flatMap("{\"userid\": null, \"pulse\": 80}", collector);
        assertTrue(out.isEmpty());
        mapper.flatMap(OLD_RECORD, collector);
        assertEquals(1, out.size());
    }

    private static Set<String> fieldNames(ObjectNode json) {
        Set<String> names = new HashSet<>();
        json.fieldNames().forEachRemaining(names::add);
        return names;
    }
}
