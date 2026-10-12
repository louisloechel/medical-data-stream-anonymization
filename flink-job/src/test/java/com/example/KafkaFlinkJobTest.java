package com.example;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class KafkaFlinkJobTest {

    private final ObjectMapper mapper = new ObjectMapper();

    private JsonNode process(String json) throws Exception {
        String out = KafkaFlinkJob.process(mapper, json);
        return out == null ? null : mapper.readTree(out);
    }

    @Test
    void syntheticRecordKeepsLegacyFields() throws Exception {
        JsonNode out = process("{\"recordid\":\"20944\",\"date\":\"11-Jul\",\"time\":\"11:18\","
                + "\"resp\":20,\"bps\":181,\"pulse\":92,\"temp\":39.3,\"userinitials\":\"AW\","
                + "\"username\":\"Amy Wheeler\",\"userid\":\"0329615\",\"icd10\":\"A21.0\","
                + "\"timestamp\":\"2026-01-01T12:00:00.123Z\"}");
        assertEquals("20944", out.get("recordid").asText());
        assertEquals(20944, out.get("recordid").asInt());
        assertEquals("0329615", out.get("userid").asText());
        assertEquals("Amy Wheeler", out.get("username").asText());
        assertEquals(20, out.get("resp").asInt());
        assertEquals(0, out.get("respNEWS").asInt());
        assertEquals(0, out.get("bpsNEWS").asInt());
        assertEquals(1, out.get("pulseNEWS").asInt());
        assertEquals(39.3, out.get("temp").asDouble(), 1e-9);
        assertEquals(2, out.get("tempNEWS").asInt());
        // no spo2 in legacy synthetic data -> null sub-score, not counted
        assertTrue(out.get("spo2").isNull());
        assertTrue(out.get("spo2NEWS").isNull());
        assertEquals(3, out.get("news2Partial").asInt());
        assertEquals(4, out.get("news2Components").asInt());
        assertEquals("A21.0", out.get("icd10").asText());
        assertEquals("2026-01-01T12:00:00.123Z", out.get("timestamp").asText());
        assertFalse(out.has("measured_at"));
    }

    @Test
    void realDataRecordWithNullsAndNewFields() throws Exception {
        JsonNode out = process("{\"recordid\":\"abc\",\"userid\":\"P1\",\"caseid\":\"C1\","
                + "\"username\":\"P1\",\"userinitials\":null,\"ward\":\"W1\",\"resp\":null,"
                + "\"bps\":null,\"pulse\":null,\"spo2\":90,\"temp\":null,\"icd10\":null,"
                + "\"measured_at\":\"2024-06-01T00:00:01.000+02:00\","
                + "\"timestamp\":\"2026-01-01T12:00:00.123Z\"}");
        assertTrue(out.get("respNEWS").isNull());
        assertTrue(out.get("bpsNEWS").isNull());
        assertTrue(out.get("pulseNEWS").isNull());
        assertTrue(out.get("tempNEWS").isNull());
        assertEquals(3, out.get("spo2NEWS").asInt());
        assertEquals(3, out.get("news2Partial").asInt());
        assertEquals(1, out.get("news2Components").asInt());
        assertEquals("C1", out.get("caseid").asText());
        assertEquals("W1", out.get("ward").asText());
        assertTrue(out.get("icd10").isNull());
        assertEquals("2024-06-01T00:00:01.000+02:00", out.get("measured_at").asText());
    }

    @Test
    void missingFieldsAreNullNotScoredAsExtreme() throws Exception {
        JsonNode out = process("{\"recordid\":\"1\",\"userid\":\"u\",\"username\":\"n\"}");
        assertTrue(out.get("resp").isNull());
        assertTrue(out.get("respNEWS").isNull());
        assertEquals(0, out.get("news2Partial").asInt());
        assertEquals(0, out.get("news2Components").asInt());
    }

    @Test
    void unusableRecordsAreSkipped() {
        assertNull(KafkaFlinkJob.process(mapper, "{}"));
        assertNull(KafkaFlinkJob.process(mapper, "not json"));
        assertNull(KafkaFlinkJob.process(mapper, "{\"userid\":null}"));
    }

    @Test
    void unknownFieldsArePassedThroughUnchanged() throws Exception {
        JsonNode out = process("{\"recordid\":7,\"userid\":\"u\",\"ageBand\":\"60-69\",\"sex\":null,"
                + "\"postcode2\":\"10\",\"rhythm\":\"Sinus-Rhythmus\",\"ectopyCount\":3,\"o2\":true,"
                + "\"nested\":{\"a\":[1,2]},\"pulse\":80}");
        assertEquals("7", out.get("recordid").asText());
        assertTrue(out.get("recordid").isTextual(), "recordid is normalized to a string");
        assertEquals("60-69", out.get("ageBand").asText());
        assertTrue(out.has("sex") && out.get("sex").isNull(), "explicit null is kept");
        assertEquals("10", out.get("postcode2").asText());
        assertTrue(out.get("postcode2").isTextual(), "string stays a string");
        assertEquals("Sinus-Rhythmus", out.get("rhythm").asText());
        assertEquals(3, out.get("ectopyCount").asInt());
        assertTrue(out.get("o2").isBoolean());
        assertEquals("{\"a\":[1,2]}", out.get("nested").toString());
        assertEquals(0, out.get("pulseNEWS").asInt());
        // fields that are not in the input and not part of the minimum schema are not invented
        assertFalse(out.has("rass"));
    }

    @Test
    void computedFieldsOverwriteInputFields() throws Exception {
        JsonNode out = process("{\"userid\":\"u\",\"pulse\":131,\"pulseNEWS\":0,\"news2Partial\":99}");
        assertEquals(3, out.get("pulseNEWS").asInt());
        assertEquals(3, out.get("news2Partial").asInt());
    }

    @Test
    void completeIcuRecordGivesNews2Total() throws Exception {
        // synthetic ICU record (schema v2): resp 22 -> 2, spo2 94 scale 1 -> 1, o2 -> 2,
        // bps 105 -> 1, pulse 115 -> 2, acvpu C -> 3, temp 38.5 -> 1
        JsonNode out = process("{\"recordid\":\"r1\",\"userid\":\"S0001\",\"caseid\":\"SC1\","
                + "\"ageBand\":\"70-79\",\"sex\":\"F\",\"postcode2\":\"12\",\"ward\":\"ICU-A\","
                + "\"icd10\":\"J96.11\",\"rhythm\":\"Afib Rhythmus\",\"ectopy\":\"none\",\"ectopyCount\":0,"
                + "\"sqi\":\"Good\",\"ventMode\":\"CPAP/ASB\",\"o2\":true,\"acvpu\":\"C\",\"rass\":-1,"
                + "\"gcs\":14,\"resp\":22,\"bps\":105,\"pulse\":115,\"spo2\":94,\"temp\":38.5,"
                + "\"timestamp\":\"2026-01-01T12:00:00.123Z\"}");
        assertEquals(2, out.get("respNEWS").asInt());
        assertEquals(1, out.get("spo2NEWS").asInt());
        assertEquals(2, out.get("o2NEWS").asInt());
        assertEquals(1, out.get("bpsNEWS").asInt());
        assertEquals(2, out.get("pulseNEWS").asInt());
        assertEquals(3, out.get("consciousnessNEWS").asInt());
        assertEquals(1, out.get("tempNEWS").asInt());
        assertEquals(12, out.get("news2Partial").asInt());
        assertEquals(7, out.get("news2Components").asInt());
        assertTrue(out.get("news2Complete").asBoolean());
        assertEquals(12, out.get("news2Total").asInt());
        // pass-through of the new fields
        for (String field : new String[]{"ageBand", "sex", "postcode2", "ward", "icd10", "rhythm", "ectopy",
                "ectopyCount", "sqi", "ventMode", "o2", "acvpu", "rass", "gcs"}) {
            assertTrue(out.has(field), field);
        }
    }

    @Test
    void spo2Scale2UsesOxygen() throws Exception {
        String base = "{\"userid\":\"u\",\"spo2\":97,\"spo2Scale\":2";
        assertEquals(3, process(base + ",\"o2\":true}").get("spo2NEWS").asInt());
        assertEquals(0, process(base + ",\"o2\":false}").get("spo2NEWS").asInt());
        assertTrue(process(base + "}").get("spo2NEWS").isNull(), "o2 unknown above the scale 2 target range");
        assertTrue(process(base + ",\"o2\":null}").get("o2NEWS").isNull());
        // scale given as string, o2 as 0/1
        assertEquals(1, process("{\"userid\":\"u\",\"spo2\":93,\"spo2Scale\":\"2\",\"o2\":1}").get("spo2NEWS").asInt());
        // spo2Scale null or missing -> scale 1
        assertEquals(0, process("{\"userid\":\"u\",\"spo2\":97,\"spo2Scale\":null,\"o2\":true}").get("spo2NEWS").asInt());
        // unknown scale -> no SpO2 sub-score
        assertTrue(process("{\"userid\":\"u\",\"spo2\":97,\"spo2Scale\":3}").get("spo2NEWS").isNull());
        assertTrue(process("{\"userid\":\"u\",\"spo2\":97,\"spo2Scale\":\"x\"}").get("spo2NEWS").isNull());
    }

    @Test
    void incompleteRecordHasNoTotal() throws Exception {
        JsonNode out = process("{\"userid\":\"u\",\"resp\":22,\"bps\":105,\"pulse\":115,\"spo2\":94,"
                + "\"temp\":38.5,\"o2\":false,\"acvpu\":null}");
        assertEquals(0, out.get("o2NEWS").asInt());
        assertTrue(out.get("consciousnessNEWS").isNull());
        assertEquals(6, out.get("news2Components").asInt());
        assertEquals(7, out.get("news2Partial").asInt());
        assertFalse(out.get("news2Complete").asBoolean());
        assertTrue(out.has("news2Total") && out.get("news2Total").isNull());
    }

    @Test
    void numericStringsAreAccepted() throws Exception {
        JsonNode out = process("{\"userid\":\"u\",\"pulse\":\"131\"}");
        assertEquals(3, out.get("pulseNEWS").asInt());
        assertEquals("131", out.get("pulse").asText());
    }
}
