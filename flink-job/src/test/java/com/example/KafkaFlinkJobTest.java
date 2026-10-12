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
    void numericStringsAreAccepted() throws Exception {
        JsonNode out = process("{\"userid\":\"u\",\"pulse\":\"131\"}");
        assertEquals(3, out.get("pulseNEWS").asInt());
        assertEquals("131", out.get("pulse").asText());
    }
}
