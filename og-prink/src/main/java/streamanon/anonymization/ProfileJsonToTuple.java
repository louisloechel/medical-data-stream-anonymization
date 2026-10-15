package streamanon.anonymization;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import streamanon.anonymization.config.AnonymizationProfile;
import streamanon.anonymization.config.FieldSpec;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

/**
 * Maps an input JSON record to a tuple following the {@link AnonymizationProfile}:
 * one position per non-dropped field (in profile order), values converted to the configured type,
 * missing / unparsable values as {@code null}, masked fields already replaced.
 * Records that are not valid JSON objects or lack the data subject key are skipped.
 */
public class ProfileJsonToTuple extends RichFlatMapFunction<String, Tuple> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ProfileJsonToTuple.class);

    private final FieldSpec[] fields;
    private final int keyPosition;
    private transient ObjectMapper objectMapper;

    public ProfileJsonToTuple(AnonymizationProfile profile) {
        List<FieldSpec> tupleFields = profile.tupleFields();
        this.fields = tupleFields.toArray(new FieldSpec[0]);
        this.keyPosition = profile.keyPosition();
    }

    @Override
    public void open(Configuration parameters) {
        objectMapper = new ObjectMapper();
    }

    @Override
    public void flatMap(String value, Collector<Tuple> out) {
        if (objectMapper == null) objectMapper = new ObjectMapper();
        JsonNode json;
        try {
            json = objectMapper.readTree(value);
        } catch (Exception e) {
            LOG.warn("Skipping record that is not valid JSON: {}", e.getMessage());
            return;
        }
        if (json == null || !json.isObject()) {
            LOG.warn("Skipping record that is not a JSON object");
            return;
        }
        Tuple tuple = toTuple(json);
        if (tuple.getField(keyPosition) == null) {
            LOG.warn("Skipping record without data subject key '{}'", fields[keyPosition].name);
            return;
        }
        out.collect(tuple);
    }

    /** Converts a JSON object to the tuple layout of the profile. */
    public Tuple toTuple(JsonNode json) {
        Tuple tuple = Tuple.newInstance(fields.length);
        for (int i = 0; i < fields.length; i++) {
            FieldSpec field = fields[i];
            Object parsed = parse(json.get(field.name), field.type);
            if (field.role == FieldSpec.Role.MASK) parsed = mask(parsed, field.mask);
            tuple.setField(parsed, i);
        }
        return tuple;
    }

    /**
     * Converts a JSON value to the configured type.
     * @return the converted value or {@code null} if the value is missing, JSON null or cannot be converted
     */
    static Object parse(JsonNode node, FieldSpec.FieldType type) {
        if (node == null || node.isNull() || node.isMissingNode()) return null;
        try {
            switch (type) {
                case INT:
                    if (node.isIntegralNumber() || node.isBoolean()) return node.asInt();
                    if (node.isNumber()) return (int) Math.round(node.doubleValue());
                    if (node.isTextual()) return (int) Math.round(Double.parseDouble(node.textValue().trim()));
                    return null;
                case LONG:
                    if (node.isIntegralNumber() || node.isBoolean()) return node.asLong();
                    if (node.isNumber()) return Math.round(node.doubleValue());
                    if (node.isTextual()) return Math.round(Double.parseDouble(node.textValue().trim()));
                    return null;
                case FLOAT:
                    if (node.isNumber()) return node.floatValue();
                    if (node.isTextual()) return Float.parseFloat(node.textValue().trim());
                    return null;
                case DOUBLE:
                    if (node.isNumber()) return node.doubleValue();
                    if (node.isTextual()) return Double.parseDouble(node.textValue().trim());
                    return null;
                case STRING:
                default:
                    if (node.isTextual()) return node.textValue();
                    if (node.isContainerNode()) return node.toString();
                    return node.asText();
            }
        } catch (NumberFormatException e) {
            LOG.debug("Could not convert '{}' to {}", node, type);
            return null;
        }
    }

    /** Applies the mask of a {@code mask} field. {@code null} stays {@code null}. */
    static Object mask(Object value, FieldSpec.MaskSpec mask) {
        if (value == null) return null;
        String strategy = mask == null ? "hash" : mask.strategy;
        if ("constant".equals(strategy)) {
            return (mask.value == null) ? "***" : mask.value;
        }
        String salt = (mask == null || mask.salt == null) ? "" : mask.salt;
        String hash = sha256Hex(salt + value);
        if (mask != null && mask.length != null && mask.length > 0 && mask.length < hash.length()) {
            hash = hash.substring(0, mask.length);
        }
        return hash;
    }

    private static String sha256Hex(String input) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(digest.length * 2);
            for (byte b : digest) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }
}
