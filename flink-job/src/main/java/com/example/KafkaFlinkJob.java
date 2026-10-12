package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaFlinkJob {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaFlinkJob.class);

    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set up Kafka consumer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka:29092");
        properties.setProperty("group.id", "flink-group");

        // Create a Kafka consumer
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("raw-topic", new SimpleStringSchema(), properties);
        DataStream<String> stream = env.addSource(consumer);

        // Process the stream: compute NEWS2 sub-scores for every raw record
        DataStream<String> processedStream = stream.flatMap(new FlatMapFunction<String, String>() {
            private transient ObjectMapper objectMapper;

            @Override
            public void flatMap(String value, Collector<String> out) {
                if (objectMapper == null) {
                    objectMapper = new ObjectMapper();
                }
                String processed = process(objectMapper, value);
                if (processed != null) {
                    out.collect(processed);
                    LOG.debug("[flink-job] Processed message: {}", processed);
                }
            }
        });

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("kafka:29092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("processed-topic")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();
        processedStream.sinkTo(sink);

        // Execute the Flink job
        env.execute("Kafka Flink Job");
    }

    /**
     * Fields that are always present in a processed-topic record (JSON null if
     * the raw record lacks them), so downstream consumers and Prink profiles
     * see a stable minimum schema. All other input fields are passed through
     * only if present.
     */
    private static final String[] ALWAYS_PRESENT = {
            "recordid", "userid", "caseid", "username", "userinitials", "ward",
            "resp", "bps", "pulse", "spo2", "temp", "icd10", "timestamp"
    };

    /**
     * Parses one raw-topic record and returns the processed-topic record, or
     * null if the record cannot be used (invalid JSON or no userid).
     *
     * <p>The output is a pass-through of the input object: every input field
     * is copied unchanged (so new optional fields reach processed-topic
     * without code changes), then the computed NEWS2 fields are added,
     * overwriting input fields of the same name. {@code recordid} is
     * normalized to a string.
     *
     * <p>Missing or null vital signs yield a null sub-score; news2Partial is
     * the sum of the available sub-scores and news2Components their number.
     */
    static String process(ObjectMapper objectMapper, String value) {
        JsonNode in;
        try {
            in = objectMapper.readTree(value);
        } catch (Exception e) {
            LOG.warn("[flink-job] Skipping invalid JSON record: {}", e.getMessage());
            return null;
        }
        if (in == null || !in.isObject() || !hasValue(in, "userid")) {
            LOG.warn("[flink-job] Skipping record without userid");
            return null;
        }

        Integer respScore = News2.respScore(numeric(in, "resp"));
        Integer bpsScore = News2.systolicScore(numeric(in, "bps"));
        Integer pulseScore = News2.pulseScore(numeric(in, "pulse"));
        Integer spo2Score = News2.spo2Score(numeric(in, "spo2"));
        Integer tempScore = News2.tempScore(numeric(in, "temp"));

        // pass-through: copy all input fields (in input order)
        ObjectNode out = ((ObjectNode) in).deepCopy();
        for (String field : ALWAYS_PRESENT) {
            if (!out.has(field)) {
                out.putNull(field);
            }
        }
        out.put("recordid", hasValue(in, "recordid") ? in.get("recordid").asText() : null);

        // computed fields (overwrite input fields of the same name)
        out.put("respNEWS", respScore);
        out.put("bpsNEWS", bpsScore);
        out.put("pulseNEWS", pulseScore);
        out.put("spo2NEWS", spo2Score);
        out.put("tempNEWS", tempScore);
        out.put("news2Partial", News2.partialSum(respScore, bpsScore, pulseScore, spo2Score, tempScore));
        out.put("news2Components", News2.components(respScore, bpsScore, pulseScore, spo2Score, tempScore));
        return out.toString();
    }

    private static boolean hasValue(JsonNode node, String field) {
        JsonNode v = node.get(field);
        return v != null && !v.isNull();
    }

    /** Numeric value of a field (number or numeric string), or null. */
    static Double numeric(JsonNode node, String field) {
        JsonNode v = node.get(field);
        if (v == null || v.isNull()) {
            return null;
        }
        if (v.isNumber()) {
            return v.asDouble();
        }
        if (v.isTextual()) {
            try {
                return Double.parseDouble(v.asText().trim());
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }
}
