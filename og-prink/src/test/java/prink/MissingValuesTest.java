package prink;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedBroadcastOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.Test;
import prink.datatypes.CastleRule;
import prink.datatypes.Cluster;
import prink.generalizations.*;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Missing JSON fields are mapped to <code>null</code>. Prink has to cope with <code>null</code> values in
 * generalized and sensitive attributes instead of failing with a NullPointerException.
 */
class MissingValuesTest {

    private static final String[][] TREE = {
            {"A", "A1"}, {"A", "A2"}, {"B", "B1"}, {"B", "B2"}
    };

    private static CastleRule[] rules() {
        return new CastleRule[]{
                new CastleRule(0, new NoneGeneralizer(), false),
                new CastleRule(1, new AggregationIntegerGeneralizer(Tuple2.of(0, 100)), false),
                new CastleRule(2, new AggregationDoubleGeneralizer(Tuple2.of(30d, 45d)), true),
                new CastleRule(3, new NonNumericalGeneralizer(TREE), true),
                new CastleRule(4, new ReductionGeneralizer(), false),
        };
    }

    @Test
    void aggregationIgnoresMissingValues() {
        Cluster cluster = new Cluster(rules(), 0, false);
        cluster.addEntry(Tuple5.of("p1", 10, null, "A1", "abc"));
        cluster.addEntry(Tuple5.of("p2", null, 37.5, "A2", null));
        cluster.addEntry(Tuple5.of("p3", 20, 38.0, null, "abd"));

        Tuple generalized = cluster.generalize(Tuple5.of("p1", 10, null, "A1", "abc"));
        assertEquals(Tuple2.of(10, 20), generalized.getField(1));
        assertEquals(Tuple2.of(37.5, 38.0), generalized.getField(2));
        // a missing value next to present ones must not be reported as one of them -> root of the hierarchy
        assertEquals("<ROOT>", generalized.getField(3));
        assertNotNull(generalized.getField(4));

        // enlargement with a tuple that only has missing values is computable
        assertTrue(cluster.enlargementValue(Tuple5.of("p4", null, null, null, null)) >= 0);
    }

    @Test
    void clusterWithOnlyMissingValuesReleasesNull() {
        Cluster cluster = new Cluster(rules(), 0, false);
        cluster.addEntry(Tuple5.of("p1", null, null, "A1", "x"));
        cluster.addEntry(Tuple5.of("p2", null, null, "A1", "x"));
        Tuple generalized = cluster.generalize(Tuple5.of("p1", null, null, "A1", "x"));
        assertNull(generalized.getField(1));
        assertNull(generalized.getField(2));
        assertEquals("A1", generalized.getField(3));

        // removing the only tuple with a present value resets the range
        Cluster other = new Cluster(rules(), 0, false);
        Tuple withValue = Tuple5.of("p1", 5, 36.6, "B1", "y");
        other.addEntry(withValue);
        other.addEntry(Tuple5.of("p2", null, null, "B1", "y"));
        other.removeEntry(withValue);
        assertNull(other.generalize(Tuple5.of("p2", null, null, "B1", "y")).getField(1));
    }

    @Test
    void diversityCountsMissingAsOwnValue() {
        Cluster cluster = new Cluster(rules(), 0, false);
        cluster.addEntry(Tuple5.of("p1", 1, null, "A1", "a"));
        cluster.addEntry(Tuple5.of("p2", 2, null, "A1", "a"));
        cluster.addEntry(Tuple5.of("p3", 3, 37.0, "A2", "a"));

        assertEquals(2, cluster.diversity(new int[]{2}));
        assertEquals(2, cluster.diversity(new int[]{3}));
        // multiple sensitive attributes (Gal et al. counting) must not throw either
        assertTrue(cluster.diversity(new int[]{2, 3}) >= 1);
        assertEquals(Cluster.MISSING_VALUE, Cluster.comparableValue(Tuple5.of("p1", 1, null, "A1", "a"), 2));
    }

    @Test
    void castleFunctionProcessesTuplesWithMissingValues() throws Exception {
        MapStateDescriptor<Integer, CastleRule> ruleStateDescriptor = new MapStateDescriptor<>(
                "RulesBroadcastState", BasicTypeInfo.INT_TYPE_INFO, TypeInformation.of(new TypeHint<CastleRule>() {}));

        int k = 3;
        int l = 2;
        int delta = 20;
        CastleFunction<String, Tuple5<String, Integer, Double, String, String>, Tuple6<Object, Object, Object, Object, Object, Object>> castleFunction =
                new CastleFunction<>(0, k, l, delta, 50, 10, 10, true, 2);

        List<String> leaves = Arrays.asList("A1", "A2", "B1", "B2", null);
        try (KeyedBroadcastOperatorTestHarness<String, Tuple5<String, Integer, Double, String, String>, CastleRule, Tuple6<Object, Object, Object, Object, Object, Object>> harness =
                     ProcessFunctionTestHarnesses.forKeyedBroadcastProcessFunction(castleFunction, tuple -> tuple.getField(0), Types.STRING, ruleStateDescriptor)) {

            for (CastleRule rule : rules()) harness.processBroadcastElement(rule, 0);

            int inputs = 0;
            for (int round = 0; round < 6; round++) {
                for (int patient = 0; patient < 10; patient++) {
                    Integer qi = (patient % 3 == 0) ? null : 60 + patient;
                    Double temp = (patient % 4 == 0) ? null : 36.0 + patient / 10.0;
                    String code = leaves.get((patient + round) % leaves.size());
                    String text = (patient % 5 == 0) ? null : "ward-" + patient;
                    harness.processElement(new StreamRecord<>(Tuple5.of(String.format("%07d", patient), qi, temp, code, text), inputs++));
                }
            }
            // every tuple beyond delta forces a release decision (released, re-used or suppressed)
            assertTrue(harness.extractOutputStreamRecords().size() >= inputs - delta);
            for (StreamRecord<? extends Tuple> record : harness.extractOutputStreamRecords()) {
                assertNotNull(record.getValue().getField(0), "data subject id passes through");
            }
        }
    }
}
