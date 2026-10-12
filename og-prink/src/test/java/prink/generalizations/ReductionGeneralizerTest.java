package prink.generalizations;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import prink.CastleFunction;
import prink.datatypes.CastleRule;
import prink.datatypes.Cluster;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ReductionGeneralizerTest {

    private static Cluster cluster(String... values) {
        CastleRule[] rules = {
                new CastleRule(0, new NoneGeneralizer(), false),
                new CastleRule(1, new ReductionGeneralizer(), false),
        };
        Cluster cluster = new Cluster(rules, 0, false);
        for (int i = 0; i < values.length; i++) cluster.addEntry(Tuple2.of("p" + i, values[i]));
        return cluster;
    }

    private static Object reduced(String... values) {
        return cluster(values).generalize(Tuple2.of("p0", values[0])).getField(1);
    }

    @Test
    void equalValuesAreReleasedUnchanged() {
        assertEquals("J96.11", reduced("J96.11", "J96.11"));
    }

    @Test
    void trailingCharactersAreReplacedUntilAllValuesAreEqual() {
        assertEquals("J96.1*", reduced("J96.11", "J96.10"));
        assertEquals("J96.**", reduced("J96.11", "J96.01"));
        assertEquals("J9****", reduced("J96.11", "J90.00"));
        assertEquals("1*", reduced("10", "12"));
        assertEquals("**", reduced("10", "22"));
    }

    @Test
    void valuesOfDifferentLengthAreReplacedCompletely() {
        // right-aligned replacement: J96.0 never shares a prefix pattern with J96.11 below full length
        assertEquals("******", reduced("J96.11", "J96.0"));
        assertEquals("*****", reduced("J96.0", "I10"));
    }

    @Test
    void missingValues() {
        assertEquals("******", reduced("J96.11", null));
        assertNull(reduced(null, null), "a cluster without any value releases null");
    }

    @Test
    void generalizeMaxIsSingleStar() {
        assertEquals(Tuple2.of("*", 1.0f), new ReductionGeneralizer().generalizeMax());
    }

    // FIXME rework tests with new Generalizer Structure
//    Cluster baseCluster;
//    ReductionGeneralizer subject;
//
//    @BeforeEach
//    void setUp() {
//        ArrayList<CastleRule> rulesTemp = new ArrayList<>();
//        rulesTemp.add(new CastleRule(0, CastleFunction.Generalization.REDUCTION, false));
//        rulesTemp.add(new CastleRule(1, CastleFunction.Generalization.AGGREGATION, Tuple2.of(0f,100f),false));
//        rulesTemp.add(new CastleRule(2, CastleFunction.Generalization.NONE, false));
//        rulesTemp.add(new CastleRule(3, CastleFunction.Generalization.NONE, true));
//
//        CastleRule[] rules = rulesTemp.toArray(new CastleRule[0]);
//
//        baseCluster = new Cluster(rules, 0,false);
//
//        subject = new ReductionGeneralizer(baseCluster);
//    }
//
//    @Test
//    void generalizeWithNoTuples() {
//        assertEquals(Tuple2.of("", 1.0f), subject.generalize(0));
//    }
//
//    @Nested
//    @DisplayName("One Tuple provided")
//    class OneValueTests {
//        @Test
//        void generalizeString() {
//            baseCluster.addEntry(Tuple4.of("123456", 10, 10, 10));
//            assertEquals(Tuple2.of("123456", 0.0f), subject.generalize(0));
//        }
//
//        @Test
//        void generalizeInt() {
//            baseCluster.addEntry(Tuple4.of(123456, 10, 10, 10));
//            assertEquals(Tuple2.of("123456", 0.0f), subject.generalize(0));
//        }
//
//        @Test
//        void generalizeFloat() {
//            baseCluster.addEntry(Tuple4.of(123456f, 10, 10, 10));
//            assertEquals(Tuple2.of("123456.0", 0.0f), subject.generalize(0));
//        }
//
//        @Test
//        void generalizeLong() {
//            baseCluster.addEntry(Tuple4.of(123456L, 10, 10, 10));
//            assertEquals(Tuple2.of("123456", 0.0f), subject.generalize(0));
//        }
//    }
//
//    @Nested
//    @DisplayName("Three Tuple provided")
//    class ThreeValuesTests {
//        @Test
//        void generalizeString() {
//            baseCluster.addEntry(Tuple4.of("123456", 10, 10, 10));
//            baseCluster.addEntry(Tuple4.of("123567", 10, 10, 10));
//            baseCluster.addEntry(Tuple4.of("123678", 10, 10, 10));
//            assertEquals(Tuple2.of("123***", 0.5f), subject.generalize(0));
//        }
//
//        @Test
//        void generalizeInt() {
//            baseCluster.addEntry(Tuple4.of(123456, 10, 10, 10));
//            baseCluster.addEntry(Tuple4.of(123567, 10, 10, 10));
//            baseCluster.addEntry(Tuple4.of(123678, 10, 10, 10));
//            assertEquals(Tuple2.of("123***", 0.5f), subject.generalize(0));
//        }
//
//        @Test
//        void generalizeFloat() {
//            baseCluster.addEntry(Tuple4.of(123456f, 10, 10, 10));
//            baseCluster.addEntry(Tuple4.of(123567f, 10, 10, 10));
//            baseCluster.addEntry(Tuple4.of(123678f, 10, 10, 10));
//            assertEquals(Tuple2.of("123*****", 0.625f), subject.generalize(0));
//        }
//
//        @Test
//        void generalizeLong() {
//            baseCluster.addEntry(Tuple4.of(123456L, 10, 10, 10));
//            baseCluster.addEntry(Tuple4.of(123567L, 10, 10, 10));
//            baseCluster.addEntry(Tuple4.of(123678L, 10, 10, 10));
//            assertEquals(Tuple2.of("123***", 0.5f), subject.generalize(0));
//        }
//    }
//
//    @Test
//    void generalizeWithDifferentInputSizes() {
//        baseCluster.addEntry(Tuple4.of(123, 10, 10, 10));
//        baseCluster.addEntry(Tuple4.of(1234, 10, 10, 10));
//        baseCluster.addEntry(Tuple4.of(123456789, 10, 10, 10));
//        assertEquals(Tuple2.of("*********", 1.0f), subject.generalize(0));
//    }
//
//    @Test
//    void generalizeCompletely() {
//        baseCluster.addEntry(Tuple4.of(123456, 10, 10, 10));
//        baseCluster.addEntry(Tuple4.of(654321, 10, 10, 10));
//        assertEquals(Tuple2.of("******", 1.0f), subject.generalize(0));
//    }
//
//    @Test
//    void generalizeWith() {
//        baseCluster.addEntry(Tuple4.of(123456, 10, 10, 10));
//        List<Tuple> tuples = new ArrayList<>();
//        tuples.add(Tuple4.of(123678, 10, 10, 10));
//        assertEquals(Tuple2.of("123***", 0.5f), subject.generalize(tuples,0));
//
//        tuples.add(Tuple4.of(129678, 10, 10, 10));
//        assertEquals(Tuple2.of("12****", 0.6666667f), subject.generalize(tuples,0));
//
//        tuples.add(Tuple4.of(223678, 10, 10, 10));
//        assertEquals(Tuple2.of("******", 1.0f), subject.generalize(tuples,0));
//    }
//
//    @Test
//    void generalizeMax() {
//        assertEquals(Tuple2.of("*", 1.0f), subject.generalizeMax(0));
//    }
}