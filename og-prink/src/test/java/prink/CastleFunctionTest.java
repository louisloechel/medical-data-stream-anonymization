package prink;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedBroadcastOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import prink.datatypes.CastleRule;
import prink.generalizations.AggregationFloatGeneralizer;
import prink.generalizations.NoneGeneralizer;

import java.util.*;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CastleFunctionTest {

    final MapStateDescriptor<Integer, CastleRule> ruleStateDescriptor =
            new MapStateDescriptor<>(
                    "RulesBroadcastState",
                    BasicTypeInfo.INT_TYPE_INFO,
                    TypeInformation.of(new TypeHint<CastleRule>(){}));

    /**
     * Tests that data is put in only one cluster when no rules are defined.
     * All entered data tuples should be emitted when delta is reached, since they are all inside one cluster.
     */
    @Test
    public void testProcessElement() throws Exception{

        int k = 4;
        int l = 0;
        int delta = 10;
        int beta = 50;
        int zeta = 10;
        int mu = 10;

        CastleFunction<Long, Tuple4<Long, Integer, Integer, Integer>, Tuple4<Object, Object,Object, Object>> castleFunction = new CastleFunction<>(0, k, l, delta, beta, zeta, mu, false, 2);

        ArrayList<StreamRecord<Tuple4<Long, Integer, Integer, Integer>>> testInputs = new ArrayList<>();
        for (int i = 0; i <= 10; i++) {
            testInputs.add(new StreamRecord<>(Tuple4.of((long) i, 10, 20 , 30), 1));
        }

        try (KeyedBroadcastOperatorTestHarness<Long, Tuple4<Long, Integer, Integer, Integer>, CastleRule, Tuple4<Object, Object,Object, Object>> harness = ProcessFunctionTestHarnesses.forKeyedBroadcastProcessFunction(castleFunction, tuple -> tuple.getField(0), Types.LONG, ruleStateDescriptor)) {

            harness.processBroadcastElement(new CastleRule(0, new NoneGeneralizer(), false), 1);
            harness.processBroadcastElement(new CastleRule(1, new NoneGeneralizer(), false), 1);
            harness.processBroadcastElement(new CastleRule(2, new NoneGeneralizer(), false), 1);
            harness.processBroadcastElement(new CastleRule(3, new NoneGeneralizer(), false), 1);

            for (StreamRecord<Tuple4<Long, Integer, Integer, Integer>> temp: testInputs) {
                harness.processElement(temp);
            }

//            assertEquals(testInputs, harness.extractOutputStreamRecords());
            assertEquals(testInputs.size(), harness.extractOutputStreamRecords().size());
        }
    }

    /**
     * Tests if the aggregation generalizer is working correctly.
     * The second and third attribute needs to be generalized through aggregation.
     * As the third attribute has always the value '1337' it should result in a generalization of [1337f,1337f] and have no information loss as no generalization is needed.
     * The second attribute should return an information loss minimized generalization.
     */
    @Test
    public void testBasicNumericalGeneralization() throws Exception{

        int k = 4;
        int l = 2;
        int delta = 10;
        int beta = 50;
        int zeta = 10;
        int mu = 10;

        CastleFunction<Long, Tuple4<Long, Float, Float, Integer>, Tuple4<Object, Object, Object, Object>> castleFunction = new CastleFunction<>(0, k, l, delta, beta, zeta, mu, false, 2);

        ArrayList<StreamRecord<Tuple4<Long, Float, Float, Integer>>> testInputs = new ArrayList<>();
        // Push 15 tuples into the stream to trigger delta (10) twice (at tuple ids: 9 and 14) to emmit two castle buckets
        for (int i = 0; i < 15; i++) {
            // Tuple structure: [id, unique second attribute, static third attribute, sensible attribute]
            testInputs.add(new StreamRecord<>(Tuple4.of((long) i, (float) i, 133.7f, 10+i), i));
        }

        try (KeyedBroadcastOperatorTestHarness<Long, Tuple4<Long, Float, Float, Integer>, CastleRule, Tuple4<Object, Object,Object, Object>> harness = ProcessFunctionTestHarnesses.forKeyedBroadcastProcessFunction(castleFunction, tuple -> tuple.getField(0), Types.LONG, ruleStateDescriptor)) {

            harness.processBroadcastElement(new CastleRule(1, new AggregationFloatGeneralizer(Tuple2.of(0f, 100f)),false), 0);
            harness.processBroadcastElement(new CastleRule(2, new AggregationFloatGeneralizer(Tuple2.of(1000f, 2000f)),false), 0);
            harness.processBroadcastElement(new CastleRule(3, new NoneGeneralizer(),true), 0);

            for (StreamRecord<Tuple4<Long, Float, Float, Integer>> temp: testInputs) {
                harness.processElement(temp);
            }

            assertEquals(8, harness.extractOutputStreamRecords().size());
            for (StreamRecord<? extends Tuple4<Object, Object, Object, Object>> emitted_tuple: harness.extractOutputStreamRecords().subList(0,3)) {
                assertEquals(Tuple2.of(0f,3f), emitted_tuple.getValue().f1, "Aggregation result for unique values not correct or optimal generalized");
                assertEquals(Tuple2.of(133.7f,133.7f), emitted_tuple.getValue().f2, "Aggregation result for only one value in the system [1337] not correct");
            }

            for (StreamRecord<? extends Tuple4<Object, Object, Object, Object>> emitted_tuple: harness.extractOutputStreamRecords().subList(4, harness.extractOutputStreamRecords().size()-1)) {
                assertEquals(Tuple2.of(4f,7f), emitted_tuple.getValue().f1, "Aggregation result for unique values not correct or optimal generalized");
                assertEquals(Tuple2.of(133.7f,133.7f), emitted_tuple.getValue().f2, "Aggregation result for only one value in the system [1337] not correct");
            }
        }
    }

    private static Stream<Arguments> provideMinKParameters() {
        int[] kValues = {0,1,2,3,4,5,10,20,50,100}; // {5};
        int[] lValues = {0, 1, 2, 3};
        int[] deltaValues = {20}; // {2,5,10,20,50}; // {20} if k < delta
        int[] betaValues = {1,2,5,50}; //{1,2,5,10,20,30,40,50,60,70,80,90,100,150,200,300,500,1000};//{50};
        int[] zetaValues = {50}; //{10};
        int[] muValues = {10}; //{1,2,3,4,5,6,7,8,9,10,20,30,40,50,75,100,200}; //{10};

        int[] uniqueDataSubjectsValues = {5, 10, 50};
        int[] dataTuplesPerDataSubjectsValues = {1, 10, 20, 100, 200};

        HashMap<String,ArrayList<CastleRule>> ruleSets = new HashMap<>();

        ArrayList<CastleRule> twoAggregationOneSensibleAtt = new ArrayList<>();
        twoAggregationOneSensibleAtt.add(new CastleRule(1, new AggregationFloatGeneralizer(Tuple2.of(0f, 500f)),false));
        twoAggregationOneSensibleAtt.add(new CastleRule(2, new AggregationFloatGeneralizer(Tuple2.of(0f, 500f)),false));
        twoAggregationOneSensibleAtt.add(new CastleRule(3, new NoneGeneralizer(),true));

        ArrayList<CastleRule> twoAggregationTwoSensibleAtt = new ArrayList<>();
        twoAggregationTwoSensibleAtt.add(new CastleRule(1, new AggregationFloatGeneralizer(Tuple2.of(0f, 500f)),false));
        twoAggregationTwoSensibleAtt.add(new CastleRule(2, new AggregationFloatGeneralizer(Tuple2.of(0f, 500f)),true));
        twoAggregationTwoSensibleAtt.add(new CastleRule(3, new NoneGeneralizer(),true));


        ruleSets.put("Two Aggregation One Sensible Attribute", twoAggregationOneSensibleAtt);
        ruleSets.put("Two Aggregation Two Sensible Attribute (one will be aggregated)", twoAggregationTwoSensibleAtt);

        ArrayList<Arguments> arguments = new ArrayList<>();

        for(int k: kValues){
            for(int l: lValues) {
                for (int delta : deltaValues) {
                    for (int beta : betaValues) {
                        for (int zeta : zetaValues) {
                            for (int mu : muValues) {
                                for (int uniqueDataSubjects : uniqueDataSubjectsValues) {
                                    for (int dataTuplesPerDataSubjects : dataTuplesPerDataSubjectsValues) {
                                        for (String desc : ruleSets.keySet()) {
                                            arguments.add(Arguments.of(k, l, delta, beta, zeta, mu, uniqueDataSubjects, dataTuplesPerDataSubjects, desc, ruleSets.get(desc)));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return arguments.stream();
    }

    /**
     * Test that for each quasi-identifier combinations exist at least k entries
     * @param k k
     * @param l l
     * @param delta delta
     * @param beta beta
     * @param zeta zeta
     * @param mu mu
     * @param uniqueDataSubjects Number of unique data subjects (unique subject ids)
     * @param dataTuplesPerDataSubjects Number of data tuples that are generated for each data subject (unique subject id)
     */
    @ParameterizedTest
    @MethodSource("provideMinKParameters")
    public void testForMinKValues(int k, int l, int delta, int beta, int zeta, int mu, int uniqueDataSubjects, int dataTuplesPerDataSubjects, String desc, ArrayList<CastleRule> rules) throws Exception{

        CastleFunction<Long, Tuple4<Long, Float, Float, Integer>, Tuple4<Object, Object, Object, Object>> castleFunction = new CastleFunction<>(0, k, l, delta, beta, zeta, mu, false, 2);

        ArrayList<StreamRecord<Tuple4<Long, Float, Float, Integer>>> testInputs = new ArrayList<>();

        Random random = new Random(1337);

        // with j data entries for i different data subjects
        for (int j = 0; j < dataTuplesPerDataSubjects; j++) {
            for (int i = 0; i < uniqueDataSubjects; i++) {
                // Tuple structure: [id, second attribute, third attribute, sensible attribute]
                testInputs.add(new StreamRecord<>(Tuple4.of((long) i, (float) j, (float) i+j, random.nextInt(i+j+1))));
            }
        }

        try (KeyedBroadcastOperatorTestHarness<Long, Tuple4<Long, Float, Float, Integer>, CastleRule, Tuple4<Object, Object,Object, Object>> harness = ProcessFunctionTestHarnesses.forKeyedBroadcastProcessFunction(castleFunction, tuple -> tuple.getField(0), Types.LONG, ruleStateDescriptor)) {

            for (CastleRule rule: rules) {
                harness.processBroadcastElement(rule, 0);
            }

            while (testInputs.size() > 0){
              harness.processElement(testInputs.remove(random.nextInt(testInputs.size())));
            }

            // Create quasi-identifier combination list
            ArrayList<String> qiIdentifiers = new ArrayList<>();
            for (Tuple4<Object, Object, Object, Object> emitted_tuple: harness.extractOutputValues()) {
                qiIdentifiers.add(emitted_tuple.f1 + "-" + emitted_tuple.f2);
            }
            // Get unique qi combination values
            Set<String> qiSet = new HashSet<>(qiIdentifiers);
            // Remove qi combination of suppressed tuples (max generalization)
            qiSet.remove("(0.0,500.0)-(0.0,500.0)");

            // Check for at least k appearances
            for(String qi: qiSet){
                assertTrue(Collections.frequency(qiIdentifiers, qi) >= k, "Group of Quasi-Identifiers has less then k=" + k + " appearances! QI:" + qi + " Appearances:" + Collections.frequency(qiIdentifiers, qi));
            }

            System.out.println("Number of emitted tuples: " + harness.extractOutputStreamRecords().size() + " of " + (dataTuplesPerDataSubjects*uniqueDataSubjects) + " with rule set: " + desc);
//            System.out.println(harness.extractOutputStreamRecords());
        }
    }

    /**
     * Tests if all data tuples get suppressed if they are all from the same data subject
     */
    @Test
    public void testHandlingValuesFromOneDataSubject() throws Exception{

        int k = 4;
        int l = 2;
        int delta = 10;
        int beta = 50;
        int zeta = 10;
        int mu = 10;

        CastleFunction<Long, Tuple4<Long, Float, Float, Integer>, Tuple4<Object, Object, Object, Object>> castleFunction = new CastleFunction<>(0, k, l, delta, beta, zeta, mu, false, 2);

        ArrayList<StreamRecord<Tuple4<Long, Float, Float, Integer>>> testInputs = new ArrayList<>();

        // with 20 data entries for 1 distinct data subjects
        for (int j = 0; j < 20; j++) {
            // Tuple structure: [id, second attribute, third attribute, sensible attribute]
            testInputs.add(new StreamRecord<>(Tuple4.of((long) 0, (float) j, (float) 10+j, 10+j), j));
        }

        try (KeyedBroadcastOperatorTestHarness<Long, Tuple4<Long, Float, Float, Integer>, CastleRule, Tuple4<Object, Object,Object, Object>> harness = ProcessFunctionTestHarnesses.forKeyedBroadcastProcessFunction(castleFunction, tuple -> tuple.getField(0), Types.LONG, ruleStateDescriptor)) {

            harness.processBroadcastElement(new CastleRule(1, new AggregationFloatGeneralizer(Tuple2.of(0f, 20f)),false), 0);
            harness.processBroadcastElement(new CastleRule(2, new AggregationFloatGeneralizer(Tuple2.of(0f, 30f)),false), 0);
            harness.processBroadcastElement(new CastleRule(3, new NoneGeneralizer(),true), 0);

            for (StreamRecord<Tuple4<Long, Float, Float, Integer>> temp: testInputs) {
                harness.processElement(temp);
            }

            assertEquals(10, harness.extractOutputStreamRecords().size());
        }
    }

    /**
     * Check that data tuples that are diverse enough get anonymized correctly
     */
    @Test
    public void testHandlingOfNonDiverseValues() throws Exception{

        int k = 4;
        int l = 2;
        int delta = 10;
        int beta = 50;
        int zeta = 10;
        int mu = 10;

        CastleFunction<Long, Tuple4<Long, Float, Float, Integer>, Tuple4<Object, Object, Object, Object>> castleFunction = new CastleFunction<>(0, k, l, delta, beta, zeta, mu, false, 0);

        ArrayList<StreamRecord<Tuple4<Long, Float, Float, Integer>>> testInputs = new ArrayList<>();

        // with 20 data entries for 1 distinct data subjects
        for (int j = 0; j < 20; j++) {
            // Tuple structure: [id, second attribute, third attribute, sensible attribute]
            testInputs.add(new StreamRecord<>(Tuple4.of((long) j, 20f, 20f, j), j));
        }

        try (KeyedBroadcastOperatorTestHarness<Long, Tuple4<Long, Float, Float, Integer>, CastleRule, Tuple4<Object, Object,Object, Object>> harness = ProcessFunctionTestHarnesses.forKeyedBroadcastProcessFunction(castleFunction, tuple -> tuple.getField(0), Types.LONG, ruleStateDescriptor)) {

            harness.processBroadcastElement(new CastleRule(1, new AggregationFloatGeneralizer(Tuple2.of(0f, 20f)),false), 0);
            harness.processBroadcastElement(new CastleRule(2, new AggregationFloatGeneralizer(Tuple2.of(0f, 30f)),false), 0);
            harness.processBroadcastElement(new CastleRule(3, new NoneGeneralizer(),true), 0);

            for (StreamRecord<Tuple4<Long, Float, Float, Integer>> temp: testInputs) {
                harness.processElement(temp);
            }

            assertEquals(12, harness.extractOutputStreamRecords().size());
        }
    }

    @Test
    public void testHandlingDiverseValuesCorrectly() throws Exception{

        int k = 4;
        int l = 2;
        int delta = 10;
        int beta = 50;
        int zeta = 10;
        int mu = 10;

        CastleFunction<Long, Tuple4<Long, Float, Float, Integer>, Tuple4<Object, Object, Object, Object>> castleFunction = new CastleFunction<>(0, k, l, delta, beta, zeta, mu, false, 2);

        ArrayList<StreamRecord<Tuple4<Long, Float, Float, Integer>>> testInputs = new ArrayList<>();

        // with 20 data entries for 1 distinct data subjects
        for (int j = 0; j < 20; j++) {
            // Tuple structure: [id, second attribute, third attribute, sensible attribute]
            testInputs.add(new StreamRecord<>(Tuple4.of((long) 0, (float) j, (float) 10+j, 10+j), j));
        }

        try (KeyedBroadcastOperatorTestHarness<Long, Tuple4<Long, Float, Float, Integer>, CastleRule, Tuple4<Object, Object,Object, Object>> harness = ProcessFunctionTestHarnesses.forKeyedBroadcastProcessFunction(castleFunction, tuple -> tuple.getField(0), Types.LONG, ruleStateDescriptor)) {

            harness.processBroadcastElement(new CastleRule(0, new NoneGeneralizer(),false), 0);
            harness.processBroadcastElement(new CastleRule(1, new AggregationFloatGeneralizer(Tuple2.of(0f, 20f)),false), 0);
            harness.processBroadcastElement(new CastleRule(2, new AggregationFloatGeneralizer(Tuple2.of(0f, 30f)),false), 0);
            harness.processBroadcastElement(new CastleRule(3, new NoneGeneralizer(),true), 0);

            for (StreamRecord<Tuple4<Long, Float, Float, Integer>> temp: testInputs) {
                harness.processElement(temp);
            }

            assertEquals(10, harness.extractOutputStreamRecords().size());
        }
    }

    private static Stream<Arguments> provideSuppressionParameters() {
        int[] suppressionStrategyValues = {-1, 3};

        ArrayList<Arguments> arguments = new ArrayList<>();

        for(int suppressionStrategy: suppressionStrategyValues){
            arguments.add(Arguments.of(suppressionStrategy));
        }
        return arguments.stream();
    }

    /**
     * Tests if unsupported suppression parameters are handled correctly
     */
    @ParameterizedTest
    @MethodSource("provideSuppressionParameters")
    public void testSuppressionMethodeUnknown(int suppressionStrategy) throws Exception{

        int k = 2;
        int l = 2;
        int delta = 2;
        int beta = 50;
        int zeta = 10;
        int mu = 10;

        CastleFunction<Long, Tuple4<Long, Float, Float, Integer>, Tuple4<Object, Object, Object, Object>> castleFunction = new CastleFunction<>(0, k, l, delta, beta, zeta, mu, false, suppressionStrategy);

        ArrayList<StreamRecord<Tuple4<Long, Float, Float, Integer>>> testInputs = new ArrayList<>();

        // Give flink some normal tuples to process to prevent "Automatic type extraction is not possible on candidates with null values. Please specify the type" errors
        testInputs.add(new StreamRecord<>(Tuple4.of(0L, 10f, 10f, 1337), 1));
        testInputs.add(new StreamRecord<>(Tuple4.of(1L, 10f, 10f, 1338), 1));

        // with 20 data entries for 1 distinct data subjects
        for (int j = 0; j < 20; j++) {
            // Tuple structure: [id, second attribute, third attribute, sensible attribute]
            testInputs.add(new StreamRecord<>(Tuple4.of((long) 0, (float) j, (float) 10+j, 1337), j+10));
        }

        try (KeyedBroadcastOperatorTestHarness<Long, Tuple4<Long, Float, Float, Integer>, CastleRule, Tuple4<Object, Object,Object, Object>> harness = ProcessFunctionTestHarnesses.forKeyedBroadcastProcessFunction(castleFunction, tuple -> tuple.getField(0), Types.LONG, ruleStateDescriptor)) {

            harness.setup(TypeInformation.of(new TypeHint<Tuple4<Object, Object, Object, Object>>() {}).createSerializer(new ExecutionConfig()));

            harness.processBroadcastElement(new CastleRule(0, new NoneGeneralizer(),false), 0);
            harness.processBroadcastElement(new CastleRule(1, new AggregationFloatGeneralizer(Tuple2.of(0f, 20f)),false), 0);
            harness.processBroadcastElement(new CastleRule(2, new NoneGeneralizer(),false), 0);
            harness.processBroadcastElement(new CastleRule(3, new NoneGeneralizer(),true), 0);

            for (StreamRecord<Tuple4<Long, Float, Float, Integer>> temp: testInputs) {
                harness.processElement(temp);
            }

            assertEquals(20, harness.extractOutputStreamRecords().size());

            for(Tuple4<Object, Object, Object, Object> outputTuple: harness.extractOutputValues().subList(2, harness.extractOutputValues().size())){
                assertEquals(Tuple4.of(null, null, null, null), outputTuple);
            }
        }
    }

    /**
     * Test if suppression methode 0 (no tuple is collected) is working correctly
     */
    @Test
    public void testSuppressionMethode0() throws Exception{

        int k = 2;
        int l = 2;
        int delta = 2;
        int beta = 50;
        int zeta = 10;
        int mu = 10;

        CastleFunction<Long, Tuple4<Long, Float, Float, Integer>, Tuple4<Object, Object, Object, Object>> castleFunction = new CastleFunction<>(0, k, l, delta, beta, zeta, mu, false, 0);

        ArrayList<StreamRecord<Tuple4<Long, Float, Float, Integer>>> testInputs = new ArrayList<>();

        // Give flink some normal tuples to process to prevent "Automatic type extraction is not possible on candidates with null values. Please specify the type" errors
        testInputs.add(new StreamRecord<>(Tuple4.of(0L, 10f, 10f, 1337), 1));
        testInputs.add(new StreamRecord<>(Tuple4.of(1L, 10f, 10f, 1338), 1));

        // with 20 data entries for 1 distinct data subjects
        for (int j = 0; j < 20; j++) {
            // Tuple structure: [id, second attribute, third attribute, sensible attribute]
            testInputs.add(new StreamRecord<>(Tuple4.of((long) 0, (float) j, (float) 10+j, 1337), j+10));
        }

        try (KeyedBroadcastOperatorTestHarness<Long, Tuple4<Long, Float, Float, Integer>, CastleRule, Tuple4<Object, Object,Object, Object>> harness = ProcessFunctionTestHarnesses.forKeyedBroadcastProcessFunction(castleFunction, tuple -> tuple.getField(0), Types.LONG, ruleStateDescriptor)) {

            harness.setup(TypeInformation.of(new TypeHint<Tuple4<Object, Object, Object, Object>>() {}).createSerializer(new ExecutionConfig()));

            harness.processBroadcastElement(new CastleRule(0, new NoneGeneralizer(),false), 0);
            harness.processBroadcastElement(new CastleRule(1, new AggregationFloatGeneralizer(Tuple2.of(0f, 20f)),false), 0);
            harness.processBroadcastElement(new CastleRule(2, new NoneGeneralizer(),false), 0);
            harness.processBroadcastElement(new CastleRule(3, new NoneGeneralizer(),true), 0);

            for (StreamRecord<Tuple4<Long, Float, Float, Integer>> temp: testInputs) {
                harness.processElement(temp);
            }

            assertEquals(2, harness.extractOutputStreamRecords().size());
        }
    }

    /**
     * Test if suppression methode 1 (every quasi-identifier gets replaced with null) is working correctly
     */
    @Test
    public void testSuppressionMethode1() throws Exception{

        int k = 2;
        int l = 2;
        int delta = 2;
        int beta = 50;
        int zeta = 10;
        int mu = 10;

        CastleFunction<Long, Tuple4<Long, Float, Float, Integer>, Tuple4<Object, Object, Object, Object>> castleFunction = new CastleFunction<>(0, k, l, delta, beta, zeta, mu, false, 1);

        ArrayList<StreamRecord<Tuple4<Long, Float, Float, Integer>>> testInputs = new ArrayList<>();

        // Give flink some normal tuples to process to prevent "Automatic type extraction is not possible on candidates with null values. Please specify the type" errors
        testInputs.add(new StreamRecord<>(Tuple4.of(0L, 10f, 10f, 1337), 1));
        testInputs.add(new StreamRecord<>(Tuple4.of(1L, 10f, 10f, 1338), 1));

        // with 20 data entries for 1 distinct data subjects
        for (int j = 0; j < 20; j++) {
            // Tuple structure: [id, second attribute, third attribute, sensible attribute]
            testInputs.add(new StreamRecord<>(Tuple4.of((long) 0, (float) j, (float) 10+j, 1337), j+10));
        }

        try (KeyedBroadcastOperatorTestHarness<Long, Tuple4<Long, Float, Float, Integer>, CastleRule, Tuple4<Object, Object,Object, Object>> harness = ProcessFunctionTestHarnesses.forKeyedBroadcastProcessFunction(castleFunction, tuple -> tuple.getField(0), Types.LONG, ruleStateDescriptor)) {

            harness.setup(TypeInformation.of(new TypeHint<Tuple4<Object, Object, Object, Object>>() {}).createSerializer(new ExecutionConfig()));

            harness.processBroadcastElement(new CastleRule(0, new NoneGeneralizer(),false), 0);
            harness.processBroadcastElement(new CastleRule(1, new AggregationFloatGeneralizer(Tuple2.of(0f, 20f)),false), 0);
            harness.processBroadcastElement(new CastleRule(2, new AggregationFloatGeneralizer(Tuple2.of(0f, 30f)),false), 0);
            harness.processBroadcastElement(new CastleRule(3, new NoneGeneralizer(),true), 0);

            for (StreamRecord<Tuple4<Long, Float, Float, Integer>> temp: testInputs) {
                harness.processElement(temp);
            }

            assertEquals(20, harness.extractOutputStreamRecords().size());

            for(Tuple4<Object, Object, Object, Object> outputTuple: harness.extractOutputValues().subList(2, harness.extractOutputValues().size())){
                assertEquals(Tuple4.of(0L, null, null, 1337), outputTuple);
            }
        }
    }

    /**
     * Test if suppression methode 2 (quasi-identifiers are generalized with max generalization values) is working correctly
     */
    @Test
    public void testSuppressionMethode2() throws Exception{

        int k = 2;
        int l = 2;
        int delta = 2;
        int beta = 50;
        int zeta = 10;
        int mu = 10;

        CastleFunction<Long, Tuple4<Long, Float, Float, Integer>, Tuple4<Object, Object, Object, Object>> castleFunction = new CastleFunction<>(0, k, l, delta, beta, zeta, mu, false, 2);

        ArrayList<StreamRecord<Tuple4<Long, Float, Float, Integer>>> testInputs = new ArrayList<>();

        // Give flink some normal tuples to process to prevent "Automatic type extraction is not possible on candidates with null values. Please specify the type" errors
        testInputs.add(new StreamRecord<>(Tuple4.of(1L, 10f, 10f, 1337), 1));
        testInputs.add(new StreamRecord<>(Tuple4.of(2L, 10f, 10f, 1338), 1));

        // with 20 data entries for 1 distinct data subjects
        for (int j = 0; j < 20; j++) {
            // Tuple structure: [id, second attribute, third attribute, sensible attribute]
            testInputs.add(new StreamRecord<>(Tuple4.of((long) 0, (float) j, (float) 10+j, 1337), j+10));
        }

        try (KeyedBroadcastOperatorTestHarness<Long, Tuple4<Long, Float, Float, Integer>, CastleRule, Tuple4<Object, Object,Object, Object>> harness = ProcessFunctionTestHarnesses.forKeyedBroadcastProcessFunction(castleFunction, tuple -> tuple.getField(0), Types.LONG, ruleStateDescriptor)) {

            harness.setup(TypeInformation.of(new TypeHint<Tuple4<Object, Object, Object, Object>>() {}).createSerializer(new ExecutionConfig()));

            harness.processBroadcastElement(new CastleRule(0, new NoneGeneralizer(),false), 0);
            harness.processBroadcastElement(new CastleRule(1, new AggregationFloatGeneralizer(Tuple2.of(-10f, 20f)),false), 0);
            harness.processBroadcastElement(new CastleRule(2, null,false), 0);
            harness.processBroadcastElement(new CastleRule(3, new NoneGeneralizer(),true), 0);

            for (StreamRecord<Tuple4<Long, Float, Float, Integer>> temp: testInputs) {
                harness.processElement(temp);
            }

            assertEquals(20, harness.extractOutputStreamRecords().size());

            for(Tuple4<Object, Object, Object, Object> outputTuple: harness.extractOutputValues().subList(2, harness.extractOutputValues().size())){
                assertEquals(Tuple4.of(0L, Tuple2.of(-10f, 20f), "*", 1337), outputTuple);
            }
        }
    }


    /**
     * With l = 0 a large cluster is split via split()/generateBuckets(). The data subject ids must not be
     * required to be Long values (e.g. patient numbers are Strings with leading zeros).
     */
    @Test
    public void testSplitWithStringDataSubjectIds() throws Exception {
        int k = 2;
        int l = 0;
        int delta = 10;

        CastleFunction<String, Tuple3<String, Float, Integer>, Tuple3<Object, Object, Object>> castleFunction =
                new CastleFunction<>(0, k, l, delta, 50, 10, 10, false, 2);

        try (KeyedBroadcastOperatorTestHarness<String, Tuple3<String, Float, Integer>, CastleRule, Tuple3<Object, Object, Object>> harness =
                     ProcessFunctionTestHarnesses.forKeyedBroadcastProcessFunction(castleFunction, tuple -> tuple.getField(0), Types.STRING, ruleStateDescriptor)) {

            harness.processBroadcastElement(new CastleRule(0, new NoneGeneralizer(), false), 0);
            harness.processBroadcastElement(new CastleRule(1, new AggregationFloatGeneralizer(Tuple2.of(0f, 100f)), false), 0);
            harness.processBroadcastElement(new CastleRule(2, new NoneGeneralizer(), false), 0);

            // identical QI values -> one cluster with 11 distinct data subjects (>= 2k) -> split() is used
            for (int i = 0; i <= delta; i++) {
                harness.processElement(new StreamRecord<>(Tuple3.of(String.format("%07d", i), 42f, i), i));
            }

            assertTrue(harness.extractOutputStreamRecords().size() >= 2 * k);
        }
    }

    /**
     * The rule broadcast stream and the data stream are not ordered. When the rule list is passed to the
     * constructor, tuples that arrive before all rules have been broadcast are buffered (not dropped and not
     * clustered with an incomplete rule set) and processed once the rule set is complete.
     */
    @Test
    public void testTuplesBeforeRulesAreBuffered() throws Exception {
        int k = 2;
        int l = 0;
        int delta = 5;
        List<CastleRule> rules = Arrays.asList(
                new CastleRule(0, new NoneGeneralizer(), false),
                new CastleRule(1, new AggregationFloatGeneralizer(Tuple2.of(0f, 100f)), false),
                new CastleRule(2, new NoneGeneralizer(), false));

        // Serialize/deserialize the function like Flink does when shipping it to a task manager:
        // the (transient) rule list passed to the constructor is lost and has to arrive via the broadcast stream
        CastleFunction<String, Tuple3<String, Float, Integer>, Tuple3<Object, Object, Object>> castleFunction =
                InstantiationUtil.clone(new CastleFunction<>(0, k, l, delta, 50, 10, 10, false, 2, rules));

        try (KeyedBroadcastOperatorTestHarness<String, Tuple3<String, Float, Integer>, CastleRule, Tuple3<Object, Object, Object>> harness =
                     ProcessFunctionTestHarnesses.forKeyedBroadcastProcessFunction(castleFunction, tuple -> tuple.getField(0), Types.STRING, ruleStateDescriptor)) {

            for (int i = 0; i < 4; i++) {
                harness.processElement(new StreamRecord<>(Tuple3.of("p" + i, (float) i, i), i));
            }
            harness.processBroadcastElement(rules.get(0), 0);
            harness.processBroadcastElement(rules.get(1), 0);
            assertEquals(0, harness.extractOutputStreamRecords().size(), "nothing is released with an incomplete rule set");
            harness.processBroadcastElement(rules.get(2), 0);

            for (int i = 4; i < 8; i++) {
                harness.processElement(new StreamRecord<>(Tuple3.of("p" + i, (float) i, i), i));
            }
            // 8 tuples, delta 5 -> the first tuples are released; none of the buffered ones got lost
            assertTrue(harness.extractOutputStreamRecords().size() >= 8 - delta);
            for (StreamRecord<? extends Tuple3<Object, Object, Object>> record : harness.extractOutputStreamRecords()) {
                assertTrue(record.getValue().f1 instanceof Tuple2, "attribute 1 is generalized with the broadcast rule");
            }
        }
    }
}
