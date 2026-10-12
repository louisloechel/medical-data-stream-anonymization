package prink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.api.java.tuple.Tuple16;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import prink.datatypes.CastleRule;
import prink.generalizations.AggregationFloatGeneralizer;
import prink.generalizations.NonNumericalGeneralizer;
import prink.generalizations.NoneGeneralizer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

class CastleFunctionJobTest {

    ArrayList<Tuple> inputData = new ArrayList<>();

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());

    private static Stream<Arguments> provideAgeEvalParameters() {
        int[] kValues = {5};//{5, 10, 25, 50, 100}; // {2,3,4,5,6,7,8,9,10,20,50,100}; // {5};
        int[] lValues = {2}; //{0, 2, 3, 4}; // {0,1,2,3,4,5,6,7,8,9,10}; // {2};
        int[] deltaValues = {200}; // {2,5,10,20,50}; // {20} if k < delta
        int[] betaValues = {50}; //{1,2,5,10,20,30,40,50,60,70,80,90,100,150,200,300,500,1000};//{50};
        int[] zetaValues = {50}; //{10};
        int[] muValues = {10}; //{1,2,3,4,5,6,7,8,9,10,20,30,40,50,75,100,200}; //{10};

        // ------------ DGH section ------------
        // [3, 10] workclass: Private, Self-emp-not-inc, Self-emp-inc, Federal-gov, Local-gov, State-gov, Without-pay, Never-worked
        ArrayList<String[]> treeWorkclass = new ArrayList<>();
        treeWorkclass.add(new String[]{"Never-worked"});
        treeWorkclass.add(new String[]{"Self-emp","Self-emp-not-inc"});
        treeWorkclass.add(new String[]{"Self-emp","Self-emp-inc"});
        treeWorkclass.add(new String[]{"gov","Federal-gov"});
        treeWorkclass.add(new String[]{"gov","Local-gov"});
        treeWorkclass.add(new String[]{"gov","State-gov"});
        treeWorkclass.add(new String[]{"Without-pay"});
        treeWorkclass.add(new String[]{"Private"});

        // [5,26] education: Bachelors, Some-college, 11th, HS-grad, Prof-school, Assoc-acdm, Assoc-voc, 9th, 7th-8th, 12th, Masters, 1st-4th, 10th, Doctorate, 5th-6th, Preschool
        ArrayList<String[]> treeEducation = new ArrayList<>();
        treeEducation.add(new String[]{"Without-Post-Secondary","Preschool"});
        treeEducation.add(new String[]{"Without-Post-Secondary","Elementary","1st-4th"});
        treeEducation.add(new String[]{"Without-Post-Secondary","Elementary","5th-6th"});
        treeEducation.add(new String[]{"Without-Post-Secondary","Elementary","7th-8th"});
        treeEducation.add(new String[]{"Without-Post-Secondary","Junior-Secondary","9th"});
        treeEducation.add(new String[]{"Without-Post-Secondary","Junior-Secondary","10th"});
        treeEducation.add(new String[]{"Without-Post-Secondary","Senior-Secondary","11th"});
        treeEducation.add(new String[]{"Without-Post-Secondary","Senior-Secondary","12th"});
        treeEducation.add(new String[]{"Without-Post-Secondary","Senior-Secondary","HS-grad"});
        treeEducation.add(new String[]{"Post-Secondary","Some-college"});
        treeEducation.add(new String[]{"Post-Secondary","Assoc","Assoc-acdm"});
        treeEducation.add(new String[]{"Post-Secondary","Assoc","Assoc-voc"});
        treeEducation.add(new String[]{"Post-Secondary","University","Bachelors"});
        treeEducation.add(new String[]{"Post-Secondary","University","Prof-school"});
        treeEducation.add(new String[]{"Post-Secondary","University","Post-grad","Masters"});
        treeEducation.add(new String[]{"Post-Secondary","University","Post-grad","Doctorate"});

        // [4,11] marital-status: Married-civ-spouse, Divorced, Never-married, Separated, Widowed, Married-spouse-absent, Married-AF-spouse
        ArrayList<String[]> treeMarital = new ArrayList<>();
        treeMarital.add(new String[]{"married","has-spouse","Married-civ-spouse"});
        treeMarital.add(new String[]{"not-married","was-married","Divorced"});
        treeMarital.add(new String[]{"not-married","never-married","Never-married"});
        treeMarital.add(new String[]{"not-married","was-married","Separated"});
        treeMarital.add(new String[]{"married","alone","Widowed"});
        treeMarital.add(new String[]{"married","alone","Married-spouse-absent"});
        treeMarital.add(new String[]{"married","has-spouse","Married-AF-spouse"});

        // [3, 18] occupation: Tech-support, Craft-repair, Other-service, Sales, Exec-managerial, Prof-specialty, Handlers-cleaners, Machine-op-inspct, Adm-clerical, Farming-fishing, Transport-moving, Priv-house-serv, Protective-serv, Armed-Forces
        ArrayList<String[]> treeOccupation = new ArrayList<>();
        treeOccupation.add(new String[]{"White-collar","Exec-managerial"});
        treeOccupation.add(new String[]{"White-collar","Prof-specialty"});
        treeOccupation.add(new String[]{"White-collar","Sales"});
        treeOccupation.add(new String[]{"White-collar","Adm-clerical"});
        treeOccupation.add(new String[]{"Blue-collar","Tech-support"});
        treeOccupation.add(new String[]{"Blue-collar","Craft-repair"});
        treeOccupation.add(new String[]{"Blue-collar","Machine-op-inspct"});
        treeOccupation.add(new String[]{"Blue-collar","Handlers-cleaners"});
        treeOccupation.add(new String[]{"Blue-collar","Priv-house-serv"});
        treeOccupation.add(new String[]{"Blue-collar","Transport-moving"});
        treeOccupation.add(new String[]{"Other","Protective-serv"});
        treeOccupation.add(new String[]{"Other","Armed-Forces"});
        treeOccupation.add(new String[]{"Other","Farming-fishing"});
        treeOccupation.add(new String[]{"Other","Other-service"});

        // [3, 9] relationship: Wife, Own-child, Husband, Not-in-family, Other-relative, Unmarried
        ArrayList<String[]> treeRelationship = new ArrayList<>();
        treeRelationship.add(new String[]{"Family","Wife"});
        treeRelationship.add(new String[]{"Family","Own-child"});
        treeRelationship.add(new String[]{"Family","Husband"});
        treeRelationship.add(new String[]{"Alone","Not-in-family"});
        treeRelationship.add(new String[]{"With-others","Other-relative"});
        treeRelationship.add(new String[]{"With-others","Unmarried"});

        // [3, 8] race: White, Asian-Pac-Islander, Amer-Indian-Eskimo, Other, Black
        ArrayList<String[]> treeRace = new ArrayList<>();
        treeRace.add(new String[]{"Group1","White"});
        treeRace.add(new String[]{"Group2","Asian-Pac-Islander"});
        treeRace.add(new String[]{"Group1","Amer-Indian-Eskimo"});
        treeRace.add(new String[]{"Other"});
        treeRace.add(new String[]{"Group2","Black"});

        // [2, 3] sex: Female, Male
        ArrayList<String[]> treeSex = new ArrayList<>();
        treeSex.add(new String[]{"Female"});
        treeSex.add(new String[]{"Male"});

        // [2, 3] sex: >50K, <=50K
        ArrayList<String[]> treeIncome = new ArrayList<>();
        treeIncome.add(new String[]{">50K"});
        treeIncome.add(new String[]{"<=50K"});

        // [] nation: United-States, Cambodia, England, Puerto-Rico, Canada, Germany, Outlying-US(Guam-USVI-etc), India, Japan, Greece, South, China, Cuba, Iran, Honduras, Philippines, Italy, Poland, Jamaica, Vietnam, Mexico, Portugal, Ireland, France, Dominican-Republic, Laos, Ecuador, Taiwan, Haiti, Columbia, Hungary, Guatemala, Nicaragua, Scotland, Thailand, Yugoslavia, El-Salvador, Trinadad&Tobago, Peru, Hong, Holand-Netherlands
        ArrayList<String[]> treeNation = new ArrayList<>();
        treeNation.add(new String[]{"America", "North-America","United-States"});
        treeNation.add(new String[]{"America", "North-America","Canada"});

        treeNation.add(new String[]{"America", "Middle-America","Puerto-Rico"});
        treeNation.add(new String[]{"America", "Middle-America","Cuba"});
        treeNation.add(new String[]{"America", "Middle-America","Honduras"});
        treeNation.add(new String[]{"America", "Middle-America","Jamaica"});
        treeNation.add(new String[]{"America", "Middle-America","Mexico"});
        treeNation.add(new String[]{"America", "Middle-America","Dominican-Republic"});
        treeNation.add(new String[]{"America", "Middle-America","Haiti"});
        treeNation.add(new String[]{"America", "Middle-America","Guatemala"});
        treeNation.add(new String[]{"America", "Middle-America","Nicaragua"});
        treeNation.add(new String[]{"America", "Middle-America","El-Salvador"});
        treeNation.add(new String[]{"America", "Middle-America","Trinadad&Tobago"});

        treeNation.add(new String[]{"America", "South-America","Ecuador"});
        treeNation.add(new String[]{"America", "South-America","Columbia"});
        treeNation.add(new String[]{"America", "South-America","Peru"});

        treeNation.add(new String[]{"Asia", "South-East-Asia","Cambodia"});
        treeNation.add(new String[]{"Asia", "South-East-Asia","Philippines"});
        treeNation.add(new String[]{"Asia", "South-East-Asia","Vietnam"});
        treeNation.add(new String[]{"Asia", "South-East-Asia","Laos"});
        treeNation.add(new String[]{"Asia", "South-East-Asia","Thailand"});

        treeNation.add(new String[]{"Asia", "South-Asia","India"});

        treeNation.add(new String[]{"Asia", "East-Asia","Japan"});
        treeNation.add(new String[]{"Asia", "East-Asia","China"});
        treeNation.add(new String[]{"Asia", "East-Asia","Taiwan"});
        treeNation.add(new String[]{"Asia", "East-Asia","Hong"});
        treeNation.add(new String[]{"Asia", "East-Asia","South"});

        treeNation.add(new String[]{"Asia", "West-Asia","Iran"});

        treeNation.add(new String[]{"Europe", "South-Europe","Greece"});
        treeNation.add(new String[]{"Europe", "South-Europe","Italy"});
        treeNation.add(new String[]{"Europe", "South-Europe","Portugal"});

        treeNation.add(new String[]{"Europe", "West-Europe","England"});
        treeNation.add(new String[]{"Europe", "West-Europe","Germany"});
        treeNation.add(new String[]{"Europe", "West-Europe","Ireland"});
        treeNation.add(new String[]{"Europe", "West-Europe","France"});
        treeNation.add(new String[]{"Europe", "West-Europe","Scotland"});
        treeNation.add(new String[]{"Europe", "West-Europe","Holand-Netherlands"});

        treeNation.add(new String[]{"Europe", "East-Europe","Poland"});
        treeNation.add(new String[]{"Europe", "East-Europe","Hungary"});
        treeNation.add(new String[]{"Europe", "East-Europe","Yugoslavia"});

        treeNation.add(new String[]{"Outlying-US(Guam-USVI-etc)"});

        ArrayList<CastleRule> rulesCombi1 = new ArrayList<>();
        rulesCombi1.add(new CastleRule(0, new NoneGeneralizer(), false));
        rulesCombi1.add(new CastleRule(1, new AggregationFloatGeneralizer(Tuple2.of(17f, 90f)), false)); // age
        rulesCombi1.add(new CastleRule(2, new NoneGeneralizer(), false));
        rulesCombi1.add(new CastleRule(3, new NoneGeneralizer(), true)); // workclass
        rulesCombi1.add(new CastleRule(4, new AggregationFloatGeneralizer(Tuple2.of(13492f, 1490400f)), false, 0.05)); // final weight
        rulesCombi1.add(new CastleRule(5, new NonNumericalGeneralizer(treeEducation.toArray(new String[][]{})),false));
        rulesCombi1.add(new CastleRule(6, new NonNumericalGeneralizer(treeMarital.toArray(new String[][]{})), false));
        rulesCombi1.add(new CastleRule(7, new NonNumericalGeneralizer(treeOccupation.toArray(new String[][]{})), false));
        rulesCombi1.add(new CastleRule(8, new NoneGeneralizer(), true)); // relationship
        rulesCombi1.add(new CastleRule(9, new NoneGeneralizer(), false)); // race
        rulesCombi1.add(new CastleRule(10, new NoneGeneralizer(), false)); // sex
        rulesCombi1.add(new CastleRule(11, new AggregationFloatGeneralizer(Tuple2.of(0f, 99999f)), false)); // capital-gain
        rulesCombi1.add(new CastleRule(12, new AggregationFloatGeneralizer(Tuple2.of(0f, 4356f)), false)); // capital-loss
        rulesCombi1.add(new CastleRule(13, new AggregationFloatGeneralizer(Tuple2.of(1f, 99f)), false)); // hours per week
        rulesCombi1.add(new CastleRule(14, new NonNumericalGeneralizer(treeNation.toArray(new String[][]{})), false)); // nation
//        rulesCombi1.add(new CastleRule(14, CastleFunction.Generalization.NONE, false)); // nation

        HashMap<String,ArrayList<CastleRule>> ruleSets = new HashMap<>();

//        ruleSets.put("RuleSet8",rulesSet8);
//        ruleSets.put("CASTLE-Set", rules);
//        ruleSets.put("Sex", rulesDGH1);
//        ruleSets.put("Martial", rulesDGH2);
//        ruleSets.put("Occupation", rulesDGH3);
//        ruleSets.put("Education", rulesDGH4);
//        ruleSets.put("Nation", rulesDGH5);
//        ruleSets.put("0_Aggregation_1", rulesAggre1);
//        ruleSets.put("1_Non-Numerical_1", rulesNonNum1);
        ruleSets.put("0_Eval_Rules", rulesCombi1);

        ArrayList<Arguments> arguments = new ArrayList<>();

        for(int k: kValues){
            for(int l: lValues) {
                for (int delta : deltaValues) {
                    for (int beta : betaValues) {
                        for (int zeta : zetaValues) {
                            for (int mu : muValues) {
                                for (String desc : ruleSets.keySet()) {
                                    arguments.add(Arguments.of(k, l, delta, beta, zeta, mu, ruleSets.get(desc), ("Rule="+desc)));
                                }
                            }
                        }
                    }
                }
            }
        }
        return arguments.stream();
    }

    @Disabled("Evaluation run that needs the Adult data set at a local path; not part of the regular test suite")
    @ParameterizedTest
    @MethodSource("provideAgeEvalParameters")
    public void prinkAgeDataSetEvaluation(int k, int l, int delta, int beta, int zeta, int mu, ArrayList<CastleRule> rules, String desc) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // configure your test environment
        env.setParallelism(1);

        // values are collected in a static variable
        PerformanceSink.values.clear();
        PerformanceSink.durations.clear();
        PerformanceSink.infoLoss.clear();
        PerformanceSink.output.clear();

        final FileSource<String> source = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("C:\\Users\\Groneberg\\Desktop\\Studium\\AAA_Master\\5_Masterarbeit\\DataSets\\AdultDataSet\\adult_big_clean.csv")).build();

        // start the data generator and arrange for watermarking
        DataStream<Tuple15<Object,Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object>> fares = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "file-source").map(new StringToTuple());

        // Prink setup
        MapStateDescriptor<Integer, CastleRule> ruleStateDescriptor =
                new MapStateDescriptor<>(
                        "RulesBroadcastState",
                        BasicTypeInfo.INT_TYPE_INFO,
                        TypeInformation.of(new TypeHint<CastleRule>(){}));


        DataStream<CastleRule> ruleStream = env.fromCollection(rules);
        BroadcastStream<CastleRule> ruleBroadcastStream = ruleStream
                .broadcast(ruleStateDescriptor);

        // create a stream of custom elements and apply transformations
        DataStream<Tuple> privateFares = fares
                .keyBy(tuple -> tuple.getField(0))
                .connect(ruleBroadcastStream)
                .process(new CastleFunction(0, k, l, delta, beta, zeta, mu, true, 1))
                .returns(TypeInformation.of(new TypeHint<Tuple16<Object,Object,Object,Object,Object,Object,Object,Object,Object,Object,Object,Object,Object,Object,Object,Object>>(){}));


        privateFares.addSink(new PerformanceSink("Rules=" + desc +";k=" + k + ";l=" + l + ";delta=" + delta + ";beta=" + beta + ";zeta=" + zeta + ";mu=" + mu));
//        privateFares.addSink(new PerformanceSink(desc));

        // execute
        env.execute();

        // verify your results
        StringBuilder sb = new StringBuilder();

        OptionalDouble averageInfoLoss = PerformanceSink.infoLoss
                .stream()
                .mapToDouble(a -> a)
                .average();

        OptionalDouble averageDuration = PerformanceSink.durations
                .stream()
                .mapToDouble(a -> a)
                .average();

        String timestamp = new SimpleDateFormat("dd-MM-HH-mm-ss").format(new Date());
        String folderName = new SimpleDateFormat("dd.MM.yy").format(new Date());

//        // To test with file result assign fitting path
//        String folderPath = "C:\\Users\\Groneberg\\Desktop\\Studium\\AAA_Master\\5_Masterarbeit\\Evaluation\\";
//        Files.createDirectories(Paths.get(folderPath + "\\" + folderName));
//        Files.createDirectories(Paths.get(folderPath + "\\" + folderName + "\\details"));
//        new File(folderPath + "\\" + folderName + "\\test_eval.txt").createNewFile();
//
//        sb.append(timestamp)
//                .append(";AvgIL=").append(averageInfoLoss.getAsDouble())
//                .append("\t;AvgDur=").append(averageDuration.getAsDouble())
//                .append("\t;parameters{k=").append(k).append("\t;l=").append(l).append("\t;delta=").append(delta).append("\t;beta=").append(beta).append("\t;zeta=").append(zeta).append("\t;mu=").append(mu)
//                .append("\t;valueSize:").append(PerformanceSink.values.size())
//                .append("\t;rules=").append(rules).append(System.lineSeparator());
//
//        try {
//            Files.write(Paths.get(folderPath + folderName + "\\test_eval.txt"), sb.toString().getBytes(), StandardOpenOption.APPEND);
//
////            sb.append("InfoLoss=").append(PerformanceSink.infoLoss.toString()).append(System.lineSeparator());
////            sb.append("Durations=").append(PerformanceSink.durations.toString()).append(System.lineSeparator());
////            Files.write(Paths.get(folderPath + folderName + "\\details\\" + timestamp +".txt"), sb.toString().getBytes(), StandardOpenOption.CREATE);
//
//            StringBuilder sbOutput = new StringBuilder();
//            for(String s: PerformanceSink.output){
//                sbOutput.append(s);
//            }
//            Files.write(Paths.get(folderPath + folderName + "\\details\\" + timestamp +".txt"), sbOutput.toString().getBytes(), StandardOpenOption.CREATE);
//        }catch (IOException e) {
//            System.out.println("ERROR: while writing file e:" + e);
//            //exception handling left as an exercise for the reader
//        }
        assertTrue(PerformanceSink.values.size() >= (inputData.size()-100));
    }

    // create a testing sink
    private static class PerformanceSink implements SinkFunction<Tuple> {

        // must be static
        public static final List<Tuple> values = Collections.synchronizedList(new ArrayList<>());
        public static final List<Long> durations = Collections.synchronizedList(new ArrayList<>());
        public static final List<Float> infoLoss = Collections.synchronizedList(new ArrayList<>());
        public static final List<String> output = Collections.synchronizedList(new ArrayList<>());
        private final String description;

        public PerformanceSink(String desc){
            super();
            this.description = desc;
        }

        @Override
        public void invoke(Tuple input, SinkFunction.Context context) throws Exception {

            long startTime = input.getField(2) != null ? (long) input.getField(2) : 0L;

            values.add(input);
            durations.add(context.currentProcessingTime() - startTime);
            infoLoss.add((float) input.getField(input.getArity()-1));
            output.add(input.getField(0) + //";" +  input +
                    ";" + description +
                    ";" + (context.currentProcessingTime() - startTime) +
                    ";" + (input.getField(input.getArity()-1)) + "\n");

            System.out.println(input.getField(0) + ";" +  input +
                            ";" + description +
                            ";" + (context.currentProcessingTime() - startTime) +
                            ";" + (input.getField(input.getArity()-1)));

        }
    }

    public static class StringToTuple implements MapFunction<String, Tuple15<Object,Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object>> {

        long counter = 0;

        @Override
        public Tuple15<Object, Object,Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object> map(String input) {
            String[] splitValues = input.split(", ");
            counter++;
            if(splitValues.length >= 15) {
                // remove 'fnlwgt' and add 'id' for user identification (expect every entry to be individual)
                return Tuple15.of(counter,
                        Float.parseFloat(splitValues[0]),
                        null,
                        splitValues[1],
                        Float.parseFloat(splitValues[2]),
                        splitValues[3],
//                        splitValues[4],
                        splitValues[5],
                        splitValues[6],
                        splitValues[7],
                        splitValues[8],
                        splitValues[9],
                        Float.parseFloat(splitValues[10]),
                        Float.parseFloat(splitValues[11]),
                        Float.parseFloat(splitValues[12]),
                        splitValues[13]);
//                        splitValues[14]);
            }else{
                return new Tuple15<>(counter, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
            }
        }
    }

}