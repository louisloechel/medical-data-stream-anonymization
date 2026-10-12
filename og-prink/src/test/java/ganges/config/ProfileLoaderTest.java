package ganges.config;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import prink.datatypes.CastleRule;
import prink.generalizations.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

class ProfileLoaderTest {

    private static final Function<String, String> NO_ENV = name -> null;

    private static AnonymizationProfile load(String... args) throws IOException {
        return ProfileLoader.load(ParameterTool.fromArgs(args), NO_ENV);
    }

    private static String minimalProfile(String fields) {
        return "name: test\n"
                + "privacy: {k: 2, l: 1}\n"
                + "fields:\n"
                + "  - {name: userid, type: string, role: identifier-passthrough}\n"
                + "  - {name: pulse, type: int, role: sensitive, generalizer: aggregation, bounds: [20, 220]}\n"
                + fields;
    }

    private static String validationError(String yaml) throws IOException {
        AnonymizationProfile profile = ProfileLoader.parse(yaml, NO_ENV);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> ProfileLoader.validate(profile));
        return e.getMessage();
    }

    @ParameterizedTest
    @ValueSource(strings = {"thesis-default", "vitals-sensitive", "news2-only", "qi-demographics"})
    void bundledProfilesLoadAndValidate(String name) throws IOException {
        AnonymizationProfile profile = load("--profile", name);
        assertEquals(name, profile.name);
        assertEquals("userid", profile.dataSubjectKey);
        assertTrue(profile.outputArity() <= AnonymizationProfile.MAX_TUPLE_ARITY);
        assertEquals(profile.inputArity(), profile.buildRules().size());
        // every bundled profile writes to its own topic
        assertTrue(profile.kafka.outputTopic.startsWith("prink-"));
    }

    @Test
    void withoutConfigTheThesisDefaultIsUsed() throws IOException {
        AnonymizationProfile profile = load();
        assertEquals("thesis-default", profile.name);
        assertEquals(5, profile.privacy.k);
        assertEquals(3, profile.privacy.l);
        assertEquals(125, profile.privacy.delta);
        assertEquals(50000, profile.privacy.beta);
        assertEquals(10000, profile.privacy.zeta);
        assertEquals(100, profile.privacy.mu);
        assertEquals(2, profile.privacy.suppressStrategy);
        assertTrue(profile.privacy.showInfoLoss);
        assertEquals("kafka:29092", profile.kafka.bootstrapServers);
        assertEquals("processed-topic", profile.kafka.inputTopic);
        assertEquals("prink-topic", profile.kafka.outputTopic);
        assertEquals("flink-group", profile.kafka.groupId);
    }

    /** The thesis-default profile reproduces the former hard-coded DatasetFields enum (after the temp/key fixes). */
    @Test
    void thesisDefaultMatchesFormerHardCodedRules() throws IOException {
        AnonymizationProfile profile = load();
        List<CastleRule> rules = profile.buildRules();

        String[] names = {"recordid", "userid", "username", "resp", "respNEWS", "bps", "bpsNEWS", "pulse",
                "pulseNEWS", "temp", "tempNEWS", "icd10", "timestamp", "infoloss"};
        assertArrayEquals(names, profile.outputNames());
        assertEquals(13, rules.size());
        assertEquals(1, profile.keyPosition(), "Prink is keyed on the patient (userid)");

        Class<?>[] generalizers = {NoneGeneralizer.class, NoneGeneralizer.class, NoneGeneralizer.class,
                AggregationIntegerGeneralizer.class, AggregationIntegerGeneralizer.class,
                AggregationIntegerGeneralizer.class, AggregationIntegerGeneralizer.class,
                AggregationIntegerGeneralizer.class, AggregationIntegerGeneralizer.class,
                AggregationDoubleGeneralizer.class, AggregationIntegerGeneralizer.class,
                NonNumericalGeneralizer.class, NoneGeneralizer.class};
        boolean[] sensitive = {false, false, false, true, true, true, true, true, true, true, true, true, false};
        for (int i = 0; i < rules.size(); i++) {
            assertEquals(i, rules.get(i).getPosition());
            assertEquals(generalizers[i], rules.get(i).getGeneralizer().getClass(), "generalizer of " + names[i]);
            assertEquals(sensitive[i], rules.get(i).getIsSensibleAttribute(), "sensitivity of " + names[i]);
        }
        // the ICD-10 hierarchy was moved verbatim into hierarchies/icd10_chapter1.yaml
        List<List<String>> icd = profile.fields.get(11).hierarchy.paths;
        assertEquals(86, icd.size());
        assertEquals(Collections.singletonList("ICD-10"), icd.get(0));
        assertEquals(Arrays.asList("ICD-10", "A00–B99", "A20–A28", "A28", "A28.9"), icd.get(icd.size() - 1));
    }

    @Test
    void commandLineOverridesProfile() throws IOException {
        AnonymizationProfile profile = load("--profile", "vitals-sensitive", "--k", "10", "--l", "2", "--delta", "500",
                "--outputTopic", "other-topic", "--bootstrapServers", "localhost:9092", "--showInfoLoss", "false");
        assertEquals(10, profile.privacy.k);
        assertEquals(2, profile.privacy.l);
        assertEquals(500, profile.privacy.delta);
        assertEquals("other-topic", profile.kafka.outputTopic);
        assertEquals("localhost:9092", profile.kafka.bootstrapServers);
        assertFalse(profile.privacy.showInfoLoss);
        assertEquals(profile.inputArity(), profile.outputArity());
    }

    @Test
    void environmentSelectsProfile(@TempDir Path dir) throws IOException {
        Map<String, String> env = new HashMap<>();
        env.put(ProfileLoader.ENV_PROFILE, "news2-only");
        assertEquals("news2-only", ProfileLoader.load(ParameterTool.fromArgs(new String[0]), env::get).name);

        Path file = dir.resolve("custom.yaml");
        Files.write(file, minimalProfile("").getBytes(StandardCharsets.UTF_8));
        env.put(ProfileLoader.ENV_CONFIG, file.toString());
        assertEquals("test", ProfileLoader.load(ParameterTool.fromArgs(new String[0]), env::get).name);
        // command line arguments win over the environment
        assertEquals("thesis-default", ProfileLoader.load(ParameterTool.fromArgs(new String[]{"--profile", "thesis-default"}), env::get).name);
    }

    @Test
    void configFileWithJsonSyntaxAndCsvHierarchyRelativeToProfile(@TempDir Path dir) throws IOException {
        Files.write(dir.resolve("wards.csv"), ("# ward hierarchy\n"
                + "Medicine;Cardiology;W1\nMedicine;Cardiology;W2\nSurgery;W3\n").getBytes(StandardCharsets.UTF_8));
        String json = "{\"name\": \"json\", \"privacy\": {\"k\": 3, \"l\": 0},"
                + " \"fields\": ["
                + "  {\"name\": \"userid\", \"role\": \"identifier-passthrough\"},"
                + "  {\"name\": \"ward\", \"role\": \"quasi-identifier\", \"generalizer\": \"hierarchy\", \"hierarchy\": {\"file\": \"wards.csv\"}}"
                + "]}";
        Path file = dir.resolve("profile.json");
        Files.write(file, json.getBytes(StandardCharsets.UTF_8));

        AnonymizationProfile profile = load("--config", file.toString());
        assertEquals(Arrays.asList(Arrays.asList("Medicine", "Cardiology", "W1"), Arrays.asList("Medicine", "Cardiology", "W2"),
                Arrays.asList("Surgery", "W3")), profile.fields.get(1).hierarchy.paths);
        assertNull(profile.fields.get(1).hierarchy.file, "hierarchy is inlined at submission time");
        assertTrue(profile.buildRules().get(1).getGeneralizer() instanceof NonNumericalGeneralizer);
    }

    @Test
    void roleAndTypeAliases() throws IOException {
        AnonymizationProfile profile = ProfileLoader.parse(minimalProfile(
                "  - {name: a, type: integer, role: QI, generalizer: range, bounds: [0, 1]}\n"
                        + "  - {name: b, type: str, role: passthrough}\n"
                        + "  - {name: c, type: double, role: Quasi-Identifier, generalizer: aggregation, bounds: [0.5, 1.5]}\n"), NO_ENV);
        ProfileLoader.validate(profile);
        assertEquals(FieldSpec.FieldType.INT, profile.fields.get(2).type);
        assertEquals(FieldSpec.Role.QUASI_IDENTIFIER, profile.fields.get(2).role);
        assertEquals(FieldSpec.GeneralizerType.AGGREGATION, profile.fields.get(2).generalizer);
        assertEquals(FieldSpec.Role.IDENTIFIER_PASSTHROUGH, profile.fields.get(3).role);
        AggregationDoubleGeneralizer generalizer = (AggregationDoubleGeneralizer) profile.buildRules().get(4).getGeneralizer();
        assertEquals(Tuple2.of(Tuple2.of(0.5, 1.5), 1.0f), generalizer.generalizeMax());
    }

    @Test
    void maskSaltIsReadFromEnvironment() throws IOException {
        AnonymizationProfile profile = ProfileLoader.parse(minimalProfile(
                "  - {name: caseid, role: mask, mask: {strategy: hash, saltEnv: MY_SALT}}\n"), name -> "MY_SALT".equals(name) ? "s3cr3t" : null);
        assertEquals("s3cr3t", profile.fields.get(2).mask.salt);
    }

    @Test
    void unknownPropertiesAreRejected() {
        assertThrows(IOException.class, () -> ProfileLoader.parse(minimalProfile("") + "privcy: {k: 3}\n", NO_ENV));
        assertThrows(IOException.class, () -> ProfileLoader.parse(minimalProfile("  - {name: x, role: sensitiv}\n"), NO_ENV));
    }

    @Test
    void validationReportsConfigurationErrors() throws IOException {
        assertTrue(validationError(minimalProfile("  - {name: ward, role: quasi-identifier}\n"))
                .contains("a quasi-identifier needs a generalizer"));
        assertTrue(validationError(minimalProfile("  - {name: ward, type: string, role: quasi-identifier, generalizer: aggregation, bounds: [0, 1]}\n"))
                .contains("aggregation needs type int, float or double"));
        assertTrue(validationError(minimalProfile("  - {name: x, type: int, role: sensitive, generalizer: aggregation}\n"))
                .contains("aggregation needs bounds"));
        assertTrue(validationError(minimalProfile("  - {name: x, type: int, role: sensitive, generalizer: aggregation, bounds: [5, 1]}\n"))
                .contains("min < max"));
        assertTrue(validationError(minimalProfile("  - {name: x, type: int, role: sensitive, generalizer: hierarchy}\n"))
                .contains("hierarchy needs type string"));
        assertTrue(validationError(minimalProfile("  - {name: username, role: drop, generalizer: reduction}\n"))
                .contains("cannot have a generalizer"));
        assertTrue(validationError(minimalProfile("  - {name: pulse, role: drop}\n"))
                .contains("defined more than once"));
        assertTrue(validationError(minimalProfile("") + "dataSubjectKey: patient\n")
                .contains("dataSubjectKey 'patient' must be one of the fields"));
        assertTrue(validationError("name: t\nprivacy: {l: 2}\nfields:\n  - {name: userid, role: identifier-passthrough}\n")
                .contains("needs at least one field with role sensitive"));
        assertTrue(validationError("name: t\nprivacy: {l: 0}\nfields:\n  - {name: userid, role: mask, mask: {strategy: constant}}\n")
                .contains("cannot be masked with a constant"));
        assertTrue(validationError("name: t\nprivacy: {l: 0}\nfields:\n  - {name: userid, type: string, role: quasi-identifier, generalizer: reduction}\n")
                .contains("must have role identifier-passthrough, mask (hash) or drop"));
        assertTrue(validationError(minimalProfile("") + "kafka: {outputTopic: processed-topic}\n")
                .contains("must differ"));

        StringBuilder many = new StringBuilder();
        for (int i = 0; i < 23; i++) many.append("  - {name: f").append(i).append(", role: identifier-passthrough}\n");
        assertTrue(validationError(minimalProfile(many.toString())).contains("maximum tuple arity of 25"));
        // dropped fields do not count towards the tuple arity
        ProfileLoader.validate(ProfileLoader.parse(minimalProfile(many.toString().replace("identifier-passthrough", "drop")), NO_ENV));
    }
}
