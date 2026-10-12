package ganges.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;

/**
 * Loads, resolves and validates {@link AnonymizationProfile}s.
 *
 * <p>Profile selection (first match wins):</p>
 * <ol>
 *     <li>{@code --config <file>}: YAML or JSON file on the machine that submits the job</li>
 *     <li>{@code --profile <name>}: profile bundled in the jar ({@code profiles/<name>.yaml})</li>
 *     <li>environment variable {@code PRINK_CONFIG} (file) or {@code PRINK_PROFILE} (bundled name)</li>
 *     <li>the bundled {@value #DEFAULT_PROFILE} profile (the original thesis setup)</li>
 * </ol>
 * <p>Command line arguments override the privacy and Kafka settings of the profile:
 * {@code --k --l --delta --beta --zeta --mu --suppressStrategy --showInfoLoss --bootstrapServers --inputTopic
 * --outputTopic --groupId}.</p>
 */
public final class ProfileLoader {

    public static final String DEFAULT_PROFILE = "thesis-default";
    public static final String ENV_CONFIG = "PRINK_CONFIG";
    public static final String ENV_PROFILE = "PRINK_PROFILE";
    private static final String CLASSPATH_PREFIX = "classpath:";

    private static final Logger LOG = LoggerFactory.getLogger(ProfileLoader.class);

    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory())
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private ProfileLoader() {
    }

    /** Loads the profile selected by the job parameters / environment and applies command line overrides. */
    public static AnonymizationProfile load(ParameterTool params) throws IOException {
        return load(params, System::getenv);
    }

    public static AnonymizationProfile load(ParameterTool params, Function<String, String> env) throws IOException {
        AnonymizationProfile profile;
        String config = params.get("config", null);
        String profileName = params.get("profile", null);
        if (config != null) {
            profile = loadFile(config, env);
        } else if (profileName != null) {
            profile = loadBundled(profileName, env);
        } else if (env.apply(ENV_CONFIG) != null && !env.apply(ENV_CONFIG).isEmpty()) {
            profile = loadFile(env.apply(ENV_CONFIG), env);
        } else if (env.apply(ENV_PROFILE) != null && !env.apply(ENV_PROFILE).isEmpty()) {
            profile = loadBundled(env.apply(ENV_PROFILE), env);
        } else {
            profile = loadBundled(DEFAULT_PROFILE, env);
        }
        applyOverrides(profile, params);
        validate(profile);
        LOG.info("Using anonymization profile '{}': {}", profile.name, profile.fields);
        return profile;
    }

    /** Loads a profile bundled in the jar under {@code profiles/<name>.yaml}. */
    public static AnonymizationProfile loadBundled(String name, Function<String, String> env) throws IOException {
        String resource = name.endsWith(".yaml") || name.endsWith(".yml") || name.endsWith(".json")
                ? "profiles/" + name : "profiles/" + name + ".yaml";
        return loadFile(CLASSPATH_PREFIX + resource, env);
    }

    /**
     * Loads a profile from a file path or a {@code classpath:} resource, resolves hierarchy files and mask salts.
     * The returned profile is not validated and has no overrides applied; see {@link #load(ParameterTool)}.
     */
    public static AnonymizationProfile loadFile(String location, Function<String, String> env) throws IOException {
        Path baseDir = null;
        AnonymizationProfile profile;
        if (location.startsWith(CLASSPATH_PREFIX)) {
            String resource = location.substring(CLASSPATH_PREFIX.length());
            try (InputStream in = openResource(resource)) {
                if (in == null) throw new IOException("Profile resource not found: " + resource);
                profile = YAML.readValue(in, AnonymizationProfile.class);
            }
        } else {
            Path path = Paths.get(location);
            if (!Files.isRegularFile(path)) throw new IOException("Profile file not found: " + path.toAbsolutePath());
            baseDir = path.toAbsolutePath().getParent();
            // YAML is a superset of JSON, so the YAML parser reads both formats
            profile = YAML.readValue(path.toFile(), AnonymizationProfile.class);
        }
        resolve(profile, baseDir, env);
        return profile;
    }

    /** Parses a profile from a YAML/JSON string (hierarchy files are resolved against the working dir / classpath). */
    public static AnonymizationProfile parse(String yamlOrJson, Function<String, String> env) throws IOException {
        AnonymizationProfile profile = YAML.readValue(yamlOrJson, AnonymizationProfile.class);
        resolve(profile, null, env);
        return profile;
    }

    private static InputStream openResource(String resource) {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream in = cl == null ? null : cl.getResourceAsStream(resource);
        if (in == null) in = ProfileLoader.class.getClassLoader().getResourceAsStream(resource);
        return in;
    }

    // ------------------------------------------------------------------------------------------

    private static void resolve(AnonymizationProfile profile, Path baseDir, Function<String, String> env) throws IOException {
        if (profile.privacy == null) profile.privacy = new AnonymizationProfile.PrivacySettings();
        if (profile.kafka == null) profile.kafka = new AnonymizationProfile.KafkaSettings();
        if (profile.fields == null) profile.fields = new ArrayList<>();
        for (FieldSpec field : profile.fields) {
            if (field.type == null) field.type = FieldSpec.FieldType.STRING;
            if (field.hierarchy != null) {
                if (field.hierarchy.paths == null) field.hierarchy.paths = new ArrayList<>();
                if (field.hierarchy.file != null) {
                    field.hierarchy.paths.addAll(readHierarchy(field.hierarchy.file, baseDir));
                    // the hierarchy is now inline; the job does not need file access on the task managers
                    field.hierarchy.file = null;
                }
            }
            if (field.role == FieldSpec.Role.MASK) {
                if (field.mask == null) field.mask = new FieldSpec.MaskSpec();
                if (field.mask.saltEnv != null) {
                    String salt = env.apply(field.mask.saltEnv);
                    if (salt == null) {
                        LOG.warn("Environment variable {} (salt for masked field '{}') is not set; hashing without salt", field.mask.saltEnv, field.name);
                    } else {
                        field.mask.salt = salt;
                    }
                }
            }
        }
    }

    /**
     * Reads a hierarchy file. Lookup order: path as given (absolute or relative to the working directory),
     * relative to the profile file, classpath resource.
     * YAML/JSON files contain either a list of paths or an object with a {@code paths} list;
     * other files are read as CSV with one path per line (separator ';' or ',', '#' starts a comment).
     */
    static List<List<String>> readHierarchy(String file, Path baseDir) throws IOException {
        String content = null;
        Path direct = Paths.get(file);
        if (Files.isRegularFile(direct)) {
            content = new String(Files.readAllBytes(direct), StandardCharsets.UTF_8);
        } else if (baseDir != null && Files.isRegularFile(baseDir.resolve(file))) {
            content = new String(Files.readAllBytes(baseDir.resolve(file)), StandardCharsets.UTF_8);
        } else {
            String resource = file.startsWith(CLASSPATH_PREFIX) ? file.substring(CLASSPATH_PREFIX.length()) : file;
            try (InputStream in = openResource(resource)) {
                if (in != null) content = readAll(in);
            }
        }
        if (content == null) throw new IOException("Hierarchy file not found: " + file);

        String lower = file.toLowerCase(Locale.ROOT);
        List<List<String>> paths = new ArrayList<>();
        if (lower.endsWith(".yaml") || lower.endsWith(".yml") || lower.endsWith(".json")) {
            JsonNode root = YAML.readTree(content);
            JsonNode list = (root != null && root.isObject()) ? root.get("paths") : root;
            if (list == null || !list.isArray()) throw new IOException("Hierarchy file " + file + " must contain a list of paths (or an object with 'paths')");
            for (JsonNode pathNode : list) {
                List<String> path = new ArrayList<>();
                if (pathNode.isArray()) {
                    for (JsonNode element : pathNode) path.add(element.asText());
                } else {
                    path.add(pathNode.asText());
                }
                paths.add(path);
            }
        } else {
            for (String line : content.split("\\r?\\n")) {
                String trimmed = line.trim();
                if (trimmed.isEmpty() || trimmed.startsWith("#")) continue;
                String separator = trimmed.contains(";") ? ";" : ",";
                List<String> path = new ArrayList<>();
                for (String element : trimmed.split(separator)) {
                    if (!element.trim().isEmpty()) path.add(element.trim());
                }
                paths.add(path);
            }
        }
        return paths;
    }

    private static String readAll(InputStream in) throws IOException {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        byte[] buffer = new byte[8192];
        int read;
        while ((read = in.read(buffer)) != -1) out.write(buffer, 0, read);
        return new String(out.toByteArray(), StandardCharsets.UTF_8);
    }

    static void applyOverrides(AnonymizationProfile profile, ParameterTool params) {
        AnonymizationProfile.PrivacySettings p = profile.privacy;
        p.k = params.getInt("k", p.k);
        p.l = params.getInt("l", p.l);
        p.delta = params.getInt("delta", p.delta);
        p.beta = params.getInt("beta", p.beta);
        p.zeta = params.getInt("zeta", p.zeta);
        p.mu = params.getInt("mu", p.mu);
        p.suppressStrategy = params.getInt("suppressStrategy", p.suppressStrategy);
        p.showInfoLoss = params.getBoolean("showInfoLoss", p.showInfoLoss);
        AnonymizationProfile.KafkaSettings kafka = profile.kafka;
        kafka.bootstrapServers = params.get("bootstrapServers", kafka.bootstrapServers);
        kafka.inputTopic = params.get("inputTopic", kafka.inputTopic);
        kafka.outputTopic = params.get("outputTopic", kafka.outputTopic);
        kafka.groupId = params.get("groupId", kafka.groupId);
    }

    /**
     * Validates a profile and throws an {@link IllegalArgumentException} listing all problems.
     */
    public static void validate(AnonymizationProfile profile) {
        List<String> errors = new ArrayList<>();
        AnonymizationProfile.PrivacySettings p = profile.privacy;
        if (p.k < 1) errors.add("privacy.k must be >= 1");
        if (p.l < 0) errors.add("privacy.l must be >= 0");
        if (p.delta < 1) errors.add("privacy.delta must be >= 1");
        if (p.beta < 1) errors.add("privacy.beta must be >= 1");
        if (p.zeta < 0) errors.add("privacy.zeta must be >= 0");
        if (p.mu < 1) errors.add("privacy.mu must be >= 1");
        if (p.suppressStrategy < 0 || p.suppressStrategy > 2) errors.add("privacy.suppressStrategy must be 0, 1 or 2");
        if (isBlank(profile.kafka.bootstrapServers)) errors.add("kafka.bootstrapServers is required");
        if (isBlank(profile.kafka.inputTopic)) errors.add("kafka.inputTopic is required");
        if (isBlank(profile.kafka.outputTopic)) errors.add("kafka.outputTopic is required");
        if (profile.kafka.inputTopic != null && profile.kafka.inputTopic.equals(profile.kafka.outputTopic)) {
            errors.add("kafka.outputTopic must differ from kafka.inputTopic");
        }

        Set<String> names = new HashSet<>();
        FieldSpec keyField = null;
        int sensitive = 0;
        for (FieldSpec field : profile.fields) {
            String prefix = "field '" + field.name + "': ";
            if (isBlank(field.name)) {
                errors.add("every field needs a name");
                continue;
            }
            if (!names.add(field.name)) errors.add(prefix + "defined more than once");
            if ("infoloss".equals(field.name) && profile.privacy.showInfoLoss) errors.add(prefix + "name is reserved for the information loss");
            if (field.role == null) {
                errors.add(prefix + "role is required (identifier-passthrough | drop | mask | quasi-identifier | sensitive)");
                continue;
            }
            if (field.name.equals(profile.dataSubjectKey)) keyField = field;
            if (field.role == FieldSpec.Role.SENSITIVE) sensitive++;

            FieldSpec.GeneralizerType generalizer = field.generalizer;
            if (!field.isPrinkAttribute()) {
                if (generalizer != null && generalizer != FieldSpec.GeneralizerType.NONE) {
                    errors.add(prefix + "role " + field.role + " cannot have a generalizer (only quasi-identifier / sensitive)");
                }
            } else if (field.role == FieldSpec.Role.QUASI_IDENTIFIER && (generalizer == null || generalizer == FieldSpec.GeneralizerType.NONE)) {
                errors.add(prefix + "a quasi-identifier needs a generalizer (aggregation | reduction | hierarchy); use role identifier-passthrough to release it unchanged");
            }
            if (field.effectiveGeneralizer() == FieldSpec.GeneralizerType.AGGREGATION) {
                if (field.type != FieldSpec.FieldType.INT && field.type != FieldSpec.FieldType.FLOAT && field.type != FieldSpec.FieldType.DOUBLE) {
                    errors.add(prefix + "aggregation needs type int, float or double");
                }
                if (field.bounds == null || field.bounds.size() != 2 || field.bounds.get(0) == null || field.bounds.get(1) == null) {
                    errors.add(prefix + "aggregation needs bounds: [min, max]");
                } else if (field.bounds.get(0) >= field.bounds.get(1)) {
                    errors.add(prefix + "bounds must satisfy min < max");
                }
            } else if (field.bounds != null) {
                errors.add(prefix + "bounds are only used by the aggregation generalizer");
            }
            if (field.effectiveGeneralizer() == FieldSpec.GeneralizerType.HIERARCHY && field.type != FieldSpec.FieldType.STRING) {
                errors.add(prefix + "hierarchy needs type string");
            }
            if (field.hierarchy != null && field.effectiveGeneralizer() != FieldSpec.GeneralizerType.HIERARCHY) {
                errors.add(prefix + "hierarchy is only used by the hierarchy generalizer");
            }
            if (field.role == FieldSpec.Role.MASK) {
                String strategy = field.mask == null ? "hash" : field.mask.strategy;
                if (!"hash".equals(strategy) && !"constant".equals(strategy)) errors.add(prefix + "mask.strategy must be hash or constant");
                if (field.name.equals(profile.dataSubjectKey) && "constant".equals(strategy)) {
                    errors.add(prefix + "the data subject key cannot be masked with a constant (all records would belong to one data subject)");
                }
            } else if (field.mask != null) {
                errors.add(prefix + "mask options are only used with role mask");
            }
            if (field.weight != null && field.weight < 0) errors.add(prefix + "weight must be >= 0");
        }

        if (keyField == null) {
            errors.add("dataSubjectKey '" + profile.dataSubjectKey + "' must be one of the fields");
        } else if (keyField.isPrinkAttribute()) {
            errors.add("dataSubjectKey '" + profile.dataSubjectKey + "' must have role identifier-passthrough, mask (hash) or drop");
        }
        if (p.l > 0 && sensitive == 0) {
            errors.add("privacy.l > 0 needs at least one field with role sensitive (or set l: 0)");
        }
        if (errors.isEmpty()) {
            int arity = profile.outputArity();
            if (arity > AnonymizationProfile.MAX_TUPLE_ARITY) {
                errors.add("too many fields: " + profile.inputArity() + " tuple fields" + (p.showInfoLoss ? " + infoloss" : "")
                        + " exceed Flink's maximum tuple arity of " + AnonymizationProfile.MAX_TUPLE_ARITY + " (drop unused fields)");
            }
        }
        if (!errors.isEmpty()) {
            throw new IllegalArgumentException("Invalid anonymization profile '" + profile.name + "':\n - " + String.join("\n - ", errors));
        }
    }

    private static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }
}
