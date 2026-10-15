package streamanon.anonymization.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Definition of one field of the input JSON records and how it is treated by the anonymization job.
 *
 * <pre>
 * - name: temp            # JSON key in the input and output records
 *   type: double          # int | long | float | double | string
 *   role: sensitive       # identifier-passthrough | drop | mask | quasi-identifier | sensitive
 *   generalizer: aggregation
 *   bounds: [30.0, 45.0]  # domain of the aggregation generalizer
 * </pre>
 */
@JsonIgnoreProperties(ignoreUnknown = false)
public class FieldSpec implements Serializable {

    private static final long serialVersionUID = 1L;

    /** JSON key of the field (used for input and output) */
    public String name;
    /** Value type the JSON value is converted to before anonymization */
    public FieldType type = FieldType.STRING;
    /** How the field is treated */
    public Role role;
    /** Generalizer applied by Prink (only for quasi-identifiers and sensitive attributes) */
    public GeneralizerType generalizer;
    /** Domain bounds [min, max] for the aggregation generalizer */
    public List<Double> bounds;
    /** Domain generalization hierarchy for the hierarchy generalizer */
    public HierarchySpec hierarchy;
    /** Masking options for role {@code mask} */
    public MaskSpec mask;
    /** Optional information-loss weight (CastleRule infoLossMultiplier). If all weights sum up to 1 Prink uses them. */
    public Double weight;
    /** Optional free-text description, ignored by the job */
    public String description;

    public FieldSpec() {
    }

    public FieldSpec(String name, FieldType type, Role role) {
        this.name = name;
        this.type = type;
        this.role = role;
    }

    /** @return true if the field is anonymized by Prink (generalized and counted for information loss) */
    public boolean isPrinkAttribute() {
        return role == Role.QUASI_IDENTIFIER || role == Role.SENSITIVE;
    }

    /** @return the generalizer that is actually applied (identifiers and masked/dropped fields are never generalized) */
    public GeneralizerType effectiveGeneralizer() {
        if (!isPrinkAttribute()) return GeneralizerType.NONE;
        return generalizer == null ? GeneralizerType.NONE : generalizer;
    }

    @Override
    public String toString() {
        return name + "(" + type + ", " + role + ", " + effectiveGeneralizer() + ")";
    }

    private static String normalize(String value) {
        return value.trim().toUpperCase(Locale.ROOT).replace('-', '_').replace(' ', '_');
    }

    /** Value type of a field */
    public enum FieldType {
        INT, LONG, FLOAT, DOUBLE, STRING;

        @JsonCreator
        public static FieldType parse(String value) {
            String v = normalize(value);
            switch (v) {
                case "INTEGER": return INT;
                case "STR": case "TEXT": return STRING;
                default: return FieldType.valueOf(v);
            }
        }
    }

    /** Role of a field inside the anonymization */
    public enum Role {
        /** Released unchanged, not generalized (NoneGeneralizer). Use for (pseudo-)identifiers you decide to keep. */
        IDENTIFIER_PASSTHROUGH,
        /** Removed before anonymization; never written to the output topic */
        DROP,
        /** Replaced by a hash or constant before anonymization; the original value never reaches Prink or the output */
        MASK,
        /** Generalized by Prink, not used for l-diversity */
        QUASI_IDENTIFIER,
        /** Generalized (if a generalizer is configured) and used for l-diversity */
        SENSITIVE;

        @JsonCreator
        public static Role parse(String value) {
            String v = normalize(value);
            switch (v) {
                case "PASSTHROUGH": case "IDENTIFIER": case "PASS_THROUGH": case "IDENTIFIER_PASS_THROUGH": return IDENTIFIER_PASSTHROUGH;
                case "QI": case "QUASI": return QUASI_IDENTIFIER;
                default: return Role.valueOf(v);
            }
        }
    }

    /** Generalizer used for a field */
    public enum GeneralizerType {
        /** No generalization (value released as-is) */
        NONE,
        /** Numerical range [min, max] of the cluster (Aggregation{Integer,Float,Double}Generalizer) */
        AGGREGATION,
        /** Replaces trailing characters with '*' until all values in the cluster are equal (ReductionGeneralizer) */
        REDUCTION,
        /** Lowest common ancestor in a domain generalization hierarchy (NonNumericalGeneralizer) */
        HIERARCHY;

        @JsonCreator
        public static GeneralizerType parse(String value) {
            String v = normalize(value);
            switch (v) {
                case "RANGE": return AGGREGATION;
                case "NON_NUMERICAL": case "NONNUMERICAL": case "TREE": return HIERARCHY;
                default: return GeneralizerType.valueOf(v);
            }
        }
    }

    /**
     * Domain generalization hierarchy. Either {@code paths} (inline) or {@code file} (YAML file with a
     * {@code paths} list, or CSV file with one path per line, separated by ';' or ',').
     * Without paths/file the hierarchy is flat: a cluster either releases the common value or the root.
     */
    @JsonIgnoreProperties(ignoreUnknown = false)
    public static class HierarchySpec implements Serializable {
        private static final long serialVersionUID = 1L;
        /** Path of a hierarchy file: absolute, relative to the working dir or the profile, or a classpath resource */
        public String file;
        /** Inline paths from the most general to the most specific value, e.g. [ICD-10, A00–B99, A00–A09, A00] */
        public List<List<String>> paths = new ArrayList<>();
        /** Name of the root node (released for fully generalized values). Default: {@code <ROOT>} */
        public String root;
    }

    /** Masking options for role {@code mask} */
    @JsonIgnoreProperties(ignoreUnknown = false)
    public static class MaskSpec implements Serializable {
        private static final long serialVersionUID = 1L;
        /** {@code hash} (salted SHA-256, default) or {@code constant} */
        public String strategy = "hash";
        /** Replacement value for strategy {@code constant} (default "***") */
        public String value;
        /** Salt for strategy {@code hash}. Prefer {@code saltEnv} so the salt is not part of the profile. */
        public String salt;
        /** Name of an environment variable (read at job submission) that holds the salt */
        public String saltEnv;
        /** Optional number of hex characters of the hash to keep (default: full 64) */
        public Integer length;
    }
}
