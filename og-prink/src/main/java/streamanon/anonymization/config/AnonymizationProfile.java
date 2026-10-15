package streamanon.anonymization.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import prink.datatypes.CastleRule;
import prink.generalizations.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * An anonymization profile: privacy parameters, Kafka wiring, the data subject key and the per-field
 * treatment of the input records. Profiles are read from YAML/JSON by {@link ProfileLoader}.
 *
 * <p>The profile is turned into a fixed tuple layout: every field except {@code drop} fields becomes one tuple
 * position (in profile order). A dropped data subject key is kept internally (Prink needs it to count distinct
 * data subjects) but never written to the output.</p>
 */
@JsonIgnoreProperties(ignoreUnknown = false)
public class AnonymizationProfile implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Flink tuples have at most 25 fields */
    public static final int MAX_TUPLE_ARITY = 25;

    /** Name of the profile (used in the job name) */
    public String name = "unnamed";
    /** Optional free-text description */
    public String description;
    /** CASTLE / Prink parameters */
    public PrivacySettings privacy = new PrivacySettings();
    /** Kafka source and sink */
    public KafkaSettings kafka = new KafkaSettings();
    /** JSON field identifying the data subject (patient). Prink counts k over distinct values of this field. */
    public String dataSubjectKey = "userid";
    /** Field definitions in tuple order */
    public List<FieldSpec> fields = new ArrayList<>();

    /** CASTLE / Prink parameters, see {@link prink.CastleFunction} */
    @JsonIgnoreProperties(ignoreUnknown = false)
    public static class PrivacySettings implements Serializable {
        private static final long serialVersionUID = 1L;
        public int k = 5;
        public int l = 3;
        public int delta = 125;
        public int beta = 50000;
        public int zeta = 10000;
        public int mu = 100;
        /** 0 = drop suppressed tuples, 1 = null all generalized fields, 2 = release with maximal generalization */
        public int suppressStrategy = 2;
        /** Append the information loss as field {@code infoloss} to every output record */
        public boolean showInfoLoss = true;
    }

    /** Kafka source and sink settings */
    @JsonIgnoreProperties(ignoreUnknown = false)
    public static class KafkaSettings implements Serializable {
        private static final long serialVersionUID = 1L;
        public String bootstrapServers = "kafka:29092";
        public String inputTopic = "processed-topic";
        public String outputTopic = "prink-topic";
        public String groupId = "flink-group";
    }

    // ------------------------------------------------------------------------------------------
    // Derived tuple layout

    /** @return the fields that are part of the internal tuple (all fields except dropped ones, but always the key) */
    public List<FieldSpec> tupleFields() {
        List<FieldSpec> result = new ArrayList<>();
        for (FieldSpec field : fields) {
            if (field.role != FieldSpec.Role.DROP || field.name.equals(dataSubjectKey)) result.add(field);
        }
        return Collections.unmodifiableList(result);
    }

    /** @return the tuple position of the data subject key (posTupleId of the CastleFunction) */
    public int keyPosition() {
        List<FieldSpec> tupleFields = tupleFields();
        for (int i = 0; i < tupleFields.size(); i++) {
            if (tupleFields.get(i).name.equals(dataSubjectKey)) return i;
        }
        throw new IllegalStateException("data subject key '" + dataSubjectKey + "' is not part of the tuple");
    }

    /** @return arity of the tuples entering Prink */
    public int inputArity() {
        return tupleFields().size();
    }

    /** @return arity of the tuples leaving Prink (+1 for the info loss if enabled) */
    public int outputArity() {
        return inputArity() + (privacy.showInfoLoss ? 1 : 0);
    }

    /** @return the output JSON key per output tuple position, {@code null} for positions that must not be emitted */
    public String[] outputNames() {
        List<FieldSpec> tupleFields = tupleFields();
        String[] names = new String[outputArity()];
        for (int i = 0; i < tupleFields.size(); i++) {
            FieldSpec field = tupleFields.get(i);
            names[i] = field.role == FieldSpec.Role.DROP ? null : field.name;
        }
        if (privacy.showInfoLoss) names[names.length - 1] = "infoloss";
        return names;
    }

    /** @return Flink type information for tuples of the given arity with arbitrary (Kryo-serialized) field values */
    public static TypeInformation<Tuple> genericTupleType(int arity) {
        TypeInformation<?>[] types = new TypeInformation<?>[arity];
        for (int i = 0; i < arity; i++) types[i] = new GenericTypeInfo<>(Object.class);
        @SuppressWarnings("unchecked")
        Class<Tuple> tupleClass = (Class<Tuple>) Tuple.getTupleClass(arity);
        return new TupleTypeInfo<>(tupleClass, types);
    }

    public TypeInformation<Tuple> inputType() {
        return genericTupleType(inputArity());
    }

    public TypeInformation<Tuple> outputType() {
        return genericTupleType(outputArity());
    }

    /** @return one CastleRule per tuple position */
    public List<CastleRule> buildRules() {
        List<FieldSpec> tupleFields = tupleFields();
        List<CastleRule> rules = new ArrayList<>();
        for (int i = 0; i < tupleFields.size(); i++) {
            FieldSpec field = tupleFields.get(i);
            boolean sensitive = field.role == FieldSpec.Role.SENSITIVE;
            double weight = field.weight == null ? 0.0d : field.weight;
            rules.add(new CastleRule(i, buildGeneralizer(field), sensitive, weight));
        }
        return rules;
    }

    static BaseGeneralizer buildGeneralizer(FieldSpec field) {
        switch (field.effectiveGeneralizer()) {
            case AGGREGATION:
                double min = field.bounds.get(0);
                double max = field.bounds.get(1);
                switch (field.type) {
                    case INT: return new AggregationIntegerGeneralizer(Tuple2.of((int) Math.floor(min), (int) Math.ceil(max)));
                    case FLOAT: return new AggregationFloatGeneralizer(Tuple2.of((float) min, (float) max));
                    case DOUBLE: return new AggregationDoubleGeneralizer(Tuple2.of(min, max));
                    default: throw new IllegalArgumentException("aggregation is not supported for type " + field.type + " of field " + field.name);
                }
            case REDUCTION:
                return new ReductionGeneralizer();
            case HIERARCHY:
                String[][] tree = new String[field.hierarchy == null ? 0 : field.hierarchy.paths.size()][];
                for (int i = 0; i < tree.length; i++) tree[i] = field.hierarchy.paths.get(i).toArray(new String[0]);
                String root = (field.hierarchy == null || field.hierarchy.root == null) ? null : field.hierarchy.root;
                return root == null ? new NonNumericalGeneralizer(tree) : new NonNumericalGeneralizer(tree, root);
            case NONE:
            default:
                return new NoneGeneralizer();
        }
    }

    /** @return short description of the privacy parameters, used as job / operator name */
    public String evalDescription() {
        return String.format("%s_k%d_delta%d_l%d_beta%d_zeta%d_mu%d", name, privacy.k, privacy.delta, privacy.l,
                privacy.beta, privacy.zeta, privacy.mu);
    }
}
