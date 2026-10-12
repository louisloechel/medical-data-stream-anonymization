package ganges;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ganges.config.AnonymizationProfile;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple;

import java.nio.charset.StandardCharsets;

/**
 * Serializes an anonymized tuple to JSON using the field names of the {@link AnonymizationProfile}.
 * Fields with role {@code drop} (including a dropped data subject key that was only kept for Prink) are
 * never written. Values are written as strings, as the original job did (generalized ranges look like
 * {@code "(36.5,37.3)"}); missing values are written as JSON {@code null}.
 */
public class ProfileTupleToJson implements SerializationSchema<Tuple> {

    private static final long serialVersionUID = 1L;

    /** Output name per tuple position, {@code null} = do not emit */
    private final String[] outputNames;
    private transient ObjectMapper objectMapper;

    public ProfileTupleToJson(AnonymizationProfile profile) {
        this.outputNames = profile.outputNames();
    }

    @Override
    public byte[] serialize(Tuple tuple) {
        return toJson(tuple).toString().getBytes(StandardCharsets.UTF_8);
    }

    public ObjectNode toJson(Tuple tuple) {
        if (objectMapper == null) objectMapper = new ObjectMapper();
        ObjectNode json = objectMapper.createObjectNode();
        for (int i = 0; i < Math.min(outputNames.length, tuple.getArity()); i++) {
            String name = outputNames[i];
            if (name == null) continue;
            Object value = tuple.getField(i);
            if (value == null) {
                json.putNull(name);
            } else {
                json.put(name, value.toString());
            }
        }
        return json;
    }
}
