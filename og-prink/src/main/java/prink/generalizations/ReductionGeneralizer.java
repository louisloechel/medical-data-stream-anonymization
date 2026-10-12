package prink.generalizations;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import prink.datatypes.Cluster;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public class ReductionGeneralizer implements BaseGeneralizer{

    private final int position;
    private final Cluster owningCluster;

    private final char replacerChar;

    private static final Logger LOG = LoggerFactory.getLogger(ReductionGeneralizer.class);

    /**
     * Generalizer that reduces the give information by replacing the final chars of the attribute until all attributes are the same.<br>
     * Examples with replacerChar '*': <br>
     *      [Cat, Car, Can] -> [Ca*, Ca*, Ca*] <br>
     *      [Cat, Car, Cow] -> [C**, C**, C**] <br>
     *      [543, 521, 576] -> [5**, 5**, 5**]
     * @param replacerChar the char that is used to replace the original values. Default: '*'
     */
    public ReductionGeneralizer(char replacerChar){
        this.position = -1;
        this.owningCluster = null;

        this.replacerChar = replacerChar;
    }

    public ReductionGeneralizer(){
        this('*');
    }

    private ReductionGeneralizer(int position, Cluster owningCluster, char replacerChar){
        this.position = position;
        this.owningCluster = owningCluster;

        this.replacerChar = replacerChar;
    }

    @Override
    public ReductionGeneralizer clone(int position, Cluster owningCluster) {
        return new ReductionGeneralizer(position, owningCluster, replacerChar);
    }

    /**
     * Reduces information by replacing the last values/chars with '*' until all values are the same
     * @return <code>Tuple2<String, Float></code> where f0 is the generalized result as a String
     * and f1 is the information loss of the generalization
     */
    @Override
    public Tuple2<String, Float> generalize() {
        return generalize(null);
    }

    /**
     * Reduces information by replacing the last values/chars with '*' until all values are the same, while considering the additional tuples 'withTuples'
     * @param withTuples Tuples to consider when calculating generalization
     * @return <code>Tuple2<String, Float></code> where f0 is the reduced value as a String
     * and f1 is the information loss of the generalization
     */
    @Override
    public Tuple2<String, Float> generalize(List<Tuple> withTuples) {
        ArrayList<Tuple> entries = owningCluster.getAllEntries();

        int arraySize = (withTuples == null) ? entries.size() : (entries.size() + withTuples.size());
        if(arraySize <= 0){
            LOG.error("ERROR: generalize called on cluster with no entries. Cluster size:{}", entries.size());
            return new Tuple2<>("", 1.0f);
        }
        String[] ids = new String[arraySize];

        for (int i = 0; i < entries.size(); i++) {
            ids[i] = Objects.toString(entries.get(i).getField(position), "");
        }

        if(withTuples != null){
            for (int i = 0; i < withTuples.size(); i++) {
                ids[i + entries.size()] = Objects.toString(withTuples.get(i).getField(position), "");
            }
        }

        int maxLength = Stream.of(ids).map(String::length).max(Integer::compareTo).get();

        // count number of redacted chars to calculate a information loss value. Start at -1 to check if nothing needs to be reducted (i=0)
        float numRedacted = -1;

        for (int i = 0; i <= maxLength; i++) {
            for (int j = 0; j < ids.length; j++) {
                String overlay = StringUtils.repeat('*', i);
                ids[j] = StringUtils.overlay(ids[j], overlay, ids[j].length() - i, ids[j].length());
            }
            numRedacted++;
            // Break loop when all values are the same
            // TODO-later check if HashSet methode to find count is faster
            if (Stream.of(ids).distinct().count() <= 1) break;
        }
        float infoLoss = (numRedacted > 0) ? (numRedacted/maxLength) : 0f ;
        return Tuple2.of(ids[0], infoLoss);
    }

    @Override
    public Tuple2<String, Float> generalizeMax() {
        return Tuple2.of("*", 1.0f);
    }

    @Override
    public void addTuple(Tuple newTuple) {

    }

    @Override
    public void removeTuple(Tuple removedTuple) {

    }
}
