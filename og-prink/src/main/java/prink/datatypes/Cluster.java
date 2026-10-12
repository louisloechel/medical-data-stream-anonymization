package prink.datatypes;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import prink.generalizations.BaseGeneralizer;
import prink.generalizations.NoneGeneralizer;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Cluster class used by the CastleFunction to group data tuples together and generalize them.
 * Also handles information loss and enlargement calculation.
 */
public class Cluster {

    private final CastleRule[] config;
    private final int posTupleId;

    private final ArrayList<Tuple> entries = new ArrayList<>();
    private final BaseGeneralizer[] generalizers;

    private static final Logger LOG = LoggerFactory.getLogger(Cluster.class);
    private float cachedInfoLoss = -1;

    /** If true caches enlargement value results per generalized attribute (gets cleared when new entity is added to the cluster) */
    private final boolean CACHE_INFO_LOSS_PER_ATT = true;
    final int MAX_CACHED_ENTRIES = 100;
    // DEBUG params
    boolean showRemoveEntry = false;
    boolean showAddedEntry = false;
    boolean showInfoLoss;
    boolean showEnlargement = false;

    List<Map<Object, Double>> infoLossValuesRequests = new ArrayList<>();

    /** Placeholder used for missing (<code>null</code>) attribute values when values are compared or counted */
    public static final String MISSING_VALUE = "<MISSING>";

    /**
     * Returns the value of an attribute as used for comparisons and diversity calculations:
     * for array values (hierarchy paths) the most specific (last) entry, and {@link #MISSING_VALUE}
     * for missing (<code>null</code>) values, so that missing values form their own group instead of
     * causing a NullPointerException.
     * @param tuple data tuple
     * @param pos attribute position
     * @return comparable attribute value (never <code>null</code>)
     */
    public static Object comparableValue(Tuple tuple, int pos) {
        Object value = tuple.getField(pos);
        if(value == null) return MISSING_VALUE;
        if(value.getClass().isArray()){
            Object[] array = (Object[]) value;
            return (array.length == 0 || array[array.length-1] == null) ? MISSING_VALUE : array[array.length-1];
        }
        return value;
    }

    /**
     * Constructor of the Cluster class
     * @param rules Array of CastleRules to follow
     * @param posTupleId Position of the tuple id inside the used tuples
     * @param showInfoLoss If true, the information loss will be added to the output tuple
     */
    public Cluster(CastleRule[] rules, int posTupleId, boolean showInfoLoss) {
        this.config = rules;
        this.posTupleId = posTupleId;
        this.showInfoLoss = showInfoLoss;

        // Create Generalizers for Cluster using prototype pattern
        generalizers = new BaseGeneralizer[rules.length];
        for (int i = 0; i < rules.length; i++) {
            generalizers[i] = rules[i].getGeneralizer().clone(i, this);
        }
    }

    @Override
    public String toString() {
        return "Cluster{" +
                "entries=" + entries +
                '}';
    }

    /**
     * Calculates the enlargement value of this cluster when adding the given input
     * @param input Cluster to consider for the enlargement calculation
     * @return Enlargement value with input
     */
    public float enlargementValue(Cluster input) {
        if(entries.size() <= 0) LOG.error("enlargementValue(Cluster) called on cluster with size: 0 ");
        if(showEnlargement) System.out.println("Enlargement Value Cluster:" + (informationLossWith(input) - infoLoss()));
        return informationLossWith(input) - infoLoss();
    }

    /**
     * Calculates the enlargement value of this cluster when adding the given input
     * @param input Tuple to consider for the enlargement calculation
     * @return Enlargement value with input
     */
    public float enlargementValue(Tuple input) {
        if(entries.size() <= 0) LOG.error("enlargementValue(tuple) called on cluster with size: 0");
        if(showEnlargement) System.out.println("Enlargement Value Tuple:" + (informationLossWith(input) - infoLoss()));
        return informationLossWith(input) - infoLoss();
    }

    /**
     * Calculates the information loss of this cluster when adding the given input
     * @param input Cluster to consider for the information loss calculation
     * @return Information loss value with input
     */
    public float informationLossWith(Cluster input) {
        return informationLossWith(input.getAllEntries());
    }

    /**
     * Calculates the information loss of this cluster when adding the given input
     * @param input Tuple to consider for the information loss calculation
     * @return Information loss value with input
     */
    public float informationLossWith(Tuple input) {
        return informationLossWith(Collections.singletonList(input));
    }

    /**
     * Calculates the information loss of this cluster when adding the given input
     * @param input List of tuples to consider for the information loss calculation
     * @return Information loss value with input
     */
    private float informationLossWith(List<Tuple> input) {
        boolean singularInput = (input.size() == 1);

        if(entries.size() <= 0){
            LOG.error("informationLossWith() called on cluster with size: 0");
            return 0.0f;
        }
        double[] infoLossWith = new double[config.length];

        for (int i = 0; i < config.length; i++) {
            // Check if the value needs to be generalized and if there is an existing cache for it. If so return it
            if(CACHE_INFO_LOSS_PER_ATT && singularInput && !(generalizers[i] instanceof NoneGeneralizer)) {
                if(infoLossValuesRequests.size() <= i) {
                    while (infoLossValuesRequests.size() <= i){
                        // Use a LinkedHashMap to limit size of cache. Set MAX_CACHED_ENTRIES for size of cache per generalized attribute
                        infoLossValuesRequests.add(new LinkedHashMap<Object, Double>(MAX_CACHED_ENTRIES*10/7, 0.7f, true) {
                            @Override
                            protected boolean removeEldestEntry(Map.Entry<Object, Double> eldest) {
                                return size() > MAX_CACHED_ENTRIES;
                            }
                        });
                    }
                }
                Map<Object, Double> requestCounter = infoLossValuesRequests.get(i);
                Object valueToCheck = input.get(0).getField(i);

                if(requestCounter.containsKey(valueToCheck)){
                    infoLossWith[i] = requestCounter.get(valueToCheck);
                    continue;
                }
            }

            // If no cache is hit calculate information loss with input
            infoLossWith[i] = (generalizers[i] instanceof NoneGeneralizer) ? 0 : generalizers[i].generalize(input).f1;

            // Add info loss to cache if input is singular and not of type NONE
            if(CACHE_INFO_LOSS_PER_ATT && singularInput && !(generalizers[i] instanceof NoneGeneralizer)) infoLossValuesRequests.get(i).put(input.get(0).getField(i), infoLossWith[i]);
        }
        return calcCombinedInfoLoss(infoLossWith);
    }

    /**
     * Calculates the information loss of this cluster
     * @return Information loss value of the cluster
     */
    public float infoLoss() {
        // check if cached value exist
        if(cachedInfoLoss != -1) return cachedInfoLoss;
        if(entries.size() <= 0){
            LOG.error("infoLoss() called on cluster with size: 0");
            return 0.0f;
        }
        if(config.length <= 0){
            LOG.error("infoLoss() called without any config/rules");
            return 0.0f;
        }
        double[] infoLoss = new double[generalizers.length];

        for (int i = 0; i < generalizers.length; i++) {
            infoLoss[i] = (generalizers[i] instanceof NoneGeneralizer) ? 0 : generalizers[i].generalize().f1;
        }
        float output = calcCombinedInfoLoss(infoLoss);
        // Cache infoLoss until new tuple is added
        cachedInfoLoss = output;
        return output;
    }

    /**
     * Calculates the combined information loss for the cluster using the provided info loss values.
     * If all InfoLoss rule multiplier combined are equal to 1 the Normalized Certainty Penalty is used.
     * If not a normal average calculation is used.
     * @param infoLoss Information Loss values to use to calculate combined information loss result
     * @return General information loss for provided values using the clusters config
     */
    private float calcCombinedInfoLoss(double[] infoLoss) {
        float output;
        int numAppliedGeneralizers = 0;
        // Check what methode should be used for the general information loss (see Doc.)
        double multiplierSum = Arrays.stream(config).mapToDouble(CastleRule::getInfoLossMultiplier).sum();
        if(Math.round(multiplierSum) == 1){
            // Multiply info loss values with provided info loss multiplier and sum together
            for (int i = 0; i < config.length; i++) {
                // Apply to all generalizer that are NOT NoneGeneralizer
                if(!(generalizers[i] instanceof NoneGeneralizer)){
                    infoLoss[i] = config[i].getInfoLossMultiplier() * infoLoss[i];
                    numAppliedGeneralizers++;
                }
            }
            output = (float) Arrays.stream(infoLoss).sum();
        }else{
            // Count number of generalizers to calculate avg info loss
            for (int i = 0; i < config.length; i++) {
                // Apply to all generalizer that are NOT NoneGeneralizer
                if(!(generalizers[i] instanceof NoneGeneralizer)){
                    numAppliedGeneralizers++;
                }
            }
            // Calculate the avg off all info loss values
            output = (float) (Arrays.stream(infoLoss).sum() / numAppliedGeneralizers);
        }
        // Return 0.0 if no generalization rules are present else output
        if(numAppliedGeneralizers == 0) return 0.0f;
        return output;
    }

    /**
     * Creates a generalized tuple based on the currently included data tuples in 'entries' and
     * the provided rules. Data fields that are not generalized use the provided value from the input.
     * @param input Tuple to use for field values that are not generalized (See {@link NoneGeneralizer} for more info on none generalized fields)
     * @return Generalized tuple
     */
    public Tuple generalize(Tuple input) {
        // Return new tuple with generalized field values
        int inputArity = input.getArity();
        if(showInfoLoss) inputArity++;
        Tuple output = Tuple.newInstance(inputArity);

        if(entries.size() <= 0){
            LOG.error("generalize(Tuple) called on cluster with size: 0");
            return output;
        }

        for (int i = 0; i < Math.min(inputArity, generalizers.length); i++) {
            if(generalizers[i] instanceof NoneGeneralizer){
                output.setField(input.getField(i), i);
            }else{
                output.setField(generalizers[i].generalize().f0, i);
            }
        }
        if(showInfoLoss) output.setField(infoLoss(),inputArity-1);
        return output;
    }

    /**
     * Generalizes the input tuple with the maximum generalization of the generalizers
     * (Can be used on a cluster without entries)
     * @param input Tuple to generalize
     * @return Maximal generalized tuple
     */
    public Tuple generalizeMax(Tuple input) {
        // Return new tuple with generalized field values
        int inputArity = input.getArity();
        if(showInfoLoss) inputArity++;
        Tuple output = Tuple.newInstance(inputArity);

        for (int i = 0; i < Math.min(inputArity, generalizers.length); i++) {
            if(generalizers[i] instanceof NoneGeneralizer){
                output.setField(input.getField(i), i);
            }else{
                output.setField(generalizers[i].generalizeMax().f0, i);
            }
        }
        if(showInfoLoss) output.setField(1.0f,inputArity-1);
        return output;
    }

    public void addEntry(Tuple input) {
        entries.add(input);
        if (showAddedEntry) System.out.println("Added " + input.toString() + " to cluster: " + this + " size:" + this.entries.size());

        invalidateCache();

        // Inform generalizer about the new data tuple
        for (BaseGeneralizer generalizer: generalizers) {
            generalizer.addTuple(input);
        }
    }

    public void addAllEntries(ArrayList<Tuple> input) {
        entries.addAll(input);
        if (showAddedEntry) System.out.println("Added multiple (" + input.size() + ") to cluster: " + this + " size:" + this.entries.size());

        invalidateCache();

        // Inform generalizer about the new data tuple
        for (BaseGeneralizer generalizer: generalizers) {
            for (Tuple inputTuple : input) {
                generalizer.addTuple(inputTuple);
            }
        }
    }

    public void removeEntry(Tuple input) {
        entries.remove(input);
        if(showRemoveEntry) System.out.println("Cluster: removeEntry -> new size:" + entries.size());

        invalidateCache();

        // Inform generalizer about the removed data tuples
        for (BaseGeneralizer generalizer: generalizers) {
            generalizer.removeTuple(input);
        }
    }

    public void removeAllEntries(ArrayList<Tuple> input) {
        entries.removeAll(input);
        if(showRemoveEntry) System.out.println("Cluster: removeAllEntry -> new size:" + entries.size());

        invalidateCache();

        // Inform generalizer about the removed data tuples
        for (BaseGeneralizer generalizer: generalizers) {
            for (Tuple inputTuple : input) {
                generalizer.removeTuple(inputTuple);
            }
        }
    }

    private void invalidateCache(){
        // Invalidate cache of infoLoss per attribute if used
        if(CACHE_INFO_LOSS_PER_ATT){
            for (Map<Object, Double> cacheMap: infoLossValuesRequests) {
                cacheMap.clear();
            }
        }
        // Invalidate cache of infoLoss
        cachedInfoLoss = -1;
    }

    public ArrayList<Tuple> getAllEntries() {
        return entries;
    }

    public boolean contains(Tuple input) {
        return entries.contains(input);
    }

    /**
     * Returns the number of distinct individuals inside the cluster. It does NOT return the number of tuples in the array!<br>
     * The distinction between individuals is made by attribute value at the defined <code>posTupleId</code> position.
     * @return Number of distinct values for the <code>posTupleId</code> inside cluster entries
     */
    public int size() {
        Set<Object> tupleIds = new HashSet<>();
        for(final Tuple entry: entries) {
            tupleIds.add(entry.getField(posTupleId));
        }
        return tupleIds.size();
    }

    /**
     * Returns the diversity of the cluster entries depending on their sensitive attribute positions.<br>
     * Depending on the number of sensitive attribute positions different algorithms are use:<br>
     *      0 sensitive positions = No calculation: returns 0<br>
     *      1 sensitive position = HashSet calculation<br>
     *   >= 2 sensitive positions = Calculation logic based on: <a href="https://mdsoar.org/bitstream/handle/11603/22463/A_Privacy_Protection_Model_for_Patient_Data_with_M.pdf?sequence=1">A privacy protection model for patient data with multiple sensitive attributes by Gal, Tamas S and Chen, Zhiyuan and Gangopadhyay, Aryya</a>
     * @return cluster diversity
     */
    public int diversity(int[] posSensibleAttributes) {
        if(posSensibleAttributes.length <= 0) return 0;
        if(posSensibleAttributes.length == 1){
            // Return the amount of different values inside the sensible attribute
            Set<Object> output = new HashSet<>();
            for(Tuple tuple: entries){
                int pos = posSensibleAttributes[0];
                // Check for arrays
                Object sensAttributeValue = comparableValue(tuple, pos);
                output.add(sensAttributeValue);
            }
            return output.size();
        }else{
            // See for concept: https://mdsoar.org/bitstream/handle/11603/22463/A_Privacy_Protection_Model_for_Patient_Data_with_M.pdf?sequence=1
            @SuppressWarnings("unchecked")
            ArrayList<Tuple> entriesCopy = (ArrayList<Tuple>) entries.clone();
            List<Tuple2<Integer, Map.Entry<Object, Long>>> numOfAppearances = new ArrayList<>();
            int counter = 0;

            while (entriesCopy.size() > 0) {
                counter++;
                for (int pos : posSensibleAttributes) {
                    //noinspection ComparatorCombinators - This is apparently slightly more performant then 'Comparator.comparingLong(Map.Entry::getValue)'
                    Map.Entry<Object, Long> temp = entriesCopy.stream().collect(Collectors.groupingBy(s -> comparableValue(s, pos), Collectors.counting()))
                        .entrySet().stream().max((attEntry1, attEntry2) -> Long.compare(attEntry1.getValue(), attEntry2.getValue())).orElse(null);
                    numOfAppearances.add(Tuple2.of(pos, temp));
                }
                //noinspection ComparatorCombinators - This is apparently slightly more performant then 'Comparator.comparingLong(Map.Entry::f1.getValue)'
                Tuple2<Integer, Map.Entry<Object, Long>> mapEntryToDelete = numOfAppearances.stream().max((attEntry1, attEntry2) -> Long.compare(attEntry1.f1.getValue(), attEntry2.f1.getValue())).get();
                // Remove all entries that have the least diverse attribute
                ArrayList<Tuple> tuplesToDelete = new ArrayList<>(); // TODO-Later maybe use iterator to delete tuples if performance is better
                for(Tuple tuple: entriesCopy){
                    // adjust to find also values from arrays
                    Object toCompare = comparableValue(tuple, mapEntryToDelete.f0);
                    if(toCompare.equals(mapEntryToDelete.f1.getKey())){
                        tuplesToDelete.add(tuple);
                    }
                }
                entriesCopy.removeAll(tuplesToDelete);
                numOfAppearances.clear();
            }
            return counter;
        }
    }

    /**
     * Returns the diversity of the cluster entries plus the additional input tuple depending on their sensible attribute positions.<br>
     * See {@link #diversity(int[])} for more information
     * @return cluster diversity
     */
    public int diversityWith(int[] posSensibleAttributes, Tuple input) { // TODO combine both diversity functions into one
        if(posSensibleAttributes.length <= 0) return 0;
        if(posSensibleAttributes.length == 1){
            @SuppressWarnings("unchecked")
            ArrayList<Tuple> entriesCopy = (ArrayList<Tuple>) entries.clone();
            entriesCopy.add(input);
            // Return the amount of different values inside the sensible attribute
            Set<Object> output = new HashSet<>();
            for(Tuple tuple: entriesCopy){
                int pos = posSensibleAttributes[0];
                // Check for arrays
                Object sensAttributeValue = comparableValue(tuple, pos);
                output.add(sensAttributeValue);
            }
            return output.size();
        }else{
            // See for concept: https://mdsoar.org/bitstream/handle/11603/22463/A_Privacy_Protection_Model_for_Patient_Data_with_M.pdf?sequence=1
            @SuppressWarnings("unchecked")
            ArrayList<Tuple> entriesCopy = (ArrayList<Tuple>) entries.clone();
            entriesCopy.add(input);
            List<Tuple2<Integer, Map.Entry<Object, Long>>> numOfAppearances = new ArrayList<>();
            int counter = 0;

            while (entriesCopy.size() > 0) {
                counter++;
                for (int pos : posSensibleAttributes) {
                    //noinspection ComparatorCombinators - This is apparently slightly more performant then 'Comparator.comparingLong(Map.Entry::getValue)'
                    Map.Entry<Object, Long> temp = entriesCopy.stream().collect(Collectors.groupingBy(s -> comparableValue(s, pos), Collectors.counting()))
                            .entrySet().stream().max((attEntry1, attEntry2) -> Long.compare(attEntry1.getValue(), attEntry2.getValue())).orElse(null);
                    numOfAppearances.add(Tuple2.of(pos, temp));
                }
                //noinspection ComparatorCombinators - This is apparently slightly more performant then 'Comparator.comparingLong(Map.Entry::f1.getValue)'
                Tuple2<Integer, Map.Entry<Object, Long>> mapEntryToDelete = numOfAppearances.stream().max((attEntry1, attEntry2) -> Long.compare(attEntry1.f1.getValue(), attEntry2.f1.getValue())).get();
                // Remove all entries that have the least diverse attribute
                ArrayList<Tuple> tuplesToDelete = new ArrayList<>(); // TODO-Later maybe use iterator to delete tuples if performance is better
                for(Tuple tuple: entriesCopy){
                    // adjust to find also values from arrays
                    Object toCompare = comparableValue(tuple, mapEntryToDelete.f0);
                    if(toCompare.equals(mapEntryToDelete.f1.getKey())){
                        tuplesToDelete.add(tuple);
                    }
                }
                entriesCopy.removeAll(tuplesToDelete);
                numOfAppearances.clear();
            }
            return counter;
        }
    }
}
