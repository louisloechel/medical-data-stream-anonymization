package prink;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import prink.datatypes.CastleRule;
import prink.datatypes.Cluster;
import prink.generalizations.NoneGeneralizer;
import prink.generalizations.ReductionGeneralizer;

import java.util.*;
import java.util.stream.Collectors;

/**
 * The core class of the PRINK Castle implementation
 * @param <KEY> the parameter that is used to distinguish unique data subjects from each other
 * @param <INPUT> the data tuple format that is inserted into PRINK and needs to be anonymized
 * @param <OUTPUT> the data tuple format that is outputted by PRINK and includes the generalized results
 */
public class CastleFunction<KEY, INPUT extends Tuple, OUTPUT extends Tuple> extends KeyedBroadcastProcessFunction<KEY, INPUT, CastleRule, OUTPUT>
        implements CheckpointedFunction {
    // Tracks the last released cluster k and l values for metrics
    private int lastReleasedClusterK = -1;
    private int lastReleasedClusterL = -1;

    // Counter for monitoring
    private transient Counter numSuppressedTuples;
    private transient Counter numMergedCluster;
    private transient Counter numCollectedClusters;
    private transient Counter numCollectedClustersKs;
    private transient Counter numCreatedClusters;
    private transient Counter numCreatedClustersSplit;

    private transient DescriptiveStatisticsHistogram eventTimeLag;
    private transient DescriptiveStatisticsHistogram tauHistogram;
    private transient DescriptiveStatisticsHistogram infoLossHistogram;

    private static final Logger LOG = LoggerFactory.getLogger(CastleFunction.class);

    private transient CastleRule[] rules;

    private final MapStateDescriptor<Integer, CastleRule> ruleStateDescriptor =
            new MapStateDescriptor<>(
                    "RulesBroadcastState",
                    BasicTypeInfo.INT_TYPE_INFO,
                    TypeInformation.of(new TypeHint<CastleRule>(){}));

    private transient ListState<Tuple4<ArrayList<Cluster>, ArrayList<Tuple>, CastleRule[], int[]>> checkpointedState;

    /** Value k for k_s-anonymity */
    private int k = 5;
    /** Value l for l-diversity (if 0 l-diversity is not applied) */
    private int l = 2;
    /** Number of 'delayed' tuples */
    private int delta = 200;
    /** Max number of clusters in bigGamma */
    private int beta = 50;
    /** Max number of clusters in bigOmega */
    private int zeta = 10;
    /** Set of non-k_s anonymized clusters */
    private ArrayList<Cluster> bigGamma = new ArrayList<>();
    /** Set of k_s anonymized clusters */
    private final LinkedList<Cluster> bigOmega = new LinkedList<>();
    /** All tuple objects currently at hold */
    private ArrayList<Tuple> globalTuples = new ArrayList<>();
    /** The average information loss per cluster */
    private float tau = 0f;
    /** Number of last infoLoss values considered for tau */
    private int mu = 10;
    /** Last infoLoss values (max size = mu) */
    private final LinkedList<Float> recentInfoLoss = new LinkedList<>(Collections.nCopies(mu, 0f));
    /** Position of the id value inside the tuples */
    private int posTupleId = 1; // TODO maybe replace with 'context.getCurrentKey()'
    /** Position of the l diversity sensible attribute */
    private int[] posSensibleAttributes = new int[]{1};
    /** If true Prink adds the info loss of a tuple at the end of the tuple (tuple size increases by 1)*/
    private boolean showInfoLoss = false;
    /** Defines how to handle tuples that need to be suppressed.
     * 0 = no tuple is collected;
     * 1 = every quasi-identifier gets replaced with <code>null</code>;
     * 2 = quasi-identifiers are generalized with max generalization values*/
    private int suppressStrategy = 2;

    /**
     * The core function of the PRINK Castle implementation
     * @param posTupleId Position of the unique identifier inside the data tuple for the data subject. This value of this position should also be used as the key for the <code>keyBy()</code> function
     * @param k Value k for k_s-anonymity
     * @param l Value l for l-diversity (if <code>l = 0</code> l-diversity is not applied)
     * @param delta Number of 'delayed' tuples inside Prink. <br>After <code>delta</code> tuples are collected inside Prink the cluster including the oldest tuple gets generalized and all included data tuples released back into the output stream
     * @param beta Maximum number of clusters in <code>bigGamma</code> (<code>bigGamma</code>: Set of non-k_s anonymized clusters)
     * @param zeta Maximum number of clusters in <code>bigOmega</code> (<code>bigOmega</code>: Set of k_s anonymized clusters)
     * @param mu Number of last infoLoss values considered for the calculation of tau
     * @param showInfoLoss If true Prink adds the info loss of a tuple at the end of the tuple (tuple size increases by 1)
     * @param suppressStrategy Defines how to handle tuples that need to be suppressed.<br>
     *                         0 = no tuple is collected <br>
     *                         1 = every quasi-identifier gets replaced with <code>null</code> <br>
     *                         2 = quasi-identifiers are generalized with max generalization values
     */
    public CastleFunction(int posTupleId, int k, int l, int delta, int beta, int zeta, int mu, boolean showInfoLoss, int suppressStrategy) {
        this.posTupleId = posTupleId;
        this.k = k;
        this.l = l;
        this.delta = delta;
        this.beta = beta;
        this.zeta = zeta;
        this.mu = mu;
        this.showInfoLoss = showInfoLoss;
        this.suppressStrategy = suppressStrategy;

    }


    public CastleFunction(int posTupleId, int k, int l, int delta, int beta, int zeta, int mu, boolean showInfoLoss, int suppressStrategy, List<CastleRule> rules) {
        this.posTupleId = posTupleId;
        this.k = k;
        this.l = l;
        this.delta = delta;
        this.beta = beta;
        this.zeta = zeta;
        this.mu = mu;
        this.showInfoLoss = showInfoLoss;
        this.suppressStrategy = suppressStrategy;

        this.rules = rules.toArray(new CastleRule[0]);

        // Redefine sensible attribute positions
        ArrayList<Integer> newPos = new ArrayList<>();
        for(int i = 0; i < this.rules.length; i++) {
            if(this.rules[i].getIsSensibleAttribute()) newPos.add(i);
        }
        posSensibleAttributes = newPos.stream().mapToInt(i -> i).toArray();
    }

    public CastleFunction(){
    }

    @Override
    public void processBroadcastElement(CastleRule rule, Context context, Collector<OUTPUT> collector) throws Exception {
        LOG.info("Rule received: {}", rule.toString());

        BroadcastState<Integer, CastleRule> currentRuleState = context.getBroadcastState(ruleStateDescriptor);

        // Ignore rules without generalization type
        if(rule.getGeneralizer() == null) return;

        // Convert rule map into sorted array
        int numOfRules = Math.max((rules != null ? rules.length : 1), (rule.getPosition() + 1));
        CastleRule[] newRuleArray = new CastleRule[numOfRules];
        for(int i = 0; i < numOfRules; i++) {
            if(i == rule.getPosition()){
                context.getBroadcastState(ruleStateDescriptor).put(i, rule);
                newRuleArray[i] = rule;
            }else{
                if (currentRuleState.contains(i)) {
                    newRuleArray[i] = currentRuleState.get(i);
                }else{
                    CastleRule missingRule = new CastleRule(i, new ReductionGeneralizer(), false);
                    context.getBroadcastState(ruleStateDescriptor).put(i, missingRule);
                    newRuleArray[i] = missingRule;
                }
            }
        }
        rules = newRuleArray;

        // Redefine sensible attribute positions
        ArrayList<Integer> newPos = new ArrayList<>();
        for(int i = 0; i < rules.length; i++) {
            if(rules[i].getIsSensibleAttribute()) newPos.add(i);
        }
        posSensibleAttributes = newPos.stream().mapToInt(i -> i).toArray();

    }

    @Override
    public void processElement(INPUT tuple, ReadOnlyContext context, Collector<OUTPUT> output) {
        LOG.debug("Element received: key: {}", (Object) tuple.getField(posTupleId));

        TimerService ts = context.timerService();

//        tuple.setField(ts.currentProcessingTime(), 2); // enable for performance testing only

        eventTimeLag.update(ts.currentProcessingTime() - ts.currentWatermark());

        // Do not process any tuples without a rule set to follow
        if(rules == null) return;

        Cluster bestCluster = bestSelection(tuple);
        if(bestCluster == null){
            // Create a new cluster on 'input' and insert it into bigGamma
            bestCluster = new Cluster(rules, posTupleId, showInfoLoss);
            numCreatedClusters.inc();
            bigGamma.add(bestCluster);
        }
        // Push 'input' into bestCluster
        bestCluster.addEntry(tuple);
        globalTuples.add(tuple);

        // Different approach than CASTLE. Approach from the CASTLEGUARD code (see: https://github.com/hallnath1/CASTLEGUARD)
        if(globalTuples.size() > delta) delayConstraint(globalTuples.get(0), output);
    }

    /**
     * This function is responsible for the release of data tuples back into the data stream in a generalized form.
     * @param input The tuple that needs to be released out of Prink
     * @param output The collector the tuple should be released to
     */
    @SuppressWarnings("unchecked")
    private void delayConstraint(Tuple input, Collector<OUTPUT> output) {
        Cluster clusterWithInput = getClusterContaining(input);
        if(clusterWithInput == null){
            LOG.error("delayConstraint -> clusterWithInput is NULL");
            return;
        }
        if(clusterWithInput.size() >= k && clusterWithInput.diversity(posSensibleAttributes) >= l){
            outputCluster(clusterWithInput, output);
        }else{
            // Try to apply re-use strategy
            Cluster[] ksClustersWithInput = getClustersContaining(input);
            if(ksClustersWithInput.length > 0){
                int random = new Random().nextInt(ksClustersWithInput.length);
                Cluster selected = ksClustersWithInput[random];
                // Output 'input' with the generalization of the selected cluster
                output.collect((OUTPUT) selected.generalize(input));
                numCollectedClustersKs.inc();
                removeTuple(input);
                return;
            }

            // Check if cluster is an outlier
            int m = 0;
            for(Cluster cluster: bigGamma){
                if(clusterWithInput.size() < cluster.size()) m++;
            }

            if(m > (bigGamma.size()/2)){
                // suppress t based on suppressStrategy
                LOG.debug("Tuple suppressed. Reason: m [{}] > (bigGamma.size() [{}] /2): true", m, bigGamma.size());
                if(suppressStrategy != 0) output.collect(suppressTuple(input));
                removeTuple(input);
                numSuppressedTuples.inc();
                return;
            }

            // Check that total big gamma size (size defined as the number of individual data subjects) is bigger than k before merging
            // it must also be checked that there exist at least 'l' distinct values of a_s among all clusters in bigGamma
            if(totalGammaSize() < k || !checkDiversityBigGamma()){
                // suppress t based on suppressStrategy
                LOG.debug("Tuple suppressed. Reason: not enough distinct [{}] or diverse tuples [{}] inside PRINK to fill cluster. Check for distinct tuples: totalGammaSize() [{}] < k [{}]: {}. Check for diverse tuples: !checkDiversityBigGamma() [{}]: {}", totalGammaSize() < k ? "X" : " ", !checkDiversityBigGamma() ? "X" : " ", totalGammaSize(), k, totalGammaSize() < k, checkDiversityBigGamma(), !checkDiversityBigGamma());
                if(suppressStrategy != 0) output.collect(suppressTuple(input));
                removeTuple(input);
                numSuppressedTuples.inc();
                return;
            }

            Cluster mergedCluster = mergeClusters(clusterWithInput);
            outputCluster(mergedCluster, output);
        }
    }

    /**
     * Calculates the total number of distinct individuals inside bigGamma
     * @return Number of distinct values for the posTupleId inside entries of all bigGamma clusters
     */
    private int totalGammaSize(){
        Set<KEY> tupleIds = new HashSet<>();
        for (Cluster gammaCluster: bigGamma) {
            for(final Tuple entry: gammaCluster.getAllEntries()) {
                tupleIds.add(entry.getField(posTupleId));
            }
        }
        return tupleIds.size();
    }

    /**
     * Generalizes the given tuple with the maximum generalization values
     * @param input Tuple to generalize
     * @return Generalized tuple
     */
    @SuppressWarnings("unchecked")
    private OUTPUT suppressTuple(Tuple input) {
        // null every QI value
        if(suppressStrategy == 1) {
            int inputArity = input.getArity();
            if(showInfoLoss) inputArity++;
            OUTPUT output = (OUTPUT) Tuple.newInstance(inputArity);

            for (int i = 0; i < Math.min(inputArity, rules.length); i++) {
                if(rules[i].getGeneralizer() instanceof NoneGeneralizer) {
                    output.setField(input.getField(i), i);
                }else{
                    output.setField(null, i);
                }
            }
            if(showInfoLoss) output.setField(1f,inputArity-1);
            return output;
        }else if(suppressStrategy == 2){
            // generalize with the most generalized QI value
            Cluster tempCluster = new Cluster(rules, posTupleId, showInfoLoss);
            return (OUTPUT) tempCluster.generalizeMax(input);
        }else{
            LOG.error("suppressTuple -> undefined suppress strategy! Strategy value: {}", suppressStrategy);
            int inputArity = input.getArity();
            if(showInfoLoss) inputArity++;
            return (OUTPUT) Tuple.newInstance(inputArity);
        }
    }

    /**
     * Checks if bigGamma has at least l diverse values for the sensible attributes
     * @return true if diversity is equal or bigger than l, false if diversity is smaller than l
     */
    private boolean checkDiversityBigGamma() {
        if(posSensibleAttributes.length <= 0) return true;

        ArrayList<Tuple> allBigGammaEntries = new ArrayList<>();
        for(Cluster cluster: bigGamma){
            allBigGammaEntries.addAll(cluster.getAllEntries());
        }

        if(posSensibleAttributes.length == 1){
            // Calculate the amount of different values inside the sensible attribute and return true if bigger than l
            Set<Object> output = new HashSet<>();
            for(Tuple tuple: allBigGammaEntries){
                int pos = posSensibleAttributes[0];
                // Check for arrays
                Object sensAttributeValue = Cluster.comparableValue(tuple, pos);
                output.add(sensAttributeValue);
                if(output.size() >= l) return true;
            }
        } else {
            // See for concept: "A Privacy Protection Model for Patient Data with Multiple sensitive Attributes" by Tamas S. Gal, Zhiyuan Chen, Aryya Gangopadhyay: https://mdsoar.org/bitstream/handle/11603/22463/A_Privacy_Protection_Model_for_Patient_Data_with_M.pdf?sequence=1
            List<Tuple2<Integer, Map.Entry<Object, Long>>> numOfAppearances = new ArrayList<>();
            int counter = 0;

            while (allBigGammaEntries.size() > 0) {
                counter++;
                if(counter >= l) return true;
                for (int pos : posSensibleAttributes) {
                    //noinspection ComparatorCombinators - This is apparently slightly more performant then 'Comparator.comparingLong(Map.Entry::getValue)'
                    Map.Entry<Object, Long> temp = allBigGammaEntries.stream().collect(Collectors.groupingBy(s -> Cluster.comparableValue(s, pos), Collectors.counting()))
                        .entrySet().stream().max((attEntry1, attEntry2) -> Long.compare(attEntry1.getValue(), attEntry2.getValue())).orElse(null);
                    numOfAppearances.add(Tuple2.of(pos, temp));
                }
                //noinspection ComparatorCombinators - This is apparently slightly more performant then 'Comparator.comparingLong(Map.Entry::getValue)'
                Tuple2<Integer, Map.Entry<Object, Long>> mapEntryToDelete = numOfAppearances.stream().max((attEntry1, attEntry2) -> Long.compare(attEntry1.f1.getValue(), attEntry2.f1.getValue())).orElse(Tuple2.of(null, null));
                // Remove all entries that have the least diverse attribute
                ArrayList<Tuple> tuplesToDelete = new ArrayList<>(); // TODO-Later maybe use iterator to delete tuples if performance is better
                for(Tuple tuple: allBigGammaEntries){
                    // adjust to find also values from arrays
                    Object toCompare = Cluster.comparableValue(tuple, mapEntryToDelete.f0);
                    if(toCompare.equals(mapEntryToDelete.f1.getKey())){
                        tuplesToDelete.add(tuple);
                    }
                }
                allBigGammaEntries.removeAll(tuplesToDelete);
                numOfAppearances.clear();
            }
        }
        return false;
    }

    /**
     * Merges the input cluster with other clusters until k is fulfilled
     * @param input Cluster to fill up
     * @return merged input cluster
     */
    private Cluster mergeClusters(Cluster input) {
        while(input.size() < k || input.diversity(posSensibleAttributes) < l){
            numMergedCluster.inc();
            float minEnlargement = Float.MAX_VALUE;
            Cluster clusterWithMinEnlargement = null;
            for(Cluster cluster: bigGamma){
                // skip 'input' to not merge with itself
                if(cluster == input) continue;

                float inputEnlargementWithCluster = input.enlargementValue(cluster);
                if(inputEnlargementWithCluster < minEnlargement){
                    minEnlargement = inputEnlargementWithCluster;
                    clusterWithMinEnlargement = cluster;
                }
            }
            if (clusterWithMinEnlargement != null) {
                input.addAllEntries(clusterWithMinEnlargement.getAllEntries());
                bigGamma.remove(clusterWithMinEnlargement);
            }
        }
        return input;
    }

    /**
     * Removes the 'input' tuple from the castle algorithm
     * @param input Tuple to remove
     */
    private void removeTuple(Tuple input) {
        globalTuples.remove(input);
        Cluster cluster = getClusterContaining(input);
        if(cluster != null){
            cluster.removeEntry(input);
            if(cluster.size() <= 0) bigGamma.remove(cluster);
        }
    }

    /**
     * Outputs a cluster in an anonymized state back into the main data stream.
     * If the cluster has enough tuples and diversity it will be split before outputting.
     * @param input Cluster to output
     * @param output anonymized data tuples inside cluster
     */
    @SuppressWarnings("unchecked")
    private void outputCluster(Cluster input, Collector<OUTPUT> output) {
        ArrayList<Cluster> clusters = new ArrayList<>();
        if(input.size() >= (2*k) && input.diversity(posSensibleAttributes) >= l && k >= 2){
            if(l > 0){
                clusters.addAll(splitL(input));
            }else{
                clusters.addAll(split(input));
            }
            bigGamma.remove(input);
        }else{
            clusters.add(input);
        }

        for(Cluster cluster: clusters){
            // Set last released cluster k and l before registering metrics
            lastReleasedClusterK = cluster.size();
            lastReleasedClusterL = cluster.diversity(posSensibleAttributes);

            // Expose k and l of released cluster as Prometheus metrics
            getRuntimeContext().getMetricGroup()
                .addGroup("Prink", "ReleasedCluster")
                .gauge("k_of_released_cluster", (Gauge<Integer>) () -> lastReleasedClusterK);
            getRuntimeContext().getMetricGroup()
                .addGroup("Prink", "ReleasedCluster")
                .gauge("l_of_released_cluster", (Gauge<Integer>) () -> lastReleasedClusterL);

            // When in DEBUG log mode make an additional check if l-diversity and k-anonymity is fulfilled. This should never trigger and is only an addition protection measure to ensure and check privacy guarantees on a new job implementation
            if(LOG.isDebugEnabled() && (cluster.diversity(posSensibleAttributes) < l || cluster.size() < k)){
                LOG.error("Cluster diversity when generalized is lower then l or k is not high enough! l: {} diversity: {} | k: {} cluster.size: {}", l, input.diversity(posSensibleAttributes), k, cluster.size());
                System.out.println("DEBUG-ERROR: Cluster diversity when generalized is lower then l or k is not high enough! l:" + l + " diversity:" + input.diversity(posSensibleAttributes) + " k:" + k + " cluster.size:" + cluster.size());
            }
            numCollectedClusters.inc();
            for(Tuple tuple: cluster.getAllEntries()){
                LOG.debug("output: {}", (Object) tuple);
                output.collect((OUTPUT) cluster.generalize(tuple));
                // Does not use removeTuple(), since the cluster is potentially reused through bigOmega
                globalTuples.remove(tuple);
            }
            float clusterInfoLoss = cluster.infoLoss();
            updateTau(clusterInfoLoss);
            if(clusterInfoLoss < tau){
                bigOmega.addLast(cluster);
                updateBigOmega();
            }

            bigGamma.remove(cluster);
        }
    }

    /**
     * Update 'tau' be calculating the average of the 'mu' last infoLoss values
     * @param clusterInfoLoss new infoLoss from the last generalized cluster (will be added to recentInfoLoss)
     */
    private void updateTau(float clusterInfoLoss) {
        recentInfoLoss.addLast(clusterInfoLoss);
        if(recentInfoLoss.size() > mu) recentInfoLoss.removeFirst();
        infoLossHistogram.update(recentInfoLoss.getLast().longValue());

        float sum = 0;
        for(float recentIL: recentInfoLoss) sum = sum + recentIL;
        tau = sum / recentInfoLoss.size();

        tauHistogram.update((long) tau);
    }

    /**
     * Remove the first value inside bigOmega if the size exceeds zeta
     */
    private void updateBigOmega() {
        if(bigOmega.size() > zeta){
            bigOmega.removeFirst();
        }
    }

    /**
     * Splits the cluster in smaller clusters still conforming k
     * @param input Cluster to split
     * @return Collection on newly generated sub-clusters
     */
    private Collection<Cluster> split(Cluster input) {
        // Output cluster. See 'SC' in CASTLE Paper definition
        ArrayList<Cluster> output = new ArrayList<>();
        HashMap<Object, ArrayList<Tuple>> buckets = generateBuckets(input);

        while(buckets.size() >= k){
            // Randomly select a bucket and select one tuple
            int random = new Random().nextInt(buckets.size());
            List<Object> ids = new ArrayList<>(buckets.keySet());
            Object selectedBucketId = ids.get(random);
            ArrayList<Tuple> selectedBucket = buckets.get(selectedBucketId);
            Tuple selectedTuple = selectedBucket.get(0);

            // Create new sub-cluster with selectedTuple and remove it from original entry
            Cluster newCluster = new Cluster(rules, posTupleId, showInfoLoss);
            newCluster.addEntry(selectedTuple);
            selectedBucket.remove(0);

            if(selectedBucket.size() <= 0) buckets.remove(selectedBucketId);

            // Find buckets with the smallest distance to 'selectedTuple'
            ArrayList<Tuple2<Object, Float>> distances = new ArrayList<>();
            for(Map.Entry<Object, ArrayList<Tuple>> bucketEntry : buckets.entrySet()){
                if(bucketEntry.getValue().equals(selectedBucket)) continue;
                // Pick one of the tuples and calculate the distance to 'selectedTuple'
                distances.add(new Tuple2<>(bucketEntry.getKey(), newCluster.enlargementValue(bucketEntry.getValue().get(0))));
            }
            distances.sort(Comparator.comparing(o -> (o.f1)));

            for(Tuple2<Object, Float> entry: distances.subList(0,k-1)){
                ArrayList<Tuple> bucket = buckets.get(entry.f0);
                Tuple firstTuple = bucket.get(0);
                // Shift tuple from bucket into new cluster
                newCluster.addEntry(firstTuple);
                bucket.remove(0);
                if(bucket.size() <= 0) buckets.remove(entry.f0);
            }

            output.add(newCluster);
        }

        for(Map.Entry<Object, ArrayList<Tuple>> bucketEntry : buckets.entrySet()){
            // Find the nearest cluster for remaining buckets
            float nearestDistance = Float.MAX_VALUE;
            Cluster nearestCluster = null;
            for(Cluster out: output){
                if(out.enlargementValue(bucketEntry.getValue().get(0)) < nearestDistance){
                    nearestDistance = out.enlargementValue(bucketEntry.getValue().get(0));
                    nearestCluster = out;
                }
            }
            // Add all tuples to cluster and delete the cluster
            if(nearestCluster != null) nearestCluster.addAllEntries(bucketEntry.getValue());
        }
        numCreatedClustersSplit.inc(output.size());
        return output;
    }

    /**
     * Attempts to split the input cluster into smaller sub-clusters, considering l-diversity.
     * @param input Cluster to split
     * @return split sub-clusters or if split not possible the input cluster
     */
    private Collection<Cluster> splitL(Cluster input) {
        ArrayList<Cluster> output = new ArrayList<>();
        HashMap<Object, ArrayList<Tuple>> buckets = generateBucketsSensAtt(input);
        Random random = new Random();

        if(buckets.size() < l){
            // Re-add bucket values to input to return input back to the system
            for(ArrayList<Tuple> bucket: buckets.values()){
                input.addAllEntries(bucket);
            }
            output.add(input);
            return output;
        }

        int sum = 0;
        for(ArrayList<Tuple> bucket: buckets.values()){
            // Can use the normal definition of ArrayList.size() and not the number of individual data subjects size definition, since only one entry per data subject is present in all buckets (see: generateBucketsSensAtt())
            sum = sum + bucket.size();
        }
        while(buckets.size() >= l && sum >= k){
            // Randomly select a bucket and select one tuple
            List<Object> ids = new ArrayList<>(buckets.keySet());
            Object selectedBucketId = ids.get(random.nextInt(buckets.size()));
            ArrayList<Tuple> selectedBucket = buckets.get(selectedBucketId);
            int randomNum = random.nextInt(selectedBucket.size());
            Tuple selectedTuple = selectedBucket.get(randomNum);

            // Create new sub-cluster with selectedTuple and remove it from original entry
            Cluster newCluster = new Cluster(rules, posTupleId, showInfoLoss);
            newCluster.addEntry(selectedTuple);
            selectedBucket.remove(randomNum);
            // Remove bucket if it has no values in them
            if(selectedBucket.size() <= 0) buckets.remove(selectedBucketId);

            ArrayList<Object> bucketKeysToDelete = new ArrayList<>();
            // The level of diversity that needs to be archived after using the current bucket (equivalent with the number of buckets already used +1). Only used when multiple sensitive attributes are present.
            int bucketDiversityGoal = 0;

            for(Map.Entry<Object, ArrayList<Tuple>> bucketEntry : buckets.entrySet()){
                bucketDiversityGoal++;
                ArrayList<Tuple> bucket = bucketEntry.getValue();

                // Find tuples with the smallest enlargement
                ArrayList<Tuple2<Tuple, Float>> enlargement = new ArrayList<>();
                for(Tuple tuple: bucket){
                    enlargement.add(new Tuple2<>(tuple, newCluster.enlargementValue(tuple)));
                }
                // Sort enlargement values to select the tuples with the smallest enlargements
                enlargement.sort(Comparator.comparing(o -> (o.f1)));

                // Select first (k*(bucket.size()/sum)) tuples or at least 1 and move them to the new cluster
                if(bucket.size() > 0) {
                    double amountToAdd = Math.max((k * (bucket.size() / (float) sum)), 1);
                    // If more than two sensible attributes are present the diversity inside the selected tuples of the bucket needs to be validated to ensure l-diversity
                    if(posSensibleAttributes.length > 1){
                        int amountAdded = 0;
                        int enlargementPositionAlreadyUsed = 0;

                        // Fulfill diversity goal by adding one tuple that reaches the needed diversity goal
                        for (int i = 0; i < enlargement.size(); i++) {
                            Tuple currentTuple = enlargement.get(i).f0;
                            if(newCluster.diversityWith(posSensibleAttributes, currentTuple) >= bucketDiversityGoal){
                                newCluster.addEntry(currentTuple);
                                bucket.remove(currentTuple);
                                amountAdded++;
                                enlargementPositionAlreadyUsed = i;
                                break;
                            }
                        }
                        // Fill up the remaining tuples to reach 'amountToAdd'
                        for (int i = 0; i < enlargement.size(); i++) {
                            if(amountAdded >= amountToAdd) break;
                            // Skip already used enlargementPosition
                            if(i == enlargementPositionAlreadyUsed) continue;
                            newCluster.addEntry(enlargement.get(i).f0);
                            bucket.remove(enlargement.get(i).f0);
                            amountAdded++;
                        }
                    } else {
                        for (int i = 0; i < amountToAdd; i++) {
                            newCluster.addEntry(enlargement.get(i).f0);
                            bucket.remove(enlargement.get(i).f0);
                        }
                    }
                }

                // Remove buckets that have no values in them
                if(bucket.size() <= 0) bucketKeysToDelete.add(bucketEntry.getKey());
            }
            // Remove buckets that have no values in them
            for(Object key: bucketKeysToDelete) buckets.remove(key);

            output.add(newCluster);

            // If more than one sensitive attribute: Recalculate buckets to have an accurate minimal diverse bucket set to ensure diversity of buckets.size
            if(posSensibleAttributes.length > 1){
                Cluster tempHolder = new Cluster(rules, posTupleId, showInfoLoss);
                for (ArrayList<Tuple> tupleList: buckets.values()) {
                    tempHolder.addAllEntries(tupleList);
                }
                buckets = generateBucketsSensAtt(tempHolder);
            }

            // Recalculate sum
            sum = 0;
            for(ArrayList<Tuple> bucket: buckets.values()){
                sum = sum + bucket.size();
            }
        }

        ArrayList<Object> bucketKeysToDelete = new ArrayList<>();

        // Find the nearest cluster for remaining bucket values
        for(Map.Entry<Object, ArrayList<Tuple>> bucketEntry : buckets.entrySet()){
            ArrayList<Tuple> bucket = bucketEntry.getValue();

            for(Tuple tuple: bucket){
                float nearestDistance = Float.MAX_VALUE;
                Cluster nearestCluster = null;
                for(Cluster out: output){
                    if(out.enlargementValue(tuple) < nearestDistance){
                        nearestDistance = out.enlargementValue(tuple);
                        nearestCluster = out;
                    }
                }
                // Add all tuples to cluster and delete the cluster
                if(nearestCluster != null) nearestCluster.addEntry(tuple);
            }
            bucketKeysToDelete.add(bucketEntry.getKey());
        }
        for(Object key: bucketKeysToDelete) buckets.remove(key);

        for(Cluster cluster: output){
            ArrayList<Tuple> idTuples = new ArrayList<>();
            for(Tuple tuple: cluster.getAllEntries()) {
                // Select all tuples inside input with the same id value as tuple
                KEY tupleId = tuple.getField(posTupleId);
                for (Tuple inputTuple : input.getAllEntries()) {
                    KEY tupleIdInput = inputTuple.getField(posTupleId);
                    if (tupleId.equals(tupleIdInput)) idTuples.add(inputTuple);
                }
            }
            if(idTuples.size() > 0) {
                // Add all tuples with the same ids to the cluster
                cluster.addAllEntries(idTuples);
                // Delete them from the input cluster
                input.removeAllEntries(idTuples);
            }
        }
        numCreatedClustersSplit.inc(output.size());
        return output;
    }

    /**
     * Generates a HashMap (Buckets inside CASTLE) with one tuple per 'pid' inside input, while using the sensible attribute as key
     * @param input Cluster with tuples to create HashMap
     * @return HashMap of one tuple per 'pid' sorted in 'buckets' based on 'sensible attribute'
     */
    private HashMap<Object, ArrayList<Tuple>> generateBucketsSensAtt(Cluster input) {
        HashMap<Object, ArrayList<Tuple>> output = new HashMap<>();
        HashSet<KEY> usedIds = new HashSet<>();
        ArrayList<Tuple> inputTuplesToDelete = new ArrayList<>(); // TODO-Later maybe delete through iterator if more performant

        if(posSensibleAttributes.length <= 0) return output;
        if(posSensibleAttributes.length == 1){
            for(Tuple tuple: input.getAllEntries()){
                KEY tupleId = tuple.getField(posTupleId);
                if(usedIds.add(tupleId)){
                    output.putIfAbsent(tuple.getField(posSensibleAttributes[0]), new ArrayList<>());
                    output.get(tuple.getField(posSensibleAttributes[0])).add(tuple);
                    inputTuplesToDelete.add(tuple);
                }
            }
        }else{
            ArrayList<Tuple> oneIdTuples = new ArrayList<>();
            // Select one tuple per pid/tupleId
            for(Tuple tuple: input.getAllEntries()){
                KEY tupleId = tuple.getField(posTupleId);
                if(usedIds.add(tupleId)){
                    oneIdTuples.add(tuple);
                    inputTuplesToDelete.add(tuple);
                }
            }

            // Generate buckets based on concept: https://mdsoar.org/bitstream/handle/11603/22463/A_Privacy_Protection_Model_for_Patient_Data_with_M.pdf?sequence=1
            List<Tuple2<Integer, Map.Entry<Object, Long>>> numOfAppearances = new ArrayList<>();
            while (oneIdTuples.size() > 0) {
                for (int pos: posSensibleAttributes) {
                    //noinspection ComparatorCombinators - This is apparently slightly more performant then 'Comparator.comparingLong(Map.Entry::getValue)'
                    Map.Entry<Object, Long> temp = oneIdTuples.stream().collect(Collectors.groupingBy(s -> Cluster.comparableValue(s, pos), Collectors.counting()))
                            .entrySet().stream().max((attEntry1, attEntry2) -> Long.compare(attEntry1.getValue(), attEntry2.getValue())).orElse(null);
                    numOfAppearances.add(Tuple2.of(pos, temp));
                }
                //noinspection ComparatorCombinators - This is apparently slightly more performant then 'Comparator.comparingLong(Map.Entry::getValue)'
                Tuple2<Integer, Map.Entry<Object, Long>> mapEntryToDelete = numOfAppearances.stream().max((attEntry1, attEntry2) -> Long.compare(attEntry1.f1.getValue(), attEntry2.f1.getValue())).orElse(Tuple2.of(null, null));
                // Remove all entries that have the least diverse attribute from input and add them to a bucket
                String generatedKey = mapEntryToDelete.f0 + "-" + mapEntryToDelete.f1.getKey().toString();
                ArrayList<Tuple> tuplesToDelete = new ArrayList<>(); // TODO-Later maybe use iterator to delete tuples if performance is better
                output.putIfAbsent(generatedKey, new ArrayList<>());
                for(Tuple tuple: oneIdTuples){
                    // Adjust to find also values from arrays TODO re-write
                    Object toCompare = Cluster.comparableValue(tuple, mapEntryToDelete.f0);
                    if(toCompare.equals(mapEntryToDelete.f1.getKey())){
                        output.get(generatedKey).add(tuple);
                        tuplesToDelete.add(tuple);
                    }
                }
                oneIdTuples.removeAll(tuplesToDelete);
                numOfAppearances.clear();
            }
        }
        // Remove all used tuples to prevent duplicates inside splitL function
        input.removeAllEntries(inputTuplesToDelete);
        return output;
    }

    /**
     * Generates a HashMap (Buckets inside CASTLE) with all the tuples inside input, while using the tupleId as key
     * @param input Cluster with tuples to create HashMap
     * @return HashMap of all tuples sorted in 'buckets' based on tuple id
     */
    private HashMap<Object, ArrayList<Tuple>> generateBuckets(Cluster input) {
        HashMap<Object, ArrayList<Tuple>> output = new HashMap<>();

        for(Tuple tuple: input.getAllEntries()){
            // Ids can be of any type (e.g. String patient numbers), so do not cast to long
            Object tupleId = tuple.getField(posTupleId);
            output.putIfAbsent(tupleId, new ArrayList<>());
            output.get(tupleId).add(tuple);
        }
        return output;
    }

    /**
     * Returns the non-k_s anonymized cluster that includes 'input'
     * @param input The tuple that needs to be included
     * @return Cluster including 'input'. 'null' if no cluster can be found
     */
    private Cluster getClusterContaining(Tuple input) {
        for(Cluster cluster: bigGamma){
            if(cluster.contains(input)) return cluster;
        }
        return null;
    }

    /**
     * Returns all k_s anonymized clusters that includes 'input' (enlargement value is 0)
     * @param input The tuple that needs to be included
     * @return Clusters including 'input'
     */
    private Cluster[] getClustersContaining(Tuple input) {
        ArrayList<Cluster> output = new ArrayList<>();
        for(Cluster cluster: bigOmega){
            if(cluster.enlargementValue(input) <= 0) output.add(cluster);
        }
        return output.toArray(new Cluster[0]);
    }


    /**
     * Finds the best cluster to add 'input' to.
     * For more information see: CASTLE paper
     * @param input the streaming tuple to add to a cluster
     * @return the best selection of all possible clusters.
     * Returns null if no fitting cluster is present.
     */
    private Cluster bestSelection(Tuple input) {
        ArrayList<Float> enlargementResults = new ArrayList<>();

        for (Cluster cluster: bigGamma){
            enlargementResults.add(cluster.enlargementValue(input));
        }
        // return null if no clusters are present
        if(enlargementResults.isEmpty()) return null;

        float minValue = Collections.min(enlargementResults);

        ArrayList<Cluster> minClusters = new ArrayList<>();
        ArrayList<Cluster> okClusters = new ArrayList<>();

        for (Cluster cluster: bigGamma){
            if(cluster.enlargementValue(input) == minValue){
                minClusters.add(cluster);

                float informationLoss = cluster.informationLossWith(input);
                if(informationLoss <= tau){
                    okClusters.add(cluster);
                }
            }
        }

        if(okClusters.isEmpty()){
            if(bigGamma.size() >= beta && !minClusters.isEmpty()){
                // Return cluster in minValue with minimum size
                //noinspection ComparatorCombinators - This is apparently slightly more performant then 'Comparator.comparingLong(Cluster::size)'
                return minClusters.size() > 1 ? minClusters.stream().min((cluster1, cluster2) -> Integer.compare(cluster1.size(), cluster2.size())).get() : minClusters.get(0);
            } else {
                return null;
            }
        } else {
            // Return cluster in okCluster with minimum size
            //noinspection ComparatorCombinators - This is apparently slightly more performant then 'Comparator.comparingLong(Cluster::size)'
            return okClusters.size() > 1 ? okClusters.stream().min((cluster1, cluster2) -> Integer.compare(cluster1.size(), cluster2.size())).get() : okClusters.get(0);

        }
    }

    // Metric section

    @Override
    public void open(Configuration config) {
        // Gauge section
        getRuntimeContext().getMetricGroup()
                .addGroup("Prink")
                .gauge("Tau (*100 as Int)", (Gauge<Integer>) () -> Math.round(tau*100));
        getRuntimeContext().getMetricGroup()
                .addGroup("Prink")
                .gauge("Information Loss (*100 as Int)", (Gauge<Integer>) () -> Math.round(recentInfoLoss.getLast()*100));
        getRuntimeContext().getMetricGroup()
                .addGroup("Prink")
                .gauge("k-Value", (Gauge<Integer>) () -> k);
        getRuntimeContext().getMetricGroup()
                .addGroup("Prink")
                .gauge("l-Value", (Gauge<Integer>) () -> l);
        getRuntimeContext().getMetricGroup()
                .addGroup("Prink")
                .gauge("Sensible Attributes", (Gauge<Integer>) () -> posSensibleAttributes.length);
        getRuntimeContext().getMetricGroup()
                .addGroup("Prink")
                .gauge("Global Tuple Size", (Gauge<Integer>) () -> globalTuples.size());
        getRuntimeContext()
                .getMetricGroup()
                .gauge("Prink", (Gauge<CastleRule[]>) () -> rules);
        // ReleasedCluster k/l gauges use lastReleasedClusterK/L set in outputCluster
        getRuntimeContext().getMetricGroup()
                .addGroup("Prink", "ReleasedCluster")
                .gauge("k_of_released_cluster", (Gauge<Integer>) () -> lastReleasedClusterK);
        getRuntimeContext().getMetricGroup()
                .addGroup("Prink", "ReleasedCluster")
                .gauge("l_of_released_cluster", (Gauge<Integer>) () -> lastReleasedClusterL);
        // Counter section
        this.numSuppressedTuples = getRuntimeContext()
                .getMetricGroup()
                .addGroup("Prink","Counter")
                .counter("Suppressed Tuples");
        this.numMergedCluster = getRuntimeContext()
                .getMetricGroup()
                .addGroup("Prink","Counter")
                .counter("Merged Clusters");
        this.numCollectedClusters = getRuntimeContext()
                .getMetricGroup()
                .addGroup("Prink","Counter")
                .counter("Collected Cluster (normal)");
        this.numCollectedClustersKs = getRuntimeContext()
                .getMetricGroup()
                .addGroup("Prink","Counter")
                .counter("Collected Cluster (ks)");
        this.numCreatedClusters = getRuntimeContext()
                .getMetricGroup()
                .addGroup("Prink","Counter")
                .counter("Created Clusters (normal)");
        this.numCreatedClustersSplit = getRuntimeContext()
                .getMetricGroup()
                .addGroup("Prink","Counter")
                .counter("Created Clusters (split)");
        // Meter section
//        this.numSuppressedTuples = getRuntimeContext()
//                .getMetricGroup()
//                .addGroup("Prink","Meter")
//                .meter("myMeter", new MyMeter());
        // Histogram section
        this.eventTimeLag = getRuntimeContext()
                .getMetricGroup()
                .addGroup("Prink")
                .histogram("eventTimeLag", new DescriptiveStatisticsHistogram(10000));
        this.tauHistogram = getRuntimeContext()
                .getMetricGroup()
                .addGroup("Prink")
                .histogram("Tau", new DescriptiveStatisticsHistogram(10000));
        this.infoLossHistogram = getRuntimeContext()
                .getMetricGroup()
                .addGroup("Prink")
                .histogram("Information Loss", new DescriptiveStatisticsHistogram(10000));
    }


    // State section

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        checkpointedState.clear();
        checkpointedState.add(new Tuple4<>(bigGamma, globalTuples, rules, posSensibleAttributes));
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Tuple4<ArrayList<Cluster>, ArrayList<Tuple>, CastleRule[], int[]>> descriptor =
                new ListStateDescriptor<>("bigGamma-globalTuples",
                        TypeInformation.of(new TypeHint<Tuple4<ArrayList<Cluster>, ArrayList<Tuple>, CastleRule[], int[]>>(){}));

        checkpointedState = context.getOperatorStateStore().getListState(descriptor);

        if (context.isRestored()) {
            //noinspection unchecked needs additional work
            Tuple4<ArrayList<Cluster>, ArrayList<Tuple>, CastleRule[], int[]> entry = (Tuple4<ArrayList<Cluster>, ArrayList<Tuple>, CastleRule[], int[]>) checkpointedState.get();
            bigGamma = entry.f0;
            globalTuples = entry.f1;
            rules = entry.f2;
            posSensibleAttributes = entry.f3;
        }
    }
}
