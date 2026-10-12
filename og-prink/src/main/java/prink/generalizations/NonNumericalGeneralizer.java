package prink.generalizations;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import prink.datatypes.Cluster;
import prink.datatypes.TreeNode;

import java.util.List;

public class NonNumericalGeneralizer implements BaseGeneralizer{

    private final int position;
//    private final Cluster owningCluster;

    private final String[][] treeDefinition;
    private final String rootName;
    private final TreeNode tree;

    /**
     * The constructor of the NonNumericalGeneralizer
     * @param treeDefinition the definition of the domain generalization tree
     * @param rootName the name of the root tree node
     */
    public NonNumericalGeneralizer(String[][] treeDefinition, String rootName){
        this.position = -1;
//        this.owningCluster = new Cluster(null, 0, false);

        this.rootName = rootName;
        this.treeDefinition = treeDefinition;
        // Do not initialize the tree since this one is only used as prototype, only store the the treeDefinition for the real tree initialization
        this.tree = new TreeNode(rootName);
    }

    /**
     * The constructor of the NonNumericalGeneralizer
     * @param treeDefinition the definition of the domain generalization tree
     */
    public NonNumericalGeneralizer(String[][] treeDefinition){
        this(treeDefinition, "<ROOT>");
    }

    private NonNumericalGeneralizer(String[][] treeDefinition, String rootName, int position){
        this.position = position;
//        this.owningCluster = owningCluster;

        this.rootName = rootName;
        this.treeDefinition = treeDefinition;
        this.tree = new TreeNode(rootName);

        // Initialize tree using treeDefinition
        for(String[] treeEntry: treeDefinition){
            tree.addByArray(treeEntry, true);
        }
        tree.removeTempNodes();
    }

    @Override
    public NonNumericalGeneralizer clone(int position, Cluster owningCluster) {
        return new NonNumericalGeneralizer(treeDefinition, rootName, position);
    }

    @Override
    public Tuple2<String, Float> generalize() {
        Tuple2<String, Float> generalization = tree.getGeneralization(false);
        // All values inside the cluster are missing: release null instead of the internal placeholder
        if(Cluster.MISSING_VALUE.equals(generalization.f0)) return Tuple2.of(null, generalization.f1);
        return generalization;
    }

    @Override
    public Tuple2<String, Float> generalize(List<Tuple> withTuples) {
        // Add withTuples temporary to the tree
        for(Tuple input: withTuples){
            addToTree(input, true);
        }
        // Calculate generalization
        Tuple2<String, Float> output = tree.getGeneralization(true);

        // Remove temporary nodes
        tree.removeTempNodes();

        return output;
    }

    @Override
    public Tuple2<String, Float> generalizeMax() {
        return Tuple2.of(rootName, 1.0f);
    }

    @Override
    public void addTuple(Tuple newTuple) {
        // Update the tree by adding new values to tree
        addToTree(newTuple, false);
    }

    /**
     * Adds the attribute value of the tuple to the tree. Missing values (<code>null</code>) are represented by
     * the {@link Cluster#MISSING_VALUE} node directly below the root, so a cluster that mixes missing and
     * present values is generalized to the root instead of pretending that a value was present.
     */
    private void addToTree(Tuple input, boolean isTemporary) {
        Object value = input.getField(position);
        if(value == null){
            tree.addByName(Cluster.MISSING_VALUE, isTemporary);
        }else if(value.getClass().isArray()){
            tree.addByArray((String[]) value, isTemporary);
        }else{
            tree.addByName(value.toString(), isTemporary);
        }
    }

    /**
     * The removal of tuple is currently not supported for the NonNumericalGeneralizer
     * @param removedTuple the removed data tuple
     */
    @Override
    public void removeTuple(Tuple removedTuple) {

    }
}
