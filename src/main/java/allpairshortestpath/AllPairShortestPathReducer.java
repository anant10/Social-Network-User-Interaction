package allpairshortestpath;

import model.NodeAllPair;
import model.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * AllPairShortestPathReducer is a reducer for all pair shortest path which calculates minimum distance for all its adjacent nodes.
 * If the weight has decreased from previous iteration, we increase the global counter to imply that more iterations are needed.
 */
public class AllPairShortestPathReducer extends Reducer<Text, NodeAllPair, Text, Text> {

    @Override
    public void reduce(final Text key, final Iterable<NodeAllPair> values, final Context context) throws IOException, InterruptedException {
        List<Pair> adjacencyListOfVertex = new ArrayList<>();
        Map<String, Double> mapOfWeights = new HashMap<>();
        for (NodeAllPair eachNode : values) {
            if (eachNode.isVertex()) {
                adjacencyListOfVertex.addAll(eachNode.getAdjacencyList());
            }
            for (Pair p : eachNode.getAdjacencyList()) {
                if (mapOfWeights.getOrDefault(p.getNodeId(), Double.MAX_VALUE) > p.getEdgeWeight()) {
                    mapOfWeights.put(p.getNodeId(), p.getEdgeWeight());
                }
            }
        }
        // check if it is converged
        for (Pair p : adjacencyListOfVertex) {
            if (mapOfWeights.containsKey(p.getNodeId()) && (mapOfWeights.get(p.getNodeId()).compareTo(p.getEdgeWeight()) < 0)) {
                context.getCounter(AllPairShortestPathRunner.AllPairCounter.ITERATION).increment(1);
            }
        }
        if (adjacencyListOfVertex.size() < mapOfWeights.size()) {
            // increment global counter here to state that there is change in distance.
            context.getCounter(AllPairShortestPathRunner.AllPairCounter.ITERATION).increment(1);
        }
        // create adj list
        StringBuilder builder = new StringBuilder();
        for (String each : mapOfWeights.keySet()) {
            builder.append(each).append(":").append(mapOfWeights.get(each)).append(",");
        }
        String adj;
        if (builder.length() > 0) {
            adj = builder.substring(0, builder.length() - 1);
        } else {
            adj = builder.toString();
        }
        context.write(key, new Text(adj + " " + "true"));
    }

}
