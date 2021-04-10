package allpairshortestpath;

import model.NodeAllPair;
import model.Pair;
import model.PairOfDoubleBoolean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * AllPairShortestPathMapper class is Mapper class for All pair shortest path.
 * Here we are using in mapper combiner to find the local minimum for each nodeI, nodeJ pair.
 * This All pair shortest path is applicable to undirected graphs. In the map call (u, v1:w1,v2:w2) we first store
 * the weights for all u,v(i) pairs.
 * For each u,v(i) we store the weights for v(i),u i.e. by reversing the pair. Then we apply the formula for each i in adjacency list,
 * to find the distance from (i,j) apply formula dist(i,u)+dist(u,j) where j is another adjacency node of u.
 * We update the map to hold the local minimum value.
 * In the cleanup we emit all the local minimums.
 */
public class AllPairShortestPathMapper extends Mapper<Object, Text, Text, NodeAllPair> {
    private static final Logger logger = LogManager.getLogger(AllPairShortestPathMapper.class);
    Map<String, Map<String, PairOfDoubleBoolean>> mapOfWeights;

    @Override
    protected void setup(Context context) {
        mapOfWeights = new HashMap<>();
    }


    @Override
    public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(" ");
        NodeAllPair node = NodeAllPair.createNode(split);
//        context.write(new Text(String.valueOf(node.getNodeId())), node);
        mapOfWeights.putIfAbsent(node.getNodeId(), new HashMap<>());

        for (Pair p : node.getAdjacencyList()) {
            if (mapOfWeights.get(node.getNodeId()).containsKey(p.getNodeId())) {
                if (mapOfWeights.get(node.getNodeId()).get(p.getNodeId()).getKey() > p.getEdgeWeight()) {
                    mapOfWeights.get(node.getNodeId()).put(p.getNodeId(), new PairOfDoubleBoolean(p.getEdgeWeight(), true));
                }
            } else {
                mapOfWeights.get(node.getNodeId()).put(p.getNodeId(), new PairOfDoubleBoolean(p.getEdgeWeight(), true));
            }
        }
        List<Pair> adjacencyList = node.getAdjacencyList();
        int len = adjacencyList.size();
        for (int i = 0; i < len; i++) {
            Pair pairI = adjacencyList.get(i);
            for (int j = 0; j < len; j++) {
                Pair pairJ = adjacencyList.get(j);
                if (i != j) {
                    double distance = pairI.getEdgeWeight() + pairJ.getEdgeWeight();
//                    context.write(new Text(pairI.getNodeId()), new NodeAllPair(pairI.getNodeId(), new Pair(pairJ.getNodeId(), distance)));
                    mapOfWeights.putIfAbsent(pairI.getNodeId(), new HashMap<>());
                    if (mapOfWeights.get(pairI.getNodeId()).containsKey(pairJ.getNodeId())) {
                        if (mapOfWeights.get(pairI.getNodeId()).get(pairJ.getNodeId()).getKey() > distance) {
                            mapOfWeights.get(pairI.getNodeId()).put(pairJ.getNodeId(), new PairOfDoubleBoolean(distance, false));
                        }
                    } else {
                        mapOfWeights.get(pairI.getNodeId()).put(pairJ.getNodeId(), new PairOfDoubleBoolean(distance, false));
                    }
                }
            }
//            context.write(new Text(pairI.getNodeId()), new NodeAllPair(pairI.getNodeId(), new Pair(node.getNodeId(), pairI.getEdgeWeight())));
            mapOfWeights.putIfAbsent(pairI.getNodeId(), new HashMap<>());
            if (mapOfWeights.get(pairI.getNodeId()).containsKey(node.getNodeId())) {
                if (mapOfWeights.get(pairI.getNodeId()).get(node.getNodeId()).getKey() > pairI.getEdgeWeight()) {
                    mapOfWeights.get(pairI.getNodeId()).put(node.getNodeId(), new PairOfDoubleBoolean(pairI.getEdgeWeight(), false));
                }
            } else {
                mapOfWeights.get(pairI.getNodeId()).put(node.getNodeId(), new PairOfDoubleBoolean(pairI.getEdgeWeight(), false));
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        logger.info("cleanup");
        logger.info("-----------------------------------------------------------------");
        for (String node : mapOfWeights.keySet()) {
            logger.info("node " + node);
            Map<String, PairOfDoubleBoolean> toWeight = mapOfWeights.get(node);

            for (String toNode : toWeight.keySet()) {
                PairOfDoubleBoolean doubleBooleanPair = toWeight.get(toNode);

                if (doubleBooleanPair.isValue()) {
                    NodeAllPair nodeAllPair = new NodeAllPair(node, new Pair(toNode, doubleBooleanPair.getKey()), true);
                    logger.info(nodeAllPair.toString());
                    context.write(new Text(node), nodeAllPair);
                } else {
                    NodeAllPair nodeAllPair = new NodeAllPair(node, new Pair(toNode, doubleBooleanPair.getKey()));
                    logger.info(nodeAllPair.toString());
                    context.write(new Text(node), nodeAllPair);
                }
            }
        }
    }
}
