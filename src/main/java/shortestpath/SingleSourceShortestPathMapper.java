package shortestpath;

import model.NodeSSSP;
import model.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * SingleSourceShortestPathMapper class is the Mapper class for Single Source shortest path. For each node, it emits the node as is.
 * If the node is active, it emits all its adjacent nodes with updated distance.
 */
public class SingleSourceShortestPathMapper extends Mapper<Object, Text, Text, NodeSSSP> {
    @Override
    public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(" ");
        NodeSSSP node = NodeSSSP.createNode(split);
        context.write(new Text(String.valueOf(node.getNodeId())), node);
        if (node.isActive()) {
            for (Pair p : node.getAdjacencyList()) {
                context.write(new Text(p.getNodeId()), new NodeSSSP(p.getNodeId(), node.getDistance() + p.getEdgeWeight()));
            }
        }
    }
}
