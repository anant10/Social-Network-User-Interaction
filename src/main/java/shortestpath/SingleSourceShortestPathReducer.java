package shortestpath;

import model.NodeSSSP;
import model.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * SingleSourceShortestPathReducer class is a Reducer class for Single Source Shortest Path. Reduce receives the vertex object for vertex key and
 * the newly computed distances for key’s inlinks. It updates the distance if it less than the previous iteration.
 */
public class SingleSourceShortestPathReducer extends Reducer<Text, NodeSSSP, Text, Text> {

    @Override
    public void reduce(final Text key, final Iterable<NodeSSSP> values, final Context context) throws IOException, InterruptedException {
        Double dMin = Double.MAX_VALUE;
        NodeSSSP M = null;

        for (NodeSSSP p : values) {
            if (p.isVertex()) {
                M = new NodeSSSP(p);
            } else {
                if (p.getDistance() < dMin) {
                    dMin = p.getDistance();
                }
            }
        }
        if (M != null) {
            if (dMin < M.getDistance()) {
                // increment global counter here to state that there is change in distance.
                context.getCounter(SingleSourceShortestPathRunner.SSSPCounter.ITERATION).increment(1);
                M.setDistance(dMin);
                M.setActive(true);

            }
            StringBuilder builder = new StringBuilder();
            for (Pair p : M.getAdjacencyList()) {
                builder.append(p.getNodeId()).append(":").append(p.getEdgeWeight()).append(",");
            }
            String adj;
            if (builder.length() > 0) {
                adj = builder.substring(0, builder.length() - 1);
            } else {
                adj = builder.toString();
            }
            String str = M.getDistance() + " " + M.isVertex() + " " + M.isActive() + " " + adj;
            context.write(new Text(String.valueOf(M.getNodeId())), new Text(str));
        }
    }
}